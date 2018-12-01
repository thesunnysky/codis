// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

// 这个过程中对于channel的使用方式是值得读者学习的。分为如下几个步骤：proxy的router负责将收到的请求写到bc.input；
// newBackendReader创建一个名为task的channel，启动一个goroutine loopReader循环读出task的内容作处理，newBackendReader
// 立即返回创建好的task给主线程loopwriter。主线程loopwriter中bc.input循环读出内容写入task。并且loopwuiter和loopReader
// 都做了range channel过程中因为异常退出的处理

// 总结一下，backendConn负责实际对redis(codis-server)请求进行处理。
// 在fillSlot的时候，主要目的就是给slot填充backend.bc(实际上是sharedBackendConn)。
// 从models.slot得到BackendAddr和MigrateFrom的地址addr，根据这个addr，首先从proxy.Router的primary sharedBackendConnPool
// 中取sharedBackendConn，如果没有获取到，就新建sharedBackendConn再放回sharedBackendConnPool。创建sharedBackendConn
// 的过程中启动了两个goroutine，分别是loopWriter和loopReader，loopWriter负责从backendConn.input中取出请求并发送，loopReader
// 负责遍历所有请求，从redis.Conn中解码得到resp并设置为相关的请求的属性，这样每一个请求及其结果就关联起来了。

package proxy

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/thesunnysky/codis/pkg/proxy/redis"
	"github.com/thesunnysky/codis/pkg/utils/errors"
	"github.com/thesunnysky/codis/pkg/utils/log"
	"github.com/thesunnysky/codis/pkg/utils/math2"
	"github.com/thesunnysky/codis/pkg/utils/sync2/atomic2"
)

const (
	stateConnected = iota + 1
	stateDataStale
)

type BackendConn struct {
	stop sync.Once
	addr string

	//size为1024的request channel,用来缓存对redis的请求
	input chan *Request
	retry struct {
		fails int
		delay Delay
	}
	state atomic2.Int64

	closed atomic2.Bool
	config *Config

	database int
}

func NewBackendConn(addr string, database int, config *Config) *BackendConn {
	bc := &BackendConn{
		addr: addr, config: config, database: database,
	}
	bc.input = make(chan *Request, 1024)
	bc.retry.delay = &DelayExp2{
		Min: 50, Max: 5000,
		Unit: time.Millisecond,
	}

	go bc.run()

	return bc
}

func (bc *BackendConn) Addr() string {
	return bc.addr
}

func (bc *BackendConn) Close() {
	bc.stop.Do(func() {
		close(bc.input)
	})
	bc.closed.Set(true)
}

func (bc *BackendConn) IsConnected() bool {
	return bc.state.Int64() == stateConnected
}

//请求放入BackendConn等待处理。如果request的sync.WaitGroup不为空，就加一，然后判断加一之后的值，如果加一之后couter为0，
//那么所有阻塞在counter上的goroutine都会得到释放
//将请求直接存入到BackendConn的chan *Request中，等待后续被取出并进行处理。
func (bc *BackendConn) PushBack(r *Request) {
	if r.Batch != nil {
		r.Batch.Add(1)
	}
	bc.input <- r
}

func (bc *BackendConn) KeepAlive() bool {
	if len(bc.input) != 0 {
		return false
	}
	switch bc.state.Int64() {
	default:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("PING")),
		}
		bc.PushBack(m)

	case stateDataStale:
		m := &Request{}
		m.Multi = []*redis.Resp{
			redis.NewBulkBytes([]byte("INFO")),
		}
		m.Batch = &sync.WaitGroup{}
		bc.PushBack(m)

		keepAliveCallback <- func() {
			m.Batch.Wait()
			var err = func() error {
				if err := m.Err; err != nil {
					return err
				}
				switch resp := m.Resp; {
				case resp == nil:
					return ErrRespIsRequired
				case resp.IsError():
					return fmt.Errorf("bad info resp: %s", resp.Value)
				case resp.IsBulkBytes():
					var info = make(map[string]string)
					for _, line := range strings.Split(string(resp.Value), "\n") {
						kv := strings.SplitN(line, ":", 2)
						if len(kv) != 2 {
							continue
						}
						if key := strings.TrimSpace(kv[0]); key != "" {
							info[key] = strings.TrimSpace(kv[1])
						}
					}
					if info["master_link_status"] == "down" {
						return nil
					}
					if info["loading"] == "1" {
						return nil
					}
					if bc.state.CompareAndSwap(stateDataStale, stateConnected) {
						log.Warnf("backend conn [%p] to %s, db-%d state = Connected (keepalive)",
							bc, bc.addr, bc.database)
					}
					return nil
				default:
					return fmt.Errorf("bad info resp: should be string, but got %s", resp.Type)
				}
			}()
			if err != nil && bc.closed.IsFalse() {
				log.WarnErrorf(err, "backend conn [%p] to %s, db-%d recover from DataStale failed",
					bc, bc.addr, bc.database)
			}
		}
	}
	return true
}

var keepAliveCallback = make(chan func(), 128)

func init() {
	go func() {
		for fn := range keepAliveCallback {
			fn()
		}
	}()
}

func (bc *BackendConn) newBackendReader(round int, config *Config) (*redis.Conn, chan<- *Request, error) {
	//创建与redis的conn
	c, err := redis.DialTimeout(bc.addr, time.Second*5,
		config.BackendRecvBufsize.AsInt(),
		config.BackendSendBufsize.AsInt())
	if err != nil {
		return nil, nil, err
	}
	c.ReaderTimeout = config.BackendRecvTimeout.Duration()
	c.WriterTimeout = config.BackendSendTimeout.Duration()
	c.SetKeepAlivePeriod(config.BackendKeepAlivePeriod.Duration())

	if err := bc.verifyAuth(c, config.ProductAuth); err != nil {
		c.Close()
		return nil, nil, err
	}

	//选择db
	if err := bc.selectDatabase(c, bc.database); err != nil {
		c.Close()
		return nil, nil, err
	}

	tasks := make(chan *Request, config.BackendMaxPipeline)
	//启动一个goroutine来处理该task chan中的请求，读取task中的请求，并将处理结果与之对应关联
	go bc.loopReader(tasks, c, round)

	return c, tasks, nil
}

func (bc *BackendConn) verifyAuth(c *redis.Conn, auth string) error {
	if auth == "" {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("AUTH")),
		redis.NewBulkBytes([]byte(auth)),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) selectDatabase(c *redis.Conn, database int) error {
	if database == 0 {
		return nil
	}

	multi := []*redis.Resp{
		redis.NewBulkBytes([]byte("SELECT")),
		redis.NewBulkBytes([]byte(strconv.Itoa(database))),
	}

	if err := c.EncodeMultiBulk(multi, true); err != nil {
		return err
	}

	resp, err := c.Decode()
	switch {
	case err != nil:
		return err
	case resp == nil:
		return ErrRespIsRequired
	case resp.IsError():
		return fmt.Errorf("error resp: %s", resp.Value)
	case resp.IsString():
		return nil
	default:
		return fmt.Errorf("error resp: should be string, but got %s", resp.Type)
	}
}

func (bc *BackendConn) setResponse(r *Request, resp *redis.Resp, err error) error {
	r.Resp, r.Err = resp, err
	if r.Group != nil {
		r.Group.Done()
	}
	if r.Batch != nil {
		//标识当前的request已经处理完成了
		r.Batch.Done()
	}
	return err
}

var (
	ErrBackendConnReset = errors.New("backend conn reset")
	ErrRequestIsBroken  = errors.New("request is broken")
)

func (bc *BackendConn) run() {
	log.Warnf("backend conn [%p] to %s, db-%d start service",
		bc, bc.addr, bc.database)
	for round := 0; bc.closed.IsFalse(); round++ {
		log.Warnf("backend conn [%p] to %s, db-%d round-[%d]",
			bc, bc.addr, bc.database, round)
		if err := bc.loopWriter(round); err != nil {
			bc.delayBeforeRetry()
		}
	}
	log.Warnf("backend conn [%p] to %s, db-%d stop and exit",
		bc, bc.addr, bc.database)
}

var (
	errRespMasterDown = []byte("MASTERDOWN")
	errRespLoading    = []byte("LOADING")
)

//读取task中的请求，并将处理结果与之对应关联
func (bc *BackendConn) loopReader(tasks <-chan *Request, c *redis.Conn, round int) (err error) {
	//从连接中取完所有请求并setResponse之后，连接就会关闭
	defer func() {
		c.Close()
		for r := range tasks {
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d reader-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()
	//遍历tasks，此时的r是所有的请求
	for r := range tasks {
		//从redis.Conn中解码得到处理结果,Decode()将获取的是所有conn处理的命令的请求结果（单条命令或者
		//multi）,循环的调用c.Decode()方法将依次的取出它所处理的命令的结果
		//? 如何保证在读取数据的时候所有命令都已经处理完成了呢？
		resp, err := c.Decode()
		//error
		if err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		//error
		if resp != nil && resp.IsError() {
			switch {
			case bytes.HasPrefix(resp.Value, errRespMasterDown):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'MASTERDOWN'",
						bc, bc.addr, bc.database)
				}
			case bytes.HasPrefix(resp.Value, errRespLoading):
				if bc.state.CompareAndSwap(stateConnected, stateDataStale) {
					log.Warnf("backend conn [%p] to %s, db-%d state = DataStale, caused by 'LOADING'",
						bc, bc.addr, bc.database)
				}
			}
		}
		//Set the "Response" of Request to Request.Resp
		bc.setResponse(r, resp, nil)
	}
	return nil
}

func (bc *BackendConn) delayBeforeRetry() {
	bc.retry.fails += 1
	if bc.retry.fails <= 10 {
		return
	}
	timeout := bc.retry.delay.After()
	for bc.closed.IsFalse() {
		select {
		case <-timeout:
			return
		case r, ok := <-bc.input:
			if !ok {
				return
			}
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
	}
}

// 循环的从input chan中取出Request来，encode request然后通过conn将请求发往codis-server
func (bc *BackendConn) loopWriter(round int) (err error) {
	//如果因为某种原因退出，还有input没来得及处理，就返回错误
	defer func() {
		for i := len(bc.input); i != 0; i-- {
			r := <-bc.input
			//将尚未的处理的request.Resp置为nil
			bc.setResponse(r, nil, ErrBackendConnReset)
		}
		log.WarnErrorf(err, "backend conn [%p] to %s, db-%d writer-[%d] exit",
			bc, bc.addr, bc.database, round)
	}()
	// 创建与codis-server的连接，并启动loopReader
	// tasks chan 用来记录所有向codis-server发送的请求
	// 从backendConn.input中获取所有的request，然后encode request，将encode后的request发送给
	// codis-server，然后在将发送的request放入task chan中,然后等待loopReader会按照task中request
	// 的顺序将请求的返回结果和request匹配
	// ?每次都要创建新的连接吗？
	c, tasks, err := bc.newBackendReader(round, bc.config)
	if err != nil {
		return err
	}
	defer close(tasks)

	defer bc.state.Set(0)

	bc.state.Set(stateConnected)
	bc.retry.fails = 0
	bc.retry.delay.Reset()

	p := c.FlushEncoder()
	p.MaxInterval = time.Millisecond
	p.MaxBuffered = cap(tasks) / 2

	//循环从BackendConn的input这个channel取redis请求
	for r := range bc.input {
		if r.IsReadOnly() && r.IsBroken() {
			bc.setResponse(r, nil, ErrRequestIsBroken)
			continue
		}
		//encode request,将所有的request一次性encode完
		if err := p.EncodeMultiBulk(r.Multi); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		}
		// len(ba.input) == 0,表明所有的request都已经encode完了，此时可以开始起 force flush
		// Flush将请求的内容发送给codis-server
		if err := p.Flush(len(bc.input) == 0); err != nil {
			return bc.setResponse(r, nil, fmt.Errorf("backend conn failure, %s", err))
		} else {
			//将发送给codis-server的请求写入tasks这个channel,等待请求返回后loopReader()会按照
			//task中request的顺序将请求的返回结果和request匹配
			tasks <- r
		}
	}
	return nil
}

type sharedBackendConn struct {
	addr string
	host []byte
	port []byte

	//所属的pool
	owner *sharedBackendConnPool
	conns [][]*BackendConn

	single []*BackendConn

	//当前sharedBackendConn的引用计数，非正数的时候表明关闭。每多一个引用就加一
	refcnt int
}

func newSharedBackendConn(addr string, pool *sharedBackendConnPool) *sharedBackendConn {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		log.ErrorErrorf(err, "split host-port failed, address = %s", addr)
	}
	s := &sharedBackendConn{
		addr: addr,
		host: []byte(host), port: []byte(port),
	}
	s.owner = pool
	s.conns = make([][]*BackendConn, pool.config.BackendNumberDatabases)
	//range用一个参数遍历二维切片，datebase是0到15
	for database := range s.conns {
		//len和cap都默认为1的一维切片
		parallel := make([]*BackendConn, pool.parallel)
		for i := range parallel {
			parallel[i] = NewBackendConn(addr, database, pool.config)
		}
		s.conns[database] = parallel
	}
	if pool.parallel == 1 {
		s.single = make([]*BackendConn, len(s.conns))
		for database := range s.conns {
			s.single[database] = s.conns[database][0]
		}
	}
	s.refcnt = 1
	return s
}

func (s *sharedBackendConn) Addr() string {
	if s == nil {
		return ""
	}
	return s.addr
}

func (s *sharedBackendConn) Release() {
	if s == nil {
		return
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed, close too many times")
	} else {
		s.refcnt--
	}
	if s.refcnt != 0 {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.Close()
		}
	}
	delete(s.owner.pool, s.addr)
}

func (s *sharedBackendConn) Retain() *sharedBackendConn {
	if s == nil {
		return nil
	}
	if s.refcnt <= 0 {
		log.Panicf("shared backend conn has been closed")
	} else {
		s.refcnt++
	}
	return s
}

func (s *sharedBackendConn) KeepAlive() {
	if s == nil {
		return
	}
	for _, parallel := range s.conns {
		for _, bc := range parallel {
			bc.KeepAlive()
		}
	}
}

func (s *sharedBackendConn) BackendConn(database int32, seed uint, must bool) *BackendConn {
	if s == nil {
		return nil
	}

	if s.single != nil {
		bc := s.single[database]
		if must || bc.IsConnected() {
			return bc
		}
		return nil
	}

	var parallel = s.conns[database]

	var i = seed
	for range parallel {
		i = (i + 1) % uint(len(parallel))
		if bc := parallel[i]; bc.IsConnected() {
			return bc
		}
	}
	if !must {
		return nil
	}
	return parallel[0]
}

//后端的共享连接池，保存了proxy到后端redis-server之间的Conn
type sharedBackendConnPool struct {
	config   *Config
	parallel int

	//key:codis-server的addr， value:sharedBackendConn
	pool map[string]*sharedBackendConn
}

func newSharedBackendConnPool(config *Config, parallel int) *sharedBackendConnPool {
	p := &sharedBackendConnPool{
		config: config, parallel: math2.MaxInt(1, parallel),
	}
	p.pool = make(map[string]*sharedBackendConn)
	return p
}

func (p *sharedBackendConnPool) KeepAlive() {
	for _, bc := range p.pool {
		bc.KeepAlive()
	}
}

func (p *sharedBackendConnPool) Get(addr string) *sharedBackendConn {
	return p.pool[addr]
}

func (p *sharedBackendConnPool) Retain(addr string) *sharedBackendConn {
	//首先从pool中直接取，取的到的话，引用计数加一
	if bc := p.pool[addr]; bc != nil {
		return bc.Retain()
	} else {
		//取不到就新建，然后放到pool里面
		bc = newSharedBackendConn(addr, p)
		p.pool[addr] = bc
		return bc
	}
}
