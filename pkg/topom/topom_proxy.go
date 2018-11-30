// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package topom

import (
	"github.com/thesunnysky/codis/pkg/models"
	"github.com/thesunnysky/codis/pkg/proxy"
	"github.com/thesunnysky/codis/pkg/utils/errors"
	"github.com/thesunnysky/codis/pkg/utils/log"
	"github.com/thesunnysky/codis/pkg/utils/sync2"
)

//传入的addr就是proxy的地址
func (s *Topom) CreateProxy(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	//这里的p就是根据proxy地址取出models.proxy，也就是/codis3/codis-wujiang/proxy路径下面的那个proxy-token中的详细信息
	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed, %s", addr, err)
	}
	//这个ApiClient中存储了proxy的地址，以及根据productName,productAuth(默认为空)以及token生成的auth
	c := s.newProxyClient(p)

	//之前我们说过，proxy启动的时候，在s.setup(config)这一步，会生成一个xauth，存储在Proxy的xauth属性中，这一步就是讲上面得到
	//的xauth和启动proxy时的xauth作比较，来唯一确定需要的xauth
	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed, %s", addr, err)
	}
	//检查上下文中的proxy是否已经有token，如果有的话，说明这个proxy已经添加到集群了
	if ctx.proxy[p.Token] != nil {
		return errors.Errorf("proxy-[%s] already exists", p.Token)
	} else {
		//上下文中所有proxy的最大id+1，赋给当前的proxy作为其id
		p.Id = ctx.maxProxyId() + 1
	}
	defer s.dirtyProxyCache(p.Token)

	//到这一步，proxy已经添加成功，更新"/codis3/codis-wujiang/proxy/proxy-token"下面的proxy信息
	if err := s.storeCreateProxy(p); err != nil {
		return err
	} else {
		return s.reinitProxy(ctx, p, c)
	}
}

func (s *Topom) OnlineProxy(addr string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := proxy.NewApiClient(addr).Model()
	if err != nil {
		return errors.Errorf("proxy@%s fetch model failed", addr)
	}
	c := s.newProxyClient(p)

	if err := c.XPing(); err != nil {
		return errors.Errorf("proxy@%s check xauth failed", addr)
	}
	defer s.dirtyProxyCache(p.Token)

	if d := ctx.proxy[p.Token]; d != nil {
		p.Id = d.Id
		if err := s.storeUpdateProxy(p); err != nil {
			return err
		}
	} else {
		p.Id = ctx.maxProxyId() + 1
		if err := s.storeCreateProxy(p); err != nil {
			return err
		}
	}
	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) RemoveProxy(token string, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(p)

	if err := c.Shutdown(); err != nil {
		log.WarnErrorf(err, "proxy-[%s] shutdown failed, force remove = %t", token, force)
		if !force {
			return errors.Errorf("proxy-[%s] shutdown failed", p.Token)
		}
	}
	defer s.dirtyProxyCache(p.Token)

	return s.storeRemoveProxy(p)
}

func (s *Topom) ReinitProxy(token string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx, err := s.newContext()
	if err != nil {
		return err
	}

	p, err := ctx.getProxy(token)
	if err != nil {
		return err
	}
	c := s.newProxyClient(p)

	return s.reinitProxy(ctx, p, c)
}

func (s *Topom) newProxyClient(p *models.Proxy) *proxy.ApiClient {
	c := proxy.NewApiClient(p.AdminAddr)
	//根据信息设置Client的XAUTH,需要和Proxy启动的时候设置的XAUTH相同
	c.SetXAuth(s.config.ProductName, s.config.ProductAuth, p.Token)
	return c
}

func (s *Topom) reinitProxy(ctx *context, p *models.Proxy, c *proxy.ApiClient) error {
	log.Warnf("proxy-[%s] reinit:\n%s", p.Token, p.Encode())
	//初始化1024个槽,
	if err := c.FillSlots(ctx.toSlotSlice(ctx.slots, p)...); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] fillslots failed", p.Token)
		return errors.Errorf("proxy-[%s] fillslots failed", p.Token)
	}
	if err := c.Start(); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] start failed", p.Token)
		return errors.Errorf("proxy-[%s] start failed", p.Token)
	}
	if err := c.SetSentinels(ctx.sentinel); err != nil {
		log.ErrorErrorf(err, "proxy-[%s] set sentinels failed", p.Token)
		return errors.Errorf("proxy-[%s] set sentinels failed", p.Token)
	}
	return nil
}

func (s *Topom) resyncSlotMappingsByGroupId(ctx *context, gid int) error {
	return s.resyncSlotMappings(ctx, ctx.getSlotMappingsByGroupId(gid)...)
}

func (s *Topom) resyncSlotMappings(ctx *context, slots ...*models.SlotMapping) error {
	if len(slots) == 0 {
		return nil
	}
	var fut sync2.Future
	for _, p := range ctx.proxy {
		fut.Add()
		go func(p *models.Proxy) {
			err := s.newProxyClient(p).FillSlots(ctx.toSlotSlice(slots, p)...)
			if err != nil {
				log.ErrorErrorf(err, "proxy-[%s] resync slots failed", p.Token)
			}
			fut.Done(p.Token, err)
		}(p)
	}
	for t, v := range fut.Wait() {
		switch err := v.(type) {
		case error:
			if err != nil {
				return errors.Errorf("proxy-[%s] resync slots failed", t)
			}
		}
	}
	return nil
}
