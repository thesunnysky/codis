# Codis codes

### 架构图



### struct

重要的数据结构：

- ServerGroup
- Slot
- Router
- SharedBackendConn
- Proxy

### proxy

1. proxy router 的填充；

2. proxy slot 的填充；

3. proxy 和session的关系； -> 完整的请求流程是什么样子的？

   proxy -> new session

**P1**

1. proxy 健康状态的监控；
2. proxy router信息的更新；

### 数据迁移



### 参考资料

1. http://www.open-open.com/lib/view/open1436360508098.html
2. https://blog.csdn.net/shmiluwei/article/details/51958359
3. https://blog.csdn.net/qifengzou/article/details/51934958
4. https://dbaplus.cn/news-141-270-1.html



###　Proxy启动

1. https://blog.csdn.net/antony9118/article/details/75268358　