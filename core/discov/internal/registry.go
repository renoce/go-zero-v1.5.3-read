package internal

import (
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/zeromicro/go-zero/core/contextx"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/threading"
	clientv3 "go.etcd.io/etcd/client/v3"
)

var (
	registry = Registry{
		clusters: make(map[string]*cluster),
	}
	connManager = syncx.NewResourceManager()
)

// A Registry is a registry that manages the etcd client connections.
// etcd集群， 监控etcd client和集群的grpc链接， 这里的监控是同一个go-zero server中可能存在的多个grpc client共有的.
type Registry struct {
	clusters map[string]*cluster
	lock     sync.Mutex
}

// GetRegistry returns a global Registry.
func GetRegistry() *Registry {
	return &registry
}

// GetConn returns an etcd client connection associated with given endpoints.
func (r *Registry) GetConn(endpoints []string) (EtcdClient, error) {
	c, _ := r.getCluster(endpoints)
	return c.getClient()
}

// Monitor monitors the key on given etcd endpoints, notify with the given UpdateListener.
// 同一个go-zero client端可能需要连接多个grpc server， 这里会被调用多次， 所以， 已经有的etcd集群数据会被重复利用， 这里看似有些不必要的代码， 其实是为了多个grpc client复用准备的。
func (r *Registry) Monitor(endpoints []string, key string, l UpdateListener) error {
	c, exists := r.getCluster(endpoints)
	// if exists, the existing values should be updated to the listener.
	// 如果grpc client连接多个grpc server， 监控只在grpc client启动的， 第一次连接监控和etcd client连接都是是不存在的，
	// 第一次会创建监控和etcd 连接， 后续的grpc client如果etcd地址一样，会直接复用第一次的etcd client链接和Registry
	if exists {
		// 根据配置中grpc server name 获取已经存在的go-zero grpc server地址
		kvs := c.getCurrent(key)
		for _, kv := range kvs {
			// 这里的UpdateListener也只是其中一个grpc客户端的UpdateListener
			l.OnAdd(kv)
		}
	}

	return c.monitor(key, l)
}

// 根据当前节点配置中的etcd集群 endpoints， 获取etcd集群， 如果没有则添加
func (r *Registry) getCluster(endpoints []string) (c *cluster, exists bool) {
	// 使用etcd集群ip地址通过，隔开，作为集群key值
	clusterKey := getClusterKey(endpoints)
	r.lock.Lock()
	defer r.lock.Unlock()
	c, exists = r.clusters[clusterKey]
	if !exists {
		c = newCluster(endpoints)
		r.clusters[clusterKey] = c
	}

	return
}

type cluster struct {
	endpoints []string // etcd 集群地址
	key       string   // 这里的key是当前节点存储的etcd集群的key， 192.168.1.1,192.xxx.xxx.xxx 类似这种 ip, 隔离的 string格式
	// values 外层map对应的key是etcd集群在配置中的key name，value map对应的是 etcd中存储的具体的每一个server的k/v， 具体内容是: K: keyName/leaseID V: grpc server address
	// 用来存储etcd 通过配置中的key name前缀查询出来的所有的grpc sever的key/value内容。
	values     map[string]map[string]string // values的作用是本地存储的grpc server历史地址， 通过和新的比对， 可以发现添加和删除的地址，进而更新listeners
	listeners  map[string][]UpdateListener  // listeners是地址监控回调， 通过回调可以重连grpc server。 map key是配置文件中grpc server name， value是UpdateListener监听， 这里为什么是slice？ 同一个 grpcservername也可能起多个链接的， 一个grpc 链接对应一个listener
	watchGroup *threading.RoutineGroup
	done       chan lang.PlaceholderType
	lock       sync.Mutex
}

func newCluster(endpoints []string) *cluster {
	return &cluster{
		endpoints:  endpoints,
		key:        getClusterKey(endpoints),
		values:     make(map[string]map[string]string),
		listeners:  make(map[string][]UpdateListener),
		watchGroup: threading.NewRoutineGroup(),
		done:       make(chan lang.PlaceholderType),
	}
}

func (c *cluster) context(cli EtcdClient) context.Context {
	return contextx.ValueOnlyFrom(cli.Ctx())
}

func (c *cluster) getClient() (EtcdClient, error) {
	// GetResource 从公共Resource中获取一个io流资源，如果没有就通过回调函数创建, 有的话就直接使用
	// 只是一个本地的Resource资源， 并不是真正的集群资源， 为什么要在池子里复用这个etcd链接？
	// 1. go-zero支持server group， 所以， 当同时启动多个服务的时候， etcd集群相同， 就可以复用同一个etcd client
	// 2. go-zero 一个grpc client端server可能要连接多个 grpc server
	val, err := connManager.GetResource(c.key, func() (io.Closer, error) {
		// 创建一个etcd client server链接， 并将client和server的grpc链接加入到监控
		return c.newClient()
	})
	if err != nil {
		return nil, err
	}
	// 将etcd client grpc链接转为EtcdClient类型
	return val.(EtcdClient), nil
}

// 根据配置文件中的etcd servername拿到本地存储的（从etcd获取存储到本地的） grpc server address
func (c *cluster) getCurrent(key string) []KV {
	c.lock.Lock()
	defer c.lock.Unlock()

	var kvs []KV
	for k, v := range c.values[key] {
		kvs = append(kvs, KV{
			Key: k,
			Val: v,
		})
	}

	return kvs
}

func (c *cluster) handleChanges(key string, kvs []KV) {
	var add []KV
	var remove []KV
	c.lock.Lock()
	// 获取本地的updatelistener列表
	listeners := append([]UpdateListener(nil), c.listeners[key]...)
	// 更新c.values, 即根据当前key查询到的etcd中存储的grpc server， 更新到本地
	vals, ok := c.values[key]
	if !ok {
		add = kvs
		vals = make(map[string]string)
		for _, kv := range kvs {
			vals[kv.Key] = kv.Val
		}
		c.values[key] = vals
	} else {
		m := make(map[string]string)
		for _, kv := range kvs {
			m[kv.Key] = kv.Val
		}
		for k, v := range vals {
			if val, ok := m[k]; !ok || v != val {
				remove = append(remove, KV{
					Key: k,
					Val: v,
				})
			}
		}
		for k, v := range m {
			if val, ok := vals[k]; !ok || v != val {
				add = append(add, KV{
					Key: k,
					Val: v,
				})
			}
		}
		c.values[key] = m
	}
	c.lock.Unlock()
	// 更新listener中的k/v
	for _, kv := range add {
		for _, l := range listeners {
			l.OnAdd(kv)
		}
	}
	for _, kv := range remove {
		for _, l := range listeners {
			l.OnDelete(kv)
		}
	}
}

// 处理etcd集群中k/v变化， 当有新的grpc server上线的时候， 会put key/address 到etcd集群，
// 当旧的grpc客户端连接监控收到的时候， 会更新本地的 grpc server地址， 进而通过listeners回调grpc的updateState函数重连
func (c *cluster) handleWatchEvents(key string, events []*clientv3.Event) {
	c.lock.Lock()
	// 重置listeners中的key的UpdateListener列表
	listeners := append([]UpdateListener(nil), c.listeners[key]...)
	c.lock.Unlock()

	for _, ev := range events {
		switch ev.Type {
		case clientv3.EventTypePut:
			c.lock.Lock()
			if vals, ok := c.values[key]; ok {
				vals[string(ev.Kv.Key)] = string(ev.Kv.Value)
			} else {
				c.values[key] = map[string]string{string(ev.Kv.Key): string(ev.Kv.Value)}
			}
			c.lock.Unlock()
			// 更新Container中的地址， 重连grpc server
			for _, l := range listeners {
				l.OnAdd(KV{
					Key: string(ev.Kv.Key),
					Val: string(ev.Kv.Value),
				})
			}
		case clientv3.EventTypeDelete:
			c.lock.Lock()
			if vals, ok := c.values[key]; ok {
				delete(vals, string(ev.Kv.Key))
			}
			c.lock.Unlock()
			// 更新Container中的地址， 重连grpc server
			for _, l := range listeners {
				l.OnDelete(KV{
					Key: string(ev.Kv.Key),
					Val: string(ev.Kv.Value),
				})
			}
		default:
			logx.Errorf("Unknown event type: %v", ev.Type)
		}
	}
}

func (c *cluster) load(cli EtcdClient, key string) int64 {
	// 先从etcd集群获取以key为前缀查询到的所有最新的grpc服务地址
	var resp *clientv3.GetResponse
	for {
		var err error
		ctx, cancel := context.WithTimeout(c.context(cli), RequestTimeout)
		resp, err = cli.Get(ctx, makeKeyPrefix(key), clientv3.WithPrefix())
		cancel()
		if err == nil {
			break

		}
		logx.Error(err)
		time.Sleep(coolDownInterval)
	}

	var kvs []KV
	for _, ev := range resp.Kvs {
		kvs = append(kvs, KV{
			Key: string(ev.Key),
			Val: string(ev.Value),
		})
	}
	// 更新监控中的地址， 进而开始连接服务
	c.handleChanges(key, kvs)
	// 返回当前key的最新修改值， 对这个修改版本进行监控， 有改动， 即可以继续更新服务地址
	return resp.Header.Revision
}

// 根据go-zero grpc client中配置的多个grpc server， 每个都会启动一个monitor监控
func (c *cluster) monitor(key string, l UpdateListener) error {
	c.lock.Lock()
	// 将同一个server中所有的grpc client UpdateListener添加到cluster listeners列表
	c.listeners[key] = append(c.listeners[key], l)
	c.lock.Unlock()
	// 获取etcd client 和 server的grpc连接客户端， 如果连接不存在， 新建连接， 存在的话， 复用之前的链接
	cli, err := c.getClient()
	if err != nil {
		return err
	}
	// 获取最新的grpc地址， 如果地址有变动， 重连， 同时得到最新的修改记录，在下面的watch监控动态变化.
	rev := c.load(cli, key)
	// 监控最新的修改版本， 如有改动， 更新地址， 重连grpc server
	// 同一个server中多个grpc client， 每一个都会启动一个watch，本质是一个waitGroup add一个任务.
	c.watchGroup.Run(func() {
		c.watch(cli, key, rev)
	})

	return nil
}

func (c *cluster) newClient() (EtcdClient, error) {
	// 构建etcd client， etcd client会和etcd server集群建立grpc的链接， etcd默认lb方式是轮循， 返回一个etcd client， client 包含grpc ClientConn结构体
	cli, err := NewClient(c.endpoints)
	if err != nil {
		return nil, err
	}
	// 监控当前节点etcd client和 etcd server的grpc链接状态， 如果etcd client重连server， 所有的监控重启
	go c.watchConnState(cli)

	return cli, nil
}

// 当etcd链接重连， 重新加载监控服务列表
func (c *cluster) reload(cli EtcdClient) {
	c.lock.Lock()
	// 关闭所有的grpc client的watchStream中的监听
	close(c.done)
	// 等待所有的watchStream监听退出
	c.watchGroup.Wait()
	// 重新初始化等待channel和watchGroup
	c.done = make(chan lang.PlaceholderType)
	c.watchGroup = threading.NewRoutineGroup()
	// 拿到所有的当前server中需要连接的所有的grpc server name
	var keys []string
	for k := range c.listeners {
		keys = append(keys, k)
	}
	c.lock.Unlock()
	// 重启启动监听， 所有的grpc server 都要重新监控
	for _, key := range keys {
		k := key
		c.watchGroup.Run(func() {
			rev := c.load(cli, k)
			c.watch(cli, k, rev)
		})
	}
}

func (c *cluster) watch(cli EtcdClient, key string, rev int64) {
	for {
		if c.watchStream(cli, key, rev) {
			return
		}
	}
}

// 实时监控etcd server集群中的k/v变化
func (c *cluster) watchStream(cli EtcdClient, key string, rev int64) bool {
	var rch clientv3.WatchChan
	if rev != 0 {
		// 监控最新的修改记录， 如果有变动， 会在channel收到数据， 下面for循环处理.
		rch = cli.Watch(clientv3.WithRequireLeader(c.context(cli)), makeKeyPrefix(key), clientv3.WithPrefix(),
			clientv3.WithRev(rev+1))
	} else {
		rch = cli.Watch(clientv3.WithRequireLeader(c.context(cli)), makeKeyPrefix(key), clientv3.WithPrefix())
	}

	for {
		select {
		case wresp, ok := <-rch:
			if !ok {
				logx.Error("etcd monitor chan has been closed")
				return false
			}
			if wresp.Canceled {
				logx.Errorf("etcd monitor chan has been canceled, error: %v", wresp.Err())
				return false
			}
			if wresp.Err() != nil {
				logx.Error(fmt.Sprintf("etcd monitor chan error: %v", wresp.Err()))
				return false
			}
			// etcd中的k/v发生变化， 将grpc server地址变化更新到每一个grpc client的updateState函数中， 重连.
			c.handleWatchEvents(key, wresp.Events)
			// 这里channel done 在etcd连接断开以后， 会收到 close (done)的关闭消息， 收到的是0值， 关闭当前监听
		case <-c.done:
			return true
		}
	}
}

// 监控etcd 的grpc 连接状态， 通过当前服务节点和etcd server集群的链接状态
func (c *cluster) watchConnState(cli EtcdClient) {
	watcher := newStateWatcher()
	watcher.addListener(func() {
		go c.reload(cli)
	})
	// 监控etcd链接状态
	watcher.watch(cli.ActiveConnection())
}

// DialClient dials an etcd cluster with given endpoints.
// 连接etcd集群， etcd client连接server同样是自配置了grpc resolver， loadbalance方式是 轮循
func DialClient(endpoints []string) (EtcdClient, error) {
	cfg := clientv3.Config{
		Endpoints:           endpoints,
		AutoSyncInterval:    autoSyncInterval,
		DialTimeout:         DialTimeout,
		RejectOldCluster:    true,
		PermitWithoutStream: true,
	}
	if account, ok := GetAccount(endpoints); ok {
		cfg.Username = account.User
		cfg.Password = account.Pass
	}
	if tlsCfg, ok := GetTLS(endpoints); ok {
		cfg.TLS = tlsCfg
	}

	return clientv3.New(cfg)
}

func getClusterKey(endpoints []string) string {
	sort.Strings(endpoints)
	return strings.Join(endpoints, endpointsSeparator)
}

func makeKeyPrefix(key string) string {
	return fmt.Sprintf("%s%c", key, Delimiter)
}
