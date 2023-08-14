package p2c

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/timex"
	"github.com/zeromicro/go-zero/zrpc/internal/codes"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

const (
	// Name is the name of p2c balancer.
	Name = "p2c_ewma"

	decayTime       = int64(time.Second * 10) // default value from finagle
	forcePick       = int64(time.Second)
	initSuccess     = 1000
	throttleSuccess = initSuccess / 2
	penalty         = int64(math.MaxInt32)
	pickTimes       = 3
	logInterval     = time.Minute
)

var emptyPickResult balancer.PickResult

func init() {
	balancer.Register(newBuilder())
}

type p2cPickerBuilder struct{}

// UpdateClientConnState在和grpc server建立连接以后， 会调用grpc中regeneratePicker， 这个函数会调用Build， 这里可以拿到ReadySCs， ready subConnects， 所有已经建立的链接
func (b *p2cPickerBuilder) Build(info base.PickerBuildInfo) balancer.Picker {
	readySCs := info.ReadySCs
	if len(readySCs) == 0 {
		return base.NewErrPicker(balancer.ErrNoSubConnAvailable)
	}

	var conns []*subConn
	// 构建subConn slice
	for conn, connInfo := range readySCs {
		conns = append(conns, &subConn{
			addr:    connInfo.Address,
			conn:    conn,
			success: initSuccess,
		})
	}
	// 构建p2cPicker
	return &p2cPicker{
		conns: conns,
		r:     rand.New(rand.NewSource(time.Now().UnixNano())),
		stamp: syncx.NewAtomicDuration(),
	}
}

// 这里使用grpc-go中的自带baseBalancer，其函数UpdateClientConnState会默认和所有的grpc server连接
// UpdateClientConnState在和grpc server建立连接以后， 会调用UpdateState， 更新连接状态，并且更新Picker， 这这里自定义Picker更新， 以供负载均衡的时候调用
func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, new(p2cPickerBuilder), base.Config{HealthCheck: true})
}

type p2cPicker struct {
	conns []*subConn            // 已经建立连接的grpc链接信息
	r     *rand.Rand            // 随机数生成器,用来从子连接列表中随机选择两个候选者
	stamp *syncx.AtomicDuration // 原子时长,用来记录上一次更新子连接列表的时间戳
	lock  sync.Mutex            // 互斥锁
}

// 负载均衡策略，p2c， 发送消息的时候会调用这个Pick函数选择发送的server，进行负载均衡
func (p *p2cPicker) Pick(_ balancer.PickInfo) (balancer.PickResult, error) {
	p.lock.Lock()
	defer p.lock.Unlock()

	var chosen *subConn
	switch len(p.conns) {
	case 0:
		// 没有节点， 返回错误
		return emptyPickResult, balancer.ErrNoSubConnAvailable
	case 1:
		// 有一个节点， 直接返回这个节点
		chosen = p.choose(p.conns[0], nil)
	case 2:
		// 有两个节点， 选择负荷低的节点
		chosen = p.choose(p.conns[0], p.conns[1])
	default:
		// 有多个节点， p2c选择两个节点， 比较并返回负荷低的节点
		var node1, node2 *subConn
		// 三次随机选择两个节点
		for i := 0; i < pickTimes; i++ {
			a := p.r.Intn(len(p.conns))
			b := p.r.Intn(len(p.conns) - 1)
			if b >= a {
				b++
			}
			node1 = p.conns[a]
			node2 = p.conns[b]
			// 如果选择的节点达到健康要求， 终止选择
			if node1.healthy() && node2.healthy() {
				break
			}
		}
		// 比较节点情况， 选择负载低的节点
		chosen = p.choose(node1, node2)
	}
	// 处理请求数+1
	atomic.AddInt64(&chosen.inflight, 1)
	atomic.AddInt64(&chosen.requests, 1)

	return balancer.PickResult{
		SubConn: chosen.conn,
		Done:    p.buildDoneFunc(chosen),
	}, nil
}

// grpc消息发送完成后, Finish函数会调用
func (p *p2cPicker) buildDoneFunc(c *subConn) func(info balancer.DoneInfo) {
	start := int64(timex.Now())
	return func(info balancer.DoneInfo) {
		// 处理请求数-1
		atomic.AddInt64(&c.inflight, -1)
		now := timex.Now()
		// 取出上次请求时的时间点, 保存本次请求结束时的时间点
		last := atomic.SwapInt64(&c.last, int64(now))
		td := int64(now) - last
		if td < 0 {
			td = 0
		}
		// 牛顿冷却定律中的衰减函数模型计算EWMA算法中的β值
		w := math.Exp(float64(-td) / float64(decayTime))
		lag := int64(now) - start
		if lag < 0 {
			lag = 0
		}
		olag := atomic.LoadUint64(&c.lag)
		if olag == 0 {
			w = 0
		}
		// 计算ewma值， 保存到c.lag
		atomic.StoreUint64(&c.lag, uint64(float64(olag)*w+float64(lag)*(1-w)))
		success := initSuccess
		if info.Err != nil && !codes.Acceptable(info.Err) {
			success = 0
		}
		osucc := atomic.LoadUint64(&c.success)
		atomic.StoreUint64(&c.success, uint64(float64(osucc)*w+float64(success)*(1-w)))

		stamp := p.stamp.Load()
		if now-stamp >= logInterval {
			if p.stamp.CompareAndSwap(stamp, now) {
				p.logStats()
			}
		}
	}
}

func (p *p2cPicker) choose(c1, c2 *subConn) *subConn {
	start := int64(timex.Now())
	if c2 == nil {
		atomic.StoreInt64(&c1.pick, start)
		return c1
	}

	if c1.load() > c2.load() {
		c1, c2 = c2, c1
	}

	pick := atomic.LoadInt64(&c2.pick)
	if start-pick > forcePick && atomic.CompareAndSwapInt64(&c2.pick, pick, start) {
		return c2
	}

	atomic.StoreInt64(&c1.pick, start)
	return c1
}

func (p *p2cPicker) logStats() {
	var stats []string

	p.lock.Lock()
	defer p.lock.Unlock()

	for _, conn := range p.conns {
		stats = append(stats, fmt.Sprintf("conn: %s, load: %d, reqs: %d",
			conn.addr.Addr, conn.load(), atomic.SwapInt64(&conn.requests, 0)))
	}

	logx.Statf("p2c - %s", strings.Join(stats, "; "))
}

type subConn struct {
	lag      uint64           // 用来保存 ewma 值
	inflight int64            // 用在保存当前正在使用此连接的请求总数
	success  uint64           // 用来标识一段时间内此连接的健康状态
	requests int64            // 用来保存请求总数
	last     int64            // 用来保存上一次请求耗时, 计算 ewma 值
	pick     int64            // 用来保存上一次被选中的时间点
	addr     resolver.Address //用来保存连接的地址,包含了IP、端口、元数据等信息
	conn     balancer.SubConn //用来保存连接的连接对象,用来发送和接收请求
}

func (c *subConn) healthy() bool {
	return atomic.LoadUint64(&c.success) > throttleSuccess
}

func (c *subConn) load() int64 {
	// plus one to avoid multiply zero
	// 通过 EWMA 计算节点的负载情况, +1 是为了避免出现0值
	lag := int64(math.Sqrt(float64(atomic.LoadUint64(&c.lag) + 1)))
	load := lag * (atomic.LoadInt64(&c.inflight) + 1)
	if load == 0 {
		return penalty
	}

	return load
}
