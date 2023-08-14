package internal

import (
	"strings"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/zrpc/resolver/internal/targets"
	"google.golang.org/grpc/resolver"
)

// 实现 grpc resolver
type discovBuilder struct{}

// 当每个go-zero业务服务grpc client连接 server的时候会调用一次Build， grpc-go内部会在获取服务地址的时候会调用Build
func (b *discovBuilder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (
	resolver.Resolver, error) {
	// 拆分地址字符串，获取etcd集群地址 endpoints
	hosts := strings.FieldsFunc(targets.GetAuthority(target), func(r rune) bool {
		return r == EndpointSepChar
	})

	// targets.GetEndpoints(target)：获取配置中的etcd的key值， 根据key值监控这个go-zero的grpc sever
	// NewSubscriber：对etcd中存储的go-zero的grpc server动态监控， 如果有变化， 则通过下面的update更新grpc server的地址，
	// 然后通过 grpc client中的UpdateState 更新地址，动态重连 grpc server
	sub, err := discov.NewSubscriber(hosts, targets.GetEndpoints(target))
	if err != nil {
		return nil, err
	}

	update := func() {
		var addrs []resolver.Address
		for _, val := range subset(sub.Values(), subsetSize) {
			addrs = append(addrs, resolver.Address{
				Addr: val,
			})
		}
		// 第一次grpc 链接的时候， 通过UpdateState连接grpc server， 后续通过这个函数更新grpc server地址， 重连server
		if err := cc.UpdateState(resolver.State{
			Addresses: addrs,
		}); err != nil {
			logx.Error(err)
		}
	}
	// 在监控中添加这个update回调函数， 以供动态更新grpc server连接
	sub.AddListener(update)
	// 这里是第一次 go-zero中 grpc client 连接 server的时候使用
	update()

	return &nopResolver{cc: cc}, nil
}

func (b *discovBuilder) Scheme() string {
	return DiscovScheme
}
