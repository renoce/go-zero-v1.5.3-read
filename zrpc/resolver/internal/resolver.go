package internal

import (
	"fmt"

	"google.golang.org/grpc/resolver"
)

const (
	// DirectScheme stands for direct scheme.
	DirectScheme = "direct"
	// DiscovScheme stands for discov scheme.
	DiscovScheme = "discov"
	// EtcdScheme stands for etcd scheme.
	EtcdScheme = "etcd"
	// KubernetesScheme stands for k8s scheme.
	KubernetesScheme = "k8s"
	// EndpointSepChar is the separator cha in endpoints.
	EndpointSepChar = ','

	subsetSize = 32
)

var (
	// EndpointSep is the separator string in endpoints.
	EndpointSep = fmt.Sprintf("%c", EndpointSepChar)

	directResolverBuilder directBuilder
	discovResolverBuilder discovBuilder
	etcdResolverBuilder   etcdBuilder
	k8sResolverBuilder    kubeBuilder
)

// RegisterResolver registers the direct and discov schemes to the resolver.
// 注册grpc直连地址和etcd链接正常的服务地址到grpc的解析器, 将resolver注册到grpc-go的库中的一个全局变量当中，
// 在go-zero grpc client连接服务的时候， 依然使用的这个库， 所以这样就关联到一起了。
func RegisterResolver() {
	resolver.Register(&directResolverBuilder)
	resolver.Register(&discovResolverBuilder)
	resolver.Register(&etcdResolverBuilder)
	resolver.Register(&k8sResolverBuilder)
}

type nopResolver struct {
	cc resolver.ClientConn
}

func (r *nopResolver) Close() {
}

func (r *nopResolver) ResolveNow(options resolver.ResolveNowOptions) {
}
