package targets

import (
	"strings"

	"google.golang.org/grpc/resolver"
)

const slashSeparator = "/"

// GetAuthority returns the authority of the target.
// 获取endpoints 地址列表,  Host格式 192.xx.xx.xx,10.xx.xx.xx, xxxx
func GetAuthority(target resolver.Target) string {
	return target.URL.Host
}

// GetEndpoints returns the endpoints from the given target.
// 获取go-zero客户端配置的etcd key值， 也是go-zero中grpc server的fullkey值， 即： ${etcdServerName}/leaseID
// 通过这个key值可以获取到go-zero grpc server在etcd server中的状态
func GetEndpoints(target resolver.Target) string {
	return strings.Trim(target.URL.Path, slashSeparator)
}
