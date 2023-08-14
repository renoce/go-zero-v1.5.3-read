package resolver

import (
	"github.com/zeromicro/go-zero/zrpc/resolver/internal"
)

// Register registers schemes defined zrpc.
// Keep it in a separated package to let third party register manually.
// 这里是注册grpc的resolver， 将特定schema和实现了grpc-go 接口的resolver Builder 通过grpc-go的Register注册到库中的全局变量中
// 在grpc连接的时候， grpc库会通过这个实现的Builder接口获取到服务的直连地址或者etcd中的动态地址
func Register() {
	internal.RegisterResolver()
}
