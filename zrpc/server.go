package zrpc

import (
	"time"

	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/core/stores/redis"
	"github.com/zeromicro/go-zero/zrpc/internal"
	"github.com/zeromicro/go-zero/zrpc/internal/auth"
	"github.com/zeromicro/go-zero/zrpc/internal/serverinterceptors"
	"google.golang.org/grpc"
)

// A RpcServer is a rpc server.
type RpcServer struct {
	// 这里的接口是grpc 的一些接口，　最终通过这些接口和grpc交互
	server internal.Server
	//　注册客户端访问的服务及方法，　这是一个回调函数，　回调函数里面可以有多个客户端访问的服务，　服务参数是grpc server, 服务会回调Grpc Server将服务列表注册到grpc　server list
	/*
		s := zrpc.MustNewServer(c.RpcServerConf, func(grpcServer *grpc.Server) {
			dm.RegisterDeviceAuthServer(grpcServer, deviceauth.NewDeviceAuthServer(svcCtx))
			dm.RegisterDeviceManageServer(grpcServer, devicemanage.NewDeviceManageServer(svcCtx))
			dm.RegisterDeviceGroupServer(grpcServer, devicegroup.NewDeviceGroupServer(svcCtx))
			dm.RegisterProductManageServer(grpcServer, productmanage.NewProductManageServer(svcCtx))
			dm.RegisterRemoteConfigServer(grpcServer, remoteconfig.NewRemoteConfigServer(svcCtx))
			if c.Mode == service.DevMode || c.Mode == service.TestMode {
				reflection.Register(grpcServer)
			}
		})


	*/
	register internal.RegisterFn
}

// MustNewServer returns a RpcSever, exits on any error.
func MustNewServer(c RpcServerConf, register internal.RegisterFn) *RpcServer {
	server, err := NewServer(c, register)
	logx.Must(err)
	return server
}

// NewServer returns a RpcServer.
func NewServer(c RpcServerConf, register internal.RegisterFn) (*RpcServer, error) {
	var err error
	if err = c.Validate(); err != nil {
		return nil, err
	}

	var server internal.Server
	metrics := stat.NewMetrics(c.ListenOn)
	serverOptions := []internal.ServerOption{
		internal.WithMetrics(metrics),
		internal.WithRpcHealth(c.Health),
	}
	// 根据是否配置了etcd构建服务，　这个返回的rpc server数据结构实现了internal.Server接口
	if c.HasEtcd() {
		//  构建rpc server以及etcd client
		server, err = internal.NewRpcPubServer(c.Etcd, c.ListenOn, c.Middlewares, serverOptions...)
		if err != nil {
			return nil, err
		}
	} else {
		// 只构建rpc server
		server = internal.NewRpcServer(c.ListenOn, c.Middlewares, serverOptions...)
	}
	// 配置 server name 提供性能检测的名称
	server.SetName(c.Name)
	// 构建rpc server的服务降载， 超时， 鉴权
	if err = setupInterceptors(server, c, metrics); err != nil {
		return nil, err
	}

	rpcServer := &RpcServer{
		server:   server,
		register: register,
	}
	// 构建日志，prometheus， 链路追踪，远程性能报告等
	if err = c.SetUp(); err != nil {
		return nil, err
	}

	return rpcServer, nil
}

// AddOptions adds given options.
func (rs *RpcServer) AddOptions(options ...grpc.ServerOption) {
	rs.server.AddOptions(options...)
}

// AddStreamInterceptors adds given stream interceptors.
func (rs *RpcServer) AddStreamInterceptors(interceptors ...grpc.StreamServerInterceptor) {
	rs.server.AddStreamInterceptors(interceptors...)
}

// AddUnaryInterceptors adds given unary interceptors.
func (rs *RpcServer) AddUnaryInterceptors(interceptors ...grpc.UnaryServerInterceptor) {
	rs.server.AddUnaryInterceptors(interceptors...)
}

// Start starts the RpcServer.
// Graceful shutdown is enabled by default.
// Use proc.SetTimeToForceQuit to customize the graceful shutdown period.
func (rs *RpcServer) Start() {
	if err := rs.server.Start(rs.register); err != nil {
		logx.Error(err)
		panic(err)
	}
}

// Stop stops the RpcServer.
func (rs *RpcServer) Stop() {
	logx.Close()
}

// DontLogContentForMethod disable logging content for given method.
// Deprecated: use ServerMiddlewaresConf.IgnoreContentMethods instead.
func DontLogContentForMethod(method string) {
	serverinterceptors.DontLogContentForMethod(method)
}

// SetServerSlowThreshold sets the slow threshold on server side.
// Deprecated: use ServerMiddlewaresConf.SlowThreshold instead.
func SetServerSlowThreshold(threshold time.Duration) {
	serverinterceptors.SetSlowThreshold(threshold)
}

func setupAuthInterceptors(svr internal.Server, c RpcServerConf) error {
	rds, err := redis.NewRedis(c.Redis.RedisConf)
	if err != nil {
		return err
	}

	authenticator, err := auth.NewAuthenticator(rds, c.Redis.Key, c.StrictControl)
	if err != nil {
		return err
	}

	svr.AddStreamInterceptors(serverinterceptors.StreamAuthorizeInterceptor(authenticator))
	svr.AddUnaryInterceptors(serverinterceptors.UnaryAuthorizeInterceptor(authenticator))

	return nil
}

func setupInterceptors(svr internal.Server, c RpcServerConf, metrics *stat.Metrics) error {
	if c.CpuThreshold > 0 {
		shedder := load.NewAdaptiveShedder(load.WithCpuThreshold(c.CpuThreshold))
		svr.AddUnaryInterceptors(serverinterceptors.UnarySheddingInterceptor(shedder, metrics))
	}

	if c.Timeout > 0 {
		svr.AddUnaryInterceptors(serverinterceptors.UnaryTimeoutInterceptor(
			time.Duration(c.Timeout) * time.Millisecond))
	}

	if c.Auth {
		if err := setupAuthInterceptors(svr, c); err != nil {
			return err
		}
	}

	return nil
}
