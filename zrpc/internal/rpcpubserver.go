package internal

import (
	"os"
	"strings"

	"github.com/zeromicro/go-zero/core/discov"
	"github.com/zeromicro/go-zero/core/netx"
)

const (
	allEths  = "0.0.0.0"
	envPodIp = "POD_IP"
)

// NewRpcPubServer returns a Server.
func NewRpcPubServer(etcd discov.EtcdConf, listenOn string, middlewares ServerMiddlewaresConf,
	opts ...ServerOption) (Server, error) {
	registerEtcd := func() error {
		pubListenOn := figureOutListenOn(listenOn)
		var pubOpts []discov.PubOption
		if etcd.HasAccount() {
			pubOpts = append(pubOpts, discov.WithPubEtcdAccount(etcd.User, etcd.Pass))
		}
		if etcd.HasTLS() {
			pubOpts = append(pubOpts, discov.WithPubEtcdTLS(etcd.CertFile, etcd.CertKeyFile,
				etcd.CACertFile, etcd.InsecureSkipVerify))
		}
		if etcd.HasID() {
			pubOpts = append(pubOpts, discov.WithId(etcd.ID))
		}
		pubClient := discov.NewPublisher(etcd.Hosts, etcd.Key, pubListenOn, pubOpts...)
		// 将go-zero配置的grpc server name 和地址通过etcd client 以k/v形式存储到etcd server，
		// 并且通过etcd keepalive监控k/v是否存活， 不存活重新添加
		return pubClient.KeepAlive()
	}
	server := keepAliveServer{
		registerEtcd: registerEtcd,
		Server:       NewRpcServer(listenOn, middlewares, opts...),
	}

	return server, nil
}

// 　实现了internal.server的接口， 即rpc server的构建， rpc server的配置的持有
type keepAliveServer struct {
	registerEtcd func() error
	Server
}

func (s keepAliveServer) Start(fn RegisterFn) error {
	// 将配置中的server配置通过etcd client保存到etcd server， 并且时刻监控，过期续租
	if err := s.registerEtcd(); err != nil {
		return err
	}
	// 启动rpc server
	return s.Server.Start(fn)
}

func figureOutListenOn(listenOn string) string {
	fields := strings.Split(listenOn, ":")
	if len(fields) == 0 {
		return listenOn
	}

	host := fields[0]
	if len(host) > 0 && host != allEths {
		return listenOn
	}

	ip := os.Getenv(envPodIp)
	if len(ip) == 0 {
		ip = netx.InternalIp()
	}
	if len(ip) == 0 {
		return listenOn
	}

	return strings.Join(append([]string{ip}, fields[1:]...), ":")
}