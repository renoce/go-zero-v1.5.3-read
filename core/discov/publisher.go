package discov

import (
	"time"

	"github.com/zeromicro/go-zero/core/discov/internal"
	"github.com/zeromicro/go-zero/core/lang"
	"github.com/zeromicro/go-zero/core/logx"
	"github.com/zeromicro/go-zero/core/proc"
	"github.com/zeromicro/go-zero/core/syncx"
	"github.com/zeromicro/go-zero/core/threading"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type (
	// PubOption defines the method to customize a Publisher.
	PubOption func(client *Publisher)

	// A Publisher can be used to publish the value to an etcd cluster on the given key.
	Publisher struct {
		endpoints  []string
		key        string // 配置中的grpc server名称
		fullKey    string // 完整key,   config 中grpc server  ${servername}/${listenOnaddress}
		id         int64  // lease id
		value      string // 地址
		lease      clientv3.LeaseID
		quit       *syncx.DoneChan
		pauseChan  chan lang.PlaceholderType
		resumeChan chan lang.PlaceholderType
	}
)

// NewPublisher returns a Publisher.
// endpoints is the hosts of the etcd cluster.
// key:value are a pair to be published.
// opts are used to customize the Publisher.
func NewPublisher(endpoints []string, key, value string, opts ...PubOption) *Publisher {
	publisher := &Publisher{
		endpoints:  endpoints,
		key:        key,
		value:      value,
		quit:       syncx.NewDoneChan(),
		pauseChan:  make(chan lang.PlaceholderType),
		resumeChan: make(chan lang.PlaceholderType),
	}

	for _, opt := range opts {
		opt(publisher)
	}

	return publisher
}

// KeepAlive keeps key:value alive.
func (p *Publisher) KeepAlive() error {
	// 注册配置中的grpc name/address的k/v到etcd集群
	cli, err := p.doRegister()
	if err != nil {
		return err
	}

	proc.AddWrapUpListener(func() {
		p.Stop()
	})
	// etcd keepalive 键值对续组
	return p.keepAliveAsync(cli)
}

// Pause pauses the renewing of key:value.
func (p *Publisher) Pause() {
	p.pauseChan <- lang.Placeholder
}

// Resume resumes the renewing of key:value.
func (p *Publisher) Resume() {
	p.resumeChan <- lang.Placeholder
}

// Stop stops the renewing and revokes the registration.
func (p *Publisher) Stop() {
	p.quit.Close()
}

func (p *Publisher) doKeepAlive() error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		select {
		case <-p.quit.Done():
			return nil
		default:
			cli, err := p.doRegister()
			if err != nil {
				logx.Errorf("etcd publisher doRegister: %s", err.Error())
				break
			}

			if err := p.keepAliveAsync(cli); err != nil {
				logx.Errorf("etcd publisher keepAliveAsync: %s", err.Error())
				break
			}

			return nil
		}
	}

	return nil
}

// 注册配置中的grpc name/address
func (p *Publisher) doRegister() (internal.EtcdClient, error) {
	// 构建etcd client 和etcd server的链接， 拿到etcd客户端中的数据，　包括conn链接等，　可以通过客户端发送数据到etcd server
	cli, err := internal.GetRegistry().GetConn(p.endpoints)
	if err != nil {
		return nil, err
	}
	// 将go-zero配置的grpc server name 和地址通过etcd client 以k/v形式存储到etcd server
	p.lease, err = p.register(cli)
	return cli, err
}

func (p *Publisher) keepAliveAsync(cli internal.EtcdClient) error {
	//  通过etcd  keepalive 定期续租， 监控是k/v否过期，　保持etcd 配置的活性
	ch, err := cli.KeepAlive(cli.Ctx(), p.lease)
	if err != nil {
		return err
	}

	threading.GoSafe(func() {
		for {
			select {
			case _, ok := <-ch:
				// k/v在etcd被删除， ch被close， 这个时候需要重新将k/v插入
				if !ok {
					p.revoke(cli)
					if err := p.doKeepAlive(); err != nil {
						logx.Errorf("etcd publisher KeepAlive: %s", err.Error())
					}
					return
				}
			case <-p.pauseChan:
				logx.Infof("paused etcd renew, key: %s, value: %s", p.key, p.value)
				p.revoke(cli)
				select {
				case <-p.resumeChan:
					if err := p.doKeepAlive(); err != nil {
						logx.Errorf("etcd publisher KeepAlive: %s", err.Error())
					}
					return
				case <-p.quit.Done():
					return
				}
			case <-p.quit.Done():
				p.revoke(cli)
				return
			}
		}
	})

	return nil
}

func (p *Publisher) register(client internal.EtcdClient) (clientv3.LeaseID, error) {
	resp, err := client.Grant(client.Ctx(), TimeToLive)
	if err != nil {
		return clientv3.NoLease, err
	}

	lease := resp.ID
	if p.id > 0 {
		p.fullKey = makeEtcdKey(p.key, p.id)
	} else {
		p.fullKey = makeEtcdKey(p.key, int64(lease))
	}
	// fullKey 完整key,   config 中grpc server  ${servername}/${listenOnaddress}
	_, err = client.Put(client.Ctx(), p.fullKey, p.value, clientv3.WithLease(lease))

	return lease, err
}

func (p *Publisher) revoke(cli internal.EtcdClient) {
	if _, err := cli.Revoke(cli.Ctx(), p.lease); err != nil {
		logx.Errorf("etcd publisher revoke: %s", err.Error())
	}
}

// WithId customizes a Publisher with the id.
func WithId(id int64) PubOption {
	return func(publisher *Publisher) {
		publisher.id = id
	}
}

// WithPubEtcdAccount provides the etcd username/password.
func WithPubEtcdAccount(user, pass string) PubOption {
	return func(pub *Publisher) {
		RegisterAccount(pub.endpoints, user, pass)
	}
}

// WithPubEtcdTLS provides the etcd CertFile/CertKeyFile/CACertFile.
func WithPubEtcdTLS(certFile, certKeyFile, caFile string, insecureSkipVerify bool) PubOption {
	return func(pub *Publisher) {
		logx.Must(RegisterTLS(pub.endpoints, certFile, certKeyFile, caFile, insecureSkipVerify))
	}
}
