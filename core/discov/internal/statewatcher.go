//go:generate mockgen -package internal -destination statewatcher_mock.go -source statewatcher.go etcdConn

package internal

import (
	"context"
	"sync"

	"google.golang.org/grpc/connectivity"
)

type (
	// 这个接口是grpc ClientConn结构体反向实现的，获取grpc状态作为etcd的链接状态
	etcdConn interface {
		GetState() connectivity.State
		WaitForStateChange(ctx context.Context, sourceState connectivity.State) bool
	}
	// 当前etcd client和etcd server集群之间的grpc的链接状态
	stateWatcher struct {
		disconnected bool
		currentState connectivity.State //grpc 中的链接状态
		listeners    []func()
		// lock only guards listeners, because only listens can be accessed by other goroutines.
		lock sync.Mutex
	}
)

func newStateWatcher() *stateWatcher {
	return new(stateWatcher)
}

// 添加registy中的etcd状态监听回调
func (sw *stateWatcher) addListener(l func()) {
	sw.lock.Lock()
	sw.listeners = append(sw.listeners, l)
	sw.lock.Unlock()
}

// 通知外部所有的回调监听
func (sw *stateWatcher) notifyListeners() {
	sw.lock.Lock()
	defer sw.lock.Unlock()

	for _, l := range sw.listeners {
		l()
	}
}

func (sw *stateWatcher) updateState(conn etcdConn) {
	// 通过grpc clientConn获取grpc状态， 更新本地etcd链接状态
	sw.currentState = conn.GetState()
	switch sw.currentState {
	case connectivity.TransientFailure, connectivity.Shutdown:
		sw.disconnected = true
	case connectivity.Ready:
		if sw.disconnected {
			sw.disconnected = false
			sw.notifyListeners()
		}
	}
}

// 监控etcd client和server的grpc状态， grpc可能会断开连接进入connectivity.TransientFailure， 然后会进行重连， 重连成功， 重新对go-zero的grpc server发起监控
func (sw *stateWatcher) watch(conn etcdConn) {
	// 获取当前的grpc状态
	sw.currentState = conn.GetState()
	for {
		// 调用Grpc中的状态接口， 这个接口默认是channel阻塞的， 如果状态改变， channel会接收消息， 返回bool
		// 然后， 调用 本地更新接口再次通过grpc链接结构体获取链接状态，更新本地的etcd 的grpc链接状态
		if conn.WaitForStateChange(context.Background(), sw.currentState) {
			sw.updateState(conn)
		}
	}
}
