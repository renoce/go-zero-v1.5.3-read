package rest

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/zeromicro/go-zero/core/codec"
	"github.com/zeromicro/go-zero/core/load"
	"github.com/zeromicro/go-zero/core/stat"
	"github.com/zeromicro/go-zero/rest/chain"
	"github.com/zeromicro/go-zero/rest/handler"
	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/internal"
	"github.com/zeromicro/go-zero/rest/internal/response"
)

// use 1000m to represent 100%
const topCpuUsage = 1000

// ErrSignatureConfig is an error that indicates bad config for signature.
var ErrSignatureConfig = errors.New("bad config for Signature")

type engine struct {
	conf   RestConf
	routes []featuredRoutes
	// timeout is the max timeout of all routes
	timeout              time.Duration
	unauthorizedCallback handler.UnauthorizedCallback
	unsignedCallback     handler.UnsignedCallback
	chain                chain.Chain
	middlewares          []Middleware
	shedder              load.Shedder
	priorityShedder      load.Shedder
	tlsConfig            *tls.Config
}

func newEngine(c RestConf) *engine {
	svr := &engine{
		conf:    c,
		timeout: time.Duration(c.Timeout) * time.Millisecond,
	}

	// CpuThreshold默认是900
	if c.CpuThreshold > 0 {
		// 启用降级
		svr.shedder = load.NewAdaptiveShedder(load.WithCpuThreshold(c.CpuThreshold))
		svr.priorityShedder = load.NewAdaptiveShedder(load.WithCpuThreshold(
			(c.CpuThreshold + topCpuUsage) >> 1))
	}

	return svr
}

func (ng *engine) addRoutes(r featuredRoutes) {
	ng.routes = append(ng.routes, r)

	// need to guarantee the timeout is the max of all routes
	// otherwise impossible to set http.Server.ReadTimeout & WriteTimeout
	if r.timeout > ng.timeout {
		ng.timeout = r.timeout
	}
}

func (ng *engine) appendAuthHandler(fr featuredRoutes, chn chain.Chain,
	verifier func(chain.Chain) chain.Chain) chain.Chain {
	if fr.jwt.enabled {
		if len(fr.jwt.prevSecret) == 0 {
			chn = chn.Append(handler.Authorize(fr.jwt.secret,
				handler.WithUnauthorizedCallback(ng.unauthorizedCallback)))
		} else {
			chn = chn.Append(handler.Authorize(fr.jwt.secret,
				handler.WithPrevSecret(fr.jwt.prevSecret),
				handler.WithUnauthorizedCallback(ng.unauthorizedCallback)))
		}
	}

	return verifier(chn)
}

// 遍历featuredRoute这一组路由， 将这一组路由分别和中间件调用链绑定
func (ng *engine) bindFeaturedRoutes(router httpx.Router, fr featuredRoutes, metrics *stat.Metrics) error {
	verifier, err := ng.signatureVerifier(fr.signature)
	if err != nil {
		return err
	}

	for _, route := range fr.routes {
		if err := ng.bindRoute(fr, router, metrics, route, verifier); err != nil {
			return err
		}
	}

	return nil
}

// 构建中间件调用链，将featuredRoute这一组路由中的其中一个路由和中间件调用链传递给router来构建 底层链接接收数据的handler
func (ng *engine) bindRoute(fr featuredRoutes, router httpx.Router, metrics *stat.Metrics,
	route Route, verifier func(chain.Chain) chain.Chain) error {
	chn := ng.chain
	if chn == nil {
		chn = ng.buildChainWithNativeMiddlewares(fr, route, metrics)
	}
	// 当签名没有的时候， verifier会不起作用， 直接返回传入的调用链
	chn = ng.appendAuthHandler(fr, chn, verifier)
	// ng.middewares 是使用ng.use添加的中间件slice
	for _, middleware := range ng.middlewares {
		// 将中间件拼接到chain的中间件slice列表
		chn = chn.Append(convertMiddleware(middleware))
	}
	// 将路由中的处理函数传给中间件slice最后一个中间件，又将最后一个中间件作为一个函数传递给倒数第二个中间件，以此类推, 传递到第一个中间件
	// 每个中间件都实例化了http.Handler这个接口（这个接口的接口函数是ServeHTTP）， 实例化以后， 变成一个函数， 调用的时候，形成了一个调用链，
	// 从第一个中间件一直调用到最后一个中间件, 最后一个中间件调用了具体的路由实现函数
	handle := chn.ThenFunc(route.Handler)
	// router.Handle具体实现在 router/patrouter.go中， patrouter结构体会作为一个handler被传递给http链接server， 收到请求时， 这个chain调用链
	// 这里Handler是绑定chain调用链到路由路径
	return router.Handle(route.Method, route.Path, handle)
}

func (ng *engine) bindRoutes(router httpx.Router) error {
	metrics := ng.createMetrics()
	// 遍历engine中的各个featuredRoute路由群组
	for _, fr := range ng.routes {
		if err := ng.bindFeaturedRoutes(router, fr, metrics); err != nil {
			return err
		}
	}

	return nil
}

// 构建go-zero原生中间件调用链
func (ng *engine) buildChainWithNativeMiddlewares(fr featuredRoutes, route Route,
	metrics *stat.Metrics) chain.Chain {
	chn := chain.New()

	if ng.conf.Middlewares.Trace {
		chn = chn.Append(handler.TraceHandler(ng.conf.Name,
			route.Path,
			handler.WithTraceIgnorePaths(ng.conf.TraceIgnorePaths))) // 链路追踪
	}
	if ng.conf.Middlewares.Log {
		chn = chn.Append(ng.getLogHandler()) //日志
	}
	if ng.conf.Middlewares.Prometheus {
		chn = chn.Append(handler.PrometheusHandler(route.Path, route.Method)) //监控
	}
	if ng.conf.Middlewares.MaxConns {
		chn = chn.Append(handler.MaxConnsHandler(ng.conf.MaxConns)) // 限流
	}
	if ng.conf.Middlewares.Breaker {
		chn = chn.Append(handler.BreakerHandler(route.Method, route.Path, metrics)) //熔断
	}
	if ng.conf.Middlewares.Shedding {
		chn = chn.Append(handler.SheddingHandler(ng.getShedder(fr.priority), metrics)) // 降载
	}
	if ng.conf.Middlewares.Timeout {
		chn = chn.Append(handler.TimeoutHandler(ng.checkedTimeout(fr.timeout))) // 超时处理, 应用层路由定义的超时优先级 > 配置文件超时
	}
	if ng.conf.Middlewares.Recover {
		chn = chn.Append(handler.RecoverHandler) // panic捕获， 返回客户端异常
	}
	if ng.conf.Middlewares.Metrics {
		chn = chn.Append(handler.MetricHandler(metrics)) // 记录log stat
	}
	if ng.conf.Middlewares.MaxBytes {
		chn = chn.Append(handler.MaxBytesHandler(ng.checkedMaxBytes(fr.maxBytes))) // http content-length 限制
	}
	if ng.conf.Middlewares.Gunzip {
		chn = chn.Append(handler.GunzipHandler) // 压缩请求body
	}

	return chn
}

func (ng *engine) checkedMaxBytes(bytes int64) int64 {
	if bytes > 0 {
		return bytes
	}

	return ng.conf.MaxBytes
}

func (ng *engine) checkedTimeout(timeout time.Duration) time.Duration {
	if timeout > 0 {
		return timeout
	}

	return time.Duration(ng.conf.Timeout) * time.Millisecond
}

func (ng *engine) createMetrics() *stat.Metrics {
	var metrics *stat.Metrics

	if len(ng.conf.Name) > 0 {
		metrics = stat.NewMetrics(ng.conf.Name)
	} else {
		metrics = stat.NewMetrics(fmt.Sprintf("%s:%d", ng.conf.Host, ng.conf.Port))
	}

	return metrics
}

func (ng *engine) getLogHandler() func(http.Handler) http.Handler {
	if ng.conf.Verbose {
		return handler.DetailedLogHandler
	}

	return handler.LogHandler
}

func (ng *engine) getShedder(priority bool) load.Shedder {
	if priority && ng.priorityShedder != nil {
		return ng.priorityShedder
	}

	return ng.shedder
}

// notFoundHandler returns a middleware that handles 404 not found requests.
func (ng *engine) notFoundHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		chn := chain.New(
			handler.TraceHandler(ng.conf.Name,
				"",
				handler.WithTraceIgnorePaths(ng.conf.TraceIgnorePaths)),
			ng.getLogHandler(),
		)

		var h http.Handler
		if next != nil {
			h = chn.Then(next)
		} else {
			h = chn.Then(http.NotFoundHandler())
		}

		cw := response.NewHeaderOnceResponseWriter(w)
		h.ServeHTTP(cw, r)
		cw.WriteHeader(http.StatusNotFound)
	})
}

func (ng *engine) print() {
	var routes []string

	for _, fr := range ng.routes {
		for _, route := range fr.routes {
			routes = append(routes, fmt.Sprintf("%s %s", route.Method, route.Path))
		}
	}

	sort.Strings(routes)

	fmt.Println("Routes:")
	for _, route := range routes {
		fmt.Printf("  %s\n", route)
	}
}

func (ng *engine) setTlsConfig(cfg *tls.Config) {
	ng.tlsConfig = cfg
}

func (ng *engine) setUnauthorizedCallback(callback handler.UnauthorizedCallback) {
	ng.unauthorizedCallback = callback
}

func (ng *engine) setUnsignedCallback(callback handler.UnsignedCallback) {
	ng.unsignedCallback = callback
}

func (ng *engine) signatureVerifier(signature signatureSetting) (func(chain.Chain) chain.Chain, error) {
	if !signature.enabled {
		return func(chn chain.Chain) chain.Chain {
			return chn
		}, nil
	}

	if len(signature.PrivateKeys) == 0 {
		if signature.Strict {
			return nil, ErrSignatureConfig
		}

		return func(chn chain.Chain) chain.Chain {
			return chn
		}, nil
	}

	decrypters := make(map[string]codec.RsaDecrypter)
	for _, key := range signature.PrivateKeys {
		fingerprint := key.Fingerprint
		file := key.KeyFile
		decrypter, err := codec.NewRsaDecrypter(file)
		if err != nil {
			return nil, err
		}

		decrypters[fingerprint] = decrypter
	}

	return func(chn chain.Chain) chain.Chain {
		if ng.unsignedCallback == nil {
			return chn.Append(handler.LimitContentSecurityHandler(ng.conf.MaxBytes,
				decrypters, signature.Expiry, signature.Strict))
		}

		return chn.Append(handler.LimitContentSecurityHandler(ng.conf.MaxBytes,
			decrypters, signature.Expiry, signature.Strict, ng.unsignedCallback))
	}, nil
}

// 启动http server
func (ng *engine) start(router httpx.Router, opts ...StartOption) error {
	// 绑定路由以及中间件到router， router会传给http.Server在启动http server后，
	// http.Server被http conn链接直接持有， 在收到请求后， router作为http request的handler 回调处理http请求
	if err := ng.bindRoutes(router); err != nil {
		return err
	}

	// make sure user defined options overwrite default options
	opts = append([]StartOption{ng.withTimeout()}, opts...)
	// 无ssl证书，启动http服务
	if len(ng.conf.CertFile) == 0 && len(ng.conf.KeyFile) == 0 {
		return internal.StartHttp(ng.conf.Host, ng.conf.Port, router, opts...)
	}

	// make sure user defined options overwrite default options
	opts = append([]StartOption{
		func(svr *http.Server) {
			if ng.tlsConfig != nil {
				svr.TLSConfig = ng.tlsConfig
			}
		},
	}, opts...)
	// 根据配置的ssl证书，监听配置文件中的地址端口， 启动http服务
	return internal.StartHttps(ng.conf.Host, ng.conf.Port, ng.conf.CertFile,
		ng.conf.KeyFile, router, opts...)
}

// 应用层添加自定义中间价
func (ng *engine) use(middleware Middleware) {
	ng.middlewares = append(ng.middlewares, middleware)
}

func (ng *engine) withTimeout() internal.StartOption {
	return func(svr *http.Server) {
		timeout := ng.timeout
		if timeout > 0 {
			// factor 0.8, to avoid clients send longer content-length than the actual content,
			// without this timeout setting, the server will time out and respond 503 Service Unavailable,
			// which triggers the circuit breaker.
			svr.ReadTimeout = 4 * timeout / 5
			// factor 1.1, to avoid servers don't have enough time to write responses.
			// setting the factor less than 1.0 may lead clients not receiving the responses.
			svr.WriteTimeout = 11 * timeout / 10
		}
	}
}

func convertMiddleware(ware Middleware) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return ware(next.ServeHTTP)
	}
}
