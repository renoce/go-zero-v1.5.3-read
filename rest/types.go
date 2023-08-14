package rest

import (
	"net/http"
	"time"
)

type (
	// Middleware defines the middleware method.
	Middleware func(next http.HandlerFunc) http.HandlerFunc

	// A Route is a http route.
	Route struct {
		Method  string
		Path    string
		Handler http.HandlerFunc
	}

	// RouteOption defines the method to customize a featured route.
	RouteOption func(r *featuredRoutes)

	jwtSetting struct {
		enabled    bool
		secret     string
		prevSecret string
	}

	signatureSetting struct {
		SignatureConf
		enabled bool
	}
	// 保存应用层添加的一组路由数据， 包括： method， path， handler。 以及这一组路由中的特有的鉴权， 超时， content-length等
	// 这里的参数区分于配置文件， 配置文件的参数是共有的,  这里的参数是这一组路由特有的。
	featuredRoutes struct {
		timeout   time.Duration
		priority  bool
		jwt       jwtSetting
		signature signatureSetting
		routes    []Route
		maxBytes  int64
	}
)
