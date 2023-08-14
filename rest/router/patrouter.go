package router

import (
	"errors"
	"net/http"
	"path"
	"strings"

	"github.com/zeromicro/go-zero/core/search"
	"github.com/zeromicro/go-zero/rest/httpx"
	"github.com/zeromicro/go-zero/rest/pathvar"
)

const (
	allowHeader          = "Allow"
	allowMethodSeparator = ", "
)

var (
	// ErrInvalidMethod is an error that indicates not a valid http method.
	ErrInvalidMethod = errors.New("not a valid http method")
	// ErrInvalidPath is an error that indicates path is not start with /.
	ErrInvalidPath = errors.New("path must begin with '/'")
)

// 实现了http.Router， 会传给http.Server
// http链接最终会调用这个结构体的ServeHTTP函数
type patRouter struct {
	trees      map[string]*search.Tree // method作为索引，每一个method索引对应一个不同的路由树
	notFound   http.Handler
	notAllowed http.Handler
}

// NewRouter returns a httpx.Router.
func NewRouter() httpx.Router {
	return &patRouter{
		trees: make(map[string]*search.Tree),
	}
}

// 构建路由搜索树
func (pr *patRouter) Handle(method, reqPath string, handler http.Handler) error {
	if !validMethod(method) {
		return ErrInvalidMethod
	}

	if len(reqPath) == 0 || reqPath[0] != '/' {
		return ErrInvalidPath
	}
	// 清除路由路径中的多余字符， 返回一个最短路由
	cleanPath := path.Clean(reqPath)
	// 获取一个方法对应的路有树， 对应方法的路由树存在的话，即将相应的请求路径和中间件/应用层路由handler调用链保存到树中
	tree, ok := pr.trees[method]
	if ok {
		return tree.Add(cleanPath, handler)
	}
	// 之前method树不存在的话， 构建一个新的method树
	tree = search.NewTree()
	// 将新的method树添加到patroute map中
	pr.trees[method] = tree
	return tree.Add(cleanPath, handler)
}

// golang 标准库中， http链接在接收数据后回调的处理函数
func (pr *patRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	reqPath := path.Clean(r.URL.Path)
	// 通过路由搜索树查找对应路由
	if tree, ok := pr.trees[r.Method]; ok {
		if result, ok := tree.Search(reqPath); ok {
			// 存在动态参数， 将动态参数存储到 r request中的context中， 以便应用层获取使用
			if len(result.Params) > 0 {
				r = pathvar.WithVars(r, result.Params)
			}
			// 调用查询到的hander中间件调用链， 最终调用到应用层路由handler
			result.Item.(http.Handler).ServeHTTP(w, r)
			return
		}
	}
	// 是否有匹配的路径， 如果没有就是404
	allows, ok := pr.methodsAllowed(r.Method, reqPath)
	if !ok {
		pr.handleNotFound(w, r)
		return
	}
	// 是否是允许的请求， 比如：跨域的一些接口请求
	if pr.notAllowed != nil {
		pr.notAllowed.ServeHTTP(w, r)
	} else {
		w.Header().Set(allowHeader, allows)
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (pr *patRouter) SetNotFoundHandler(handler http.Handler) {
	pr.notFound = handler
}

func (pr *patRouter) SetNotAllowedHandler(handler http.Handler) {
	pr.notAllowed = handler
}

// 如果没有自定义notfound， 即使用默认notfound 404
func (pr *patRouter) handleNotFound(w http.ResponseWriter, r *http.Request) {
	if pr.notFound != nil {
		pr.notFound.ServeHTTP(w, r)
	} else {
		http.NotFound(w, r)
	}
}

// 根据路径查找方法， 是否有匹配的path
func (pr *patRouter) methodsAllowed(method, path string) (string, bool) {
	var allows []string

	for treeMethod, tree := range pr.trees {
		if treeMethod == method {
			continue
		}

		_, ok := tree.Search(path)
		if ok {
			allows = append(allows, treeMethod)
		}
	}

	if len(allows) > 0 {
		return strings.Join(allows, allowMethodSeparator), true
	}

	return "", false
}

// 匹配常用rest方法
func validMethod(method string) bool {
	return method == http.MethodDelete || method == http.MethodGet ||
		method == http.MethodHead || method == http.MethodOptions ||
		method == http.MethodPatch || method == http.MethodPost ||
		method == http.MethodPut
}
