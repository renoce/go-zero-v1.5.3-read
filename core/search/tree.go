package search

import (
	"errors"
	"fmt"
)

const (
	colon = ':'
	slash = '/'
)

var (
	// errDupItem means adding duplicated item.
	errDupItem = errors.New("duplicated item")
	// errDupSlash means item is started with more than one slash.
	errDupSlash = errors.New("duplicated slash")
	// errEmptyItem means adding empty item.
	errEmptyItem = errors.New("empty item")
	// errInvalidState means search tree is in an invalid state.
	errInvalidState = errors.New("search tree is in an invalid state")
	// errNotFromRoot means path is not starting with slash.
	errNotFromRoot = errors.New("path should start with /")

	// NotFound is used to hold the not found result.
	NotFound Result
)

type (
	innerResult struct {
		key   string
		value string
		named bool
		found bool
	}
	// map中/分割的每一段路径作为map的索引， node作为索引值。 所有的分段路径和node 都存储在一个map（动态/静态各一个map）中。
	// /分割的字符串路径， 左边的是右边的上一级， 具体表现为， 上一级的node会存储下一级的node。
	// 这样， 路由通过/分割的路径字符串作为索引， 就形成了一个路由树， 每一级的分段路径是树的查询索引，分支节点不存储最终的item数据， 只保存下一级的节点
	// 最终的节点， 叶子节点会保存item数据， 也就是handler 调用链
	node struct {
		item     any                 // 中间件以及应用handler构成的调用链
		children [2]map[string]*node // map 0 ， 不包含:, 没有动态参数 map 1: 包含:字符， 表示会有动态参数
	}

	// A Tree is a search tree.
	Tree struct {
		root *node
	}

	// A Result is a search result from tree.
	Result struct {
		Item   any
		Params map[string]string
	}
)

// NewTree returns a Tree.
func NewTree() *Tree {
	return &Tree{
		root: newNode(nil),
	}
}

// Add adds item to associate with route.
func (t *Tree) Add(route string, item any) error {
	if len(route) == 0 || route[0] != slash {
		return errNotFromRoot
	}

	if item == nil {
		return errEmptyItem
	}

	err := add(t.root, route[1:], item)
	switch err {
	case errDupItem:
		return duplicatedItem(route)
	case errDupSlash:
		return duplicatedSlash(route)
	default:
		return err
	}
}

// Search searches item that associates with given route.
func (t *Tree) Search(route string) (Result, bool) {
	if len(route) == 0 || route[0] != slash {
		return NotFound, false
	}
	// result 存储请求路径中动态参数
	var result Result
	ok := t.next(t.root, route[1:], &result)
	return result, ok
}

func (t *Tree) next(n *node, route string, result *Result) bool {
	if len(route) == 0 && n.item != nil {
		result.Item = n.item
		return true
	}

	for i := range route {
		if route[i] != slash {
			continue
		}

		token := route[:i]
		return n.forEach(func(k string, v *node) bool {
			r := match(k, token)
			if !r.found || !t.next(v, route[i+1:], result) {
				return false
			}
			// 包含动态参数， 将动态参数保存到map， 返回给上层
			if r.named {
				addParam(result, r.key, r.value)
			}

			return true
		})
	}

	return n.forEach(func(k string, v *node) bool {
		if r := match(k, route); r.found && v.item != nil {
			// 将叶子节点的handler调用链给到result， 以便于回调
			result.Item = v.item
			if r.named {
				addParam(result, r.key, r.value)
			}

			return true
		}

		return false
	})
}

func (nd *node) forEach(fn func(string, *node) bool) bool {
	// 遍历两个不同map, 动态/静态map
	for _, children := range nd.children {
		for k, v := range children {
			if fn(k, v) {
				return true
			}
		}
	}

	return false
}

// 如果路径中首字符是:, 返回children map 1， 否则返回 map 0， : 标记路由路径中的会传递动态参数
func (nd *node) getChildren(route string) map[string]*node {
	// 返回动态路径map
	if len(route) > 0 && route[0] == colon {
		return nd.children[1]
	}
	// 返回静态路径map
	return nd.children[0]
}

// 将路由路径和handler加入到node节点
// 递归调用，递归传递/分割的路径
func add(nd *node, route string, item any) error {
	if len(route) == 0 {
		if nd.item != nil {
			return errDupItem
		}

		nd.item = item
		return nil
	}

	if route[0] == slash {
		return errDupSlash
	}
	// 如果是不包含/的路径字符串(递归调用， / 分割的最后一个路径字符串)， 会越过for， 在for下面执行
	for i := range route {
		// route字符串中包含/,才会继续， 否则continue直到跳出
		if route[i] != slash {
			continue
		}
		// 截取/分割左边的一段路径
		token := route[:i]
		// 根据token中是否包含:,获取node节点中children slice中的静态/动态路径 map
		children := nd.getChildren(token)
		// 如果token路径存在添加到map
		if child, ok := children[token]; ok {
			// child node存在， 继续将/右边的路径传递给 add， 直到有新的路径会跳过这个if， 在下面创建的新的node节点
			if child != nil {
				return add(child, route[i+1:], item)
			}

			return errInvalidState
		}
		// 新的token路径， 构建新节点node， 将path， node 添加到map
		child := newNode(nil) // 分支节点不会保存item数据， 只有叶子节点才会保存handler
		children[token] = child
		// 继续添加/分割的下一级路由
		return add(child, route[i+1:], item)
	}
	// 这里一般是执行/分割的最后的一段route，最后的map 节点， 也就是叶子节点会保存handler
	children := nd.getChildren(route)
	if child, ok := children[route]; ok {
		if child.item != nil {
			return errDupItem
		}

		child.item = item
	} else {
		children[route] = newNode(item)
	}

	return nil
}

func addParam(result *Result, k, v string) {
	if result.Params == nil {
		result.Params = make(map[string]string)
	}

	result.Params[k] = v
}

func duplicatedItem(item string) error {
	return fmt.Errorf("duplicated item for %s", item)
}

func duplicatedSlash(item string) error {
	return fmt.Errorf("duplicated slash for %s", item)
}

func match(pat, token string) innerResult {
	// 包含动态参数
	if pat[0] == colon {
		return innerResult{
			key:   pat[1:],
			value: token,
			named: true,
			found: true,
		}
	}
	// 不包含动态参数
	return innerResult{
		found: pat == token,
	}
}

func newNode(item any) *node {
	return &node{
		item: item,
		children: [2]map[string]*node{
			make(map[string]*node),
			make(map[string]*node),
		},
	}
}
