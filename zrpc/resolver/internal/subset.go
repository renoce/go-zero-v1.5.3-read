package internal

import "math/rand"

// sub默认值subsetSize：32， 最多支持32个grpc server， 可修改
func subset(set []string, sub int) []string {
	// 打乱顺序， 随机排序
	rand.Shuffle(len(set), func(i, j int) {
		set[i], set[j] = set[j], set[i]
	})
	if len(set) <= sub {
		return set
	}

	return set[:sub]
}
