package index

import (
	"log"

	cmodel "github.com/open-falcon/common/model"
)

// 初始化索引功能模块
func Start() {
	InitCache()
	go StartIndexUpdater()
	log.Println("indexUpdater.Start ok")
}

// index收到一条新上报的监控数据,尝试用于更新索引
func BuildIndexQueues(item *cmodel.GraphItem) {
	if item == nil {
		return
	}

	uuid := item.UUID()

	// 已上报过的数据
	if indexedItemCache.ContainsKey(uuid) {
		return
	} else {
		// 缓存未命中, 放入本地增量更新队列
		unIndexedItemCache.Put(uuid, NewIndexCacheItem(uuid, item))
	}
}
