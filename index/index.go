package index

import (
	"fmt"
	"log"

	cmodel "github.com/open-falcon/common/model"
)

// 初始化索引功能模块
func Start() {
	InitCache()
	go StartIndexUpdateIncrTask()
	log.Println("index.Start ok")
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

// DELETE?
func GetIndexedItemCache(endpoint string, metric string, tags map[string]string, dstype string, step int) (r *cmodel.GraphItem, rerr error) {
	itemDemo := &cmodel.GraphItem{
		Endpoint: endpoint,
		Metric:   metric,
		Tags:     tags,
		DsType:   dstype,
		Step:     step,
	}
	md5 := itemDemo.Checksum()
	uuid := itemDemo.UUID()

	cached := indexedItemCache.Get(md5)
	if cached == nil {
		rerr = fmt.Errorf("not found")
		return
	}

	icitem := cached.(*IndexCacheItem)
	if icitem.UUID != uuid {
		rerr = fmt.Errorf("counter found, uuid not found: bad step or type")
		return
	}

	r = icitem.Item
	return
}
