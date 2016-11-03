package index

import (
	"sync"
	"time"

	cmodel "github.com/open-falcon/common/model"
	"github.com/open-falcon/graph/proc"
)

const (
	DefaultMaxCacheSize                     = 5000000 // 默认 最多500w个,太大了内存会耗尽
	DefaultCacheProcUpdateTaskSleepInterval = time.Duration(1) * time.Second
)

// item缓存
var (
	indexedItemCache   = NewIndexCacheBase(DefaultMaxCacheSize)
	unIndexedItemCache = NewIndexCacheBase(DefaultMaxCacheSize)
)

// 初始化cache
func InitCache() {
	go startCacheProcUpdateTask()
}

// 更新 cache的统计信息
func startCacheProcUpdateTask() {
	for {
		time.Sleep(DefaultCacheProcUpdateTaskSleepInterval)
		proc.IndexedItemCacheCnt.SetCnt(int64(indexedItemCache.Size()))
		proc.UnIndexedItemCacheCnt.SetCnt(int64(unIndexedItemCache.Size()))
	}
}

// INDEX CACHE
// 索引缓存的元素数据结构
type IndexCacheItem struct {
	UUID string
	Item *cmodel.GraphItem
}

func NewIndexCacheItem(uuid string, item *cmodel.GraphItem) *IndexCacheItem {
	return &IndexCacheItem{UUID: uuid, Item: item}
}

// 索引缓存-基本缓存容器
type IndexCacheBase struct {
	sync.RWMutex
	maxSize int
	data    map[string]interface{}
}

func NewIndexCacheBase(max int) *IndexCacheBase {
	return &IndexCacheBase{maxSize: max, data: make(map[string]interface{})}
}

func (this *IndexCacheBase) GetMaxSize() int {
	return this.maxSize
}

func (this *IndexCacheBase) Put(key string, item interface{}) {
	this.Lock()
	defer this.Unlock()
	this.data[key] = item
}

func (this *IndexCacheBase) Remove(key string) {
	this.Lock()
	defer this.Unlock()
	delete(this.data, key)
}

func (this *IndexCacheBase) Get(key string) interface{} {
	this.RLock()
	defer this.RUnlock()
	return this.data[key]
}

func (this *IndexCacheBase) ContainsKey(key string) bool {
	this.RLock()
	defer this.RUnlock()
	return this.data[key] != nil
}

func (this *IndexCacheBase) Size() int {
	this.RLock()
	defer this.RUnlock()
	return len(this.data)
}

func (this *IndexCacheBase) Keys() []string {
	this.RLock()
	defer this.RUnlock()

	count := len(this.data)
	if count == 0 {
		return []string{}
	}

	keys := make([]string, 0, count)
	for key := range this.data {
		keys = append(keys, key)
	}

	return keys
}
