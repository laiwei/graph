package index

import (
	"fmt"
	"sync"
	"time"

	tcache "github.com/toolkits/cache/localcache/timedcache"

	cmodel "github.com/open-falcon/common/model"
	cutils "github.com/open-falcon/common/utils"
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

// db本地缓存
var (
	// endpoint表的内存缓存, key:endpoint(string) / value:id(int64)
	dbEndpointCache = tcache.New(600*time.Second, 60*time.Second)
	// endpoint_counter表的内存缓存, key:endpoint_id-counter(string) / val:dstype-step(string)
	dbEndpointCounterCache = tcache.New(600*time.Second, 60*time.Second)
)

// 初始化cache
func InitCache() {
	go startCacheProcUpdateTask()
}

// indexedItemCache 不能随便清空
// USED WHEN QUERY
func GetTypeAndStep(endpoint string, counter string) (dsType string, step int, found bool) {
	// get it from index cache
	pk := cutils.Md5(fmt.Sprintf("%s/%s", endpoint, counter))
	if icitem := indexedItemCache.Get(pk); icitem != nil {
		if item := icitem.(*IndexCacheItem).Item; item != nil {
			dsType = item.DsType
			step = item.Step
			found = true
			return
		}
	}

	// statistics
	proc.GraphLoadDbCnt.Incr()

	// get it from db, this should rarely happen
	// TODO

	// do not find it, this must be a bad request
	found = false
	return
}

// 更新 cache的统计信息
func startCacheProcUpdateTask() {
	for {
		time.Sleep(DefaultCacheProcUpdateTaskSleepInterval)
		proc.IndexedItemCacheCnt.SetCnt(int64(indexedItemCache.Size()))
		proc.UnIndexedItemCacheCnt.SetCnt(int64(unIndexedItemCache.Size()))
		proc.EndpointCacheCnt.SetCnt(int64(dbEndpointCache.Size()))
		proc.CounterCacheCnt.SetCnt(int64(dbEndpointCounterCache.Size()))
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
