package index

import (
	"log"

	"fmt"
	cmodel "github.com/open-falcon/common/model"
	"github.com/open-falcon/graph/g"
	"github.com/patrickmn/go-cache"
	"strconv"
	"strings"
	"time"
)

var counterPropsCache *cache.Cache = cache.New(5*time.Minute, 30*time.Second)

// 初始化索引功能模块
func Start() {
	InitCache()
	go startIndexUpdater()
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
		//TODO:add api to reset this queue for rebuild
		return
	} else {
		unIndexedItemCache.Put(uuid, NewIndexCacheItem(uuid, item))
	}
}

func GetTypeAndStep(endpoint string, counter string) (dsType string, step int, err error) {
	pk := fmt.Sprintf("%s/%s", endpoint, counter)

	v, found := counterPropsCache.Get(pk)
	if g.Config().Debug {
		log.Printf("get_type_and_step_from_cache, pk:%s, v:%v, found:%v\n", pk, v, found)
	}
	if found {
		fields := strings.SplitN(v.(string), ":", 2)
		dsType = fields[0]
		step, err = strconv.Atoi(fields[1])
		return dsType, step, err
	}

	graph_item, err := getItemFromKVStore(pk)
	if g.Config().Debug {
		log.Printf("get_type_and_step_from_kvstore, pk:%s, v:%v, error:%v\n", pk, graph_item, err)
	}
	if err != nil {
		return "", 0, err
	}

	dsType = graph_item.DsType
	step = graph_item.Step

	counterPropsCache.Set(pk, fmt.Sprintf("%s:%d", dsType, step), cache.DefaultExpiration)

	return dsType, step, nil
}

func ClearIndexedCache() {
	indexedItemCache.Reset()
}

func AddItemToUnindexedCache(item *cmodel.GraphItem) {
	uuid := item.UUID()
	unIndexedItemCache.Put(uuid, NewIndexCacheItem(uuid, item))
}
