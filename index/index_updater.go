package index

import (
	"encoding/json"
	"github.com/boltdb/bolt"
	ntime "github.com/toolkits/time"
	"log"
	"time"

	cmodel "github.com/open-falcon/common/model"
	"github.com/open-falcon/graph/g"
	proc "github.com/open-falcon/graph/proc"
)

const (
	IndexUpdateIncrTaskSleepInterval = time.Duration(1) * time.Second
)

// 启动索引的 异步、增量更新 任务
func StartIndexUpdater() {
	for {
		time.Sleep(IndexUpdateIncrTaskSleepInterval)
		startTs := time.Now().Unix()
		cnt := publishToQueue()
		endTs := time.Now().Unix()
		// statistics
		proc.IndexUpdateIncrCnt.SetCnt(int64(cnt))
		proc.IndexUpdateIncr.Incr()
		proc.IndexUpdateIncr.PutOther("lastStartTs", ntime.FormatTs(startTs))
		proc.IndexUpdateIncr.PutOther("lastTimeConsumingInSec", endTs-startTs)
	}
}

// 进行一次增量更新
// TODO:测试queue writer的连接断开时的表现，测试非批量publish消息的性能
func publishToQueue() int {
	ret := 0
	if unIndexedItemCache == nil || unIndexedItemCache.Size() <= 0 {
		return ret
	}

	keys := unIndexedItemCache.Keys()
	for _, key := range keys {
		icitem := unIndexedItemCache.Get(key)
		unIndexedItemCache.Remove(key)

		if icitem == nil {
			continue
		}

		graph_item := icitem.(*IndexCacheItem).Item
		json_data, err := json.Marshal(graph_item)
		if err != nil {
			log.Printf("marshal error:%v\n", err)
			continue
		}

		uuid := graph_item.UUID()
		log.Printf("write msg:%s to queue\n", json_data)
		err = g.MQWriter.Publish("metric_index", json_data)
		if err != nil {
			unIndexedItemCache.Put(uuid, NewIndexCacheItem(uuid, graph_item))
			log.Printf("publish msg error:%v\n", err)
		} else {
			indexedItemCache.Put(uuid, NewIndexCacheItem(uuid, graph_item))
			if err = setItemToKVStore(uuid, graph_item); err != nil {
				log.Printf("set item go kvstore fail:%s\n", err.Error())
			}
			ret++
		}
	}

	return ret
}

func setItemToKVStore(uuid string, item *cmodel.GraphItem) error {
	json_item, err := json.Marshal(item)
	if err != nil {
		return err
	}

	err = g.KVDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("items"))
		b.Put([]byte(uuid), json_item)
		return nil
	})

	if err != nil {
		return err
	} else {
		return nil
	}
}

func getItemFromKVStore(uuid string) (*cmodel.GraphItem, error) {
	item := cmodel.GraphItem{}
	err := g.KVDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("items"))
		d := b.Get([]byte(uuid))
		if err := json.Unmarshal(d, &item); err != nil {
			return err
		}
		return nil
	})

	if err != nil {
		return nil, err
	} else {
		return &item, nil
	}

}
