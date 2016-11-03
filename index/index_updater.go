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
	indexUpdaterSleepInterval = time.Duration(10) * time.Second
)

// 启动索引的 异步、增量更新 任务
func startIndexUpdater() {
	for {
		time.Sleep(indexUpdaterSleepInterval)
		startTs := time.Now().Unix()
		consumeItemsOnce()
		endTs := time.Now().Unix()

		// statistics
		proc.IndexUpdateIncr.Incr()
		proc.IndexUpdateIncr.PutOther("lastStartTs", ntime.FormatTs(startTs))
		proc.IndexUpdateIncr.PutOther("lastTimeConsumingInSec", endTs-startTs)
	}
}

func consumeItemsOnce() error {
	if unIndexedItemCache == nil || unIndexedItemCache.Size() <= 0 {
		return nil
	}

	keys := unIndexedItemCache.Keys()
	for _, key := range keys {
		icitem := unIndexedItemCache.Get(key)
		unIndexedItemCache.Remove(key)

		if icitem == nil {
			continue
		}

		graph_item := icitem.(*IndexCacheItem).Item
		err := publishToQueue(graph_item)
		if err != nil {
			log.Printf("publish to queue error:%s\n", err.Error())
			uuid := graph_item.UUID()
			unIndexedItemCache.Put(uuid, NewIndexCacheItem(uuid, graph_item))
			continue
		}

		err = setItemToKVStore(graph_item)
		if err != nil {
			log.Printf("set to local kvstore error:%s\n", err.Error())
			uuid := graph_item.UUID()
			unIndexedItemCache.Put(uuid, NewIndexCacheItem(uuid, graph_item))
		}
	}

	return nil
}

// TODO:测试queue writer的连接断开时的表现，测试非批量publish消息的性能
func publishToQueue(item *cmodel.GraphItem) error {
	json_item, err := json.Marshal(item)
	if err != nil {
		return err
	}
	err = g.MQWriter.Publish("metric_index", json_item)
	return err
}

func setItemToKVStore(item *cmodel.GraphItem) error {
	pk := item.PrimaryKey()
	json_item, err := json.Marshal(item)
	if err != nil {
		return err
	}

	err = g.KVDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("items"))
		b.Put([]byte(pk), json_item)
		return nil
	})

	return err
}

func getItemFromKVStore(pk string) (*cmodel.GraphItem, error) {
	item := cmodel.GraphItem{}
	err := g.KVDB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("items"))
		d := b.Get([]byte(pk))
		if err := json.Unmarshal(d, &item); err != nil {
			return err
		}
		return nil
	})

	return &item, err
}
