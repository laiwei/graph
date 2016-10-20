package index

import (
	"encoding/json"
	ntime "github.com/toolkits/time"
	"log"
	"time"

	"github.com/open-falcon/graph/g"
	proc "github.com/open-falcon/graph/proc"
)

const (
	IndexUpdateIncrTaskSleepInterval = time.Duration(1) * time.Second
)

// 启动索引的 异步、增量更新 任务
func StartIndexUpdateIncrTask() {
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
func publishToQueue() int {
	ret := 0
	if unIndexedItemCache == nil || unIndexedItemCache.Size() <= 0 {
		return ret
	}

	var send_buff [][]byte
	cnt := 0
	sz := 100

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
		send_buff = append(send_buff, []byte(json_data))
		cnt++
		ret++

		if cnt >= sz {
			err = g.MQWriter.MultiPublish("metric_index", send_buff)
			if err != nil {
				log.Printf("publish msg error:%v\n", err)
			}
			cnt = 0
			send_buff = [][]byte{}
		}
	}

	return ret
}
