package http

import (
	"fmt"
	"github.com/gin-gonic/gin"
	cmodel "github.com/open-falcon/common/model"
	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/index"
	"log"
	"strconv"
	"strings"
)

func configIndexRoutes() {
	//清空indexedItemCache，以便重新写入counter到消息队列中
	router.POST("/api/v2/index/rebuild", func(c *gin.Context) {
		index.ClearIndexedCache()
		c.JSON(200, gin.H{"msg": "ok"})
	})

	router.POST("/api/v2/index/build-item", func(c *gin.Context) {
		endpoint := c.PostForm("endpoint")
		metric := c.PostForm("metric")
		step := c.PostForm("step")
		dstype := c.PostForm("dstype")
		tag_str := c.DefaultPostForm("tags", "")

		if endpoint == "" || metric == "" || dstype == "" {
			c.AbortWithError(500, fmt.Errorf("missing_params"))
		}

		step_i, err := strconv.Atoi(step)
		if err != nil {
			c.AbortWithError(500, err)
		}

		tag_str = strings.Replace(tag_str, " ", "", -1)
		tagVals := strings.Split(tag_str, ",")
		tags := make(map[string]string)
		for _, tag := range tagVals {
			tagPairs := strings.Split(tag, "=")
			if len(tagPairs) != 2 {
				c.AbortWithError(500, fmt.Errorf("wrong_tags_fmt"))
			} else {
				tags[tagPairs[0]] = tagPairs[1]
			}
		}

		if g.Config().Debug {
			log.Printf("build index manually, endpoint:%v, metric:%v, tags:%v, dstype:%v, step:%v\n", endpoint, metric, tags, dstype, step)
		}

		graph_item := &cmodel.GraphItem{
			Endpoint: endpoint,
			Metric:   metric,
			DsType:   dstype,
			Step:     step_i,
			Tags:     tags,
		}

		index.AddItemToUnindexedCache(graph_item)
		c.JSON(200, gin.H{"msg": "ok"})
	})
}
