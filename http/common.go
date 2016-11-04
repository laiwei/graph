package http

import (
	"github.com/toolkits/file"

	"github.com/gin-gonic/gin"
	"github.com/open-falcon/graph/g"
	"github.com/open-falcon/graph/store"
	"time"
)

func configCommonRoutes() {

	router.GET("/api/v2/health", func(c *gin.Context) {
		c.JSON(200, gin.H{"msg": "ok"})
	})

	router.GET("/api/v2/version", func(c *gin.Context) {
		c.JSON(200, gin.H{"value": g.VERSION})
	})

	router.GET("/api/v2/workdir", func(c *gin.Context) {
		c.JSON(200, gin.H{"value": file.SelfDir()})
	})

	router.GET("/api/v2/config", func(c *gin.Context) {
		c.JSON(200, gin.H{"value": g.Config()})
	})

	router.POST("/api/v2/config/reload", func(c *gin.Context) {
		g.ParseConfig(g.ConfigFile)
		c.JSON(200, gin.H{"msg": "ok"})
	})

	router.GET("/api/v2/stats/graph-queue-size", func(c *gin.Context) {
		rt := make(map[int]int)
		for i := 0; i < store.GraphItems.Size; i++ {
			keys := store.GraphItems.KeysByIndex(i)
			oneHourAgo := time.Now().Unix() - 3600

			count := 0
			for _, ckey := range keys {
				item := store.GraphItems.First(ckey)
				if item == nil {
					continue
				}

				if item.Timestamp > oneHourAgo {
					count++
				}
			}
			rt[i] = count
		}
		c.JSON(200, gin.H{"value": rt})
	})
}
