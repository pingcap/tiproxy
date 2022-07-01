package api

import (
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func registerMetrics(group *gin.RouterGroup) {
	group.GET("/", gin.WrapF(promhttp.Handler().ServeHTTP))
}
