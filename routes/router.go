package routes

import (
	"streamingservice/controllers"

	"github.com/gin-gonic/gin"
)

func CORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		c.Writer.Header().Set("Access-Control-Allow-Origin", "http://localhost:8080")
		c.Writer.Header().Set("Access-Control-Allow-Credentials", "true")
		c.Writer.Header().Set("Access-Control-Allow-Headers", "*")

		c.Writer.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, POST, PUT, DELETE, OPTIONS, PATCH")

		if c.Request.Method == "OPTIONS" {
			c.AbortWithStatus(204)
			return
		}

		c.Next()
	}
}

func New() *gin.Engine {

	router := gin.Default()
	// router.Use(cors.Default())
	router.Use(CORS())
	router.GET("/connectors", controllers.FindConnectors)
	router.POST("/connectors", controllers.CreateConnector)
	router.GET("/connector-classes", controllers.GetConnectorClasses)
	router.GET("/convertors", controllers.GetConvertors)
	router.POST("/connectors/start/:entityId", controllers.PostConnector)
	router.GET("/connectors/:entityId", controllers.FindConnector)
	router.PUT("/connectors/:entityId", controllers.EditConnector)
	router.DELETE("/connectors/:entityId", controllers.RemoveConnector)
	router.POST("/connectors/stop/:entityName", controllers.StopConnector)
	router.POST("/connectors-plugins/upload", controllers.UploadConnectorPlugin)
	router.PUT("/connectors-validate", controllers.ValidateConnector)
	return router
}
