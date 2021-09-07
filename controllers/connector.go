package controllers

import (
	"net/http"
	"streamingservice/db"
	"streamingservice/models"

	"github.com/gin-gonic/gin"
)

var DB = db.New()

// GET CONNECTORS
func FindConnectors(c *gin.Context) {
	var connectors []models.ConnectorEntity
	DB.Find(&connectors)

	c.JSON(http.StatusOK, gin.H{"data": connectors})
}

//POST CONNECTORS
func CreateConnector(c *gin.Context) {
	// Validate input
	var input models.ConnectorEntity
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	DB.Create(&input)

	c.JSON(http.StatusOK, gin.H{"data": input})

}
