package controllers

import (
	"net/http"
	"streamingservice/models"

	"github.com/gin-gonic/gin"
)

func CreateTeam(c *gin.Context) {
	// Validate input
	var input models.TeamEntity
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	DB.Create(&input)

	c.JSON(http.StatusOK, gin.H{"data": input})

}

func FindTeams(c *gin.Context) {
	var teams []models.TeamEntity

	if err := DB.Find(&teams).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": teams})

}
func FindTeam(c *gin.Context) {
	var team *models.TeamEntity
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such team"})
	}

	if err := DB.First(&team, id).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": team})

}
