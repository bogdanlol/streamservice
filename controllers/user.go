package controllers

import (
	"net/http"
	"streamingservice/models"
	"streamingservice/utils"

	"github.com/gin-gonic/gin"
)

func IsAdmin(c *gin.Context) {
	loggedInUser, err := utils.GetCurrentlyLoggedinUser(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
	}

	c.JSON(http.StatusOK, gin.H{"admin": loggedInUser.Admin})

}
func CreateUser(c *gin.Context) {
	// Validate input
	var input models.UserEntity
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	pass, err := input.HashPassword(input.Password)
	if err != nil {
		return
	}
	input.Password = pass
	DB.Create(&input)

	c.JSON(http.StatusOK, gin.H{"data": input})

}
