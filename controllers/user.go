package controllers

import (
	"net/http"
	"strconv"
	"streamingservice/models"
	"streamingservice/utils"

	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
)

func GetCurrentUser(c *gin.Context) {
	loggedInUser, err := utils.GetCurrentlyLoggedinUser(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
	}
	var TO models.TeamOwnerEntity
	teamOwner := false
	if err := DB.First(&TO, "user_id=?", loggedInUser.ID).Error; err != gorm.ErrRecordNotFound {
		teamOwner = true
	}
	var team models.TeamEntity
	DB.First(&team, "id=?", loggedInUser.TeamId)
	c.JSON(http.StatusOK, gin.H{"user": loggedInUser, "teamOwner": teamOwner, "team": team})

}
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
func FindUsersByTeam(c *gin.Context) {
	var users []models.UserEntity
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such user"})
	}
	if err := DB.Find(&users, "team_id=?", id).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": users})

}
func FindUsers(c *gin.Context) {
	var users []models.UserEntity

	if err := DB.Find(&users).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": users})

}
func FindUser(c *gin.Context) {
	var user models.UserEntity
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such user"})
	}

	if err := DB.First(&user, id).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": user})

}
func RemoveUser(c *gin.Context) {
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such user"})
	}
	DB.Delete(&models.UserEntity{}, id)
	c.JSON(http.StatusOK, gin.H{"data": "user has been deleted"})

}
func EditUser(c *gin.Context) {
	// Validate input
	StrId, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such user"})
	}
	var input models.UserEntity
	id, err := strconv.Atoi(StrId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such user"})
	}

	DB.First(&input, id)
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	DB.Save(&input)

	c.JSON(http.StatusOK, gin.H{"data": input})

}
