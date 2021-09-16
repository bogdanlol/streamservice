package controllers

import (
	"fmt"
	"net/http"
	"streamingservice/models"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/process"
)

func FindWorkers(c *gin.Context) {
	var workers []models.WorkerEntity

	DB.Find(&workers)

	c.JSON(http.StatusOK, gin.H{"data": workers})

}
func StopKafkaConnect(c *gin.Context) {

	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	fmt.Println(id)
	processes, _ := process.Processes()
	for _, p := range processes {
		Slc, _ := p.CmdlineSlice()
		for _, s := range Slc {
			if strings.Contains(s, "connect-distributed") {
				p.KillWithContext(c)
			}
		}

	}
}
func StartKafkaConnect(c *gin.Context) {

}
