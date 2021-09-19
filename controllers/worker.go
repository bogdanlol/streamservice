package controllers

import (
	"net/http"
	"os/exec"
	"strconv"
	"streamingservice/models"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/process"
)

func FindWorkers(c *gin.Context) {
	var workers []models.WorkerEntity
	var workersWithStatus []models.WorkerEntity
	DB.Find(&workers)

	for _, worker := range workers {

		if worker.Name == "localhost" {
			worker.ConnectStatus = "STOPPED"
			processes, _ := process.Processes()
			for _, p := range processes {
				Slc, _ := p.CmdlineSlice()
				for _, s := range Slc {
					if strings.Contains(s, "connect-distributed") {
						worker.ConnectStatus = "RUNNING"

					}
				}

			}
			workersWithStatus = append(workersWithStatus, worker)
		}
	}

	// for now only works on localhost
	if workersWithStatus != nil {
		c.JSON(http.StatusOK, gin.H{"data": workersWithStatus})
	} else {
		c.JSON(http.StatusOK, gin.H{"data": workers})
	}

}
func StopKafkaConnect(c *gin.Context) {

	_, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}

	processes, _ := process.Processes()
	for _, p := range processes {
		Slc, _ := p.CmdlineSlice()
		for _, s := range Slc {
			if strings.Contains(s, "connect-distributed") {
				p.KillWithContext(c)
			}
		}

	}
	c.JSON(http.StatusOK, gin.H{"data": "worker stopped successfully"})
}
func StartKafkaConnect(c *gin.Context) {
	StrId, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Id is needed "})
		return
	}
	var worker models.WorkerEntity
	id, err := strconv.Atoi(StrId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Id is not int "})
		return
	}

	if err := DB.First(&worker, id).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such worker"})
		return
	}

	cmd := exec.Command(worker.WorkerPath+"/bin/connect-distributed", worker.WorkerPath+"/etc/kafka/connect-distributed.properties")
	e := cmd.Start()
	if e != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "couldn't start worker"})
	}

	c.JSON(http.StatusOK, gin.H{"data": "worker started successfully"})
}
func FindWorker(c *gin.Context) {
	StrId, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Id is needed "})
		return
	}
	worker := &models.WorkerEntity{}
	id, err := strconv.Atoi(StrId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Id is not int "})
		return
	}

	if err := DB.First(&worker, id).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": worker})

}
