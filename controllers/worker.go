package controllers

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os/exec"
	"strconv"
	"streamingservice/models"
	"streamingservice/utils"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/shirou/gopsutil/v3/process"
)

func FindWorkers(c *gin.Context) {
	var workers []models.WorkerEntity
	var workersWithStatus []models.WorkerEntity
	loggedInUser, err := utils.GetCurrentlyLoggedinUser(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
	}
	if !loggedInUser.Admin {
		DB.Where("team_id=?", loggedInUser.TeamId).Or("team_id IS NULL").Find(&workers)
	} else {
		DB.Find(&workers)
	}

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
		} else {
			worker.ConnectStatus = "STOPPED"
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
func CreateWorker(c *gin.Context) {
	// Validate input
	loggedInUser, err := utils.GetCurrentlyLoggedinUser(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
	}

	var input models.WorkerEntity
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if loggedInUser.Admin {
		input.TeamId = loggedInUser.TeamId
	}
	DB.Create(&input)

	c.JSON(http.StatusOK, gin.H{"data": input})

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
	type respStr struct {
		Worker     models.WorkerEntity `json:"worker"`
		ErrorLog   string              `json:"errorLog,omitempty"`
		Connectors []struct {
			Name   string `json:"name"`
			Status string `json:"status"`
		} `json:"connectors,omitempty"`
		WorkerProperties string `json:"WorkerProperties,omitempty"`
	}
	resp := respStr{}
	resp.Worker = *worker
	_, connectionError := http.Get(conf.KafkaEndpoint + "connectors/")

	if worker.Name == "localhost" && connectionError == nil {
		//scan for worker properties
		wpop, err := ioutil.ReadFile(worker.WorkerPath + "/bin/worker.properties")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no worker properties found"})
		}
		errorlog, err := ioutil.ReadFile(worker.WorkerPath + "/logs/connect.log")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no error log found"})
		}

		resp.ErrorLog = string(errorlog)
		resp.WorkerProperties = string(wpop)
		response, err := http.Get(conf.KafkaEndpoint + "connectors/")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		responseData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		type cnctResp struct {
			Connectors []string
		}
		type responseStatus struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
		}
		var responseConnectors cnctResp

		_ = json.Unmarshal(responseData, &responseConnectors.Connectors)
		if responseConnectors.Connectors != nil {
			for _, connector := range responseConnectors.Connectors {
				response, err := http.Get(conf.KafkaEndpoint + "connectors/" + connector + "/status")
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				}

				responseData, err := ioutil.ReadAll(response.Body)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				}

				var state responseStatus
				_ = json.Unmarshal(responseData, &state)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				}
				type connWithStatus struct {
					Name   string `json:"name"`
					Status string `json:"status"`
				}
				var newConn connWithStatus
				newConn.Name = connector
				newConn.Status = state.Connector.State
				resp.Connectors = append(resp.Connectors, newConn)

			}

		}
	} else {
		c.JSON(http.StatusOK, gin.H{"data": resp})
	}
	c.JSON(http.StatusOK, gin.H{"data": resp})
}
