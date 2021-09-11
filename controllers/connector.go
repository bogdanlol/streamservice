package controllers

import (
	"bytes"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"streamingservice/config"
	"streamingservice/db"
	"streamingservice/models"

	"github.com/gin-gonic/gin"
)

var DB = db.New()
var conf = config.NewConfig()

// GET CONNECTORS

func FindConnectors(c *gin.Context) {
	var connectors []models.ConnectorEntity
	var connectorsWithStatus []models.ConnectorEntity
	DB.Find(&connectors)
	var _, isKafkaConnectOpenErr = http.Get(conf.KafkaEndpoint)
	if connectors != nil && isKafkaConnectOpenErr == nil {
		type responseStatus struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
		}
		for _, connector := range connectors {
			response, err := http.Get(conf.KafkaEndpoint + "connectors/" + connector.Name + "/status")
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			}
			if response.StatusCode != 200 {
				continue
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

			connector.Status = state.Connector.State
			connectorsWithStatus = append(connectorsWithStatus, connector)
		}
	}

	if connectorsWithStatus != nil {
		c.JSON(http.StatusOK, gin.H{"data": connectorsWithStatus})
	} else {
		c.JSON(http.StatusOK, gin.H{"data": connectors})
	}

}
func FindConnector(c *gin.Context) {
	var connector *models.ConnectorEntity
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}

	if err := DB.First(&connector, id).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	c.JSON(http.StatusOK, gin.H{"data": connector})

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
func EditConnector(c *gin.Context) {
	// Validate input
	_, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	var input models.ConnectorEntity

	DB.First(&input)
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	DB.Save(&input)

	c.JSON(http.StatusOK, gin.H{"data": input})

}
func RemoveConnector(c *gin.Context) {
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	DB.Delete(&models.ConnectorEntity{}, id)
	c.JSON(http.StatusOK, gin.H{"data": "connector has been deleted"})

}

//get connector classes
func GetConnectorClasses(c *gin.Context) {
	type connectorClass struct {
		Class   string `json:"class"`
		Type    string `json:"type"`
		Version string `json:"version"`
	}
	var conn []connectorClass

	response, err := http.Get(conf.KafkaEndpoint + "connector-plugins")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	err = json.Unmarshal(responseData, &conn)
	if err != nil {
		c.AbortWithError(400, err)
	}
	c.JSON(http.StatusOK, gin.H{"data": conn})

}

func GetConvertors(c *gin.Context) {
	var convertors []string
	convertors = append(convertors, "io.confluent.connect.avro.AvroConverter")
	convertors = append(convertors, "io.confluent.connect.protobuf.ProtobufConverter")
	convertors = append(convertors, "org.apache.kafka.connect.storage.StringConverter")
	convertors = append(convertors, "org.apache.kafka.connect.json.JsonConverter")
	convertors = append(convertors, "io.confluent.connect.json.JsonSchemaConverter")
	convertors = append(convertors, "org.apache.kafka.connect.converters.ByteArrayConverter")
	c.JSON(http.StatusOK, gin.H{"data": convertors})
}

func PostConnector(c *gin.Context) {
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	var connector models.ConnectorEntity
	err := DB.First(&connector, id).Error
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	type Config struct {
		ConnectorClass string `json:"connector.class"`
		TasksMax       uint16 `json:"tasks.max"`
		KeyConverter   string `json:"key.converter,omitempty"`
		ValueConverter string `json:"value.converter,omitempty"`
		Topics         string `json:"topics"`
		File           string `json:"file,omitempty"`
	}
	type KafkaConnect struct {
		Name   string  `json:"name"`
		Config *Config `json:"config"`
	}
	configuration := Config{
		ConnectorClass: connector.ConnectorClass,
		TasksMax:       connector.TasksMax,
		KeyConverter:   connector.KeyConverter,
		ValueConverter: connector.ValueConverter,
		Topics:         connector.Topics,
	}
	conn := KafkaConnect{
		Name:   connector.Name,
		Config: &configuration,
	}

	// c.JSON(http.StatusOK, gin.H{"data": conn})
	jsonToSend, _ := json.Marshal(conn)
	resp, err := http.Post(conf.KafkaEndpoint+"connectors", "application/json", bytes.NewBuffer(jsonToSend))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	c.JSON(http.StatusOK, gin.H{"data": responseData})
}
