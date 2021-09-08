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

//get connector classes
func GetConnectorClasses(c *gin.Context) {
	type connectorClass struct {
		Class   string `json:"class"`
		Type    string `json:"type"`
		Version string `json:"version"`
	}
	var conn []connectorClass

	conf := config.NewConfig()

	response, err := http.Get(conf.KafkaEndpoint + "connector-plugins")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
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
	conf := config.NewConfig()
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
