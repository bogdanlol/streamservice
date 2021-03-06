package controllers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"reflect"
	"strconv"
	"streamingservice/config"
	"streamingservice/db"
	"streamingservice/models"
	"streamingservice/utils"

	"github.com/gin-gonic/gin"
	"github.com/h2non/filetype"
)

var DB = db.New()
var conf = config.NewConfig()

// GET CONNECTORS

func FindConnectors(c *gin.Context) {
	var connectors []models.ConnectorEntity
	var connectorsWithStatus []models.ConnectorEntity
	var worker models.WorkerEntity

	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	loggedInUser, err := utils.GetCurrentlyLoggedinUser(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err})
	}
	if loggedInUser.Admin {
		DB.Find(&connectors)
	} else {
		DB.Where("team_id=?", loggedInUser.TeamId).Find(&connectors)
	}
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)

	var _, isKafkaConnectOpenErr = http.Get(conf.KafkaEndpoint)
	if connectors != nil && isKafkaConnectOpenErr == nil {
		type responseStatus struct {
			Connector struct {
				State string `json:"state"`
			} `json:"connector"`
		}
		for _, connector := range connectors {
			var response *http.Response
			if endpoint == conf.SshRelayApi {
				response, err = http.Get(endpoint + "api/" + worker.Name + "/" + port + connector.Name + "/status")
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				}
			} else {
				response, err = http.Get(endpoint + "connectors/" + connector.Name + "/status")
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				}
			}

			if response.StatusCode != 200 {
				connectorsWithStatus = append(connectorsWithStatus, connector)
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
	var connector models.ConnectorEntity
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
	StrId, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	var input models.ConnectorEntity
	id, err := strconv.Atoi(StrId)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}

	DB.First(&input, id)
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
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var response *http.Response
	var err error
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)
	type connectorClass struct {
		Class   string `json:"class"`
		Type    string `json:"type"`
		Version string `json:"version"`
	}
	var conn []connectorClass

	if endpoint == conf.SshRelayApi {
		response, err = http.Get(endpoint + "api/" + worker.Name + "/" + port + "/connector-plugins")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		response, err = http.Get(endpoint + "connector-plugins")
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
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
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
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
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	var response *http.Response
	var err error
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)
	id, isPresent := c.Params.Get("entityId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	var connector models.ConnectorEntity
	err = DB.First(&connector, id).Error
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	type KafkaConnect struct {
		Name   string      `json:"name"`
		Config interface{} `json:"config"`
	}

	m := map[string]interface{}{}

	v := reflect.ValueOf(connector)
	typeOfS := v.Type()
	ignoredFields := []string{"CustomFields", "TeamEntity", "Model", "Status", "Type", "TeamId"}
	for i := 0; i < v.NumField(); i++ {
		if !utils.StringInSlice(typeOfS.Field(i).Name, ignoredFields) {
			m[typeOfS.Field(i).Tag.Get("json")] = v.Field(i).Interface()
		}
	}
	type customF struct {
		Field string `json:"field"`
		Value string `json:"value"`
	}
	if connector.CustomFields != nil {
		var data []customF
		err := json.Unmarshal(connector.CustomFields, &data)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		for _, v := range data {
			if v.Field != "" && v.Value != "" {
				m[v.Field] = v.Value
			}
		}
	}
	conn := KafkaConnect{
		Name:   connector.Name,
		Config: &m,
	}

	jsonToSend, _ := json.Marshal(conn)
	if endpoint == conf.SshRelayApi {
		response, err = http.Post(endpoint+"api/"+worker.Name+"/"+port+"/"+"connectors", "application/json", bytes.NewBuffer(jsonToSend))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		response, err = http.Post(endpoint+"connectors", "application/json", bytes.NewBuffer(jsonToSend))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}
	responseData, err := ioutil.ReadAll(response.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}

	c.JSON(http.StatusOK, gin.H{"data": responseData})
}

func PostConnectors(c *gin.Context) {
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	var response *http.Response
	var err error
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)
	type conns struct {
		Connectors []int `json:"connectors"`
	}
	var input conns
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var Response []interface{}

	for _, id := range input.Connectors {

		var connector models.ConnectorEntity
		err = DB.First(&connector, id).Error
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
		type KafkaConnect struct {
			Name   string      `json:"name"`
			Config interface{} `json:"config"`
		}

		m := map[string]interface{}{}

		v := reflect.ValueOf(connector)
		typeOfS := v.Type()
		ignoredFields := []string{"CustomFields", "TeamEntity", "Model", "Status", "Type", "TeamId"}
		for i := 0; i < v.NumField(); i++ {
			if !utils.StringInSlice(typeOfS.Field(i).Name, ignoredFields) {
				m[typeOfS.Field(i).Tag.Get("json")] = v.Field(i).Interface()
			}
		}
		type customF struct {
			Field string `json:"field"`
			Value string `json:"value"`
		}
		if connector.CustomFields != nil {
			var data []customF
			err := json.Unmarshal(connector.CustomFields, &data)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				continue
			}

			for _, v := range data {
				if v.Field != "" && v.Value != "" {
					m[v.Field] = v.Value
				}
			}
		}
		conn := KafkaConnect{
			Name:   connector.Name,
			Config: &m,
		}

		jsonToSend, _ := json.Marshal(conn)
		if endpoint == conf.SshRelayApi {
			response, err = http.Post(endpoint+"api/"+worker.Name+"/"+port+"/"+"connectors", "application/json", bytes.NewBuffer(jsonToSend))
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
		} else {
			response, err = http.Post(endpoint+"connectors", "application/json", bytes.NewBuffer(jsonToSend))
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
		}
		responseData, err := ioutil.ReadAll(response.Body)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			continue
		}

		Response = append(Response, responseData)

	}

	c.JSON(http.StatusOK, gin.H{"data": Response})
}

func StopConnector(c *gin.Context) {
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	client := &http.Client{}
	var req *http.Request
	var err error
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)
	name, isPresent := c.Params.Get("entityName")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}

	// c.JSON(http.StatusOK, gin.H{"data": conn})
	if endpoint == conf.SshRelayApi {
		req, err = http.NewRequest(http.MethodDelete, endpoint+"api/"+worker.Name+"/"+port+"/connectors/"+name, nil)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	} else {
		req, err = http.NewRequest(http.MethodDelete, endpoint+"connectors/"+name, nil)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	defer resp.Body.Close()

	c.JSON(http.StatusOK, gin.H{})
}

func StopConnectors(c *gin.Context) {
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	var req *http.Request
	var err error
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)
	client := &http.Client{}
	type conns struct {
		Connectors []string `json:"connectors"`
	}
	var input conns
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	for _, name := range input.Connectors {
		// c.JSON(http.StatusOK, gin.H{"data": conn})

		if endpoint == conf.SshRelayApi {
			req, err = http.NewRequest(http.MethodDelete, endpoint+"api/"+worker.Name+"/"+port+"/connectors/"+name, nil)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
		} else {
			req, err = http.NewRequest(http.MethodDelete, endpoint+"connectors/"+name, nil)
			if err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		defer resp.Body.Close()
	}
	c.JSON(http.StatusOK, gin.H{})
}

func UploadConnectorPlugin(c *gin.Context) {
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, fmt.Sprintf("file err : %s", err.Error()))
		return
	}

	filename := header.Filename

	out, err := os.Create("/opt/kafka/confluent-6.1.0/share/java/" + filename)
	if err != nil {
		log.Fatal(err)
	}
	defer out.Close()
	_, err = io.Copy(out, file)
	if err != nil {
		log.Fatal(err)
	}

	buf, _ := ioutil.ReadFile("/opt/kafka/confluent-6.1.0/share/java/" + filename)
	kind, _ := filetype.Match(buf)
	if kind == filetype.Unknown {
		fmt.Println("Unknown file type")
		return
	}
	if kind.Extension == "zip" {
		_, err := utils.Unzip("/opt/kafka/confluent-6.1.0/share/java/"+filename, "/opt/kafka/confluent-6.1.0/share/java/")
		if err != nil {
			c.JSON(http.StatusBadRequest, fmt.Sprintf("file err : %s", err.Error()))
			return
		}
	} else {
		r, err := os.Open("/opt/kafka/confluent-6.1.0/share/java/" + filename)
		if err != nil {
			c.JSON(http.StatusBadRequest, fmt.Sprintf("file err : %s", err.Error()))
		}

		err = utils.Untar("/opt/kafka/confluent-6.1.0/share/java/", r)
		if err != nil {
			c.JSON(http.StatusBadRequest, fmt.Sprintf("Untaring err : %s", err.Error()))
		}
	}
	err = os.Remove("/opt/kafka/confluent-6.1.0/share/java/" + filename)
	if err != nil {
		c.JSON(http.StatusBadRequest, fmt.Sprintf("Untaring err : %s", err.Error()))
	}

}

func ValidateConnector(c *gin.Context) {
	var worker models.WorkerEntity
	workerId, isPresent := c.Params.Get("workerId")
	if !isPresent {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no such connector"})
	}
	if err := DB.First(&worker, workerId).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	var req *http.Request
	var err error
	endpoint, port := utils.GetEndpointAndPort(worker.Name, worker.ConnectPort)
	// Validate input
	client := &http.Client{}

	var input models.ConnectorEntity

	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	// c.JSON(http.StatusOK, gin.H{"data": conn})
	m := map[string]interface{}{}

	v := reflect.ValueOf(input)
	typeOfS := v.Type()
	ignoredFields := []string{"CustomFields", "TeamEntity", "Model", "Status", "Type", "TeamId"}
	for i := 0; i < v.NumField(); i++ {
		if !utils.StringInSlice(typeOfS.Field(i).Name, ignoredFields) {
			m[typeOfS.Field(i).Tag.Get("json")] = v.Field(i).Interface()
		}
	}
	type customF struct {
		Field string `json:"field"`
		Value string `json:"value"`
	}
	if input.CustomFields != nil {
		var data []customF
		err := json.Unmarshal(input.CustomFields, &data)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}

		for _, v := range data {
			m[v.Field] = v.Value
		}
	}

	jsonToSend, _ := json.Marshal(m)
	if endpoint == conf.SshRelayApi {
		req, err = http.NewRequest(http.MethodPut, endpoint+"api/"+worker.Name+"/"+port+"/connector-plugins/"+input.ConnectorClass+"/config/validate/", bytes.NewBuffer(jsonToSend))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
	} else {
		req, err = http.NewRequest(http.MethodPut, endpoint+"connector-plugins/"+input.ConnectorClass+"/config/validate/", bytes.NewBuffer(jsonToSend))
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		}
	}

	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	resp, err := client.Do(req)
	if err != nil {
		panic(err)
	}
	responseData, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	type validateResponse struct {
		ErrorCode  int    `json:"error_code"`
		Message    string `json:"message"`
		ErrorCount int    `json:"error_count"`
		Configs    []struct {
			Value struct {
				Name   string   `json:"name"`
				Errors []string `json:"errors"`
			} `json:"value"`
		} `json:"configs"`
	}

	var valResp validateResponse
	var validationErrors []string

	err = json.Unmarshal(responseData, &valResp)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
	}
	if valResp.ErrorCode == 500 {
		c.JSON(http.StatusOK, gin.H{"errors": valResp.Message})
		return
	}
	if valResp.ErrorCount != 0 {
		for _, conf := range valResp.Configs {
			if conf.Value.Errors != nil {
				for _, err := range conf.Value.Errors {
					validationErrors = append(validationErrors, err)
				}
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"errors": validationErrors})

}
