package controllers

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"

	"github.com/gin-gonic/gin"
)

func UploadConfigsSF(c *gin.Context) {
	config, headerC, err := c.Request.FormFile("config")
	if err != nil {
		c.JSON(http.StatusBadRequest, fmt.Sprintf("file err : %s", err.Error()))
		return
	}
	input, headerI, err := c.Request.FormFile("input")
	if err != nil {
		c.JSON(http.StatusBadRequest, fmt.Sprintf("file err : %s", err.Error()))
		return
	}

	configFileName := headerC.Filename
	inputFileName := headerI.Filename

	if _, err := os.Stat("/tmp/sf/"); os.IsNotExist(err) {
		err := os.Mkdir("/tmp/sf/", 0755)
		if err != nil {
			log.Fatal(err)
		}
	}
	if _, err := os.Stat("/tmp/sf/sda/"); os.IsNotExist(err) {
		err := os.Mkdir("/tmp/sf/sda/", 0755)
		if err != nil {
			log.Fatal(err)
		}
	}
	outConfig, err := os.Create("/tmp/sf/" + configFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer outConfig.Close()
	_, err = io.Copy(outConfig, config)
	if err != nil {
		log.Fatal(err)
	}

	outInput, err := os.Create("/tmp/sf/" + inputFileName)
	if err != nil {
		log.Fatal(err)
	}
	defer outInput.Close()
	_, err = io.Copy(outInput, input)
	if err != nil {
		log.Fatal(err)
	}
	cmd := exec.Command("python3", "/home/bogdanl/go/streamservice/streamfactory/main.py", "-c", "/tmp/sf/"+configFileName, "-m", "/tmp/sf/"+inputFileName)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Println(cmd.Run())
	cmd2 := exec.Command("tar", "-czvf", "/tmp/sf/sda.tar.gz", "/tmp/sf/sda/")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	log.Println(cmd2.Run())
	c.File("/tmp/sf/sda.tar.gz")
}
