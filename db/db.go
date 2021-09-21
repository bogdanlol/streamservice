package db

import (
	"fmt"
	"streamingservice/models"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
)

func New() *gorm.DB {
	db, err := gorm.Open("mysql", "root:pass@/streamservice?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		fmt.Println("eror during connection to the database :", err)
	}

	db.LogMode(true)
	return db
}
func TestDB() *gorm.DB {
	db, err := gorm.Open("mysql", "root:pass@/streamservice?charset=utf8&parseTime=True&loc=Local")
	if err != nil {
		fmt.Println("eror during connection to the database :", err)
	}
	db.LogMode(true)
	return db
}

func AutoMigrate(db *gorm.DB) {
	db.AutoMigrate(
		&models.ConnectorEntity{},
		&models.WorkerEntity{},
		&models.UserEntity{},
	)
}
