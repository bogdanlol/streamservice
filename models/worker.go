package models

import "github.com/jinzhu/gorm"

type WorkerEntity struct {
	gorm.Model
	Name                string `json:"name" gorm:"not null"`
	Ip                  string `json:"ip" gorm:"null"`
	WorkerPath          string `json:"path" gorm:"not null"`
	ConnectStatus       string `json:"status"`
	Username            string `json:"username" gorm:"null"`
	Password            string `json:"password" gorm:"null"`
	HasKafkaConnectOpen bool   `json:"available"`
}

func (WorkerEntity) GetTableName() string {
	return "worker"
}

type WorkerModel interface {
	GetById(uint64) (*WorkerEntity, error)
	Create(*WorkerEntity) (*WorkerEntity, error)
	Update(*WorkerEntity) (*WorkerEntity, error)
	Delete(uid uint32) error
}
