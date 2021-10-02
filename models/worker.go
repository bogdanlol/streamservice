package models

import "github.com/jinzhu/gorm"

type WorkerEntity struct {
	gorm.Model
	Name          string     `json:"name" gorm:"not null"`
	Ip            string     `json:"ip" gorm:"null"`
	WorkerPath    string     `json:"path" gorm:"not null"`
	ConnectStatus string     `json:"status"`
	Username      string     `json:"username" gorm:"null"`
	Password      string     `json:"password" gorm:"null"`
	ConnectPort   uint       `json:"port" gorm:"default:8083;not null"`
	TeamId        int        `json:"teamId"`
	TeamEntity    TeamEntity `gorm:"foreignKey:TeamId"`

	Environment string `json:"environment" gorm:"default:'test';not null"`
	Type        string `json:"type" gorm:"default:'worker';not null"`
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
