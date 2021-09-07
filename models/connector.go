package models

import "github.com/jinzhu/gorm"

type ConnectorEntity struct {
	gorm.Model
	Name           string `json:"name" gorm:"not null"`
	ConnectorClass string `json:"connectorClass" gorm:"not null"`
	TasksMax       uint16 `json:"tasksMax" gorm:"not null"`
	KeyConverter   string `json:"keyConverter,omitempty" gorm:"null"`
	ValueConverter string `json:"valueConverter,omitempty" gorm:"null"`
	Topics         string `json:"topics" gorm:"not null"`
	File           string `json:"file,omitempty" gorm:"null"`
	Type           string `gorm:"null"`
	Status         string `json:"status" gorm:"null"`
}

func (ConnectorEntity) GetTableName() string {
	return "connector"
}

type ConnectorModel interface {
	GetById(uint64) (*ConnectorEntity, error)
	Create(*ConnectorEntity) (*ConnectorEntity, error)
	Update(*ConnectorEntity) (*ConnectorEntity, error)
	Delete(uid uint32) error
}
