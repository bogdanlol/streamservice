package models

import "github.com/jinzhu/gorm"

type ConnectorEntity struct {
	gorm.Model
	Name           string     `json:"name" gorm:"not null"`
	ConnectorClass string     `json:"connector.class" gorm:"not null"`
	TasksMax       uint16     `json:"tasks.max" gorm:"not null"`
	KeyConverter   string     `json:"key.converter,omitempty" gorm:"null"`
	ValueConverter string     `json:"value.converter,omitempty" gorm:"null"`
	Topics         string     `json:"topics" gorm:"not null"`
	File           string     `json:"file,omitempty" gorm:"null"`
	Type           string     `json:"type" gorm:"null"`
	Status         string     `json:"status" gorm:"null"`
	TeamId         int        `json:"teamId"`
	TeamEntity     TeamEntity `gorm:"foreignKey:TeamId"`
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
