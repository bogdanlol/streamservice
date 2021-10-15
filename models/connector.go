package models

import "github.com/jinzhu/gorm"

type ConnectorEntity struct {
	gorm.Model
	Name           string     `json:"name" gorm:"not null"`
	ConnectorClass string     `json:"connector.class" gorm:"not null"`
	TasksMax       uint16     `json:"tasks.max" gorm:"not null"`
	KeyConverter   string     `json:"key.converter" gorm:"null"`
	ValueConverter string     `json:"value.converter" gorm:"null"`
	Topic          string     `json:"topic" gorm:"not null"`
	File           string     `json:"file" gorm:"null"`
	Type           string     `json:"type" gorm:"null"`
	Status         string     `json:"status" gorm:"null"`
	CustomFields   JSON       `json:"customFields" gorm:"null"`
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
