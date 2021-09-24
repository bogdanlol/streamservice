package models

import (
	"github.com/jinzhu/gorm"
)

type TeamEntity struct {
	gorm.Model
	Name string `gorm:"not null"`
}

func (TeamEntity) GetTableName() string {
	return "team"
}

type TeamModel interface {
	GetById(uint64) (*TeamEntity, error)
	Create(*TeamEntity) (*TeamEntity, error)
	Update(*TeamEntity) (*TeamEntity, error)
	Delete(uid uint32) error
}
