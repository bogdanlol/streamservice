package models

import (
	"github.com/jinzhu/gorm"
)

type TeamOwnerEntity struct {
	gorm.Model
	TeamId     int        `json:"teamId"`
	TeamEntity TeamEntity `gorm:"foreignKey:TeamId"`
	UserId     int        `json:"userId"`
	UserEntity UserEntity `gorm:"foreignKey:UserId"`
}

func (TeamOwnerEntity) GetTableName() string {
	return "team_owners"
}

type TeamOwnerModel interface {
	GetById(uint64) (*TeamOwnerEntity, error)
	Create(*TeamOwnerEntity) (*TeamEntity, error)
	Update(*TeamOwnerEntity) (*TeamOwnerEntity, error)
	Delete(uid uint32) error
}
