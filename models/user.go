package models

import (
	"errors"

	"github.com/jinzhu/gorm"
	"golang.org/x/crypto/bcrypt"
)

type UserEntity struct {
	gorm.Model
	Username string `gorm:"not null"`
	Password string `gorm:"not null"`
	Admin    bool   `gorm:"default:false;not null"`
}

func (UserEntity) GetTableName() string {
	return "user"
}

func (u *UserEntity) HashPassword(plain string) (string, error) {
	if len(plain) == 0 {
		return "", errors.New("password should not be empty")
	}
	h, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	return string(h), err
}

func (u *UserEntity) CheckPassword(plain string) bool {
	err := bcrypt.CompareHashAndPassword([]byte(u.Password), []byte(plain))
	return err == nil
}

type UserModel interface {
	GetById(uint64) (*UserEntity, error)
	Create(*UserEntity) (*UserEntity, error)
	Update(*UserEntity) (*UserEntity, error)
	Delete(uid uint32) error
}
