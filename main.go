package main

import (
	"streamingservice/db"
	"streamingservice/routes"
)

func main() {
	d := db.New()
	db.AutoMigrate(d)
	r := routes.New()

	r.Run("0.0.0.0:8000")
}
