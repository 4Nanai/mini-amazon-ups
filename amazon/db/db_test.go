package db

import (
	"testing"

	"github.com/joho/godotenv"
)

func TestDatabaseConnection(t *testing.T) {
	godotenv.Load("../.env")
	db := NewDatabase()
	if db.db == nil {
		t.Fatal("Failed to connect to the database")
	}
}
