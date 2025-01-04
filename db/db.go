// db/db.go
package db

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/lib/pq"
)

var (
	dbInstance *sql.DB
	once       sync.Once
)

func GetDB() *sql.DB {
	once.Do(func() {
		var err error
		dbInstance, err = sql.Open("postgres", "user=postgres password=secret dbname=postgres sslmode=disable")
		if err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
		// Optionally, set connection pool settings
		dbInstance.SetMaxOpenConns(10)
		dbInstance.SetMaxIdleConns(5)
		dbInstance.SetConnMaxLifetime(30 * time.Minute)
	})
	return dbInstance
}

// Init initializes the database connection
func Init() *sql.DB {
	var err error

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		os.Getenv("DB_HOST"), os.Getenv("DB_PORT"), os.Getenv("DB_USER"), os.Getenv("DB_PASSWORD"), os.Getenv("DB_NAME"))

	// Establish DB connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Error opening database connection: %v", err)
	}

	// Ping DB server
	if err := db.Ping(); err != nil {
		log.Fatalf("Error pinging database: %v", err)
	}

	return db
}
