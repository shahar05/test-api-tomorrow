package main

import (
	"api-tomorrow/api/batches"
	"api-tomorrow/api/weather"
	"api-tomorrow/db"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {
	// Init DB
	db := db.GetDB()

	// Init Router
	r := mux.NewRouter()

	// Register the HealthCheckHandler
	r.HandleFunc("/", HealthCheckHandler).Methods("GET")

	batches.RegisterRoutes(r, db)
	weather.RegisterRoutes(r, db)

	// Start server
	portServer := "8080"
	log.Printf("Server is running on port %s", portServer)
	log.Fatal(http.ListenAndServe(":"+portServer, r))
}

func HealthCheckHandler(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Health Check ok"))
}
