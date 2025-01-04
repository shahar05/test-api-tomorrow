package utils

import (
	"log"
	"net/http"
)

func RespondWithError(w http.ResponseWriter, message string, statusCode int) {
	http.Error(w, message, statusCode)
	log.Printf("Error: %s", message)
}
