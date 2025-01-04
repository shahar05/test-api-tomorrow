package batches

import (
	"api-tomorrow/utils"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

var db *sql.DB

const partyURL = "/batches"

func RegisterRoutes(r *mux.Router, DB *sql.DB) {
	db = DB
	r.HandleFunc(partyURL, getBatchesHandler).Methods("GET")
}

func getBatchesHandler(w http.ResponseWriter, r *http.Request) {
	results, err := GetBatches()
	if err != nil {
		utils.RespondWithError(w, "Failed to parse batch metadata", http.StatusInternalServerError)
		return
	}

	// Set the response header to JSON and encode the results
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)

}
