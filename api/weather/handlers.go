package weather

import (
	"api-tomorrow/utils"
	"database/sql"
	"encoding/json"
	"net/http"

	"github.com/gorilla/mux"
)

var db *sql.DB

const partyURL = "/weather"

func RegisterRoutes(r *mux.Router, DB *sql.DB) {
	db = DB
	r.HandleFunc(partyURL+"/data", getWeatherDataHandler).Methods("GET")
	r.HandleFunc(partyURL+"/summarize", getWeatherSummaryHandler).Methods("GET")
}

func getWeatherDataHandler(w http.ResponseWriter, r *http.Request) {
	lat, lng, err := utils.GetLatLng(r)
	if err != nil {
		utils.RespondWithError(w, err.Error(), http.StatusBadRequest)
		return
	}

	results, err := GetWeatherData(lat, lng)
	if err != nil {
		utils.RespondWithError(w, "Server failed fetch weather", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func getWeatherSummaryHandler(w http.ResponseWriter, r *http.Request) {
	lat, lng, err := utils.GetLatLng(r)
	if err != nil {
		utils.RespondWithError(w, err.Error(), http.StatusBadRequest)
		return
	}

	summary, err := GetWeatherSummary(lat, lng)
	if err != nil {
		utils.RespondWithError(w, "Server failed fetch weather summary", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)
}
