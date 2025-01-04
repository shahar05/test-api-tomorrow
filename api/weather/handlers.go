package weather

import (
	"api-tomorrow/utils"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"strconv"

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
	latStr := r.URL.Query().Get("lat")
	lngStr := r.URL.Query().Get("lng")
	if latStr == "" || lngStr == "" {
		http.Error(w, "Missing lat or lng query parameters", http.StatusBadRequest)
		return
	}

	// Parse lat and lng as float64
	lat, err := strconv.ParseFloat(latStr, 64)
	if err != nil {
		utils.RespondWithError(w, "Invalid lat value", http.StatusInternalServerError)
		return
	}

	lng, err := strconv.ParseFloat(lngStr, 64)
	if err != nil {
		utils.RespondWithError(w, "Invalid lng value", http.StatusInternalServerError)
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
	lat := r.URL.Query().Get("lat")
	lng := r.URL.Query().Get("lng")
	if lat == "" || lng == "" {
		http.Error(w, "Missing lat or lng query parameters", http.StatusBadRequest)
		return
	}

	query := `
			SELECT
				MAX(temperature), MIN(temperature), AVG(temperature),
				MAX(precipitation_rate), MIN(precipitation_rate), AVG(precipitation_rate),
				MAX(humidity), MIN(humidity), AVG(humidity)
			FROM weather_data
			WHERE latitude = $1 AND longitude = $2`

	row := db.QueryRow(query, lat, lng)
	var summary WeatherSummaryResponse
	if err := row.Scan(
		&summary.Max.Temperature, &summary.Min.Temperature, &summary.Avg.Temperature,
		&summary.Max.PrecipitationRate, &summary.Min.PrecipitationRate, &summary.Avg.PrecipitationRate,
		&summary.Max.Humidity, &summary.Min.Humidity, &summary.Avg.Humidity,
	); err != nil {
		http.Error(w, "Failed to fetch weather summary", http.StatusInternalServerError)
		log.Printf("Query error: %v", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summary)

}
