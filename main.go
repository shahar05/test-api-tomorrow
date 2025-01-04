// weather_service_api.go

package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

const (
	weatherDataURL     = "https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data"
	dbConnectionString = "user=postgres password=secret dbname=postgres sslmode=disable"
)

type WeatherDataResponse struct {
	Latitude          float64 `json:"latitude"`
	Longitude         float64 `json:"longitude"`
	ForecastTime      string  `json:"forecast_time"`
	Temperature       float64 `json:"temperature"`
	PrecipitationRate float64 `json:"precipitation_rate"`
	Humidity          float64 `json:"humidity"`
}

type WeatherAVG struct {
	Temperature       float64 `json:"Temperature"`
	PrecipitationRate float64 `json:"Precipitation_rate"`
	Humidity          float64 `json:"Humidity"`
}

type WeatherSummaryResponse struct {
	Max WeatherAVG `json:"max"`
	Min WeatherAVG `json:"min"`
	Avg WeatherAVG `json:"avg"`
}

type BatchMetadata struct {
	BatchID         string  `json:"batch_id"`
	ForecastTime    string  `json:"forecast_time"`
	NumberOfRows    int     `json:"number_of_rows"`
	StartIngestTime string  `json:"start_ingest_time"`
	EndIngestTime   *string `json:"end_ingest_time"`
	Status          string  `json:"status"`
}

type Batch struct {
	BatchID      string    `json:"batch_id"`
	ForecastTime time.Time `json:"forecast_time"`
}

func getWeatherDataHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lat := r.URL.Query().Get("lat")
		lng := r.URL.Query().Get("lng")
		if lat == "" || lng == "" {
			http.Error(w, "Missing lat or lng query parameters", http.StatusBadRequest)
			return
		}

		query := `
			SELECT latitude, longitude, forecast_time, temperature, precipitation_rate, humidity
			FROM weather_data
			WHERE latitude = $1 AND longitude = $2
			ORDER BY forecast_time`

		rows, err := db.Query(query, lat, lng)
		if err != nil {
			http.Error(w, "Failed to fetch weather data", http.StatusInternalServerError)
			log.Printf("Query error: %v", err)
			return
		}
		defer rows.Close()

		var results []WeatherDataResponse
		for rows.Next() {
			var data WeatherDataResponse
			if err := rows.Scan(&data.Latitude, &data.Longitude, &data.ForecastTime, &data.Temperature, &data.PrecipitationRate, &data.Humidity); err != nil {
				http.Error(w, "Failed to parse weather data", http.StatusInternalServerError)
				log.Printf("Scan error: %v", err)
				return
			}
			results = append(results, data)
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

func getWeatherSummaryHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
}

func fetchBatches() ([]Batch, error) {
	resp, err := http.Get(fmt.Sprintf("%s/batches", weatherDataURL))
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var batches []Batch
	if err := json.NewDecoder(resp.Body).Decode(&batches); err != nil {
		return nil, err
	}

	return batches, nil
}

func fetchBatchesAsMap() (batchesMap map[string]struct{}) {
	batchesMap = make(map[string]struct{})
	batches, err := fetchBatches()
	if err != nil {
		log.Println("/batches endpoint is currently offline")
		return
	}
	for _, b := range batches {
		batchesMap[b.BatchID] = struct{}{}
	}
	return
}

func getBatchesHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Modify the query to include the number of rows from the weather_data table and the status from the batches table
		query := `
			SELECT 
				b.batch_id, 
				b.forecast_time, 
				b.start_ingest_time, 
				b.end_ingest_time, 
				b.running, 
				COALESCE(COUNT(wd.id), 0) AS number_of_rows
			FROM batches b
			LEFT JOIN weather_data wd ON b.batch_id = wd.batch_id
			WHERE b.deleted = FALSE
			GROUP BY b.batch_id, b.forecast_time, b.start_ingest_time, b.end_ingest_time, b.running
		`

		// Execute the query
		rows, err := db.Query(query)
		if err != nil {
			http.Error(w, "Failed to fetch batch metadata", http.StatusInternalServerError)
			log.Printf("Query error: %v", err)
			return
		}
		defer rows.Close()

		batchesMap := fetchBatchesAsMap()

		var results []BatchMetadata
		for rows.Next() {
			var batch BatchMetadata
			var isRuning bool
			// Scan the values from the query result into the BatchMetadata struct
			if err := rows.Scan(&batch.BatchID, &batch.ForecastTime, &batch.StartIngestTime, &batch.EndIngestTime, &isRuning, &batch.NumberOfRows); err != nil {
				http.Error(w, "Failed to parse batch metadata", http.StatusInternalServerError)
				log.Printf("Scan error: %v", err)
				return
			}

			batch.Status = "INACTIVE"
			if isRuning {
				batch.Status = "RUNNING"
			} else if _, exists := batchesMap[batch.BatchID]; exists {
				batch.Status = "ACTIVE"
			}

			// Append the batch metadata to the results slice
			results = append(results, batch)
		}

		// Set the response header to JSON and encode the results
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(results)
	}
}

func main() {
	db, err := sql.Open("postgres", dbConnectionString)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	http.HandleFunc("/weather/data", getWeatherDataHandler(db))
	http.HandleFunc("/weather/summarize", getWeatherSummaryHandler(db))
	http.HandleFunc("/batches", getBatchesHandler(db))

	log.Println("Starting server on :8080")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}
