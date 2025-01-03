// weather_service_api.go

package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	_ "github.com/lib/pq" // PostgreSQL driver
)

const (
	dbConnectionString = "user=postgres password=secret dbname=postgres sslmode=disable"
)

type WeatherDataResponse struct {
	ForecastTime      string  `json:"forecastTime"`
	Temperature       float64 `json:"Temperature"`
	PrecipitationRate float64 `json:"Precipitation_rate"`
	Humidity          float64 `json:"Humidity"`
}

type WeatherSummaryResponse struct {
	Max struct {
		Temperature       float64 `json:"Temperature"`
		PrecipitationRate float64 `json:"Precipitation_rate"`
		Humidity          float64 `json:"Humidity"`
	} `json:"max"`
	Min struct {
		Temperature       float64 `json:"Temperature"`
		PrecipitationRate float64 `json:"Precipitation_rate"`
		Humidity          float64 `json:"Humidity"`
	} `json:"min"`
	Avg struct {
		Temperature       float64 `json:"Temperature"`
		PrecipitationRate float64 `json:"Precipitation_rate"`
		Humidity          float64 `json:"Humidity"`
	} `json:"avg"`
}

type BatchMetadata struct {
	BatchID         string `json:"batch_id"`
	ForecastTime    string `json:"forecast_time"`
	NumberOfRows    int    `json:"number_of_rows"`
	StartIngestTime string `json:"start_ingest_time"`
	EndIngestTime   string `json:"end_ingest_time"`
	Status          string `json:"status"`
}

func getWeatherDataHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		lat := r.URL.Query().Get("lat")
		lon := r.URL.Query().Get("lon")
		if lat == "" || lon == "" {
			http.Error(w, "Missing lat or lon query parameters", http.StatusBadRequest)
			return
		}

		query := `
			SELECT forecast_time, temperature, precipitation_rate, humidity
			FROM weather_data
			WHERE latitude = $1 AND longitude = $2
			ORDER BY forecast_time`

		rows, err := db.Query(query, lat, lon)
		if err != nil {
			http.Error(w, "Failed to fetch weather data", http.StatusInternalServerError)
			log.Printf("Query error: %v", err)
			return
		}
		defer rows.Close()

		var results []WeatherDataResponse
		for rows.Next() {
			var data WeatherDataResponse
			if err := rows.Scan(&data.ForecastTime, &data.Temperature, &data.PrecipitationRate, &data.Humidity); err != nil {
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
		lon := r.URL.Query().Get("lon")
		if lat == "" || lon == "" {
			http.Error(w, "Missing lat or lon query parameters", http.StatusBadRequest)
			return
		}

		query := `
			SELECT
				MAX(temperature), MIN(temperature), AVG(temperature),
				MAX(precipitation_rate), MIN(precipitation_rate), AVG(precipitation_rate),
				MAX(humidity), MIN(humidity), AVG(humidity)
			FROM weather_data
			WHERE latitude = $1 AND longitude = $2`

		row := db.QueryRow(query, lat, lon)
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

func getBatchesHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		query := `
			SELECT batch_id, forecast_time, COUNT(*) as number_of_rows,
				start_ingest_time, end_ingest_time, status
			FROM batches
			LEFT JOIN weather_data ON batches.batch_id = weather_data.batch_id
			GROUP BY batches.batch_id, batches.forecast_time, batches.start_ingest_time, batches.end_ingest_time, batches.status`

		rows, err := db.Query(query)
		if err != nil {
			http.Error(w, "Failed to fetch batch metadata", http.StatusInternalServerError)
			log.Printf("Query error: %v", err)
			return
		}
		defer rows.Close()

		var results []BatchMetadata
		for rows.Next() {
			var batch BatchMetadata
			if err := rows.Scan(&batch.BatchID, &batch.ForecastTime, &batch.NumberOfRows, &batch.StartIngestTime, &batch.EndIngestTime, &batch.Status); err != nil {
				http.Error(w, "Failed to parse batch metadata", http.StatusInternalServerError)
				log.Printf("Scan error: %v", err)
				return
			}
			results = append(results, batch)
		}

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
