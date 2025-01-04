package batches

import (
	"database/sql"
	"encoding/json"
	"log"
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
