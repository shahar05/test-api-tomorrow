package batches

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

const weatherDataURL = "https://us-east1-climacell-platform-production.cloudfunctions.net/weather-data"

func FetchBatches() ([]Batch, error) {
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
	batches, err := FetchBatches()
	if err != nil {
		log.Println("/batches endpoint is currently offline")
		return
	}
	for _, b := range batches {
		batchesMap[b.BatchID] = struct{}{}
	}
	return
}

func GetBatches() (results []BatchMetadata, err error) {

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
		log.Printf("Query error: %v", err)
		return
	}
	defer rows.Close()

	batchesMap := fetchBatchesAsMap()

	for rows.Next() {
		var batch BatchMetadata
		var isRuning bool
		// Scan the values from the query result into the BatchMetadata struct
		if err = rows.Scan(&batch.BatchID, &batch.ForecastTime, &batch.StartIngestTime, &batch.EndIngestTime, &isRuning, &batch.NumberOfRows); err != nil {
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

	return
}
