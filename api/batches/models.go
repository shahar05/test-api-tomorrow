package batches

import "time"

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
