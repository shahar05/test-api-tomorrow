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
