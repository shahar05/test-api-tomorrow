package utils

import (
	"fmt"
	"net/http"
	"strconv"
)

// Function to get and validate lat and lng from query parameters
func GetLatLng(r *http.Request) (float64, float64, error) {
	latStr := r.URL.Query().Get("lat")
	lngStr := r.URL.Query().Get("lng")
	if latStr == "" || lngStr == "" {
		return 0, 0, fmt.Errorf("missing lat or lng query parameters")
	}

	lat, err := ParseStr2Float(latStr)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid lat value")
	}

	lng, err := ParseStr2Float(lngStr)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid lng value")
	}

	return lat, lng, nil
}

// Function to parse float64 from query parameter
func ParseStr2Float(value string) (float64, error) {
	return strconv.ParseFloat(value, 64)
}
