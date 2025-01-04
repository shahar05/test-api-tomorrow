package weather

import (
	"log"
)

func GetWeatherData(lat, lng float64) ([]WeatherDataResponse, error) {
	query := `
			SELECT latitude, longitude, forecast_time, temperature, precipitation_rate, humidity
			FROM weather_data
			WHERE latitude = $1 AND longitude = $2
			ORDER BY forecast_time`

	rows, err := db.Query(query, lat, lng)
	if err != nil {
		log.Printf("Query error: %v", err)
		return nil, err
	}
	defer rows.Close()

	var results []WeatherDataResponse
	for rows.Next() {
		var data WeatherDataResponse
		if err := rows.Scan(&data.Latitude, &data.Longitude, &data.ForecastTime, &data.Temperature, &data.PrecipitationRate, &data.Humidity); err != nil {
			log.Printf("Scan error: %v", err)
			return nil, err
		}
		results = append(results, data)
	}

	return results, nil
}
