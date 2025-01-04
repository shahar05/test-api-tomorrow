package weather

import (
	"database/sql"
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

func GetWeatherSummary(lat, lng float64) (*WeatherSummaryResponse, error) {
	summary := &WeatherSummaryResponse{}
	var err error
	query := `
			SELECT
				MAX(temperature), MIN(temperature), AVG(temperature),
				MAX(precipitation_rate), MIN(precipitation_rate), AVG(precipitation_rate),
				MAX(humidity), MIN(humidity), AVG(humidity)
			FROM weather_data
			WHERE latitude = $1 AND longitude = $2`

	row := db.QueryRow(query, lat, lng)

	if err = row.Scan(
		&summary.Max.Temperature, &summary.Min.Temperature, &summary.Avg.Temperature,
		&summary.Max.PrecipitationRate, &summary.Min.PrecipitationRate, &summary.Avg.PrecipitationRate,
		&summary.Max.Humidity, &summary.Min.Humidity, &summary.Avg.Humidity,
	); err != nil {
		if err == sql.ErrNoRows {
			// If no rows are returned, return nil without an error
			return nil, nil
		}
		log.Printf("Query error: %v", err)
		return nil, err
	}

	return summary, nil
}
