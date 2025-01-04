package weather

type WeatherDataResponse struct {
	Latitude          float64 `json:"latitude"`
	Longitude         float64 `json:"longitude"`
	ForecastTime      string  `json:"forecast_time"`
	Temperature       float64 `json:"temperature"`
	PrecipitationRate float64 `json:"precipitation_rate"`
	Humidity          float64 `json:"humidity"`
}

type WeatherSummaryResponse struct {
	Max WeatherAVG `json:"max"`
	Min WeatherAVG `json:"min"`
	Avg WeatherAVG `json:"avg"`
}

type WeatherAVG struct {
	Temperature       float64 `json:"Temperature"`
	PrecipitationRate float64 `json:"Precipitation_rate"`
	Humidity          float64 `json:"Humidity"`
}
