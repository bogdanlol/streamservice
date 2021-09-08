package config

type config struct {
	KafkaEndpoint string
}

func NewConfig() *config {
	config := config{
		KafkaEndpoint: "http://localhost:8083/",
	}

	return &config
}
