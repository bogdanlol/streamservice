package config

type config struct {
	KafkaEndpoint string
	SshRelayApi   string
}

func NewConfig() *config {
	config := config{
		KafkaEndpoint: "http://localhost:8083/",
		SshRelayApi:   "http://localhost:8095/",
	}

	return &config
}
