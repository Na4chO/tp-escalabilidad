package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	ServerHost string
	ServerPort string
	MomHost    string
	MomPort    int
}

func NewConfig() (*Config, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return nil, errors.New("SERVER_HOST environment variable is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return nil, errors.New("SERVER_PORT environment variable is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return nil, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return nil, errors.New("MOM_HOST environment variable is required")
	}

	return &Config{
		ServerHost: serverHost,
		ServerPort: serverPort,
		MomHost:    momHost,
		MomPort:    momPort,
	}, nil
}
