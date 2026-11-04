package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	MomHost    string
	MomPort    int
	InstanceID string
}

func NewConfig() (*Config, error) {
	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return nil, errors.New("MOM_PORT environment variable is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return nil, errors.New("MOM_HOST environment variable is required")
	}

	instanceID := os.Getenv("INSTANCE_ID")
	if instanceID == "" {
		return nil, errors.New("INSTANCE_ID environment variable is required")
	}

	return &Config{
		MomHost:    momHost,
		MomPort:    momPort,
		InstanceID: instanceID,
	}, nil
}
