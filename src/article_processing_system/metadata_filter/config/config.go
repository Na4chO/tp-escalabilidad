package config

import (
	"errors"
	"os"
	"strconv"

	"github.com/Na4chO/tp-escalabilidad/common"
)

type Config struct {
	MomHost           string
	MomPort           int
	InstanceID        string
	ImageFetcherCount int
	BatchSize         int
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

	imageFetcherCount, err := strconv.Atoi(os.Getenv("IMAGE_FETCHER_COUNT"))
	if err != nil {
		return nil, errors.New("IMAGE_FETCHER_COUNT environment variable is required")
	}

	batchSize, err := common.LoadBatchSize()
	if err != nil {
		return nil, err
	}

	return &Config{
		MomHost:           momHost,
		MomPort:           momPort,
		InstanceID:        instanceID,
		ImageFetcherCount: imageFetcherCount,
		BatchSize:         batchSize,
	}, nil
}
