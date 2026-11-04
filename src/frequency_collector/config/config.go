package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	TopSize             int
	ID                  int
	TopAggregatorAmount int
	MomHost             string
	MomPort             int
}

func NewConfig() (*Config, error) {
	topSize, err := strconv.Atoi(os.Getenv("TOP_SIZE"))
	if err != nil {
		return nil, errors.New("TOP_SIZE environment variable is required and must be a number")
	}
	if topSize <= 0 {
		return nil, errors.New("TOP_SIZE must be greater than zero")
	}

	id, err := strconv.Atoi(os.Getenv("ID"))
	if err != nil || id < 0 {
		return nil, errors.New("ID environment variable is required and must be a non-negative number")
	}

	topAggregatorAmount, err := strconv.Atoi(os.Getenv("TOP_AGGREGATOR_AMOUNT"))
	if err != nil || topAggregatorAmount < 1 {
		return nil, errors.New("TOP_AGGREGATOR_AMOUNT environment variable is required and must be a positive number")
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
		TopSize:             topSize,
		ID:                  id,
		TopAggregatorAmount: topAggregatorAmount,
		MomHost:             momHost,
		MomPort:             momPort,
	}, nil
}
