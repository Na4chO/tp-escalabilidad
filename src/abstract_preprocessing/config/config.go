package config

import (
	"errors"
	"github.com/Na4chO/tp-escalabilidad/common"
	"os"
	"strconv"
)

type Config struct {
	MomHost                  string
	MomPort                  int
	InstanceID               string
	FrequencyCollectorAmount int
	AverageCalculatorAmount  int
	BatchSize                int
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

	frequencyCollectorAmount, err := loadAmount("FREQUENCY_COLLECTOR_AMOUNT")
	if err != nil {
		return nil, err
	}

	averageCalculatorAmount, err := loadAmount("AVERAGE_CALCULATOR_AMOUNT")
	if err != nil {
		return nil, err
	}

	batchSize, err := common.LoadBatchSize()
	if err != nil {
		return nil, err
	}

	return &Config{
		MomHost:                  momHost,
		MomPort:                  momPort,
		InstanceID:               instanceID,
		FrequencyCollectorAmount: frequencyCollectorAmount,
		AverageCalculatorAmount:  averageCalculatorAmount,
		BatchSize:                batchSize,
	}, nil
}

func loadAmount(name string) (int, error) {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value < 1 {
		return 0, errors.New(name + " environment variable is required and must be a positive number")
	}
	return value, nil
}
