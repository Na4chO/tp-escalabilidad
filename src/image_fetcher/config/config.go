package config

import (
	"errors"
	"os"
	"strconv"
)

type Config struct {
	MomHost        string
	MomPort        int
	UserAgent      string
	ImageCacheSize int
	Partition      int
	// Bandwidth limit of this instance for the Wikimedia downloads
	DownloadRateMbps float64
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

	userAgent := os.Getenv("WIKIMEDIA_USER_AGENT")
	if userAgent == "" {
		return nil, errors.New("WIKIMEDIA_USER_AGENT environment variable is required")
	}

	imageCacheSize, err := strconv.Atoi(os.Getenv("IMAGE_CACHE_SIZE"))
	if err != nil {
		return nil, errors.New("IMAGE_CACHE_SIZE environment variable is required and must be a number")
	}

	partition, err := strconv.Atoi(os.Getenv("PARTITION"))
	if err != nil {
		return nil, errors.New("PARTITION environment variable is required and must be a number")
	}

	downloadRateMbps, err := strconv.ParseFloat(os.Getenv("DOWNLOAD_RATE_MBPS"), 64)
	if err != nil || downloadRateMbps <= 0 {
		return nil, errors.New("DOWNLOAD_RATE_MBPS environment variable is required and must be a positive number")
	}

	return &Config{
		MomHost:          momHost,
		MomPort:          momPort,
		UserAgent:        userAgent,
		ImageCacheSize:   imageCacheSize,
		Partition:        partition,
		DownloadRateMbps: downloadRateMbps,
	}, nil
}
