package config

import (
	"errors"
	"os"
	"strconv"
	"strings"
)

type Config struct {
	ServerID   string
	ServerHost string
	ServerPort string
	MomHost    string
	MomPort    int
	Queries    []uint8
}

func NewConfig() (*Config, error) {
	serverID := os.Getenv("SERVER_ID")
	if serverID == "" {
		return nil, errors.New("SERVER_ID environment variable is required")
	}
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

	// Queries whose EOF the server waits for before closing a client (all by default)
	queries := []uint8{1, 2, 3, 4, 5}
	if value := os.Getenv("QUERIES"); value != "" {
		queries = nil
		for _, field := range strings.Split(value, ",") {
			query, err := strconv.ParseUint(strings.TrimSpace(field), 10, 8)
			if err != nil {
				return nil, errors.New("QUERIES must be a comma separated list of query numbers")
			}
			queries = append(queries, uint8(query))
		}
	}

	return &Config{
		ServerID:   serverID,
		ServerHost: serverHost,
		ServerPort: serverPort,
		MomHost:    momHost,
		MomPort:    momPort,
		Queries:    queries,
	}, nil
}
