SHELL := /bin/bash

build:
	cd src && go build -o bin/client ./client
	cd src && go build -o bin/server ./server
.PHONY: build

check:
	cd src && gofmt -l . && go vet ./... && go test ./...
.PHONY: check

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs