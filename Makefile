SHELL := /bin/bash

build:
	cd src && go build -o bin/client ./client
	cd src && go build -o bin/server ./server
	cd src && go build -o bin/frequency_collector ./frequency_collector
	cd src && go build -o bin/top_aggregator ./top_aggregator
	cd src && go build -o bin/abstract_preprocessing ./abstract_preprocessing
.PHONY: build

check:
	cd src && gofmt -l . && go vet ./... && go test ./...
.PHONY: check

CLIENTS ?= 3
METADATA_FILTER ?= 3
ABSTRACT_PREPROCESSING ?= 3
FREQUENCY_COLLECTOR ?= 3
TOP_AGGREGATOR ?= 3
AVERAGE_CALCULATOR ?= 3
WORD_COUNTER ?= 3

compose:
	python3 scripts/generate_compose.py --clients $(CLIENTS) --metadata-filter $(METADATA_FILTER) \
		--abstract-preprocessing $(ABSTRACT_PREPROCESSING) --frequency-collector $(FREQUENCY_COLLECTOR) \
		--top-aggregator $(TOP_AGGREGATOR) \
		--average-calculator $(AVERAGE_CALCULATOR) --word-counter $(WORD_COUNTER)
	scripts/split_data.sh $(CLIENTS)
.PHONY: compose

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