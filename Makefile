SHELL := /bin/bash

build:
	mkdir -p src/bin
	cd src && go build -o bin/client ./client
	cd src && go build -o bin/server ./article_processing_system/server
	cd src && go build -o bin/metadata_filter ./article_processing_system/metadata_filter
	cd src && go build -o bin/image_fetcher ./article_processing_system/image_fetcher
	cd src && go build -o bin/abstract_preprocessing ./article_processing_system/abstract_preprocessing
	cd src && go build -o bin/frequency_collector ./article_processing_system/frequency_collector
	cd src && go build -o bin/top_aggregator ./article_processing_system/top_aggregator
	cd src && go build -o bin/average_calculator ./article_processing_system/average_calculator
	cd src && go build -o bin/abstract_word_counter ./article_processing_system/abstract_word_counter
.PHONY: build

check:
	cd src && test -z "$$(gofmt -l .)" && go vet ./... && go test ./...
.PHONY: check

BATCH_SIZE ?= 100
N ?= 3
METADATA_FILTER ?= 3
ABSTRACT_PREPROCESSING ?= 3
FREQUENCY_COLLECTOR ?= 3
TOP_AGGREGATOR ?= 3
AVERAGE_CALCULATOR ?= 3
WORD_COUNTER ?= 3

compose:
	python3 scripts/generate_compose.py --batch-size $(BATCH_SIZE) --metadata-filter $(METADATA_FILTER) \
		--abstract-preprocessing $(ABSTRACT_PREPROCESSING) --frequency-collector $(FREQUENCY_COLLECTOR) \
		--top-aggregator $(TOP_AGGREGATOR) \
		--average-calculator $(AVERAGE_CALCULATOR) --word-counter $(WORD_COUNTER)
.PHONY: compose

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose up --build --remove-orphans --detach --wait
	docker compose logs --follow
.PHONY: up

system:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose up --build --remove-orphans --detach --wait
.PHONY: system

expected:
	docker build --quiet --tag tp-expected scripts/expected
	docker run --rm --entrypoint rm -v "$(CURDIR)/output:/output" tp-expected -rf /output/expected
	mkdir -p output/expected
	docker run --rm -v "$(CURDIR)/data:/data" -v "$(CURDIR)/output:/output" \
		-e IMAGE_CACHE=/data/image_status.json \
		tp-expected generate /data /output/expected
	find data -maxdepth 1 -name '*.parquet' -printf '%f\n' | sort > output/expected/files.txt
.PHONY: expected

clients:
	scripts/run_clients.sh $(N)
.PHONY: clients

test: expected
	mkdir -p output
	rm -rf output/client_run_*
	@set +e; \
	COMPOSE_HTTP_TIMEOUT=300 docker compose up --build --remove-orphans --detach --wait; system=$$?; \
	if [ $$system -ne 0 ]; then \
		docker compose stop -t 5; \
		docker compose down; \
		exit 1; \
	fi; \
	WAIT=1 scripts/run_clients.sh $(N); clients=$$?; \
	go run ./scripts/verify_output.go -clients $(N); verify=$$?; \
	docker compose stop -t 5; \
	docker compose down; \
	[ $$clients -eq 0 ] && [ $$verify -eq 0 ]
.PHONY: test

down:
	docker compose stop -t 5
	docker compose down
.PHONY: down

logs:
	docker compose logs
.PHONY: logs
