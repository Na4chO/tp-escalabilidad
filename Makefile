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
ID ?= 0
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
	rm -rf output/client_run_*
	COMPOSE_HTTP_TIMEOUT=300 docker compose up --build --remove-orphans --detach --wait
	docker compose logs --follow server
.PHONY: system

VERIFY_CMD = docker build --quiet --tag tp-verify scripts/verify && \
	docker run --rm -v "$(CURDIR)/data:/data:ro" -v "$(CURDIR)/output:/output" \
		tp-verify -clients $(N)

verify:
	mkdir -p output
	$(VERIFY_CMD)
.PHONY: verify

clients:
	scripts/run_clients.sh $(N)
.PHONY: clients

client:
	@mkdir -p output/client_run_$(ID)
	docker rm -f client_run_$(ID) >/dev/null 2>&1 || true
	docker compose -f docker-compose.yaml -f docker-compose.clients.yaml build client_run
	docker compose -f docker-compose.yaml -f docker-compose.clients.yaml run -T --no-deps --rm \
		--name client_run_$(ID) \
		-v "$(CURDIR)/data:/data:ro" \
		-v "$(CURDIR)/output/client_run_$(ID):/output" \
		client_run
.PHONY: client

test:
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
	$(VERIFY_CMD); verify=$$?; \
	docker compose stop -t 5; \
	docker compose down; \
	if [ $$clients -eq 0 ]; then echo "Clients: OK"; else echo "Clients: FAIL"; fi; \
	if [ $$verify -eq 0 ]; then echo "Verification: OK"; else echo "Verification: FAIL"; fi; \
	[ $$clients -eq 0 ] && [ $$verify -eq 0 ]
.PHONY: test

down:
	docker compose stop -t 5
	docker compose down
.PHONY: down

logs:
	docker compose logs
.PHONY: logs
