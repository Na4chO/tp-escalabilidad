FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY article_processing_system/average_calculator ./article_processing_system/average_calculator

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/average_calculator ./article_processing_system/average_calculator

FROM busybox:latest
COPY --from=builder /build/bin/average_calculator /average_calculator
ENTRYPOINT ["./average_calculator"]
