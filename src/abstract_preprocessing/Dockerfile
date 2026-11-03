FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY abstract_preprocessing ./abstract_preprocessing

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/abstract_preprocessing ./abstract_preprocessing

FROM busybox:latest
COPY --from=builder /build/bin/abstract_preprocessing /abstract_preprocessing
ENTRYPOINT ["./abstract_preprocessing"]
