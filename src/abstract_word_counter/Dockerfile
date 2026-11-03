FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod go.sum ./
COPY common ./common
COPY abstract_word_counter ./abstract_word_counter

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/abstract_word_counter ./abstract_word_counter

FROM busybox:latest
COPY --from=builder /build/bin/abstract_word_counter /abstract_word_counter
ENTRYPOINT ["./abstract_word_counter"]
