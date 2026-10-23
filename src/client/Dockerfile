FROM golang:1.24-alpine3.23 AS builder

RUN mkdir -p /build
WORKDIR /build

COPY go.mod ./
COPY common ./common
COPY client ./client

RUN CGO_ENABLED=0 GOOS=linux go build -o bin/client ./client

FROM busybox:latest
COPY --from=builder /build/bin/client /client
ENTRYPOINT ["./client"]
