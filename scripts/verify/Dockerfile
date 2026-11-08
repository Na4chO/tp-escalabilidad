FROM golang:1.24-alpine3.23 AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY verify ./verify
RUN CGO_ENABLED=0 GOOS=linux go build -o /verify .

FROM busybox:latest
# CA certificates for the HTTPS requests to Wikimedia
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /verify /verify
ENTRYPOINT ["/verify"]
