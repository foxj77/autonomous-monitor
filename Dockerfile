FROM golang:1.23-alpine AS builder

RUN apk add --no-cache gcc musl-dev librdkafka-dev pkgconf

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -tags musl -o /autonomous-monitor .

FROM alpine:3.23
RUN apk add --no-cache librdkafka ca-certificates
COPY --from=builder /autonomous-monitor /autonomous-monitor

CMD ["/autonomous-monitor"]
