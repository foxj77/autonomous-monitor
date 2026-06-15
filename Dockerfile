FROM golang:1.26-alpine AS builder

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /autonomous-monitor .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates \
	&& addgroup -g 65532 -S autonomous-monitor \
	&& adduser -u 65532 -S -D -H -G autonomous-monitor autonomous-monitor
COPY --from=builder /autonomous-monitor /autonomous-monitor

USER 65532:65532
CMD ["/autonomous-monitor"]
