FROM golang:1.26.11-bookworm AS build
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY cmd cmd
COPY internal internal
RUN --mount=type=cache,target=/root/.cache/go-build CGO_ENABLED=0 go build -o /out/trading-api ./cmd/trading-api && CGO_ENABLED=0 go build -o /out/cdc-consumer ./cmd/cdc-consumer && CGO_ENABLED=0 go build -o /out/analytics-api ./cmd/analytics-api && CGO_ENABLED=0 go build -o /out/load-generator ./cmd/load-generator
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/ /usr/local/bin/
USER 65532:65532
CMD ["trading-api"]
