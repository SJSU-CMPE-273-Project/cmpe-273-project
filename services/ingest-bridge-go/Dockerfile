FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY contracts ./contracts
COPY gen/go ./gen/go
COPY services/ingest-bridge-go/cmd/ingest-bridge ./services/ingest-bridge-go/cmd/ingest-bridge
RUN CGO_ENABLED=0 go build -trimpath -o /ingest-bridge ./services/ingest-bridge-go/cmd/ingest-bridge

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /ingest-bridge /ingest-bridge
ENTRYPOINT ["/ingest-bridge"]
