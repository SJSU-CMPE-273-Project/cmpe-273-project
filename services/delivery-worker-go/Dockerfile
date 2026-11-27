FROM golang:1.25 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY contracts ./contracts
COPY gen/go ./gen/go
COPY internal ./internal
COPY services/delivery-worker-go ./services/delivery-worker-go
RUN CGO_ENABLED=0 go build -trimpath -o /delivery-worker ./services/delivery-worker-go/cmd/delivery-worker

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /delivery-worker /delivery-worker
ENTRYPOINT ["/delivery-worker"]
