# Build stage: static binary, no cgo, no build paths baked in.
FROM golang:1.26 AS build

WORKDIR /src

# Dependencies first, so a source-only change does not re-download the module
# graph on every build.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags="-s -w" \
    -o /out/streammesh ./cmd/streammesh

# Runtime stage: distroless static, non-root, no shell to compromise.
FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/streammesh /usr/local/bin/streammesh
COPY --from=build /src/examples /etc/streammesh/examples

# Runtime state (write-ahead log, dead letter files) lives here, so mount a
# volume over it rather than baking data into the image layer.
WORKDIR /state

USER nonroot:nonroot

EXPOSE 8080 8081 5514/udp

ENTRYPOINT ["/usr/local/bin/streammesh"]
CMD ["run", "-config", "/etc/streammesh/examples/demo.yaml"]
