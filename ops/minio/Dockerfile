# Official registries no longer serve the required community images.
# Build upstream source rather than depending on an unofficial image mirror.
FROM golang:1.24.7-bookworm AS build
RUN git clone --depth 1 --branch RELEASE.2025-09-07T16-13-09Z https://github.com/minio/minio.git /src/minio \
    && cd /src/minio \
    && test "$(git rev-parse RELEASE.2025-09-07T16-13-09Z)" = 01ce918d8279a20e4706b96a64396146894adee4 \
    && CGO_ENABLED=0 go build -o /out/minio .
RUN git clone --depth 1 --branch RELEASE.2025-08-13T08-35-41Z https://github.com/minio/mc.git /src/mc \
    && cd /src/mc \
    && test "$(git rev-parse RELEASE.2025-08-13T08-35-41Z)" = d6541ea280b73a834b64d4097e21f2be77676104 \
    && CGO_ENABLED=0 go build -o /out/mc .
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /out/minio /out/mc /usr/local/bin/
ENTRYPOINT ["minio"]
