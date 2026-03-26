# Stage 1: Build
FROM rust:1.85-bookworm as builder

WORKDIR /app
COPY Cargo.toml ./
COPY src ./src

RUN cargo build --release

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /app/target/release/chickie-worker /app/chickie-worker

RUN useradd -m -u 1000 workeruser && chown -R workeruser:workeruser /app
USER workeruser


CMD ["./chickie-worker"]


