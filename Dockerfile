FROM rust:1.88 AS builder
WORKDIR /usr/src/app
RUN apt-get update && apt-get install -y pkg-config libssl-dev libpq-dev && rm -rf /var/lib/apt/lists/*
COPY . .
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates libssl-dev libpq5 && rm -rf /var/lib/apt/lists/*
COPY --from=builder /usr/src/app/target/release/britespeck_regression_tracker /usr/local/bin/regression-tracker
COPY --from=builder /usr/src/app/migrations /migrations
CMD ["regression-tracker"]