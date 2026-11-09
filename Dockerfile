# syntax=docker/dockerfile:1

FROM rust:bookworm AS rust-toolchain

FROM python:3.12-slim-bookworm AS builder

COPY --from=rust-toolchain /usr/local/cargo /usr/local/cargo
COPY --from=rust-toolchain /usr/local/rustup /usr/local/rustup

ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH="/usr/local/cargo/bin:${PATH}" \
    PYO3_PYTHON=/usr/local/bin/python3.12

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       build-essential pkg-config ca-certificates \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

WORKDIR /build

COPY Cargo.toml Cargo.lock pyproject.toml ./
COPY src ./src
COPY python ./python

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    uv tool run maturin build \
      --release \
      --locked \
      --bindings bin \
      --compatibility linux \
      --out /wheels \
      -v

FROM python:3.12-slim-bookworm AS runtime

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONPATH=/usr/local/lib/python3.12/site-packages \
    LD_LIBRARY_PATH=/usr/local/lib

WORKDIR /app

COPY --from=builder /wheels /wheels

RUN pip install --no-cache-dir /wheels/*.whl \
    && rm -rf /wheels \
    && useradd --create-home --uid 10001 worker

USER worker

ENTRYPOINT ["agent-worker"]