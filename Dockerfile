# uv + python 3.13 on slim debian
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1

# TLS certs for NS API + Kafka brokers
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# install deps first for layer caching
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-install-project --no-dev

# then the project source
COPY . .
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
VOLUME /app/data

CMD ["python", "main.py"]
