# minimal linux distro + uv(x) baked in
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

# readable python/system logs
ENV LANG=C.UTF-8 \
    LC_ALL=C.UTF-8 \
    UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    PYTHONUNBUFFERED=1

# show your train ticket sir (TLS certs for NS API + kafka)
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# haul env in their and build the python part
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --frozen --no-install-project --no-dev

# haul project into it
COPY . .
RUN uv sync --frozen --no-dev

ENV PATH="/app/.venv/bin:$PATH"
VOLUME /app/data

# all aboard, choo choo
CMD ["python", "main.py"]
