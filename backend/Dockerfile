FROM python:3.11-slim

LABEL maintainer="VolGuard"
LABEL description="VolGuard Intelligence Edition — Overnight options trading for Indian markets."
LABEL version="6.0.0"

# Build deps for numpy, scipy, arch (C extensions)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install dependencies first (layer-cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy application
COPY volguard_v6_final.py .
COPY seed_journal.py .

# Persistent storage dirs — mounted as volumes at runtime
RUN mkdir -p /app/data /app/logs

# Runtime env defaults (overridden by docker-compose / .env)
ENV DATABASE_URL=sqlite:////app/data/volguard.db
ENV LOG_FILE=/app/logs/volguard.log
ENV PORT=8000
ENV HOST=0.0.0.0
ENV TZ=Asia/Kolkata

# Non-root user for security
RUN useradd -m -u 1000 volguard && \
    chown -R volguard:volguard /app
USER volguard

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:${PORT}/api/health || exit 1

CMD ["sh", "-c", "uvicorn volguard_v6_final:app --host ${HOST} --port ${PORT} --log-level info --workers 1"]
