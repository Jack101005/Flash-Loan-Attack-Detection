FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
RUN pip install --no-cache-dir \
    pymongo>=4.6.0 \
    certifi>=2024.0.0 \
    python-dotenv>=1.0.0

# Copy storage module from current context
COPY mongo_store.py ./

# Expose port for potential health checks or APIs
EXPOSE 8001

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD python -c "from mongo_store import ping; exit(0 if ping() else 1)" || exit 0

# Keep container running
CMD ["tail", "-f", "/dev/null"]

