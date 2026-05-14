FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies directly
RUN pip install --no-cache-dir \
    fastapi>=0.135.3 \
    pydantic>=2.13.0 \
    uvicorn>=0.44.0 \
    redis \
    certifi>=2024.0.0 \
    python-dotenv>=1.0.0 \
    pymongo>=4.6.0

# Copy application code
COPY . .
COPY storage/ /app/storage/

# Expose port
EXPOSE 8000

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD curl -f http://localhost:8000/ || exit 1

# Run the application
CMD ["uvicorn", "backend.Main:app", "--host", "0.0.0.0", "--port", "8000"]
