FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir redis requests

# Copy script to container
COPY price_feed.py .

# Run script
CMD ["python", "-u","price_feed.py"]