FROM python:3.12

WORKDIR /app

RUN pip install --no-cache-dir redis requests dotenv

# Copy script to container
COPY price_feed.py .

# Run script
CMD ["python", "-u","price_feed.py"]