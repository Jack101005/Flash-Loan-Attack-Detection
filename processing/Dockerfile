FROM apache/spark:3.5.0
WORKDIR /app
USER root
RUN pip install --no-cache-dir redis pymongo python-dotenv requests certifi
COPY streaming_job.py .
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "streaming_job.py"]
