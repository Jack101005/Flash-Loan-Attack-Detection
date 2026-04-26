FROM apache/spark:latest
WORKDIR /app
USER root
RUN pip install --no-cache-dir pyspark confluent-kafka redis pymongo python-dotenv
COPY streaming_job.py .
# Lệnh để submit job vào cluster
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", "streaming_job.py"]