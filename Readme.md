Real-Time Flashloan Detection System 🚀

This project is a high-performance, real-time data pipeline designed to detect Flashloan transactions on the blockchain. It leverages a modern tech stack including Kafka for streaming, Spark for processing, and Redis/MongoDB for storage and state management.

📂 Project Structure

Following the actual project organization as shown in your workspace:

flashloan-detection/
├── infra/
│   ├── docker-compose.yml     # Master configuration for all services
│   └── .env                   # Environment variables (API Keys, Ports, etc.)
├── db/                        # P4: Storage & Data Feed
│   ├── Dockerfile             
│   ├── price_feed.py          # Real-time ETH price fetcher
│   ├── mongo_writer.py        
│   └── redis_client.py        
├── ingestion/                 # P2: Data Ingestion
│   ├── Dockerfile             
│   └── main.py                # Blockchain listener & Kafka producer
├── processing/                # P3: Real-time Processing
│   ├── Dockerfile             
│   └── streaming_job.py       # Spark Structured Streaming logic
├── dashboard/                 # P5: Monitoring UI
│   ├── Dockerfile             
│   └── app.py                 # Streamlit visualization
└── schemas.md                 # Data format definitions for the team


🛠️ Deployment Instructions

Navigate to the infra/ directory before running these commands.

1. Start Core Infrastructure (Pre-built Services)

Run this command first to initialize the core ecosystem. These services use official images from Docker Hub.

docker-compose up -d zookeeper kafka redis mongodb spark-master spark-worker-1 spark-worker-2


2. Build and Start Custom Services (Team Source Code)

Once the code is ready in the respective folders, use these commands to build and run our custom microservices.

P4: Price Feed (Operational):

docker-compose up -d --build price-feed


P2: Ingestion:

docker-compose up -d --build ingestion


P3: Spark Processing:

docker-compose up -d --build processing-job


P5: Dashboard:

docker-compose up -d --build dashboard


Pro Tip: To rebuild and start EVERYTHING at once: docker-compose up -d --build

📡 Technical Specs for Developers

Kafka (Message Broker)

Bootstrap Server: localhost:9094 (External) | kafka:9092 (Internal)

Topic Name: raw_txns

Partitions: 4 (Configured for high-throughput parallel processing).

Format: JSON.

Redis (State Management)

Host: localhost:6379

Purpose: Stores real-time ETH price updates for instant calculation.

🧹 Maintenance & Cleanup

To stop all services and remove containers:

docker-compose down


To reset the entire environment including database volumes:

docker-compose down -v


Maintained by P1 Infrastructure Team.