# 🛡️ DeFi Flash-Loan Attack Detector

Real-time, distributed mempool monitoring system designed to detect and alert on malicious Flash-loan transactions before they are finalized on the blockchain.

## 📌 Problem Statement
Flash-loan attacks cause billions of dollars in damages within the DeFi ecosystem. Exchanges and protocols require a robust system capable of:
* Analyzing massive volumes of pending transaction data directly from the Mempool.
* Detecting circular trades and abnormal instantaneous price changes.
* Operating with ultra-low latency (< 100ms), a computational load that a single standalone server cannot handle.

## ✨ Core Functionalities
* **Data Ingestion:** Connects to Blockchain Nodes (such as Infura or Alchemy) to stream real-time transactions via WebSockets.
* **Pattern Matching Engine:** Utilizes algorithms to detect flash-loan related transaction sequences (e.g., borrowing, swapping, and returning funds within a single block).
* **Alerting System:** Dispatches immediate warnings to a dedicated Dashboard and Telegram/Discord when suspicious activities are flagged.

## 🏗️ System Architecture
The system is built on a Master-Worker/Pipeline model, consisting of the following layers:
* **Message Broker (Kafka/RabbitMQ):** Acts as the distributed data transit hub for the entire system.
* **Processing Nodes:** Multiple workers running in parallel to continuously evaluate different segments of the data stream.
* **State Management:** Utilizes Redis to temporarily store transaction states for lightning-fast comparisons.

## 🌐 Distributed Characteristics
To ensure enterprise-grade reliability, the architecture incorporates:
* **Fault Tolerance:** If a worker node crashes, the system automatically redistributes the data to another active worker without dropping the transaction, achieved via Kafka Consumer Groups or Replicas.
* **Scalability (Horizontal):** During extreme market volatility and transaction spikes, new nodes can be seamlessly added to the system to distribute the load.

## 💻 Tech Stack
* **Languages:** Python (Data Science/Logic), Next.js/React (Frontend Dashboard).
* **Blockchain Interaction:** Web3.py / Ethers.js.
* **Message Broker:** Apache Kafka / RabbitMQ.
* **Stream Processing:** Apache Flink / Spark Streaming.
* **Databases:** Redis (In-memory state), MongoDB / PostgreSQL (Historical storage).
* **Monitoring:** Grafana, Telegram Bot API.

## 📂 Project Structure

```text
flash-loan-detector/
├── docker-compose.yml       # Container orchestration for distributed nodes
├── data-ingestion/          # WebSocket listeners & Smart Contract filters
├── message-broker/          # Kafka/RabbitMQ configurations & routing
├── stream-processing/       # Spark/Flink cycle detection & pattern matching
├── storage/                 # Redis state management & PostgreSQL schemas
├── dashboard/               # Next.js frontend for live monitoring
└── alerting/                # Telegram/Discord bot integrations
