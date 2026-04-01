# 🛡️ DeFi Flash-Loan Attack Detector

Real-time, distributed mempool monitoring system designed to detect and alert on malicious Flash-loan transactions before they are finalized on the blockchain.

## 📌 Problem Statement
[cite_start]Flash-loan attacks cause billions of dollars in damages within the DeFi ecosystem[cite: 55]. Exchanges and protocols require a robust system capable of:
* [cite_start]Analyzing massive volumes of pending transaction data directly from the Mempool[cite: 57].
* [cite_start]Detecting circular trades and abnormal instantaneous price changes[cite: 58].
* [cite_start]Operating with ultra-low latency (< 100ms), a computational load that a single standalone server cannot handle[cite: 59].

## ✨ Core Functionalities
* [cite_start]**Data Ingestion:** Connects to Blockchain Nodes (such as Infura or Alchemy) to stream real-time transactions via WebSockets[cite: 63].
* [cite_start]**Pattern Matching Engine:** Utilizes algorithms to detect flash-loan related transaction sequences (e.g., borrowing, swapping, and returning funds within a single block)[cite: 64].
* [cite_start]**Alerting System:** Dispatches immediate warnings to a dedicated Dashboard and Telegram/Discord when suspicious activities are flagged[cite: 65].

## 🏗️ System Architecture
[cite_start]The system is built on a Master-Worker/Pipeline model[cite: 67], consisting of the following layers:
* [cite_start]**Message Broker (Kafka/RabbitMQ):** Acts as the distributed data transit hub for the entire system[cite: 68].
* [cite_start]**Processing Nodes:** Multiple workers running in parallel to continuously evaluate different segments of the data stream[cite: 69].
* [cite_start]**State Management:** Utilizes Redis to temporarily store transaction states for lightning-fast comparisons[cite: 70].

## 🌐 Distributed Characteristics
To ensure enterprise-grade reliability, the architecture incorporates:
* [cite_start]**Fault Tolerance:** If a worker node crashes, the system automatically redistributes the data to another active worker without dropping the transaction, achieved via Kafka Consumer Groups or Replicas[cite: 73, 74].
* [cite_start]**Scalability (Horizontal):** During extreme market volatility and transaction spikes, new nodes can be seamlessly added to the system to distribute the load[cite: 75].

## 💻 Tech Stack
* [cite_start]**Languages:** Python (Data Science/Logic), Next.js/React (Frontend Dashboard)[cite: 83, 115].
* [cite_start]**Blockchain Interaction:** Web3.py / Ethers.js[cite: 84].
* [cite_start]**Message Broker:** Apache Kafka / RabbitMQ[cite: 86].
* [cite_start]**Stream Processing:** Apache Flink / Spark Streaming[cite: 87].
* [cite_start]**Databases:** Redis (In-memory state), MongoDB / PostgreSQL (Historical storage)[cite: 89, 90].
* [cite_start]**Monitoring:** Grafana, Telegram Bot API[cite: 91].

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