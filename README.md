## 🌐 Real-Time Data Pipeline: WebSocket → Kafka → Flink -> S3

A scalable real-time data ingestion system that captures messages from WebSocket clients, streams them through Apache Kafka, and persistently stores them in Amazon S3 for further analytics and archival.

---

### 📌 Overview

This project implements a robust real-time data pipeline:

1. **WebSocket Client/Server**: Accepts live messages from clients.
2. **Apache Kafka**: Acts as a high-throughput message broker.
3. **Apache Flink**: Acts as a High-Throughput Processing Engine
4. **Amazon S3**: Stores messages durably for batch processing, analytics, or ML workflows.

Ideal for use cases like live event tracking, IoT telemetry, log aggregation, and user activity monitoring.

### Components

- **WebSocket Server**: Listens for real-time messages from clients.
- **Kafka Producer**: Publishes incoming messages to a Kafka topic.
- **Kafka Cluster**: Buffers and decouples message flow.
- **S3 Sink (via Kafka Connect or Custom Consumer)**: Persists messages to S3.
- **Amazon S3**: Stores structured data (e.g., JSON, Parquet) with time-based partitioning.
