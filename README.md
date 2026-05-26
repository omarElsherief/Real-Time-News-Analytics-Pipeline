
# Real-Time News Analytics Pipeline

A real-time big data streaming pipeline built using **Apache Kafka**, **PySpark Structured Streaming**, and **Docker**.  
The system continuously fetches live technology news articles from the NewsAPI, streams them through Kafka, processes them with Spark Streaming, and performs real-time analytics such as keyword extraction, text cleaning, and article size distribution.

---

# Project Overview

This project simulates a real-world streaming analytics system used in data engineering and big data environments.

The pipeline performs the following workflow:

1. Fetches live news articles from NewsAPI
2. Filters articles using AI, Big Data, Kafka, Spark, and Python related keywords
3. Publishes articles into a Kafka topic
4. Consumes streaming data using PySpark Structured Streaming
5. Cleans and enriches text data
6. Performs real-time analytics:
   - Top keyword frequency
   - Text length classification
   - Streaming batch monitoring
7. Displays processed analytics in real time

---

# Architecture

```text
                ┌─────────────────┐
                │    NewsAPI      │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │  Kafka Producer │
                │   (producer.py) │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │ Apache Kafka    │
                │ Topic:          │
                │ social-media    │
                └────────┬────────┘
                         │
                         ▼
                ┌─────────────────┐
                │ Spark Streaming │
                │ spark_consumer  │
                └────────┬────────┘
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
 Keyword Analysis   Text Cleaning   Size Analytics
````

---

# Technologies Used

## Big Data & Streaming

* Apache Kafka
* Apache Spark Structured Streaming
* PySpark
* Docker & Docker Compose

## Programming

* Python 3

## APIs

* NewsAPI

---

# Features

* Real-time streaming pipeline
* Kafka producer/consumer architecture
* Structured Streaming with Spark
* Real-time keyword analytics
* Text preprocessing and normalization
* Stop-word filtering
* Article size classification
* Batch-based stream processing
* Dockerized Kafka environment

---

# Project Structure

```text
├── docker-compose.yml
├── producer.py
├── spark_consumer.py
└── README.md
```

---

# How It Works

## 1. Kafka Infrastructure

Kafka and Zookeeper are started using Docker Compose.

```bash
docker-compose up -d
```

---

## 2. Producer Service

The producer:

* Connects to NewsAPI
* Fetches live technology-related articles
* Filters articles using target keywords:

  * Python
  * Kafka
  * Spark
  * AI
  * Machine Learning
  * Big Data
* Sends matching articles into Kafka topic:

```text
social-media
```

Run:

```bash
python producer.py
```

---

## 3. Spark Streaming Consumer

The Spark consumer:

* Reads streaming data from Kafka
* Parses JSON messages
* Cleans text data
* Removes special characters
* Converts text to lowercase
* Removes stop words
* Performs analytics in real time

Run:

```bash
python spark_consumer.py
```

---

# Real-Time Analytics

## Keyword Frequency Analysis

The system extracts the most frequent words from streaming articles.

Example:

```text
TOP KEYWORDS

1. kafka              ███████████ (11)
2. spark              ████████ (8)
3. python             ██████ (6)
```

---

## Article Size Distribution

Articles are classified into:

* Short
* Medium
* Long

Based on text length.

Example:

```text
POST SIZE DISTRIBUTION

short      ███████ (7)
medium     ████ (4)
long       ██ (2)
```

---

# Data Processing Pipeline

## Cleaning Steps

The streaming pipeline performs:

* HTML removal
* Special character removal
* Lowercase normalization
* Whitespace normalization
* Tokenization
* Stop-word filtering

---

# Kafka Message Schema

```json
{
  "text": "Article title and description",
  "timestamp": "2026-05-26T10:00:00Z",
  "user": "News Source"
}
```

---

# Setup Instructions

## Prerequisites

Install:

* Python 3
* Docker Desktop
* Apache Spark
* Java 17
* Hadoop binaries for Windows
* Kafka Python package

---

## Install Python Dependencies

```bash
pip install kafka-python pyspark newsapi-python
```

---

# Start Kafka

```bash
docker-compose up -d
```

---

# Run Producer

```bash
python producer.py
```

---

# Run Spark Consumer

```bash
python spark_consumer.py
```

---

# Example Streaming Output

```text
BATCH 1 — 5 NEW ARTICLES

Timestamp  : 2026-05-26
Source     : TechCrunch
Size       : medium (145 chars)

TOP KEYWORDS
1. ai
2. kafka
3. spark
```

---

# Future Improvements

* Store streaming results in MongoDB or PostgreSQL
* Add sentiment analysis
* Create real-time dashboard with Streamlit
* Integrate Apache Airflow
* Add Spark ML models
* Deploy using Kubernetes
* Use Elasticsearch for indexing
* Add real-time visualizations

---

# Learning Outcomes

This project demonstrates practical experience with:

* Event-driven architecture
* Real-time data pipelines
* Distributed streaming systems
* Spark Structured Streaming
* Kafka messaging systems
* Data preprocessing pipelines
* Big data engineering fundamentals

---

# Author

Developed as a Big Data & Real-Time Streaming Analytics project using Kafka and Spark.

```
Omar Mohsen Elsherief
```
