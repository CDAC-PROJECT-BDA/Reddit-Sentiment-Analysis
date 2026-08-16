# Reddit Sentiment Analysis Pipeline

This is my Big Data project where I built an automated pipeline to fetch Reddit posts, stream them through Kafka, clean the text with PySpark, analyze sentiment using NLP, and store the output in Databricks Delta Lake using the Medallion Architecture.


## Project Summary

Goal: Collect Reddit posts and classify sentiment as Positive, Negative, or Neutral.
Workflow: Airflow 
→
→ Reddit API 
→
→ JSONL Staging 
→
→ Kafka 
→
→ PySpark Databricks (Bronze 
→
→ Silver 
→
→ Gold Delta Lake).


## Tech Stack & Role

Python: API data collection & Kafka producer scripts (requests, json, kafka-python).
Apache Airflow: Workflow orchestration & hourly cron DAG scheduling (0 * * * *).
Apache Kafka (Aiven Cloud): Real-time event streaming buffer with SSL security & offset state tracking.
Apache Spark / PySpark: Distributed big data processing & Structured Streaming (trigger(availableNow=True)).
Databricks & Delta Lake: Medallion Lakehouse storage (Bronze, Silver, Gold) with ACID transactions & MERGE INTO upserts.
TextBlob NLP: Text sentiment polarity scoring (-1.0 to +1.0) inside PySpark UDFs.
