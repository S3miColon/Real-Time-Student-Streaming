*# Real-Time Student Data Processing*



*## Project Overview*

*This project demonstrates real-time student data generation and processing using Python, Apache Kafka, and PySpark Structured Streaming.*



*Student records are automatically generated every 2 seconds and published to a Kafka topic. Spark continuously consumes these records, processes student marks, and determines whether each student has passed or failed.*



*## Architecture*



*Python Data Generator → Apache Kafka → PySpark Structured Streaming → Real-Time Results*



*## Technologies Used*

*- Python*

*- Apache Kafka*

*- PySpark Structured Streaming*

*- Docker*

*- JSON*



*## Project Features*

*- Automatic student data generation.*

*- Random student names, subjects, and marks.*

*- Real-time data streaming through Kafka.*

*- JSON message processing using PySpark.*

*- PASS/FAIL result calculation.*

*- Continuous stream processing.*



*## Project Structure*



*```text*

*Real-Time-Student-Streaming/*

*│*

*├── student\_generator.py*

*├── student\_streaming.py*

*└── README.md*

*```*



*## How It Works*



*1. Python generates student records automatically.*

*2. Kafka receives and stores student records in the `student\_data` topic.*

*3. PySpark reads the incoming Kafka messages.*

*4. Spark converts JSON messages into structured data.*

*5. Students scoring 40 or above are marked PASS; others are marked FAIL.*

*6. Processed results are displayed in real time.*



*## Execution*



*Start the automatic data generator:*



*```bash*

*python student\_generator.py*

*```*



*Start the Spark streaming application:*



*```bash*

*spark-submit --packages org.apache.spark:spark-sql-kafka-0-10\_2.12:3.5.0 student\_streaming.py*

*```*



*## Purpose*



*The objective of this project is to understand real-time data ingestion, streaming, and processing using Apache Kafka and PySpark.*



*## Author*

*Aditya Choudhary*

