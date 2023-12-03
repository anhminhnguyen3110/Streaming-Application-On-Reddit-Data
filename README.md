# Real-time Sentiment Analysis and Toxic Comment Classification

This dynamic project harnesses the power of Apache Kafka and Apache Spark to conduct real-time sentiment analysis and classify toxic comments within streaming data. Seamlessly integrating data from the Reddit API, the system leverages Kafka for real-time streaming and Spark for near real-time analysis. Employing advanced Natural Language Processing (NLP) techniques, it determines sentiment and classifies comments, providing a robust solution to combat online toxicity.

## Features

- **Data Source:** Utilizes the Reddit API for seamless streaming data ingestion.
- **Architecture:** Implements a resilient architecture, utilizing Kafka for streaming and Spark for analysis.
- **Sentiment Analysis:** Applies NLP techniques such as VADER and TextBlob for accurate sentiment analysis.
- **Toxic Comment Classification:** Employs a cutting-edge toxic comment classification model based on the XLNet architecture.
- **Data Visualization:** Utilizes Grafana for real-time monitoring and insightful historical data analysis.

## Architecture

### 1. Data Source

Efficiently collects streaming data from the Reddit API using the "prawn" library.

### 2. Kafka

Utilizes Kafka as a distributed and fault-tolerant stream processing platform, implementing a robust Kafka setup on Docker with brokers and Zookeeper.

**Kafka Components:**
- **Broker:** Manages storage and distribution of data.
- **Topic:** Organizes data streams logically for efficient publishing and consumption.
- **Producer:** Publishes data to Kafka topics.
- **Consumer and Consumer Group:** Extracts data from Kafka topics, facilitating parallel processing.

### 3. Spark Streaming

Leverages Spark Streaming for near real-time data processing, overcoming challenges related to Spark setup, dependencies, and interactive sessions.

**Spark Components:**
- **Spark Master:** Manages the Spark application.
- **Spark Worker:** Executes tasks directed by the Spark Master.
- **Spark Session:** Initiates data processing and analysis.
- **Structured Spark Stream:** Processes batches of messages from Kafka topics.

### 4. Toxic Comment Classification Model

Trains a sophisticated toxic comment classification model based on the XLNet architecture.

**Model Components:**
- **Tokenization:** Utilizes the XLNet-based model through the Hugging Face Transformers library.
- **Pretrained Model:** Selects XLNet-Base-Case for superior performance in sentiment and text classification tasks.
- **Data Preparation:** Cleans and preprocesses data for efficient model training.
- **Result:** Achieves an impressive accuracy rate of around 91%.

### 5. Cassandra and Grafana

Deploys Cassandra for durable data storage and Grafana for compelling data visualization, offering valuable insights into system performance.

**Cassandra:**
- **Data storage:** Provides scalable and fault-tolerant storage for sentiment analysis and toxic comment results.

**Grafana:**
- **Dashboards:** Customized dashboards facilitate real-time monitoring and historical data analysis.
- **Alerting:** Supports alerting for specific data conditions.

## Architecture Diagrams

- Data pipeline architecture diagram
  ![image](https://github.com/emyeucanha5/Spark-Streaming-with-Sentiment-Analysis-and-Toxic-Comment-classification/assets/57170354/99298c25-d4fc-4e83-a458-43a1b2b8bdf0)
- Kafka infrastructure diagram
  ![image](https://github.com/emyeucanha5/Spark-Streaming-with-Sentiment-Analysis-and-Toxic-Comment-classification/assets/57170354/39264dcc-d4e2-4291-ae29-a2f014a86c64)
- Spark infrastructure diagram 
  ![image](https://github.com/emyeucanha5/Spark-Streaming-with-Sentiment-Analysis-and-Toxic-Comment-classification/assets/57170354/6ef64208-3304-4a76-955d-a5366e6d945b)

## Challenges and Future Enhancements

- **Challenges Faced:**
  - Successfully addressed hurdles in setting up Spark and Kafka, resolving issues related to dependencies and configurations.

- **Recommendations for Future Enhancements:**
  - Proposes enhancements in the toxicity model's handling of double-negative cases.
  - Suggests exploring cloud deployment on services like GCP or AWS for enhanced scalability.
  - Considers investigating alternative data visualization tools for more comprehensive business representation.

## Conclusion

This project pioneers a groundbreaking solution for real-time sentiment analysis and toxic comment classification, contributing significantly to the creation of safer online spaces. Despite initial challenges, the system adeptly processes streaming data, adapts to language trends, and emerges as a valuable tool for real-time data analysis.
