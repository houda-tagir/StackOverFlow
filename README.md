# 
Commands I  used in wsl -d Ubuntu (root@Houda:/mnt/c/Users/htagi#):
cd  kafka-spark-hbase
##################
 docker compose up -d
###################
 docker ps --format "table {{.Names}}\t{{.Status}}"
###################
pip install \
    kafka-python \
    requests \
    happybase \
    python-dateutil \
    pandas \
    pyspark
##################
python3 -m venv stackoverflow-env
####################
cd ~
####################
source venv/bin/activate
#################
 docker run --rm -it \
  --network kafka-spark-hbase_default \
  bitnami/kafka:latest \
  kafka-topics.sh \
    --create \
    --topic stackoverflow-questions \
    --bootstrap-server kafka:9092 \
    --partitions 1 \
    --replication-factor 1
####################
create the second topic:
kafka-topics.sh \
  --bootstrap-server localhost:29092 \
  --create \
  --replication-factor 1 \
  --partitions 1 \
  --topic stackoverflow-trends
#########################
 python3 hbase_implementation.py
 ####################
 python3 stack_api_to_kafka.py
#####################
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0   spark_stream_to_hbase.py


and these are the makets Of my Application:
this maket is for the Admin:
![image alt](https://github.com/houda-tagir/StackOverFlow/blob/main/Screenshot%202025-05-25%20121639.png?raw=true)
the makets of The User are the following ones:
![image alt](https://github.com/houda-tagir/StackOverFlow/blob/main/Screenshot%202025-05-25%20121659.png?raw=true)
important one
![image alt](https://github.com/houda-tagir/StackOverFlow/blob/main/Screenshot%202025-05-25%20121732.png?raw=true)

![image alt](https://github.com/houda-tagir/StackOverFlow/blob/main/Screenshot%202025-05-25%20121854.png?raw=true)
important one
![image alt](https://github.com/houda-tagir/StackOverFlow/blob/main/Screenshot%202025-05-25%20121919.png?raw=true)

#now what i need you to do for me
I want you to act as a prompt engineer and project guide for my AI agent.
I’ll explain the context of my project, and you'll help me to corredt my codes or generate code for me to realise this application:

Project Overview
My application is a StackOverflow Search Optimizer and Tag Trend Analyzer.
It has two main features:

Search Optimizer: Fetches top 3 answers for each question using filters like score, upvotes, is_accepted=true, and user reputation.

Trend Analyzer: Shows trends over year/month/week for tags like python, including how many questions were: unanswered, answered, accepted.

Big Data Stack (In Progress)

Kafka gets data from the StackExchange API.

Spark processes the data, but we’re struggling to:

Strip HTML tags (e.g. <p>).

Get full content from question/answer bodies (only partial sentence appears).

We use HBase for storage.

Backend (Spring Boot)

A project already exists on the master branch.

I need help improving it and integrating features like:

Search functionality (with filters and suggestions).

Real-time data access from HBase/Spark.

Frontend (React.js)

We’ll build a user interface based on a Figma design (will be provided).

Users can search questions and visualize tag trends.

Trends should be animated for real-time effect.

Resources Available

I’ll provide code for:

StackApiToKafka

StreamSparkToHBase

HBase implementation

Figma file for UI mockups
