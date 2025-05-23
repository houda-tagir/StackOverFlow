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
 python3 stack_api_to_kafka.py
#####################
spark-submit   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0   spark_stream_to_hbase.py
