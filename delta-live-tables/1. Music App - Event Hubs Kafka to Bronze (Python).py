# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

EH_NAMESPACE = "musicapp-<eventhubname>"
EH_KAFKA_TOPIC = "musicapp-events"
EH_LISTEN_KEY_NAME = f"ehListen{EH_NAMESPACE}AccessKey"

# COMMAND ----------

# Get Databricks secret value 
connSharedAccessKeyName = "adbListenMusicAppEvents"
connSharedAccessKey = dbutils.secrets.get(scope = "access_creds", key = EH_LISTEN_KEY_NAME)

# COMMAND ----------

# Set Kafka Config (Do not modify)
EH_BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={connSharedAccessKeyName};SharedAccessKey={connSharedAccessKey};EntityPath={EH_KAFKA_TOPIC}\";"

# COMMAND ----------

# # Event Hubs Kafka Connection
# connSharedAccessKey = dbutils.secrets.get(scope = "adls_creds", key = "eventHubKafkaKey")

# TOPIC = "music-listen-events"
# BOOTSTRAP_SERVERS = "tfayyaz-kafka.servicebus.windows.net:9093"
# EH_SASL = "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://tfayyaz-kafka.servicebus.windows.net/;SharedAccessKeyName=KafkaSendListen;SharedAccessKey=" + connSharedAccessKey + "\";"

# COMMAND ----------

stream_musicapp_events_kafka_raw = (spark.readStream
    .format("kafka")
    .option("subscribe", EH_KAFKA_TOPIC)
    .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("kafka.request.timeout.ms", "60000")
    .option("kafka.session.timeout.ms", "60000")
    .option("failOnDataLoss", "false")
    .option("startingOffsets", "latest")
    .load())

# display(stream_musicapp_events_kafka_raw)

# COMMAND ----------

# stream_musicapp_events_kafka_raw = (spark.readStream
#     .format("kafka")
#     .option("subscribe", EH_KAFKA_TOPIC)
#     .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS)
#     .option("kafka.sasl.mechanism", "PLAIN")
#     .option("kafka.security.protocol", "SASL_SSL")
#     .option("kafka.sasl.jaas.config", EH_SASL)
#     .option("kafka.request.timeout.ms", "60000")
#     .option("kafka.session.timeout.ms", "60000")
#     .option("failOnDataLoss", "false")
#     .option("startingOffsets", "latest")
#     .load())

# display(stream_musicapp_events_kafka_raw)

# COMMAND ----------

import dlt

@dlt.table(
  name="events_kafka_raw",
  comment="The music app listen events dataset, ingested from Event Hubs kafka topic.",
  table_properties={
    "quality": "raw"
  }
)
@dlt.expect("valid_kafka_key_not_null", "key IS NOT NULL")
def events_kafka_raw():
  return stream_musicapp_events_kafka_raw

# COMMAND ----------

# musicapp_events_parsed_schema = StructType([ \
#     StructField("event_type", StringType(), True), \
#     StructField("user_id", IntegerType(),True), \
#     StructField("user_country", StringType(),True), \
#     StructField("song_id", IntegerType(),True)
#   ])

# COMMAND ----------

# stream_musicapp_events_parsed = (stream_musicapp_events_kafka_raw  
#     .select(col("timestamp"),
#             col("key").cast("string"),
#             from_json(col("value").cast("string"), musicapp_events_parsed_schema).alias("parsed_value")
#            )
#     .select("timestamp",
#             "key",
#             "parsed_value.*")
# )

# # display(stream_musicapp_events_parsed)

# COMMAND ----------

musicapp_events_parsed_schema = StructType([ \
    StructField("event_type", StringType(), True), \
    StructField("user_id", IntegerType(),True), \
    StructField("user_country", StringType(),True), \
    StructField("song_id", IntegerType(),True)
  ])

@dlt.table(
  name="events_parsed_bronze",
  comment="Music app events with value parsed as columns.",
  table_properties={
    "quality": "bronze"
  }
)
@dlt.expect("valid_user_id", "user_id IS NOT NULL")
def events_parsed_bronze():
  return (
    dlt.read_stream("events_kafka_raw")  
    .select(col("timestamp"),
            col("key").cast("string"),
            from_json(col("value").cast("string"), musicapp_events_parsed_schema).alias("parsed_value")
           )
    .select("timestamp",
            "key",
            "parsed_value.*")
  )
