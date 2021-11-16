# Databricks notebook source
# MAGIC %md
# MAGIC # Generate music app catalog and events

# COMMAND ----------

# MAGIC %md
# MAGIC ### Producer
# MAGIC 
# MAGIC The producer will generate music app catalog data in ADLS and listen event data as a real-time stream in Event Hubs (Kafka protocol)

# COMMAND ----------

from pyspark.sql.functions import lit, col
from pyspark.sql.types import LongType, StringType
from pyspark.sql.functions import udf
import random
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define file path for all generated data to be stored

# COMMAND ----------

# Change STORAGE_ACCOUNT to your storage account name
STORAGE_ACCOUNT="musicapp<storageaccountname>"

file_path = f"abfss://data@{STORAGE_ACCOUNT}.dfs.core.windows.net/musicapp_lakehouse/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Music App Catalog CSV to ADLS
# MAGIC 
# MAGIC - Read Music App Catalog CSV file from github repo as pandas DataFrame
# MAGIC - Convert Pandas DataFrame to Spark DataFrame
# MAGIC - Write Spark DataFrame as one JSON file to ADLS to be read by Delta Live Tables pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Music App Catalog JSON in ADLS

# COMMAND ----------

# select columns for ADLS and cast song_id as integer
from pyspark.sql.types import IntegerType,BooleanType,DateType

df = (df_catalog.select(["song_id", "artist_name", "album_name", "song_name"])
     .withColumn("song_id",df_catalog.song_id.cast('int')))

display(df)

# COMMAND ----------

# Write Dataframe as one JSON file to landing zone in ADLS
df.coalesce(1).write.mode("overwrite").json(file_path + '/landing/musicapp_catalog')

# COMMAND ----------

# check file was written
dbutils.fs.ls(file_path + '/landing/musicapp_catalog/')

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Music App Listen Events

# COMMAND ----------

def randomCountry():
  validCountries= [u'UK', u'DE', u'US', u'FR', u'JP', u'ES', u'NL', u'SE', u'PT', u'IT']
  return validCountries[random.randint(0,len(validCountries)-1)]
  
# def randomArtistSong():
#   validArtistSongs = musicCatalog
#   return validArtistSongs[random.randint(0,len(validArtistSongs)-1)]

def randomArtistSong():
  return [random.randint(30001,30084)]

def randomUserId():
  return random.randint(1000,50000)

# COMMAND ----------

def genMusicAppEventSchema1():
    
    artist_song = randomArtistSong()
    event = {
      "event_type": u"song_listen",
      "user_id": randomUserId(),
      "user_country": randomCountry(),
      "song_id": artist_song[0]
    }

    event_string = json.dumps(event)
    return event_string
#     return event
  
print(genMusicAppEventSchema1())

# COMMAND ----------

# MAGIC %md
# MAGIC #### Create stream of events

# COMMAND ----------

gen_event_schema_1_udf = udf(genMusicAppEventSchema1, StringType())

source_schema = (
  spark.readStream
    .format("rate")
    .option("rowsPerSecond", 1000)
    .load()
    .withColumn("key", lit("event"))
    .withColumn("value", lit(gen_event_schema_1_udf()))
    .select("key", col("value").cast("string"))
)

display(source_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write to stream to Event Hubs Kafka

# COMMAND ----------

EH_NAMESPACE = "musicapp-<eventhubname>"
EH_KAFKA_TOPIC = "musicapp-events"

# COMMAND ----------

# Get Databricks secret value 
connSharedAccessKeyName = "adbSendMusicAppEvents"
connSharedAccessKey = dbutils.secrets.get(scope = "access_creds", key = f"ehSend{EH_NAMESPACE}AccessKey")

# COMMAND ----------

EH_BOOTSTRAP_SERVERS = f"{EH_NAMESPACE}.servicebus.windows.net:9093"
EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"Endpoint=sb://{EH_NAMESPACE}.servicebus.windows.net/;SharedAccessKeyName={connSharedAccessKeyName};SharedAccessKey={connSharedAccessKey};EntityPath={EH_KAFKA_TOPIC}\";"

# COMMAND ----------

from datetime import datetime
# helps avoiding loading and writing all historical data. 
datetime_checkpoint = datetime.now().strftime('%Y%m%d%H%M%S')

# Write df to EventHubs using Spark's Kafka connector
write = (source_schema.writeStream
    .format("kafka")
    .outputMode("append")
    .option("topic", EH_KAFKA_TOPIC)
    .option("kafka.bootstrap.servers", EH_BOOTSTRAP_SERVERS)
    .option("kafka.sasl.mechanism", "PLAIN")
    .option("kafka.security.protocol", "SASL_SSL")
    .option("kafka.sasl.jaas.config", EH_SASL)
    .option("checkpointLocation", f"/tmp/{EH_NAMESPACE}/{EH_KAFKA_TOPIC}/{datetime_checkpoint}/_checkpoint")
    .trigger(processingTime='10 seconds')
    .start())
