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

# # music catalog
# musicCatalog = [
#     [10001, u'Damian Marley', u'Album 1', u'Welcome to Jamrock'],
#     [10002, u'Gorillaz', u'Album 2', u'Feel Good Inc.'],
#     [10002, u'Amy Winehouse', u'Album 3', u'Back to Black'],
#     [10003, u'Radiohead', u'Album 4', u'Pyramid Song'],
#     [10004, u'LCD Soundsystem', u'Album 5', u'Daft Punk is Playing at My House'],
#     [20001, u'LCD Soundsystem', u'Daft Punk is Playing at My House', u'Daft Punk is Playing at My House'],
#     [20002, u'LCD Soundsystem', u'Daft Punk is Playing at My House', u'Daft Punk Is Playing At My House (Soulwax Shibuya Mix)'],
#     [20003, u'Damian Marley', u'Welcome To Jamrock', u'Confrontation'],
#     [20004, u'Damian Marley', u'Welcome To Jamrock', u'There For You'],
#     [20005, u'Damian Marley', u'Welcome To Jamrock', u'Welcome To Jamrock'],
#     [20006, u'Damian Marley', u'Welcome To Jamrock', u'The Master Has Come Back'],
#     [20007, u'Damian Marley', u'Welcome To Jamrock', u'All Night'],
#     [20008, u'Damian Marley', u'Welcome To Jamrock', u'Beautiful'],
#     [20009, u'Damian Marley', u'Welcome To Jamrock', u'Pimpas Paradise'],
#     [20010, u'Damian Marley', u'Welcome To Jamrock', u'Move!'],
#     [20011, u'Damian Marley', u'Welcome To Jamrock', u'For The Babies'],
#     [20012, u'Damian Marley', u'Welcome To Jamrock', u'Hey Girl'],
#     [20013, u'Damian Marley', u'Welcome To Jamrock', u'Road To Zion'],
#     [20014, u'Damian Marley', u'Welcome To Jamrock', u'Were Gonna Make It'],
#     [20015, u'Damian Marley', u'Welcome To Jamrock', u'In 2 Deep'],
#     [20016, u'Damian Marley', u'Welcome To Jamrock', u'Khaki Suit'],
#     [20017, u'Gorillaz', u'Demon Days', u'Intro'],
#     [20018, u'Gorillaz', u'Demon Days', u'Last Living Souls'],
#     [20019, u'Gorillaz', u'Demon Days', u'Kids With Guns'],	
#     [20020, u'Gorillaz', u'Demon Days', u'O Green World'],	
#     [20021, u'Gorillaz', u'Demon Days', u'Dirty Harry'],	
#     [20022, u'Gorillaz', u'Demon Days', u'Feel Good Inc.'],	
#     [20023, u'Gorillaz', u'Demon Days', u'El Manana'],	
#     [20024, u'Gorillaz', u'Demon Days', u'Every Planet We Reach Is Dead'],	
#     [20025, u'Gorillaz', u'Demon Days', u'November Has Come'],	
#     [20026, u'Gorillaz', u'Demon Days', u'All Alone'],	
#     [20027, u'Gorillaz', u'Demon Days', u'White Light'],	
#     [20028, u'Gorillaz', u'Demon Days', u'Dare'],	
#     [20029, u'Gorillaz', u'Demon Days', u'Fire Coming Out Of The Monkeys Head'],
#     [20030, u'Gorillaz', u'Demon Days', u'Dont Get Lost In Heaven'],	
#     [20031, u'Gorillaz', u'Demon Days', u'Demon Days']
# ]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Define file path for all generated data to be stored

# COMMAND ----------

# Change STORAGE_ACCOUNT to your storage account name
STORAGE_ACCOUNT="musicapp<storageaccountname>"

# COMMAND ----------

file_path = f"abfss://data@{STORAGE_ACCOUNT}.dfs.core.windows.net/musicapp_lakehouse/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upload Music App Catalog CSV to ADLS
# MAGIC 
# MAGIC - Download Music App Catalog CSV file from github repo
# MAGIC - Go to ADLS storage account and create new directories `/musicapp_lakehouse/staging/music_catalog`
# MAGIC - Upload the CSV `musicapp_catalog_001.csv` to `/musicapp_lakehouse/staging/music_catalog`
# MAGIC - Check the file was uploaded correctrly using the command below

# COMMAND ----------

dbutils.fs.ls(f"{file_path}/staging/musicapp_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Music App Catalog JSON in ADLS

# COMMAND ----------

# read data from staging
df_catalog = (spark.read
               .option("header", True)
               .csv(f"{file_path}/staging/musicapp_catalog"))

display(df_catalog)

# COMMAND ----------

from pyspark.sql.types import IntegerType,BooleanType,DateType

df = (df_catalog.select(["song_id", "artist_name", "album_name", "song_name"])
     .withColumn("song_id",df_catalog.song_id.cast('int')))

display(df)

# COMMAND ----------

# df = spark.createDataFrame(musicCatalog, ["song_id", "artist_name", "album_name", "song_name"])
# display(df)

# COMMAND ----------

# Write Dataframe as JSON file to landing zone
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
EH_KAFKA_TOPIC = "music-listen-events"

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

# COMMAND ----------


