# Databricks notebook source
file_path = "abfss://data@dltdemostorage.dfs.core.windows.net/dlt_musicapp_demo/landing/music_catalog"

df = (spark.readStream.format("cloudFiles") 
  .option("cloudFiles.format", "json") 
  # The schema location directory keeps track of your data schema over time
  .option("cloudFiles.schemaLocation", f"{file_path}/_schema_checkpoint") 
  .load(file_path) )

display(df)

# COMMAND ----------

  .writeStream \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", "<path_to_checkpoint>") \
  .start("<path_to_target")

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   `artist_name`,
# MAGIC   `album_name`,
# MAGIC   count(`user_id`) as `C1`,
# MAGIC   cast(count(distinct(`user_id`)) as DOUBLE) + cast(max(`C1`) as DOUBLE) as `C2`
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       `event_date`,
# MAGIC       `event_timestamp`,
# MAGIC       `key`,
# MAGIC       `event_type`,
# MAGIC       `user_id`,
# MAGIC       `user_country`,
# MAGIC       `song_id`,
# MAGIC       `song_name`,
# MAGIC       `artist_name`,
# MAGIC       `album_name`,
# MAGIC       case
# MAGIC         when `user_id` is null then 1
# MAGIC         else 0
# MAGIC       end as `C1`
# MAGIC     from
# MAGIC       `hive_metastore`.`dlt_musicapp_lakehouse`.`events_enriched_silver`
# MAGIC     where
# MAGIC       `user_country` = 'NL'
# MAGIC   ) as `ITBL`
# MAGIC group by
# MAGIC   `artist_name`,
# MAGIC   `album_name`
# MAGIC limit
# MAGIC   1000001

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   `artist_name`,
# MAGIC   `album_name`,
# MAGIC   count(`user_id`) as `C1`,
# MAGIC   cast(count(distinct(`user_id`)) as DOUBLE) + cast(max(`C1`) as DOUBLE) as `C2`
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       `event_date`,
# MAGIC       `event_timestamp`,
# MAGIC       `key`,
# MAGIC       `event_type`,
# MAGIC       `user_id`,
# MAGIC       `user_country`,
# MAGIC       `song_id`,
# MAGIC       `song_name`,
# MAGIC       `artist_name`,
# MAGIC       `album_name`,
# MAGIC       case
# MAGIC         when `user_id` is null then 1
# MAGIC         else 0
# MAGIC       end as `C1`
# MAGIC     from
# MAGIC       `hive_metastore`.`dlt_musicapp_lakehouse`.`events_enriched_silver`
# MAGIC     where
# MAGIC       `user_country` = 'UK'
# MAGIC   ) as `ITBL`
# MAGIC group by
# MAGIC   `artist_name`,
# MAGIC   `album_name`
# MAGIC limit
# MAGIC   1000001

# COMMAND ----------


