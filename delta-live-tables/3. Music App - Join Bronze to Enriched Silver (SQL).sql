-- Databricks notebook source
-- show tables in dlt_musicapp_lakehouse

-- COMMAND ----------

-- select * 
-- from dlt_musicapp_lakehouse.musicapp_events_parsed_bronze
-- limit 10

-- COMMAND ----------

-- select * 
-- from dlt_musicapp_lakehouse.musicapp_catalog_bronze
-- limit 10

-- COMMAND ----------

-- select 
--   to_date(timestamp) as event_date
--   ,timestamp as event_timestamp
--   ,key
--   ,event_type
--   ,user_id
--   ,user_country
--   ,events.song_id
--   ,song_name
--   ,artist_name
--   ,album_name
-- from dlt_musicapp_lakehouse.musicapp_events_parsed_bronze as events
-- left join dlt_musicapp_lakehouse.musicapp_catalog_bronze as catalog
-- on events.song_id = catalog.song_id
-- limit 10

-- COMMAND ----------

create INCREMENTAL LIVE table events_enriched_silver(
  CONSTRAINT valid_song_id EXPECT (song_id > 10000) ON VIOLATION FAIL UPDATE
)
partitioned by ( event_date )
comment "partitioned events enriched table"
as select 
  to_date(timestamp) as event_date
  ,timestamp as event_timestamp
  ,key
  ,event_type
  ,user_id
  ,user_country
  ,events.song_id
  ,song_name
  ,artist_name
  ,album_name
from stream(live.events_parsed_bronze) as events
left join live.catalog_bronze as catalog
on events.song_id = catalog.song_id
