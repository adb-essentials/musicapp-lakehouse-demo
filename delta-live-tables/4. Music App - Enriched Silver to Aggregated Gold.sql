-- Databricks notebook source
-- show tables in dlt_musicapp_lakehouse

-- COMMAND ----------

-- select * from dlt_musicapp_lakehouse.musicapp_events_enriched_silver

-- COMMAND ----------

-- select 
--   user_country 
--   , artist_name
--   , count(user_id) as total_listens
--   , count(distinct user_id) as unique_listens
-- from dlt_musicapp_lakehouse.musicapp_events_enriched_silver
-- group by 
--   user_country 
--   , artist_name
-- order by unique_listens desc

-- COMMAND ----------

create live table top_artists_gold(
  constraint valid_unique_listens expect (unique_listens > 0) on violation fail update
)
comment "top artists by country - gold table"
as select 
  user_country 
  , artist_name
  , count(user_id) as total_listens
  , count(distinct user_id) as unique_listens
from live.events_enriched_silver
group by 
  user_country 
  , artist_name
order by unique_listens desc 

-- COMMAND ----------

create live table top_songs_gold(
  constraint valid_unique_listens expect (unique_listens > 0) on violation fail update
)
comment "top songs by country - gold table"
as select 
  user_country 
  , song_name
  , artist_name
  , count(user_id) as total_listens
  , count(distinct user_id) as unique_listens
from live.events_enriched_silver
group by 
  user_country 
  , song_name
  , artist_name
order by unique_listens desc 

-- COMMAND ----------

create live table top_albums_gold(
  constraint valid_unique_listens expect (unique_listens > 0) on violation fail update
)
comment "top albums by country - gold table"
as select 
  user_country 
  , album_name
  , artist_name
  , count(user_id) as total_listens
  , count(distinct user_id) as unique_listens
from live.events_enriched_silver
group by 
  user_country 
  , album_name
  , artist_name
order by unique_listens desc 
