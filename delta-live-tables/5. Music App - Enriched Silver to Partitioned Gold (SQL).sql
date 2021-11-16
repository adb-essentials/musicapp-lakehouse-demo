-- Databricks notebook source
-- MAGIC %md
-- MAGIC Link to [DLT pipeline](https://eastus2.azuredatabricks.net/?o=5206439413157315#joblist/pipelines/24aa553b-8285-4ad4-8094-d61759d3d8ee)

-- COMMAND ----------

CREATE LIVE TABLE top_musicapp_users
COMMENT "A table containing the top users"
AS SELECT
  to_date(timestamp) as eventdate,
  user_id,
  count(*) as play_count
FROM LIVE.musicapp_events_parsed
GROUP BY eventdate, user_id
ORDER BY play_count DESC
LIMIT 50;

-- COMMAND ----------

CREATE LIVE TABLE top_musicapp_artists
COMMENT "A table containing the top artists"
AS SELECT
  to_date(timestamp) as eventdate,
  artist,
  count(*) as play_count
FROM LIVE.musicapp_events_parsed
GROUP BY eventdate, artist
ORDER BY play_count DESC
LIMIT 50;

-- COMMAND ----------

CREATE LIVE TABLE top_musicapp_albums
COMMENT "A table containing the top albums"
AS SELECT
  to_date(timestamp) as eventdate,
  artist,
  album,
  count(*) as play_count
FROM LIVE.musicapp_events_parsed
GROUP BY eventdate, artist, album
ORDER BY play_count DESC
LIMIT 50;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create [INCREMENTAL AGGREGATION tables](https://docs.microsoft.com/en-us/azure/databricks/data-engineering/delta-live-tables/delta-live-tables-user-guide#--incremental-aggregation)

-- COMMAND ----------

CREATE INCREMENTAL LIVE TABLE top_musicapp_albums_incremental
COMMENT "A table containing the top albums with incremental agregation"
AS SELECT
  to_date(timestamp) as eventdate,
  artist,
  album,
  count(*) as play_count
FROM STREAM(LIVE.musicapp_events_parsed)
GROUP BY eventdate, artist, album

-- COMMAND ----------

-- SHOW TABLES IN tfayyaz_musicdemo2

-- COMMAND ----------

-- SELECT
--   to_date(timestamp) as eventdate,
--   ( CAST(timestamp AS DATE) ) as yymmdd,
--   userid,
--   count(*) as play_count
-- FROM tfayyaz_musicdemo2.musicapp_events_parsed
-- GROUP BY eventdate, userid
-- ORDER BY play_count DESC
-- LIMIT 50;

-- COMMAND ----------

-- CREATE LIVE TABLE top_musicapp_users
-- COMMENT "A table containing the top users"
-- AS SELECT
--   to_date(timestamp) as eventdate,
--   userid,
--   count(*) as play_count
-- FROM LIVE.musicapp_events_parsed
-- GROUP BY eventdate, userid
-- ORDER BY play_count DESC
-- LIMIT 50;
