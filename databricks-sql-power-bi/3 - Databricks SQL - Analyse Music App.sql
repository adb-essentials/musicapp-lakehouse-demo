-- Databricks notebook source
describe history dlt_musicapp_lakehouse.top_artists_gold

-- COMMAND ----------

describe history dlt_musicapp_lakehouse.top_artists_gold

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Analyse Silver Table (fact table)
-- MAGIC 
-- MAGIC - Music App Listen Events (fact table) 

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT
-- MAGIC   artist_name,
-- MAGIC   COUNT(*) as total_listens,
-- MAGIC   COUNT(DISTINCT user_name) as unique_listens
-- MAGIC FROM
-- MAGIC   tfayyaz_musicapp.fct_listenbrainz_events
-- MAGIC WHERE event_date = "2019-01-01"
-- MAGIC GROUP BY
-- MAGIC   artist_name
-- MAGIC ORDER BY total_listens DESC
-- MAGIC LIMIT 1000

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Join Silver Tables and Create Gold Table
-- MAGIC 
-- MAGIC - Music App Listen Events (fact table) & Music Release Genres (dimension table)

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC WITH artist_release_listens AS (SELECT 
-- MAGIC   artist_name, 
-- MAGIC   release_name, 
-- MAGIC   COUNT(*) as listens
-- MAGIC FROM tfayyaz_musicapp.fct_listenbrainz_events
-- MAGIC WHERE event_date = "2019-01-01"
-- MAGIC GROUP BY 1, 2
-- MAGIC ORDER BY listens DESC),
-- MAGIC genres AS (
-- MAGIC SELECT *
-- MAGIC FROM tfayyaz_musicapp.dim_discogs_primary_releases
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   artist_name, 
-- MAGIC   release_name, 
-- MAGIC   b.genres,
-- MAGIC   listens
-- MAGIC FROM artist_release_listens AS a
-- MAGIC LEFT JOIN genres AS b
-- MAGIC ON lower(a.artist_name) = lower(b.artists) AND lower(a.release_name) = lower(b.title)
-- MAGIC ORDER BY listens DESC

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE TABLE tfayyaz_musicapp.gld_listens_genres 
-- MAGIC AS WITH artist_release_listens AS (SELECT 
-- MAGIC   artist_name, 
-- MAGIC   release_name, 
-- MAGIC   COUNT(*) as listens
-- MAGIC FROM tfayyaz_musicapp.fct_listenbrainz_events
-- MAGIC WHERE event_date = "2019-01-01"
-- MAGIC GROUP BY 1, 2
-- MAGIC ORDER BY listens DESC),
-- MAGIC genres AS (
-- MAGIC SELECT *
-- MAGIC FROM tfayyaz_musicapp.dim_discogs_primary_releases
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT 
-- MAGIC   artist_name, 
-- MAGIC   release_name, 
-- MAGIC   b.genres,
-- MAGIC   listens
-- MAGIC FROM artist_release_listens AS a
-- MAGIC LEFT JOIN genres AS b
-- MAGIC ON lower(a.artist_name) = lower(b.artists) AND lower(a.release_name) = lower(b.title)
-- MAGIC ORDER BY listens DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualise Gold Table in DBSQL Dashboard

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM tfayyaz_musicapp.gld_listens_genres

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Visualise Gold Table in PowerBI - Small Multiples

-- COMMAND ----------


