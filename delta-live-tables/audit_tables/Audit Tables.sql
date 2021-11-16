-- Databricks notebook source
use dlt_musicapp_lakehouse_<USERNAME>;

-- COMMAND ----------

show tables

-- COMMAND ----------

describe events_enriched_silver

-- COMMAND ----------

describe detail events_enriched_silver

-- COMMAND ----------

describe history events_enriched_silver
