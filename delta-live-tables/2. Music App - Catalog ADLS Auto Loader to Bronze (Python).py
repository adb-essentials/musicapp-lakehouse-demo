# Databricks notebook source
import dlt
from pyspark.sql.types import *

# Path to ADLS json files 
source_file_path = "abfss://data@musicapp<storageaccountname>.dfs.core.windows.net/dlt_musicapp_demo/landing/musicapp_catalog"

# Path to store schema checkpoint for musicapp_catalog_bronze table
autoloader_schema_checkpoint_path = "abfss://data@musicapp<storageaccountname>.dfs.core.windows.net/dlt_musicapp_demo/musicapp_catalog_bronze/_schema"


@dlt.table(
  name="catalog_bronze",
  comment="The musicapp catalog bronze table",
  table_properties={
    "quality": "bronze"
  }
)
def catalog_bronze():
  return (spark
          .readStream.format("cloudFiles")
          .option("cloudFiles.format","json")
          .option("cloudFiles.schemaLocation", autoloader_schema_checkpoint_path)
          .load(source_file_path))
