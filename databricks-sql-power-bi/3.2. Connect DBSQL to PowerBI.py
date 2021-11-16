# Databricks notebook source
# MAGIC %md
# MAGIC # 3.2. Connect DBSQL to PowerBI

# COMMAND ----------

# MAGIC %md
# MAGIC ## Refresh datasets in PowerBI
# MAGIC 
# MAGIC ### Option 1: Use Auto Page Refresh

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   artist_name, 
# MAGIC   COUNT(*) as total_listens
# MAGIC FROM tfayyaz_musicapp.events_partitions_gencol
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY total_listens DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   artist_name, 
# MAGIC   COUNT(*) as total_listens
# MAGIC FROM tfayyaz_musicapp.events_partitions_gencol
# MAGIC WHERE LOWER(artist_name) = "billie eilish"
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY total_listens DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE tfayyaz_musicapp.events_partitions_gencol 
# MAGIC SET artist_name = 'Billie Eilish' 
# MAGIC WHERE LOWER(artist_name) = "billie eilish"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   artist_name, 
# MAGIC   COUNT(*) as total_listens
# MAGIC FROM tfayyaz_musicapp.events_partitions_gencol
# MAGIC WHERE LOWER(artist_name) = "billie eilish"
# MAGIC GROUP BY artist_name
# MAGIC ORDER BY total_listens DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE tfayyaz_musicapp.events_partitions_gencol 
# MAGIC SET artist_name = 'Billie Eilish' 
# MAGIC WHERE LOWER(artist_name) = "billie eilish update"

# COMMAND ----------


