# Databricks notebook source
# MAGIC %run ../Includes/Classroom-Setup-05.2.4L
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-2b2de306-5587-4e60-aa3f-34ae6c344907
# MAGIC %md
# MAGIC # DLTパイプラインの結果を確認(Exploring the Results of a DLT Pipeline)
# MAGIC
# MAGIC 次のセルを実行して、保存場所を列挙してください：

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-f8f98f46-f3d3-41e5-b86a-fc236813e67e
# MAGIC %md
# MAGIC **system** ディレクトリがパイプラインに関連するイベントをキャプチャしています。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-b8afa35d-667e-40b8-915c-d754d5bdb5ee
# MAGIC %md
# MAGIC これらのイベントログはDeltaテーブルに保存されています。
# MAGIC
# MAGIC テーブルにクエリーを出してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.storage_location}/system/events`

# COMMAND ----------

# DBTITLE 0,--i18n-adc883c4-30ec-4391-8d7f-9554f77a0feb
# MAGIC %md
# MAGIC *tables* ディレクトリのコンテンツを見てみましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-72eb29d2-cde0-4488-954b-a0ed47ead8eb
# MAGIC %md
# MAGIC Goldテーブルにクエリーを出してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM ${DA.schema_name}.daily_patient_avg

# COMMAND ----------

DA.cleanup()
