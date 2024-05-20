# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-cee8cef7-2341-42cd-beef-9a60958d8e70
# MAGIC %md
# MAGIC # 新しいデータの取り込み(Land New Data)
# MAGIC
# MAGIC このノートブックは、すでに設定されているDelta Live Tablesパイプラインで処理される新しいデータバッチを手動でトリガーすることのみを目的として提供されています。
# MAGIC
# MAGIC 提供されるロジックは、最初のインタラクティブなノートブックで提供されるものと同じですが、パイプラインのソースまたはターゲットディレクトリをリセットしません。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.99

# COMMAND ----------

# DBTITLE 0,--i18n-884cce97-7746-4b9e-abe8-1fdbd38124bf
# MAGIC %md
# MAGIC このセルを実行するたびに、新しいデータファイルのバッチが、これらのレッスンで使用されるソースディレクトリにロードされます。

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-f9e15f42-07c9-4522-9c3f-7e0c975ad41d
# MAGIC %md
# MAGIC # 残りの全データの取り込み(Land All Remaining Data)
# MAGIC
# MAGIC または、以下のセルのコメントを解除して実行することで、残りのすべてのバッチのデータをロードすることができます。

# COMMAND ----------

# TODO
This should run for a little over 5 minutes. To abort
early, click the Stop Execution button above
DA.dlt_data_factory.load(continuous=True, delay_seconds=10)

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
