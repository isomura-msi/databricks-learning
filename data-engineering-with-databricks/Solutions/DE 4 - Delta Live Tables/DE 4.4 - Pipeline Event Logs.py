# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-fea707eb-314a-41a8-8da5-fdac27ebe622
# MAGIC %md
# MAGIC # パイプラインのイベントログを調べる (Exploring the Pipeline Events Logs)
# MAGIC DLTは、パイプラインの実行中に何が起こっているかを管理、報告、理解するために使用される重要な情報の多くをイベントログに保存します。
# MAGIC
# MAGIC 以下では、イベントログを探索し、DLTパイプラインをより深く理解するための有用なクエリーをいくつか紹介します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.4

# COMMAND ----------

# DBTITLE 0,--i18n-db58d66a-73bf-412a-ae17-b00f98338f56
# MAGIC %md
# MAGIC ## イベントログへのクエリ(Query Event Log)
# MAGIC イベントログはDelta Lakeのテーブルとして管理され、より重要なフィールドの一部はネストしたJSONデータとして保存されます。
# MAGIC
# MAGIC 次のセルで、このテーブルを読むのがいかに簡単かを示します。インタラクティブなクエリのため、DataFrameと一時ビューを作成しました。

# COMMAND ----------

event_log_path = f"{DA.paths.storage_location}/system/events"

event_log = spark.read.format('delta').load(event_log_path)
event_log.createOrReplaceTempView("event_log_raw")

display(event_log)

# COMMAND ----------

# DBTITLE 0,--i18n-b5f6dcac-b958-4809-9942-d45e475b6fb7
# MAGIC %md
# MAGIC ## 最新のアップデートIDを設定する(Set Latest Update ID)
# MAGIC
# MAGIC 多くの場合、パイプラインの最新アップデート（または直近のN回のアップデート）に関する情報を得たいと思うかもしれません。
# MAGIC
# MAGIC SQLクエリで簡単に最新の更新IDを取得することができます。

# COMMAND ----------

latest_update_id = spark.sql("""
    SELECT origin.update_id
    FROM event_log_raw
    WHERE event_type = 'create_update'
    ORDER BY timestamp DESC LIMIT 1""").first().update_id

print(f"Latest Update ID: {latest_update_id}")

# Push back into the spark config so that we can use it in a later query.
spark.conf.set('latest_update.id', latest_update_id)

# COMMAND ----------

# DBTITLE 0,--i18n-de7c7817-fcfd-4994-beb0-704099bd5c30
# MAGIC %md
# MAGIC ## 監査ログの振る舞いの(Perform Audit Logging)
# MAGIC
# MAGIC パイプラインの実行や設定の編集に関連するイベントは、**`user_action`**として取り込まれます。
# MAGIC
# MAGIC このレッスンで設定したパイプラインの **`user_name`** は、あなたのものだけであるべきです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'user_action'

# COMMAND ----------

# DBTITLE 0,--i18n-887a16ce-e1a5-4d27-bacb-7e6c84cbaf37
# MAGIC %md
# MAGIC ## リネージュの確認(Examine Lineage)
# MAGIC
# MAGIC DLTは、データがテーブルの中をどのように流れるかについてのbuilt-inのリネージュ情報を提供しています。
# MAGIC
# MAGIC 以下のクエリーは、各テーブルの直接の入力テーブルを示すだけですが、この情報を組み合わせることで、どのテーブルについても入力データがレイクハウスに入る時点まで簡単にさかのぼることができます。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
# MAGIC FROM event_log_raw 
# MAGIC WHERE event_type = 'flow_definition' AND 
# MAGIC       origin.update_id = '${latest_update.id}'

# COMMAND ----------

# DBTITLE 0,--i18n-1b1c0687-163f-4684-a570-3cf4cc32c272
# MAGIC %md
# MAGIC ## データ品質指標の確認(Examine Data Quality Metrics)
# MAGIC
# MAGIC 最後に、データ品質メトリクスは、データの長期的な洞察と短期的な洞察の両方において非常に有用です。
# MAGIC
# MAGIC 以下では、テーブルのライフタイム全体を通して、各制約のメトリクスをキャプチャします。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT row_expectations.dataset as dataset,
# MAGIC        row_expectations.name as expectation,
# MAGIC        SUM(row_expectations.passed_records) as passing_records,
# MAGIC        SUM(row_expectations.failed_records) as failing_records
# MAGIC FROM
# MAGIC   (SELECT explode(
# MAGIC             from_json(details :flow_progress :data_quality :expectations,
# MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
# MAGIC           ) row_expectations
# MAGIC    FROM event_log_raw
# MAGIC    WHERE event_type = 'flow_progress' AND 
# MAGIC          origin.update_id = '${latest_update.id}'
# MAGIC   )
# MAGIC GROUP BY row_expectations.dataset, row_expectations.name

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
