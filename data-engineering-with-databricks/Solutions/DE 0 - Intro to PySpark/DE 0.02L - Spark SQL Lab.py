# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-da4e23df-1911-4f58-9030-65da697d7b61
# MAGIC %md
# MAGIC # Spark SQL ラボ (Spark SQL Lab)
# MAGIC
# MAGIC ##### タスク (Tasks)
# MAGIC 1. <strong>`events`</strong>テーブルからDataFrameを作成します
# MAGIC 1. DataFrame を表示し、そのスキーマを調べる
# MAGIC 1. <strong>`macOS`</strong>イベントのフィルタリングとソートにトランスフォーメーションを適用する
# MAGIC 1. 結果をカウントし、最初の5行を取る
# MAGIC 1. SQLクエリを使用して同じDataFrameを作成する
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>トランスフォーメーション: **`select`**, **`where`**, **`orderBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.html" target="_blank">DataFrame</a>アクション: **`select`**, **`count`**, **`take`**
# MAGIC - その他の<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>メソッド: **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.02L

# COMMAND ----------

# DBTITLE 0,--i18n-e0f3f405-8c97-46d1-8550-fb8ff14e5bd6
# MAGIC %md
# MAGIC ### 1.**`events`** テーブルから DataFrame を作成します (Create a DataFrame from the **`events`** table)
# MAGIC - SparkSessionを使用して<strong>`events`</strong>テーブルからDataFrameを作成します

# COMMAND ----------

# ANSWER
events_df = spark.table("events")

# COMMAND ----------

# DBTITLE 0,--i18n-fb5458a0-b475-4d77-b06b-63bb9a18d586
# MAGIC %md
# MAGIC ### 2.DataFrame を表示してスキーマを調べる (Display DataFrame and inspect schema)
# MAGIC - 上記のメソッドを使用して、DataFrame のコンテンツとスキーマを確認します

# COMMAND ----------

# ANSWER
display(events_df)

# COMMAND ----------

# ANSWER
events_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-76adfcb2-f182-485c-becd-9e569d4148b6
# MAGIC %md
# MAGIC ### 3. <strong>`macOS`</strong>イベントの抽出とソートにトランスフォーメーションを適用する (Apply transformations to filter and sort **`macOS`** events)
# MAGIC - <strong>`device`</strong>が<strong>`macOS`</strong>である行を抽出
# MAGIC - <strong>`event_timestamp`</strong>で行をソート
# MAGIC
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_32.png" alt="Hint"> 抽出のSQL式で一重引用符と二重引用符を使用します

# COMMAND ----------

# ANSWER
mac_df = (events_df
          .where("device == 'macOS'")
          .sort("event_timestamp")
         )

# COMMAND ----------

# DBTITLE 0,--i18n-81f8748d-a154-468b-b02e-ef1a1b6b2ba8
# MAGIC %md
# MAGIC ### 4. 結果をカウントし、最初の 5 行を取る (Count results and take first 5 rows)
# MAGIC - DataFrame アクションを使用して、行を数えて取得します

# COMMAND ----------

# ANSWER
num_rows = mac_df.count()
rows = mac_df.take(5)

# COMMAND ----------

# DBTITLE 0,--i18n-4e340689-5d23-499a-9cd2-92509a646de6
# MAGIC %md
# MAGIC **4.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

from pyspark.sql import Row

assert(num_rows == 97150)
assert(len(rows) == 5)
assert(type(rows[0]) == Row)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-cbb03650-db3b-42b3-96ee-54ea9b287ab5
# MAGIC %md
# MAGIC ### 5. SQLクエリを使用して同じDataFrameを作成する (Create the same DataFrame using SQL query)
# MAGIC - SparkSession を使用して<strong>`events`</strong>テーブルに対してSQLクエリを実行します
# MAGIC - SQL コマンドを使用して、前に使用したものと同じフィルターおよびソートのクエリを記述します。

# COMMAND ----------

# ANSWER
mac_sql_df = spark.sql("""
SELECT *
FROM events
WHERE device = 'macOS'
ORDER By event_timestamp
""")

display(mac_sql_df)

# COMMAND ----------

# DBTITLE 0,--i18n-1d203e4e-e835-4778-a245-daf30cc9f4bc
# MAGIC %md
# MAGIC **5.1: 作業結果の確認 (CHECK YOUR WORK)**
# MAGIC - **`device`** 列には **`macOS`** の値のみが表示されるはずです。
# MAGIC - 5行目は、タイムスタンプが **`1592539226602157`** のイベントが表示されているはずです。

# COMMAND ----------

verify_rows = mac_sql_df.take(5)
assert (mac_sql_df.select("device").distinct().count() == 1 and len(verify_rows) == 5 and verify_rows[0]['device'] == "macOS"), "Incorrect filter condition"
assert (verify_rows[4]['event_timestamp'] == 1592540419446946), "Incorrect sorting"
del verify_rows
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-5b3843b3-e615-4dc6-aec4-c8ce4d684464
# MAGIC %md
# MAGIC ### クラスルームで使ったリソースの削除 (Clean up classroom)
# MAGIC このレッスンで作成された一時ファイル、テーブル、およびデータベースをクリーンアップします。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
