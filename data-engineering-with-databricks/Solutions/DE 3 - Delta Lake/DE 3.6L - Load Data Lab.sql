-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-02080b66-d11c-47fb-a096-38ce02af4dbb
-- MAGIC %md
-- MAGIC # データを読み込むラボ(Load Data Lab)
-- MAGIC
-- MAGIC このラボでは、データを新規および既存のDeltaテーブルに読み込みます。
-- MAGIC
-- MAGIC ## Learning Objectives
-- MAGIC
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - スキーマを指定した空のDeltaテーブルを作成する
-- MAGIC - 既存のテーブルからDeltaテーブルにレコードを挿入する
-- MAGIC - CTAS文を使用してファイルからDeltaテーブルを作成する

-- COMMAND ----------

-- DBTITLE 0,--i18n-50357195-09c5-4ab4-9d60-ca7fd44feecc
-- MAGIC %md
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC
-- MAGIC 次のセルを実行してこのレッスン用の変数とデータセットを設定します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.6L

-- COMMAND ----------

-- DBTITLE 0,--i18n-5b20c9b2-6658-4536-b79b-171d984b3b1e
-- MAGIC %md
-- MAGIC ## データの概要（Overview of the Data）
-- MAGIC
-- MAGIC JSONファイルとして書き込まれる未加工のKafkaデータのサンプルを扱っていきます。
-- MAGIC
-- MAGIC 各ファイルには、5秒の間隔で消費されるすべてのレコードが含まれています。ファイルは、複数レコードのJSONファイルとして完全なKafkaスキーマで保存されています。
-- MAGIC
-- MAGIC テーブルのスキーマ：
-- MAGIC
-- MAGIC
-- MAGIC | フィールド     | 型       | 説明                                               |
-- MAGIC | --------- | ------- | ----------------------------------------------------------------------- |
-- MAGIC | key       | BINARY  |  **`user_id`** フィールドはキーとして使用されます。これは、セッション/クッキーの情報に対応する固有の英数字フィールドです      |
-- MAGIC | offset    | LONG    | これは各パーティションに対して単調に増加していく固有値です                                           |
-- MAGIC | partition | INTEGER | こちらのKafkaの実装では2つのパーティションのみ（0および1）が使用されています                              |
-- MAGIC | timestamp | LONG    | このタイムスタンプは、エポックからの経過ミリ秒数として記録され、作成者がパーティションにレコードを加えた時間を表します             |
-- MAGIC | topic     | STRING  | Kafkaサービスには複数のトピックがホスティングされていますが、ここには **`clickstream`** トピックのレコードのみが含まれます |
-- MAGIC | value     | BINARY  | これはJSONとして送信される完全なデータペイロード（後ほど説明します）です

-- COMMAND ----------

-- DBTITLE 0,--i18n-5140f012-898a-43ac-bed9-b7e01a916505
-- MAGIC %md
-- MAGIC ## 空のDeltaテーブルのためにスキーマを定義する(Define Schema for Empty Delta Table)
-- MAGIC 同じスキーマに、空のマネージドDeltaテーブル **`events_raw`** を作成します。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE events_raw
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY);

-- COMMAND ----------

-- DBTITLE 0,--i18n-70f4dbf1-f4cb-4ec6-925a-a939cbc71bd5
-- MAGIC %md
-- MAGIC 以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Define Schema")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 0, "The table should have 0 records")
-- MAGIC
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "key", "BinaryType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "offset", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "partition", "IntegerType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "timestamp", "LongType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "topic", "StringType")
-- MAGIC suite.test_schema_field(lambda: expected_table().schema, "value", "BinaryType")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite

-- COMMAND ----------

-- DBTITLE 0,--i18n-7b4e55f2-737f-4996-a51b-4f18c2fc6eb7
-- MAGIC %md
-- MAGIC ## Deltaテーブルに未加工のイベントを挿入する（Insert Raw Events Into Delta Table）
-- MAGIC
-- MAGIC 抽出されたデータとDeltaテーブルの準備ができたら、 **`events_json`** のテーブルから新しい **`events_raw`** のDeltaテーブルにJSONレコードを挿入します。

-- COMMAND ----------

-- ANSWER
INSERT INTO events_raw
SELECT * FROM events_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-65ffce96-d821-4792-b545-5725814003a0
-- MAGIC %md
-- MAGIC 手動でテーブルの内容を確認し、データが期待通りに書き込まれたことを確認します。

-- COMMAND ----------

-- ANSWER
SELECT * FROM events_raw

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cbff40d-e916-487c-958f-2eb7807c5aeb
-- MAGIC %md
-- MAGIC 次のセルを実行してデータが正しく読み込まれたことを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate events_raw")
-- MAGIC expected_table = lambda: spark.table("events_raw")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"events_raw\"")
-- MAGIC suite.test_equals(lambda: expected_table().count(), 2252, "The table should have 2252 records")
-- MAGIC
-- MAGIC first_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").asc()).limit(5).collect()]
-- MAGIC suite.test_sequence(first_five, [1593879303631, 1593879304224, 1593879305465, 1593879305482, 1593879305746], True, "First 5 values are correct")
-- MAGIC
-- MAGIC last_five = lambda: [r["timestamp"] for r in expected_table().orderBy(F.col("timestamp").desc()).limit(5).collect()]
-- MAGIC suite.test_sequence(last_five, [1593881096290, 1593881095799, 1593881093452, 1593881093394, 1593881092076], True, "Last 5 values are correct")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3c62fea-b75d-41d6-8214-660f9cfa3acd
-- MAGIC %md
-- MAGIC ## クエリ結果からDeltaテーブルを作成する（Create a Delta Table From Query Results）
-- MAGIC 新しいイベントデータに加えて、コースの後半で使用する製品の詳細を獲得できる小さなルックアップテーブルも読み込みましょう。 
-- MAGIC CTAS文を使用して、以下のparquetディレクトリからデータを抽出する **`item_lookup`** というのマネージドDeltaテーブルを作成します。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE item_lookup 
AS SELECT * FROM parquet.`${da.paths.datasets}/ecommerce/raw/item-lookup`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a971532-0003-4665-9064-26196cd31e89
-- MAGIC %md
-- MAGIC 次のセルを実行してルックアップテーブルが正しく読み込まれていることを確認しましょう。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC suite = DA.tests.new("Validate item_lookup")
-- MAGIC expected_table = lambda: spark.table("item_lookup")
-- MAGIC suite.test_not_none(lambda: expected_table(), "Created the table \"item_lookup\"")
-- MAGIC
-- MAGIC actual_values = lambda: [r["item_id"] for r in expected_table().collect()]
-- MAGIC expected_values = ['M_PREM_Q','M_STAN_F','M_PREM_F','M_PREM_T','M_PREM_K','P_DOWN_S','M_STAN_Q','M_STAN_K','M_STAN_T','P_FOAM_S','P_FOAM_K','P_DOWN_K']
-- MAGIC suite.test_sequence(actual_values, expected_values, False, "Contains the 12 expected item IDs")
-- MAGIC
-- MAGIC suite.display_results()
-- MAGIC assert suite.passed

-- COMMAND ----------

-- DBTITLE 0,--i18n-4db73493-3920-44e2-a19b-f335aa650f76
-- MAGIC %md
-- MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
