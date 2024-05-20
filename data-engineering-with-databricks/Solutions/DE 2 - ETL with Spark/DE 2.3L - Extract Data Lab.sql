-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-037a5204-a995-4945-b6ed-7207b636818c
-- MAGIC %md
-- MAGIC # データ抽出ラボ（Extract Data Lab）
-- MAGIC
-- MAGIC このラボでは、JSONファイルから未加工のデータを抽出します。
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - 外部テーブルを作成してJSONファイルからデータを抽出する

-- COMMAND ----------

-- DBTITLE 0,--i18n-3b401203-34ae-4de5-a706-0dbbd5c987b7
-- MAGIC %md
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC
-- MAGIC 次のセルを実行してこのレッスン用の変数とデータセットを設定します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-9adc06e2-7298-4306-a062-7ff70adb8032
-- MAGIC %md
-- MAGIC ## データの概要（Overview of the Data）
-- MAGIC
-- MAGIC JSONファイルとして書き込まれる未加工のKafkaデータのサンプルを扱っていきます。
-- MAGIC
-- MAGIC 各ファイルには、5秒の間隔で消費されるすべてのレコードが含まれています。レコードは、複数のレコードのJSONファイルとして完全なKafkaスキーマで保存されています。
-- MAGIC
-- MAGIC テーブルのスキーマ：
-- MAGIC
-- MAGIC | フィールド     | 型       | 説明                                                                      |
-- MAGIC | --------- | ------- | ----------------------------------------------------------------------- |
-- MAGIC | key       | BINARY  |  **`user_id`** フィールドはキーとして使用されます。これは、セッション/クッキーの情報に対応する固有の英数字フィールドです      |
-- MAGIC | offset    | LONG    | これは各パーティションに対して単調に増加していく固有値です                                           |
-- MAGIC | partition | INTEGER | こちらのKafkaの実装では2つのパーティションのみ（0および1）が使用されています                              |
-- MAGIC | timestamp | LONG    | このタイムスタンプは、エポックからの経過ミリ秒数として記録され、作成者がパーティションにレコードを加えた時間を表します             |
-- MAGIC | topic     | STRING  | Kafkaサービスには複数のトピックがホスティングされていますが、ここには **`clickstream`** トピックのレコードのみが含まれます |
-- MAGIC | value     | BINARY  | これはJSONとして送信される完全なデータペイロード（後ほど説明します）です                                  |

-- COMMAND ----------

-- DBTITLE 0,--i18n-8cca978a-3034-4339-8e6e-6c48d389cce7
-- MAGIC %md
-- MAGIC ## JSONファイルから未加工のイベントを抽出する（Extract Raw Events From JSON Files）
-- MAGIC データを正しくDeltaに読み込むには、まずは正しいスキーマを使用してJSONデータを抽出する必要があります。
-- MAGIC
-- MAGIC 以下で指定されているファイルパスにあるJSONファイルに対して外部テーブルを作成しましょう。 このテーブルに **`events_json`** というを付けて、上記のスキーマを宣言します。

-- COMMAND ----------

-- ANSWER
CREATE TABLE IF NOT EXISTS events_json
  (key BINARY, offset BIGINT, partition INT, timestamp BIGINT, topic STRING, value BINARY)
USING JSON 
LOCATION "${DA.paths.kafka_events}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-33231985-3ff1-4f44-8098-b7b862117689
-- MAGIC %md
-- MAGIC **注**：このラボでは、Pythonを使って時々チェックを実行します。 手順に従っていない場合、次のセルは変更すべきことについてのメッセージを記載したエラーを返します。 セルを実行しても出力がない場合、このステップは完了です。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("events_json"), "Table named `events_json` does not exist"
-- MAGIC assert spark.table("events_json").columns == ['key', 'offset', 'partition', 'timestamp', 'topic', 'value'], "Please name the columns in the order provided above"
-- MAGIC assert spark.table("events_json").dtypes == [('key', 'binary'), ('offset', 'bigint'), ('partition', 'int'), ('timestamp', 'bigint'), ('topic', 'string'), ('value', 'binary')], "Please make sure the column types are identical to those provided above"
-- MAGIC
-- MAGIC total = spark.table("events_json").count()
-- MAGIC assert total == 2252, f"Expected 2252 records, found {total}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6919d58a-89e4-4c02-812c-98a15bb6f239
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
