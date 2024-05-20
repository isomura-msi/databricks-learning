-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0d28fb8-0d0f-4354-9720-79ce468b5ea8
-- MAGIC %md
-- MAGIC # データをファイルから直接抽出する方法（Extracting Data Directly from Files）
-- MAGIC
-- MAGIC このノートブックでは、DatabricksでSpark SQLを使用してデータをファイルから直接抽出する方法を学びます。
-- MAGIC
-- MAGIC このオプションは複数のファイル形式でサポートされていますが、（parquetやJSONなど）自己記述的なデータ形式に最も役立ちます。
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC - Spark SQLを使用してデータファイルを直接照会する
-- MAGIC - ビューやCTEのレイヤーによりデータファイルへの参照を容易にする
-- MAGIC -  **`text`** および **`binaryFile`** メソッドを活用して生のファイルコンテンツを確認する

-- COMMAND ----------

-- DBTITLE 0,--i18n-73162404-8907-47f6-9b3e-dd17819d71c9
-- MAGIC %md
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-480bfe0b-d36d-4f67-8242-6a6d3cca38dd
-- MAGIC %md
-- MAGIC ## データの概要（Data Overview）
-- MAGIC
-- MAGIC この例では、JSONファイルとして書き込まれる未加工のKafkaデータのサンプルを扱っていきます。
-- MAGIC
-- MAGIC 各ファイルには、5秒の間隔で消費されるすべてのレコードが含まれています。レコードは、複数のレコードのJSONファイルとして完全なKafkaスキーマで保存されています。
-- MAGIC
-- MAGIC | フィールド     | 型       | 説明                                                                      |
-- MAGIC | --------- | ------- | ----------------------------------------------------------------------- |
-- MAGIC | key       | BINARY  |  **`user_id`** フィールドはキーとして使用されます。これは、セッション/クッキーの情報に対応する固有の英数字フィールドです      |
-- MAGIC | value     | BINARY  | これはJSONとして送信される完全なデータペイロード（後ほど説明します）です                                  |
-- MAGIC | topic     | STRING  | Kafkaサービスには複数のトピックがホスティングされていますが、ここには **`clickstream`** トピックのレコードのみが含まれます |
-- MAGIC | partition | INTEGER | こちらのKafkaの実装では2つのパーティションのみ（0および1）が使用されています                              |
-- MAGIC | offset    | LONG    | これは各パーティションに対して単調に増加していく固有値です                                           |
-- MAGIC | timestamp | LONG    | このタイムスタンプは、エポックからの経過ミリ秒数として記録され、作成者がパーティションにレコードを加えた時間を表します             |

-- COMMAND ----------

-- DBTITLE 0,--i18n-65941466-ca87-4c29-903e-658e24e48cee
-- MAGIC %md
-- MAGIC ソースディレクトリにたくさんのJSONファイルが含まれていることにご注意ください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(DA.paths.kafka_events)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(DA.paths.kafka_events)
-- MAGIC display(files)

-- COMMAND ----------

-- DBTITLE 0,--i18n-f1ddfb40-9c95-4b9a-84e5-2958ac01166d
-- MAGIC %md
-- MAGIC ここでは、DBFSルートに書き込まれたデータへの相対ファイルパスを使用します。
-- MAGIC
-- MAGIC ほとんどのワークフローでは、ユーザーが外部のクラウドストレージの場所からデータにアクセスする必要があります。
-- MAGIC
-- MAGIC ほとんどの会社では、それらの格納先へのアクセスの設定はワークスペース管理者が行います。
-- MAGIC
-- MAGIC これらの格納先を設定してアクセスするための手順は、自分のペースで進められるクラウドベンダー特有の「クラウドアーキテクチャとシステムの統合(Cloud Architecture & Systems Integrations)」というコースをご覧ください。

-- COMMAND ----------

-- DBTITLE 0,--i18n-9abfecfc-df3f-4697-8880-bd3f0b58a864
-- MAGIC %md
-- MAGIC ## 単一のファイルを照会する（Query a Single File）
-- MAGIC
-- MAGIC 単一のファイルのデータを照会するには、クエリを次のパターンで実行しましょう：
-- MAGIC
-- MAGIC <strong><code>SELECT * FROM file_format.&#x60;/path/to/file&#x60;</code></strong>
-- MAGIC
-- MAGIC パスを一重引用符ではなくバックティックで囲っていることにご注意ください。

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c2891f1-e055-4fde-8bf9-3f448e4cdb2b
-- MAGIC %md
-- MAGIC こちらのプレビューには、ソースファイルの321行すべてが表示されています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-0f45ecb7-4024-4798-a9b8-e46ac939b2f7
-- MAGIC %md
-- MAGIC ## ## ファイルのディレクトリを照会する（Query a Directory of Files）
-- MAGIC
-- MAGIC ディレクトリにあるファイルがすべて同じ形式とスキーマを持っている場合は、個別のファイルではなくディレクトリパスを指定することですべてのファイルを同時にクエリできます。

-- COMMAND ----------

SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-6921da25-dc10-4bd9-9baa-7e589acd3139
-- MAGIC %md
-- MAGIC デフォルトでは、このクエリは、最初の1000行のみを表示します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-035ddfa2-76af-4e5e-a387-71f26f8c7f76
-- MAGIC %md
-- MAGIC ## ファイルへの参照の作成（Create References to Files）
-- MAGIC ファイルとディレクトリを直接クエリできるのは、ファイルに対するクエリに追加のSparkロジックを連結できるということです。
-- MAGIC
-- MAGIC パスに対してクエリからビューを作成すると、後のクエリでこのビューを参照できます。

-- COMMAND ----------

CREATE OR REPLACE VIEW event_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c29b73b-b4b0-48ab-afbb-7b1422fce6e4
-- MAGIC %md
-- MAGIC ユーザーにビューとそのストレージロケーションに対する権限があるなら、ビューの定義を使うことでその背後にあるデータをクエリーすることができます。これは、ワークスペースの他のユーザー、他のノートブック、他のクラスターにも当てはまります。

-- COMMAND ----------

SELECT * FROM event_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-efd0c0fc-5346-4275-b083-4ee96ce8a852
-- MAGIC %md
-- MAGIC ## ファイルへの一時的な参照の作成（Create Temporary References to Files）
-- MAGIC
-- MAGIC テンポラリビュー（Temporary views）は同じように、その後の検索のために、クエリに名前をつけます。

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW events_temp_view
AS SELECT * FROM json.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a9f9827b-2258-4481-a9d9-6fecf55aeb9b
-- MAGIC %md
-- MAGIC テンポラリビュー（Temporary views）は、そのSparkSession内でだけ存在します。Databricksでは、そのノートブック、ジョブ、またはDBSQLクエリ内でだけ有効であるということです。

-- COMMAND ----------

SELECT * FROM events_temp_view

-- COMMAND ----------

-- DBTITLE 0,--i18n-dcfaeef2-0c3b-4782-90a6-5e0332dba614
-- MAGIC %md
-- MAGIC ## クエリー内での参照としてCTEを適用（Apply CTEs for Reference within a Query）
-- MAGIC 共通テーブル式 (Common table expressions、CTEs)は、クエリ結果に対する短期的で人間が判読可能な参照として最適です。

-- COMMAND ----------

WITH cte_json
AS (SELECT * FROM json.`${DA.paths.kafka_events}`)
SELECT * FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-c85e1553-f643-47b8-b909-0d10d2177437
-- MAGIC %md
-- MAGIC CTEは、クエリがプランされ実行されている最中にだけクエリ結果のエイリアスとなります。
-- MAGIC
-- MAGIC そのため、**以下のセルを実行するとエラーが投げられます**。

-- COMMAND ----------

-- SELECT COUNT(*) FROM cte_json

-- COMMAND ----------

-- DBTITLE 0,--i18n-106214eb-2fec-4a27-b692-035a86b8ec8d
-- MAGIC %md
-- MAGIC ## ## テキストファイルを未加工の文字列として抽出する（Extract Text Files as Raw Strings）
-- MAGIC
-- MAGIC （JSON、CSV、TSV、およびTXT形式を含む）テキストベースのファイルを扱っているときは、 **`text`** 形式を使用してファイルの各行を **`value`** というの文字列の1列がある行として読み込みこませることができます。 これは、データソースが破損しがちでテキスト解析の関数がテキストフィールドから値を抽出するために使用される場合に役立ちます。

-- COMMAND ----------

SELECT * FROM text.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-732e648b-4274-48f4-86e9-8b42fd5a26bd
-- MAGIC %md
-- MAGIC ## ファイルの未加工のバイトとメタデータを抽出する（Extract the Raw Bytes and Metadata of a File）
-- MAGIC
-- MAGIC 一部のワークフローでは、画像もしくは非構造化データを扱うときなど、ファイルを丸ごと扱う必要があります ディレクトリを照会するのに **`binaryFile`** を使用すると、ファイルコンテンツの2進法表示とともにメタデータも表示されます。
-- MAGIC
-- MAGIC 具体的には、作成されたフィールドは、 **`path`** 、 **`modificationTime`** 、 **`length`** および **`content`** を示します。

-- COMMAND ----------

SELECT * FROM binaryFile.`${DA.paths.kafka_events}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9ac20d39-ae6a-400e-9e13-14af5d4c91df
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
