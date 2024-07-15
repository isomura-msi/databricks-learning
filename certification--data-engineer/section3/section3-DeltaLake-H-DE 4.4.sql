-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## ● イベントログへのクエリ
-- MAGIC イベントログはDelta Lakeのテーブルとして管理され、より重要なフィールドの一部はネストしたJSONデータとして保存される。
-- MAGIC
-- MAGIC ```python
-- MAGIC event_log_path = f"{DA.paths.storage_location}/system/events"
-- MAGIC
-- MAGIC event_log = spark.read.format('delta').load(event_log_path)
-- MAGIC event_log.createOrReplaceTempView("event_log_raw")
-- MAGIC
-- MAGIC display(event_log)
-- MAGIC ```
-- MAGIC
-- MAGIC - `event_log_path = f"{DA.paths.storage_location}/system/events"`: 
-- MAGIC   - この行では、イベントログが保存されているパスを指定している。`DA.paths.storage_location`は、データの保存場所を指す変数であり、ここに`/system/events`を追加することでイベントログの正確なパスを構築している。
-- MAGIC
-- MAGIC - `event_log = spark.read.format('delta').load(event_log_path)`: 
-- MAGIC   - ここでは、Sparkの`read`メソッドを使用して、指定したパスからDelta形式のデータを読み込んでいる。読み込んだデータは`event_log`というDataFrameに格納される。
-- MAGIC
-- MAGIC - `event_log.createOrReplaceTempView("event_log_raw")`: 
-- MAGIC   - 読み込んだイベントログのDataFrameを`event_log_raw`という名前の一時ビューとして登録している。これにより、SQLクエリを使用してこのデータに対してインタラクティブな分析を行うことが可能になる。
-- MAGIC
-- MAGIC - `display(event_log)`: 
-- MAGIC   - 最後に、`display`関数を使用して`event_log`DataFrameの内容を表示している。これにより、イベントログのデータを直接確認することができる。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ● 最新のアップデートIDを設定する
-- MAGIC パイプラインの最新アップデート（または直近のN回のアップデート）に関する情報を得たい場合がある。
-- MAGIC
-- MAGIC SQLクエリで簡単に最新の更新IDを取得することができる。
-- MAGIC
-- MAGIC ```sql
-- MAGIC latest_update_id = spark.sql("""
-- MAGIC     SELECT origin.update_id
-- MAGIC     FROM event_log_raw
-- MAGIC     WHERE event_type = 'create_update'
-- MAGIC     ORDER BY timestamp DESC LIMIT 1""").first().update_id
-- MAGIC
-- MAGIC print(f"Latest Update ID: {latest_update_id}")
-- MAGIC
-- MAGIC # Push back into the spark config so that we can use it in a later query.
-- MAGIC spark.conf.set('latest_update.id', latest_update_id)
-- MAGIC
-- MAGIC # 最新のDLT上の実行ID ↓
-- MAGIC ```
-- MAGIC
-- MAGIC
-- MAGIC - `spark.sql("""...""").first().update_id`: 
-- MAGIC   - この行では、`event_log_raw`ビューから`event_type`が`'create_update'`であるレコードをタイムスタンプの降順で並べ替え、最も新しいレコード（最新のアップデート）の`update_id`を取得している。`first()`メソッドは結果セットの最初の行を返し、`.update_id`でその行の`update_id`フィールドの値を取得している。
-- MAGIC
-- MAGIC - `print(f"Latest Update ID: {latest_update_id}")`: 
-- MAGIC   - 取得した最新のアップデートIDを出力している。
-- MAGIC
-- MAGIC - `spark.conf.set('latest_update.id', latest_update_id)`: 
-- MAGIC   - 取得した最新のアップデートIDをSparkの設定に`'latest_update.id'`というキーで保存している。これにより、後のクエリでこのアップデートIDを参照することができるようになる。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ● 監査ログの振る舞い
-- MAGIC
-- MAGIC パイプラインの実行や設定の編集に関連するイベントは、 **`user_action`** として取り込まれる。
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT timestamp, details:user_action:action, details:user_action:user_name
-- MAGIC FROM event_log_raw 
-- MAGIC WHERE event_type = 'user_action'
-- MAGIC ```
-- MAGIC
-- MAGIC - `SELECT timestamp, details:user_action:action, details:user_action:user_name`: 
-- MAGIC   - この行では、イベントのタイムスタンプ、ユーザーアクションの詳細（行われたアクションとユーザー名）を選択している。`details:user_action:action`は行われた具体的なアクション（例：パイプラインの起動、停止など）、`details:user_action:user_name`はそのアクションを行ったユーザーの名前を指している。
-- MAGIC
-- MAGIC - `FROM event_log_raw`: 
-- MAGIC   - このクエリは`event_log_raw`というビューまたはテーブルからデータを取得している。`event_log_raw`はDelta Lakeのイベントログを格納しており、パイプラインの実行や設定の編集など、さまざまなイベントに関する情報が含まれている。
-- MAGIC
-- MAGIC - `WHERE event_type = 'user_action'`: 
-- MAGIC   - この条件は、イベントタイプが`user_action`であるレコードのみをフィルタリングするために使用される。これにより、ユーザーによるアクションに関連するイベントのみが選択される。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ● リネージュの確認
-- MAGIC
-- MAGIC DLT（Delta Live Tables）は、データがテーブル間でどのように流れるかについての組み込みのリネージュ情報を提供する。この情報を利用することで、データの流れを追跡し、データの起源を特定することが可能になる。
-- MAGIC
-- MAGIC 以下のSQLクエリは、特定のアップデートIDに関連するフロー定義イベントから、各テーブルの出力データセットとその入力データセットを取得する方法を示している。
-- MAGIC
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets 
-- MAGIC FROM event_log_raw 
-- MAGIC WHERE event_type = 'flow_definition' AND 
-- MAGIC       origin.update_id = '${latest_update.id}'
-- MAGIC ```
-- MAGIC
-- MAGIC
-- MAGIC - `SELECT details:flow_definition.output_dataset, details:flow_definition.input_datasets`: 
-- MAGIC   - この行では、フロー定義イベントの詳細から、出力データセット（テーブル）とその入力データセット（テーブル）を選択している。これにより、データがどのテーブルからどのテーブルへと流れているかのリネージュ情報を取得する。
-- MAGIC
-- MAGIC - `FROM event_log_raw`: 
-- MAGIC   - `event_log_raw`ビューからデータを取得している。このビューは、DLTによって生成されたイベントログのデータを含んでいる。
-- MAGIC
-- MAGIC - `WHERE event_type = 'flow_definition' AND origin.update_id = '${latest_update.id}'`: 
-- MAGIC   - この条件は、イベントタイプが`flow_definition`であり、かつ特定のアップデートID（`latest_update.id`で指定された値）に関連するレコードのみをフィルタリングするために使用される。これにより、最新のデータフローのリネージュ情報のみが選択される。

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## ● データ品質指標の確認
-- MAGIC
-- MAGIC データ品質メトリクスは、データの品質を評価し、データの整合性を保証するために重要である。
-- MAGIC 以下のSQLクエリは、テーブルのライフタイム全体を通して、Delta Live Tables（DLT）のイベントログから各制約・データ品質に関するメトリクスを取得する方法を示している。
-- MAGIC
-- MAGIC
-- MAGIC ```sql
-- MAGIC SELECT row_expectations.dataset as dataset,
-- MAGIC        row_expectations.name as expectation,
-- MAGIC        SUM(row_expectations.passed_records) as passing_records,
-- MAGIC        SUM(row_expectations.failed_records) as failing_records
-- MAGIC FROM
-- MAGIC   (SELECT explode(
-- MAGIC             from_json(details :flow_progress :data_quality :expectations,
-- MAGIC                       "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")
-- MAGIC           ) row_expectations
-- MAGIC    FROM event_log_raw
-- MAGIC    WHERE event_type = 'flow_progress' AND 
-- MAGIC          origin.update_id = '${latest_update.id}'
-- MAGIC   )
-- MAGIC GROUP BY row_expectations.dataset, row_expectations.name
-- MAGIC ```
-- MAGIC
-- MAGIC - SELECT row_expectations.dataset as dataset, row_expectations.name as expectation, SUM(row_expectations.passed_records) as passing_records, SUM(row_expectations.failed_records) as failin-g_records: 
-- MAGIC   - この行では、データセット名、期待（制約）名、合格したレコードの合計数、失敗したレコードの合計数を選択している。
-- MAGIC
-- MAGIC - FROM (SELECT explode(from_json(details :flow_progress :data_quality :expectations, "array<struct<name: string, dataset: string, passed_records: int, failed_records: int>>")) row_expecta-tions: 
-- MAGIC   - ここでは、イベントログのdetails:flow_progress:data_quality:expectationsフィールドからJSONデータを読み込み、それを構造体の配列に変換している。explode関数は、配列内の各要素を別々の-行として展開する。
-- MAGIC
-- MAGIC - FROM event_log_raw WHERE event_type = 'flow_progress' AND origin.update_id = '${latest_update.id}': 
-- MAGIC   - event_log_rawテーブルから、イベントタイプがflow_progressであり、指定されたアップデー-トIDに一致するレコードをフィルタリングしている。
-- MAGIC
-- MAGIC - GROUP BY row_expectations.dataset, row_expectations.name: 
-- MAGIC   - 最後に、データセット名と期待（制約）名でグループ化し、各データセットと期待（制約）に対する合格したレコードと失敗したレコードの合計数を集計している。