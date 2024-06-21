# Databricks notebook source
# MAGIC %md
# MAGIC ## ● Delta Lake の ACID トランザクション

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake が ACID トランザクションを提供する場所
# MAGIC Delta Lakeはクラウドのオブジェクトストレージ上にACIDトランザクションを提供する。これにより、データの格納や処理の一貫性を保ちながら、特に大量データの書き込みや既存データの変更が行われる際に、安全かつ安定して作業が行える。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ACID トランザクションのメリット
# MAGIC ACID特性は、Atomicity(原子性)、Consistency(一貫性)、Isolation(独立性)、Durability(永続性)の頭文字を取ったもので、トランザクション処理における信頼性と整合性を保つために重要な要素である。以下に、Delta Lakeにおける各ACID特性のメリットを列挙する:
# MAGIC
# MAGIC - Atomicity: すべての操作が一括して完了するか、全く行われないかを保証。大量データ書き込み中に失敗してもデータが中途半端にならない。
# MAGIC - Consistency: トランザクションの前後でデータの整合性が保たれる。データの追加や更新が簡単になり、整合性が損なわれる心配がない。
# MAGIC - Isolation: 複数トランザクションを平行して実行した場合でも、それぞれの結果が独立していて他のトランザクションに影響されない。
# MAGIC - Durability: トランザクションが完了したら、その結果は永続的に保存され、システム障害が生じても失われない。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### トランザクションが ACID に準拠しているかどうか
# MAGIC ACIDに準拠しているかどうかを確認するためには、各トランザクションが上記四つの特性を満たしているか確認する必要がある。例示のために、SQLとPythonを用いた具体的な操作を示す。  
# MAGIC DatabricksのSQLコマンドでは、一般的なデータベースシステムで使用されるBEGINやCOMMITを使用した明示的なトランザクション制御は直接サポートされていない。そのため、SQLのトランザクション制御を実現するためには、Delta Lakeの各種操作が自動的にACID特性を保持することに依存することになる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC
# MAGIC 以下に、SQLを用いたDelta LakeのACIDトランザクションに関する操作例を示す。まずテーブル作成用のコードを示し、実際のトランザクション操作を行う。

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. テーブルの作成と初期データの挿入

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE transactions;
# MAGIC CREATE TABLE IF NOT EXISTS transactions (
# MAGIC   id INT,
# MAGIC   value STRING
# MAGIC );
# MAGIC
# MAGIC -- 挿入するデータ
# MAGIC INSERT INTO transactions (id, value) VALUES (1, 'data1');
# MAGIC INSERT INTO transactions (id, value) VALUES (2, 'data2');
# MAGIC INSERT INTO transactions (id, value) VALUES (3, 'data3');
# MAGIC INSERT INTO transactions (id, value) VALUES (4, 'data4');
# MAGIC INSERT INTO transactions (id, value) VALUES (5, 'data5');
# MAGIC INSERT INTO transactions (id, value) VALUES (6, 'data6');
# MAGIC
# MAGIC -- データの確認
# MAGIC SELECT * FROM transactions LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. データの追加操作

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- データの追加操作
# MAGIC INSERT INTO transactions (id, value) VALUES (7, 'data7');
# MAGIC
# MAGIC -- コミット後のデータ確認
# MAGIC SELECT * FROM transactions LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. ロールバックのシナリオ
# MAGIC
# MAGIC Delta Lakeはバージョニング機能を持つため、手動でのロールバックが可能。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- データの追加操作
# MAGIC INSERT INTO transactions (id, value) VALUES (8, 'data8');
# MAGIC
# MAGIC -- 履歴の確認
# MAGIC DESCRIBE HISTORY transactions;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- バージョンの確認と取得（実運用では最新の1つ前のバージョンを取得）
# MAGIC -- Assuming previous version is 0
# MAGIC RESTORE TABLE transactions TO VERSION AS OF 0;
# MAGIC
# MAGIC -- ロールバック後のデータ確認
# MAGIC SELECT * FROM transactions LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC RESTORE TABLE transactions TO VERSION AS OF 4;
# MAGIC -- ロールバック後のデータ確認
# MAGIC SELECT * FROM transactions LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python
# MAGIC
# MAGIC 以下のPythonコードは、Delta Lakeのトランザクション操作を示すサンプル

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 1. テーブルの作成と初期データの挿入

# COMMAND ----------


from pyspark.sql import SparkSession

# Sparkセッションの作成
spark = SparkSession.builder.appName("DeltaLakeACIDExample").getOrCreate()

# サンプルデータの作成
data = [
    (1, 'data1'),
    (2, 'data2'),
    (3, 'data3'),
    (4, 'data4'),
    (5, 'data5'),
    (6, 'data6')
]

# データフレームの作成
df = spark.createDataFrame(data, ["id", "value"])

# テーブルの作成
df.write.format("delta").mode("overwrite").saveAsTable("transactions_py")

# データの確認
display(spark.sql("SELECT * FROM transactions_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. データの追加操作

# COMMAND ----------

from delta.tables import *

# Deltaテーブルの読み込み
delta_table = DeltaTable.forName(spark, "transactions_py")

# トランザクション操作
new_data = [(7, 'data7')]
new_df = spark.createDataFrame(new_data, ["id", "value"])

delta_table.alias("oldData").merge(
    new_df.alias("newData"),
    "oldData.id = newData.id"
).whenNotMatchedInsertAll().execute()

# データの確認
display(spark.sql("SELECT * FROM transactions_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. ロールバックのシナリオ
# MAGIC
# MAGIC ロールバック操作をバージョン管理を用いて実現する。
# MAGIC

# COMMAND ----------

# データの追加操作
new_data = [(8, 'data8')]
new_df = spark.createDataFrame(new_data, ["id", "value"])
new_df.write.format("delta").mode("append").saveAsTable("transactions_py")

# 履歴の確認
history_df = spark.sql("DESCRIBE HISTORY transactions_py")
display(history_df)

# 現在のバージョン取得
current_version = history_df.collect()[0]['version']

# ロールバック操作によるバージョンダウングレード（実運用では最新の1つ前のバージョンを指定）
previous_version = current_version - 1
spark.sql(f"RESTORE TABLE transactions_py TO VERSION AS OF {previous_version}")

# ロールバック後のデータ確認
display(spark.sql("SELECT * FROM transactions_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
