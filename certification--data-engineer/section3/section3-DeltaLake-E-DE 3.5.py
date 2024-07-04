# Databricks notebook source
# MAGIC %md
# MAGIC ## ● データの準備

# COMMAND ----------

# 現在のノートブックのパスを取得
import os
dbutils.fs.rm("dbfs:/mnt/my-external-data/external_table", recurse=True)

# --------------------------
# 現在のノートブックのフルパスを取得
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(notebook_path)
# 親フォルダのパスを取得
csv_dir_full_path = os.path.join(os.path.dirname(notebook_path), "data")

print("###############################  sales-historical  ######################################")
print(csv_dir_full_path)
spark.conf.set("spark.sql.csvDirFullPath", csv_dir_full_path)
# ----
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"file:/Workspace/{csv_dir_full_path}/sales-historical.csv")
display(df)

df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/sales-historical")

df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/sales-historical")
display(df_parquet)

# -----
print("###############################  sales-30m  ######################################")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"file:/Workspace/{csv_dir_full_path}/sales-30m.csv")
display(df)

df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/sales-30m")

df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/sales-30m")
display(df_parquet)


# -----
print("###############################  users-30m  ######################################")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"file:/Workspace/{csv_dir_full_path}/users-30m.csv")
display(df)

df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/users-30m")

df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/users-30m")
display(df_parquet)

# -----
print("###############################  users  ######################################")
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"file:/Workspace/{csv_dir_full_path}/users.csv")
display(df)

df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/users")

df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/users")
display(df_parquet)

display(spark.sql("DROP TABLE users"))

df_parquet.write.format("delta").mode("overwrite").saveAsTable("users")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● CREATE OR REPLACE TABLE と INSERT OVERWRITE の比較
# MAGIC
# MAGIC CREATE OR REPLACE TABLE (CRAS) 文と INSERT OVERWRITE 文は、既存のテーブルを完全に上書きするための2つの異なるアプローチを提供する。以下にその違いを示す。
# MAGIC
# MAGIC - **CREATE OR REPLACE TABLE (CRAS) 文**:
# MAGIC   - テーブルを完全に再作成し、同時に既存のテーブルデータを置き換える。
# MAGIC   - 新しいテーブルを作成することができる。
# MAGIC   - テーブルの前のバージョンは保持され、タイムトラベルを使用して以前のデータを取得可能。
# MAGIC   - テーブルのスキーマを変更することができる。
# MAGIC
# MAGIC - **INSERT OVERWRITE 文**:
# MAGIC   - 既存のテーブルデータをクエリの結果で上書きする。
# MAGIC   - 新しいテーブルを作成することはできず、既存のテーブルのデータのみを置き換える。
# MAGIC   - 現在のテーブルスキーマに一致する新しいレコードでのみ上書きが可能。
# MAGIC   - 特定のパーティションを上書きできる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルとデータの準備
# MAGIC DROP TABLE IF EXISTS sales;
# MAGIC CREATE OR REPLACE TABLE sales (id INT, item STRING, amount DOUBLE);
# MAGIC INSERT INTO sales VALUES (1, 'book', 10.99), (2, 'pen', 1.99), (3, 'notebook', 5.49);
# MAGIC SELECT * FROM sales LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- CREATE OR REPLACE TABLE の使用
# MAGIC CREATE OR REPLACE TABLE sales AS
# MAGIC SELECT id, item, amount * 1.1 AS amount FROM sales;
# MAGIC SELECT * FROM sales LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- INSERT OVERWRITE の使用
# MAGIC INSERT OVERWRITE sales
# MAGIC SELECT * FROM sales WHERE id < 3;
# MAGIC SELECT * FROM sales LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python

# COMMAND ----------

# Databricksノートブック内で実行
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.sql("DROP TABLE IF EXISTS sales")

# テーブルとデータの準備
data = [(1, 'book', 10.99), (2, 'pen', 1.99), (3, 'notebook', 5.49)]
columns = ['id', 'item', 'amount']
df = spark.createDataFrame(data, columns)
df.write.format("delta").mode("overwrite").saveAsTable("sales")

history_df = spark.sql("DESCRIBE HISTORY sales")
display(history_df)

# データの表示
df_sales = spark.read.table("sales")
display(df_sales)

# CREATE OR REPLACE TABLE の使用
df_new = spark.sql("SELECT id, item, amount * 1.1 AS amount FROM sales")
df_new.write.format("delta").mode("overwrite").saveAsTable("sales")


history_df = spark.sql("DESCRIBE HISTORY sales")
display(history_df)

# データの表示
df_sales_updated = spark.read.table("sales")
display(df_sales_updated)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● 行の追加（Append Rows）
# MAGIC
# MAGIC - 既存のDeltaテーブルに新しい行を追加する。
# MAGIC - 既存のレコードをそのまま保持し、新しいデータを追加するため、インクリメンタルな更新が可能。
# MAGIC - 既存のデータを保持するため、データの重複が発生する可能性がある。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- sales テーブルをいったん削除
# MAGIC DROP TABLE IF EXISTS sales;
# MAGIC CREATE OR REPLACE TABLE sales AS
# MAGIC SELECT * FROM parquet.`/mnt/my-external-data/external_table/raw/sales-historical`;
# MAGIC
# MAGIC DESCRIBE EXTENDED sales;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- データの追加
# MAGIC INSERT INTO sales VALUES 
# MAGIC (257611, 'johnsmith@example.com', 1593879956731672, 3, 1200.75, 2, '[{"coupon":"SUMMER20","item_id":"M_STAN_Q","item_name":"Standard Queen Mattress","item_revenue_in_usd":950,"price_in_usd":1050,"quantity":1},{"coupon":"SUMMER20","item_id":"P_DOWN_S","item_name":"Standard Down Pillow","item_revenue_in_usd":250.75,"price_in_usd":300,"quantity":2}]'),
# MAGIC (257585, 'janedoe@example.com', 1593879844080773, 2, 900.5, 2, '[{"coupon":"WINTER15","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":950,"quantity":1},{"coupon":"WINTER15","item_id":"B_BLANKET","item_name":"Warm Blanket","item_revenue_in_usd":50,"price_in_usd":100,"quantity":1}]'),
# MAGIC (257859, 'alexbrown@example.com', 1593880860456070, 1, 1300, 1, '[{"coupon":null,"item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1300,"price_in_usd":1300,"quantity":1}]');
# MAGIC
# MAGIC SELECT * FROM sales LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- パーケットファイルからデータの追加
# MAGIC INSERT INTO sales
# MAGIC SELECT * FROM parquet.`/mnt/my-external-data/external_table/raw/sales-30m`;
# MAGIC SELECT count(*) FROM sales ;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY sales;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python

# COMMAND ----------

# 既存のテーブル (sales) を使用
data_append = [(1, 'example@email.com', 1617981217, 2, 120.00, 2, '["item1", "item2"]')]

append_df = spark.createDataFrame(data_append, columns)
append_df.createOrReplaceTempView("append_table")

# データの追加
df_new = spark.sql("SELECT * FROM sales UNION ALL SELECT * FROM append_table")
df_new.createOrReplaceTempView("sales")
display(df_new)

# パーケットファイルからデータの追加
df_parquet = spark.read.parquet('/mnt/my-external-data/external_table/raw/sales-30m')
df_final = spark.sql("SELECT * FROM sales UNION ALL SELECT * FROM parquet.`/mnt/my-external-data/external_table/raw/sales-30m`")
df_final.createOrReplaceTempView("sales")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 更新のマージ（Merge Updates）

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### MERGEを使用すべきシナリオ
# MAGIC
# MAGIC `MERGE`コマンドは、ソーステーブル、ビュー、またはデータフレームからターゲットDeltaテーブルへのデータのアップサート（挿入・更新・削除）に使用される。以下のシナリオで利用されることが多い。
# MAGIC
# MAGIC 1. **データの同期**: 異なるシステム間でデータを同期する際に使用。例えば、ユーザーデータを定期的に更新する場合など。
# MAGIC 2. **重複排除**: 重複したデータを一元管理するために、insert-onlyマージを使用して不要なデータの挿入を防ぐ。
# MAGIC 3. **条件付きのデータ挿入・更新**: 特定の条件が満たされた場合にのみデータを更新する場合。例えば、メールアドレスが更新された場合に限りユーザー情報を更新するなど。
# MAGIC
# MAGIC Databricks SQL の MERGE オペレーションがマージ対象としてデルタ・テーブルのみをサポートしている。DataFrame から作成された一時ビューに対して MERGE 操作は実行できない。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### MERGEコマンドのメリットを説明する
# MAGIC
# MAGIC `MERGE`コマンドの利点は以下の通りである。
# MAGIC
# MAGIC - **単一トランザクション**: 更新、挿入、削除が単一のトランザクションとして行われ、一貫性が保証される。
# MAGIC - **複雑な条件文のサポート**: 一致するフィールドの他にも、複数の条件文を追加することが可能であり、柔軟な処理が行える。
# MAGIC - **カスタムロジックの実装**: 様々な条件に基づいたカスタムロジックの実装が容易であり、高度なデータ管理が可能。
# MAGIC
# MAGIC 以下に例を示す。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from users;

# COMMAND ----------

# MAGIC %sql
# MAGIC desc extended users;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW users_update AS 
# MAGIC SELECT *, current_timestamp() AS updated 
# MAGIC FROM parquet.`/mnt/my-external-data/external_table/raw/users-30m`;
# MAGIC
# MAGIC select * from users_update;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC MERGE INTO users a
# MAGIC USING users_update b
# MAGIC ON a.user_id = b.user_id
# MAGIC WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
# MAGIC   UPDATE SET email = b.email, updated = b.updated
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC -- 結果の確認
# MAGIC SELECT * FROM users LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python

# COMMAND ----------


# テーブルとデータの作成
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType
from pyspark.sql.functions import current_timestamp

spark = SparkSession.builder.appName("MergeExample").getOrCreate()

users = spark.createDataFrame([
    (1, 'john.doe@example.com'),
    (2, None),
    (3, 'alice@example.com')
], ['user_id', 'email'])

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("email", StringType(), True)
])

users_update = spark.createDataFrame([
    (2, 'jane.doe@example.com'),
    (4, 'bob@example.com')
], schema=schema).withColumn("updated", current_timestamp())

# 一時ビューとして登録（temp view は merge 使えない）
# users.createOrReplaceTempView("users_py")
# users_update.createOrReplaceTempView("users_update_py")

# Write DataFrames to Delta tables
spark.sql("DROP VIEW users_py")
spark.sql("DROP VIEW users_update_py")
users.write.format("delta").mode("overwrite").saveAsTable("users_py")
users_update.write.format("delta").mode("overwrite").saveAsTable("users_update_py")

# マージ操作
spark.sql("""
MERGE INTO users_py a
USING users_update_py b
ON a.user_id = b.user_id
WHEN MATCHED AND a.email IS NULL AND b.email IS NOT NULL THEN
  UPDATE SET a.email = b.email
WHEN NOT MATCHED THEN
  INSERT (user_id, email) VALUES (b.user_id, b.email)
""")

# 結果の確認
result = spark.sql("SELECT * FROM users_py")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 保存時にデータの重複を排除するためのMERGE
# MAGIC
# MAGIC `MERGE`は、重複したデータの挿入を防ぐために `WHEN NOT MATCHED`句を使用したinsert-onlyマージを行う際に利用される。これにより、特定の条件を満たした場合にのみデータが挿入され、重複が避けられる。
# MAGIC
# MAGIC
# MAGIC 連続の追加操作でログもしくは常に追加されるその他のデータセットをDeltaテーブルに集めるのが一般的なETLの使い方である。
# MAGIC 多くのソースシステムは重複レコードを生成する。 マージでは、insert-onlyマージを実行すれば重複レコードの挿入を予防できる。
# MAGIC この最適化されたコマンドは同様の **`MERGE`** 構文を使用するが、 **`WHEN NOT MATCHED`** 句のみを指定する。
# MAGIC 以下では、これを使用して、同じ **`user_id`** および **`event_timestamp`** を持つレコードが既に **`events`** テーブルにないことを確認する。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 初期データの作成
# MAGIC CREATE OR REPLACE TEMP VIEW events AS
# MAGIC SELECT 1 AS user_id, '2023-10-01 00:00:00' AS event_timestamp, 'ad' AS traffic_source
# MAGIC UNION ALL SELECT 2, '2023-10-02 00:00:00', 'email';
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW events_update AS
# MAGIC SELECT 1 AS user_id, '2023-10-01 00:00:00' AS event_timestamp, 'email' AS traffic_source
# MAGIC UNION ALL SELECT 3, '2023-10-03 00:00:00', 'email';
# MAGIC

# COMMAND ----------

# Convert the 'events' temporary view into a Delta table
spark.sql("""
CREATE OR REPLACE TABLE events_delta
USING DELTA
AS SELECT * FROM events
""")

# Convert the 'events_update' temporary view into a Delta table
spark.sql("""
CREATE OR REPLACE TABLE events_update_delta
USING DELTA
AS SELECT * FROM events_update
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- マージ操作
# MAGIC MERGE INTO events_delta a
# MAGIC USING events_update_delta b
# MAGIC ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
# MAGIC WHEN NOT MATCHED AND b.traffic_source = 'email' THEN 
# MAGIC   INSERT (user_id, event_timestamp, traffic_source) VALUES (b.user_id, b.event_timestamp, b.traffic_source);
# MAGIC
# MAGIC -- 結果の確認
# MAGIC SELECT * FROM events_delta LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python

# COMMAND ----------


# 初期データの作成
events = spark.createDataFrame([
    (1, '2023-10-01 00:00:00', 'ad'),
    (2, '2023-10-02 00:00:00', 'email')
], ['user_id', 'event_timestamp', 'traffic_source'])

events_update = spark.createDataFrame([
    (1, '2023-10-01 00:00:00', 'email'),
    (3, '2023-10-03 00:00:00', 'email')
], ['user_id', 'event_timestamp', 'traffic_source'])

# 一時ビューとして登録
events.createOrReplaceTempView("events")
events_update.createOrReplaceTempView("events_update")

# Convert the 'events' temporary view into a Delta table
spark.sql("""
CREATE OR REPLACE TABLE events_delta_py
USING DELTA
AS SELECT * FROM events
""")

# Convert the 'events_update' temporary view into a Delta table
spark.sql("""
CREATE OR REPLACE TABLE events_update_delta_py
USING DELTA
AS SELECT * FROM events_update
""")

# マージ操作
spark.sql("""
MERGE INTO events_delta_py a
USING events_update_delta_py b
ON a.user_id = b.user_id AND a.event_timestamp = b.event_timestamp
WHEN NOT MATCHED AND b.traffic_source = 'email' THEN
  INSERT (user_id, event_timestamp, traffic_source) VALUES (b.user_id, b.event_timestamp, b.traffic_source)
""")

# 結果の確認
result = spark.sql("SELECT * FROM events_delta_py")
display(result)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Delta Lakeテーブルの複製
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### COPY INTOを使用すべきシナリオ
# MAGIC
# MAGIC `COPY INTO`は、外部ソースからDeltaテーブルへデータをインクリメンタルに効率よく取り込むためのコマンドである。この操作は以下のシナリオで適用するのが望ましい。
# MAGIC
# MAGIC 1. **定期的なデータ取り込み**: 外部システムから新しいデータファイルが定期的に追加されるシステムに対して、増分データを効率よく取り込む場合。
# MAGIC 2. **大規模データの処理**: 巨大なデータセットを取り込む場合、全テーブルスキャンを行わず、ファイルベースでデータを効率的に取り込みたい場合。
# MAGIC 3. **データのベキ等性**: データ取り込み操作が途中で失敗しても、再実行することで一貫したデータ取り込みが保証される場合。
# MAGIC
# MAGIC
# MAGIC この操作は、予想どおりに増大するデータに対する全テーブルスキャンよりもはるかに軽い可能性がある。
# MAGIC 効果が高いのは、時間の経過に伴い複数回実行した場合にソース内の新しいファイルを自動的に取得するパターンである。
# MAGIC
# MAGIC この操作にはいくつかの条件があることに注意：
# MAGIC - データスキーマは一貫している必要がある
# MAGIC - 重複レコードは、除外するか、ダウンストリームで処理する必要がある

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### COPY INTOを使用してデータを挿入する
# MAGIC
# MAGIC `COPY INTO`の使用方法は以下の通りである。特に外部ストレージ（例えば、データレイク、S3、ADLSなど）からDeltaテーブルにデータを取り込むことが多い。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの作成
# MAGIC DROP TABLE IF EXISTS sales;
# MAGIC CREATE TABLE IF NOT EXISTS sales
# MAGIC (
# MAGIC   order_id INT,
# MAGIC   order_date STRING,
# MAGIC   customer_id INT,
# MAGIC   amount DOUBLE
# MAGIC ) USING DELTA;;
# MAGIC
# MAGIC -- データの表示（初期状態）
# MAGIC SELECT * FROM sales LIMIT 10;
# MAGIC
# MAGIC -- 外部ファイルからのデータ取り込み
# MAGIC COPY INTO sales
# MAGIC FROM 'dbfs:/mnt/my-external-data/external_table/raw/sales-30m'
# MAGIC FILEFORMAT = PARQUET;
# MAGIC
# MAGIC -- データの表示
# MAGIC SELECT * FROM sales LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python

# COMMAND ----------


# Sparkセッションの取得
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("CopyIntoExample").getOrCreate()

# テーブルの作成
spark.sql("""
CREATE TABLE IF NOT EXISTS sales
(
  order_id INT,
  order_date STRING,
  customer_id INT,
  amount DOUBLE
)
""")

# データの表示（初期状態）
display(spark.sql("SELECT * FROM sales LIMIT 10"))

# DBFSから外部データを表示（例ファイル確認）
display(dbutils.fs.ls("dbfs:/mnt/my-external-data/external_table/raw/sales-30m"))

# COPY INTO操作
spark.sql("""
COPY INTO sales
FROM 'dbfs:/mnt/my-external-data/external_table/raw/sales-30m'
FILEFORMAT = PARQUET
""")

# データの表示
display(spark.sql("SELECT * FROM sales LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### COPY INTOステートメントではターゲットテーブルのデータの重複が排除されない理由
# MAGIC
# MAGIC `COPY INTO`ステートメントでは、ターゲットテーブルにデータを追加する際に重複レコードの排除が自動的には行われない。その理由は以下の通りである。
# MAGIC
# MAGIC 1. **効率性の重視**: COPY INTOはファイル単位でデータを取り込むため、重複排除を行うとパフォーマンスが低下する可能性がある。
# MAGIC 2. **ベキ等性のサポート**: COPY INTOは一貫したデータ取り込みを保証するため、ユーザーが重複排除の責任を持つ必要がある。重複レコードが発生する場合は、後続のプロセスで処理することが推奨される。
# MAGIC 3. **スキーマの柔軟性**: 各ファイルやデータソースがバリエーションを持つ可能性があるため、一括で取り込み、後で重複処理を行う方が柔軟な運用が可能である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### まとめ
# MAGIC
# MAGIC COPY INTOは、外部ソースからのデータを増分で取り込む強力な手法であるが、重複レコードの排除は自動的には行われない。重複処理は後続のETLプロセスで実装する必要がある。
# MAGIC
