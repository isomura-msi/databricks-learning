# Databricks notebook source
# MAGIC %md
# MAGIC ■ セクション 2: Apache Spark での ELT-C

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 値が特定のフィールドにないことを確認する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 値がフィールドにないか確認する方法
# MAGIC 特定の値がフィールドに存在しないことを確認するためには、SQLおよびPythonの両方で異なる手法を用いることができる。以下に具体的な例を示す。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC SQLでは、`NOT IN`演算子を用いて特定の値がフィールドに存在しないことを確認する。
# MAGIC
# MAGIC 例：`user_id`フィールドが特定の値（例：123, 456, 789）を持たない行を選択する。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM users
# MAGIC WHERE user_id NOT IN (123, 456, 789);
# MAGIC ```
# MAGIC
# MAGIC 一方、他の方法として、`NOT EXISTS`を使用することも可能である。
# MAGIC
# MAGIC 例：`orders`テーブル内に存在しない`user_id`を持つ行を`users`テーブルから選択する。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT *
# MAGIC FROM users u
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM orders o
# MAGIC     WHERE o.user_id = u.user_id
# MAGIC );
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- users テーブルの作成
# MAGIC -- CREATE TABLE users (
# MAGIC --     user_id INT PRIMARY KEY,
# MAGIC --     name VARCHAR(50)
# MAGIC -- );
# MAGIC
# MAGIC -- -- users テーブルへのデータ挿入
# MAGIC -- INSERT INTO users (user_id, name) VALUES (101, 'Alice');
# MAGIC -- INSERT INTO users (user_id, name) VALUES (102, 'Bob');
# MAGIC -- INSERT INTO users (user_id, name) VALUES (123, 'Charlie');
# MAGIC -- INSERT INTO users (user_id, name) VALUES (104, 'David');
# MAGIC -- INSERT INTO users (user_id, name) VALUES (456, 'Eve');
# MAGIC
# MAGIC -- SELECT *
# MAGIC -- FROM users
# MAGIC -- WHERE user_id NOT IN (123, 456, 789);
# MAGIC
# MAGIC -- users テーブルの作成（非Unity Catalog環境用）
# MAGIC CREATE TABLE users (
# MAGIC     user_id INT,
# MAGIC     name STRING
# MAGIC );
# MAGIC
# MAGIC -- users テーブルへのデータ挿入
# MAGIC INSERT INTO users (user_id, name) VALUES (101, 'Alice');
# MAGIC INSERT INTO users (user_id, name) VALUES (102, 'Bob');
# MAGIC INSERT INTO users (user_id, name) VALUES (123, 'Charlie');
# MAGIC INSERT INTO users (user_id, name) VALUES (104, 'David');
# MAGIC INSERT INTO users (user_id, name) VALUES (456, 'Eve');
# MAGIC
# MAGIC -- クエリ: user_idが特定の値にない行を選択
# MAGIC SELECT *
# MAGIC FROM users
# MAGIC WHERE user_id NOT IN (123, 456, 789);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- orders テーブルの作成
# MAGIC CREATE TABLE orders (
# MAGIC     order_id INT,
# MAGIC     user_id INT
# MAGIC );
# MAGIC
# MAGIC -- orders テーブルへのデータ挿入
# MAGIC INSERT INTO orders (order_id, user_id) VALUES (1, 101);
# MAGIC INSERT INTO orders (order_id, user_id) VALUES (2, 102);
# MAGIC INSERT INTO orders (order_id, user_id) VALUES (3, 123);
# MAGIC
# MAGIC -- クエリ: ordersテーブル内に存在しないuser_idを持つ行をusersテーブルから選択
# MAGIC SELECT *
# MAGIC FROM users u
# MAGIC WHERE NOT EXISTS (
# MAGIC     SELECT 1
# MAGIC     FROM orders o
# MAGIC     WHERE o.user_id = u.user_id
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonでは、Pandasライブラリを用いることで同様のフィルタリングを実施できる。以下に示すのは、特定の値がフィールドに存在しない行をフィルタリングする方法である。
# MAGIC
# MAGIC 例：`user_id`フィールドが特定の値（例：123, 456, 789）を持たない行を選択する場合。
# MAGIC
# MAGIC ```python
# MAGIC # 必要なライブラリのインポート
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql import Row
# MAGIC from pyspark.sql.functions import col
# MAGIC
# MAGIC # Sparkセッションの作成
# MAGIC spark = SparkSession.builder.appName("DatabricksExample").getOrCreate()
# MAGIC
# MAGIC # usersデータフレームの作成
# MAGIC users_data = [
# MAGIC     Row(user_id=101, name='Alice'),
# MAGIC     Row(user_id=102, name='Bob'),
# MAGIC     Row(user_id=123, name='Charlie'),
# MAGIC     Row(user_id=104, name='David'),
# MAGIC     Row(user_id=456, name='Eve')
# MAGIC ]
# MAGIC users_df = spark.createDataFrame(users_data)
# MAGIC
# MAGIC # ordersデータフレームの作成
# MAGIC orders_data = [
# MAGIC     Row(order_id=1, user_id=101),
# MAGIC     Row(order_id=2, user_id=102),
# MAGIC     Row(order_id=3, user_id=123)
# MAGIC ]
# MAGIC orders_df = spark.createDataFrame(orders_data)
# MAGIC
# MAGIC # 特定の値が存在しない行をフィルタリング（例: user_idフィールドが特定の値を持たない行を選択）
# MAGIC filter_values = [123, 456, 789]
# MAGIC filtered_users_df = users_df.filter(~col('user_id').isin(filter_values))
# MAGIC filtered_users_df.show()
# MAGIC ```
# MAGIC
# MAGIC また、特定の列に値が存在しないことを確認するために、他のPandasメソッドを用いることもできる。
# MAGIC
# MAGIC 例：`orders`データフレーム内に存在しない`user_id`を持つ行を`users`データフレームから選択する。
# MAGIC
# MAGIC ```python
# MAGIC # ordersデータフレーム内に存在しないuser_idを持つ行をusersデータフレームからフィルタリング
# MAGIC filtered_users_not_in_orders_df = users_df.join(
# MAGIC     orders_df, 
# MAGIC     users_df.user_id == orders_df.user_id, 
# MAGIC     how='left_anti'
# MAGIC )
# MAGIC filtered_users_not_in_orders_df.show()
# MAGIC ```
# MAGIC

# COMMAND ----------

# 必要なライブラリのインポート
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

# Sparkセッションの作成
spark = SparkSession.builder.appName("DatabricksExample").getOrCreate()

# usersデータフレームの作成
users_data = [
    Row(user_id=101, name='Alice'),
    Row(user_id=102, name='Bob'),
    Row(user_id=123, name='Charlie'),
    Row(user_id=104, name='David'),
    Row(user_id=456, name='Eve')
]
users_df = spark.createDataFrame(users_data)

# ordersデータフレームの作成
orders_data = [
    Row(order_id=1, user_id=101),
    Row(order_id=2, user_id=102),
    Row(order_id=3, user_id=123)
]
orders_df = spark.createDataFrame(orders_data)

# 特定の値が存在しない行をフィルタリング（例: user_idフィールドが特定の値を持たない行を選択）
filter_values = [123, 456, 789]
# ※ ~ は論理否定を表し、isin によるチェックを否定することで、指定した値が含まれていない行を選択する
filtered_users_df = users_df.filter(~col('user_id').isin(filter_values))
filtered_users_df.show()

# ordersデータフレーム内に存在しないuser_idを持つ行をusersデータフレームからフィルタリング
filtered_users_not_in_orders_df = users_df.join(
    orders_df, 
    users_df.user_id == orders_df.user_id,
    # ordersに存在しないuser_idを持つ行を取得 
    how='left_anti'
)
filtered_users_not_in_orders_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 列をタイムスタンプにキャストする。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 型変換の概要
# MAGIC データを効率的に扱うために、テキストや数字などのデータ型をタイムスタンプ型に変換することがある。この変換は、日時データを標準形式のタイムスタンプとして一貫性を持たせるために重要である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC SQLでは、`CAST`または`TO_TIMESTAMP`関数を用いてタイムスタンプ型に変換する。これらの関数は、文字列形式の日時やエポック秒をタイムスタンプ形式に変換するために使用される。
# MAGIC
# MAGIC **確認用コード:**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルとデータの作成
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sample_table AS
# MAGIC SELECT '2023-10-01 12:34:56' as string_date, 1696150496 as epoch_seconds;
# MAGIC
# MAGIC SELECT * FROM sample_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 文字列からタイムスタンプへの変換
# MAGIC SELECT string_date, CAST(string_date as TIMESTAMP) as timestamp_date FROM sample_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- エポック秒からタイムスタンプへの変換
# MAGIC -- epoch_secondsを一旦BIGINT型にキャストしている理由: 
# MAGIC -- 元のデータ型が明示的ではない場合や、エポック秒が文字列や整数として保存されている可能性があるためである。
# MAGIC -- エポック秒は通常、非常に大きな整数で表されるため、BIGINT型（64ビット整数）にキャストしてからタイムスタンプに変換することが一般的である。
# MAGIC SELECT epoch_seconds, TO_TIMESTAMP(CAST(epoch_seconds as BIGINT)) as timestamp_date FROM sample_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonでは、`pyspark.sql.functions`モジュール内の`to_timestamp`関数および`cast`を使用してタイムスタンプ型に変換する。また、Pythonの標準ライブラリである`datetime`を併用することで、より柔軟なデータ操作が可能である。
# MAGIC
# MAGIC **確認用コード:**
# MAGIC

# COMMAND ----------

# Databricks Notebook 用に記載
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, col

# Spark セッションの作成
spark = SparkSession.builder.appName("TimestampConversion").getOrCreate()

# サンプルデータの作成
data = [("2023-10-01 12:34:56", 1696150496)]
columns = ["string_date", "epoch_seconds"]

# データフレームの作成
df = spark.createDataFrame(data, columns)
df.show()

# 文字列からタイムスタンプへの変換
df_with_timestamp = df.withColumn("timestamp_date", to_timestamp(col("string_date")))
df_with_timestamp.show()

# エポック秒からタイムスタンプへの変換
df_with_epoch_timestamp = df.withColumn("timestamp_date", to_timestamp(col("epoch_seconds").cast("timestamp")))
df_with_epoch_timestamp.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● タイムスタンプからカレンダーのデータを抽出する。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 日付・時刻の抽出
# MAGIC タイムスタンプ型データから、年、月、日、時間、分、秒などのカレンダー情報を抽出することは、データ解析において重要な操作の一つである。Databricksでは、SQLおよびPySparkを使用してこれらの抽出を簡単に行うことができる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC SQLでは、`YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`などの関数を用いてタイムスタンプから各カレンダー情報を抽出する。
# MAGIC
# MAGIC **確認用コード:**

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルとデータの作成
# MAGIC CREATE OR REPLACE TEMPORARY VIEW sample_table AS
# MAGIC SELECT '2023-10-01 12:34:56' as timestamp_date;
# MAGIC
# MAGIC SELECT * FROM sample_table;
# MAGIC
# MAGIC -- タイムスタンプからカレンダー情報を抽出
# MAGIC SELECT 
# MAGIC     timestamp_date,
# MAGIC     YEAR(timestamp_date) as year,
# MAGIC     MONTH(timestamp_date) as month,
# MAGIC     DAY(timestamp_date) as day,
# MAGIC     HOUR(timestamp_date) as hour,
# MAGIC     MINUTE(timestamp_date) as minute,
# MAGIC     SECOND(timestamp_date) as second
# MAGIC FROM sample_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonでは、`pyspark.sql.functions`モジュール内の`year`, `month`, `dayofmonth`, `hour`, `minute`, `second`関数を用いてタイムスタンプから各カレンダー情報を抽出する。
# MAGIC
# MAGIC **確認用コード:**

# COMMAND ----------


# Databricks Notebook 用に記載
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth, hour, minute, second, col

# Spark セッションの作成
spark = SparkSession.builder.appName("CalendarDataExtraction").getOrCreate()

# サンプルデータの作成
data = [("2023-10-01 12:34:56",)]
columns = ["timestamp_date"]

# データフレームの作成
df = spark.createDataFrame(data, columns)
df = df.withColumn("timestamp_date", col("timestamp_date").cast("timestamp"))
# df.show()
display(df)

# タイムスタンプからカレンダー情報を抽出
df_with_calendar_data = df.select(
    col("timestamp_date"),
    year("timestamp_date").alias("year"),
    month("timestamp_date").alias("month"),
    dayofmonth("timestamp_date").alias("day"),
    hour("timestamp_date").alias("hour"),
    minute("timestamp_date").alias("minute"),
    second("timestamp_date").alias("second")
)
# df_with_calendar_data.show()
display(df_with_calendar_data)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### withColumn や alias の使い分け
# MAGIC
# MAGIC `withColumn`関数と`alias`メソッドは双方ともDataFrameに新しいカラムを追加したり、既存のカラム名を変更したりするために用いられる。しかし、その適用シナリオおよび用途は異なる。
# MAGIC
# MAGIC #### withColumn関数
# MAGIC
# MAGIC `withColumn`関数は、DataFrameに新しいカラムを追加する場合や既存のカラムを変更する場合に使用される。新しいカラム名と、そのカラムに対応する計算や変換を指定する必要がある。
# MAGIC
# MAGIC ##### 使い方と例：
# MAGIC 以下のコードは、`timestamp`カラムから年、月、曜日、分、秒を抽出し、新しいカラムとしてDataFrameに追加する例である。
# MAGIC
# MAGIC ```python
# MAGIC datetime_df = (timestamp_df
# MAGIC                .withColumn("year", year(col("timestamp")))
# MAGIC                .withColumn("month", month(col("timestamp")))
# MAGIC                .withColumn("dayofweek", dayofweek(col("timestamp")))
# MAGIC                .withColumn("minute", minute(col("timestamp")))
# MAGIC                .withColumn("second", second(col("timestamp")))
# MAGIC               )
# MAGIC display(datetime_df)
# MAGIC ```
# MAGIC
# MAGIC この例では、`withColumn`メソッドを連鎖的に呼び出して各カラムを追加している。これにより、新しいカラム名がそれぞれ`year`, `month`, `dayofweek`, `minute`, `second`となる。
# MAGIC
# MAGIC #### aliasメソッド
# MAGIC
# MAGIC `alias`メソッドは、カラムの名前を一時的に変更する場合に使用される。特に`select`メソッドと併用することが多い。
# MAGIC
# MAGIC ##### 使い方と例：
# MAGIC 以下のコードは、`timestamp_date`カラムから年、月、日、時、分、秒を抽出し、エイリアスをつけて新しい名前を付けた上で新しいDataFrameを作成する例である。
# MAGIC
# MAGIC ```python
# MAGIC df_with_calendar_data = df.select(
# MAGIC     col("timestamp_date"),
# MAGIC     year("timestamp_date").alias("year"),
# MAGIC     month("timestamp_date").alias("month"),
# MAGIC     dayofmonth("timestamp_date").alias("day"),
# MAGIC     hour("timestamp_date").alias("hour"),
# MAGIC     minute("timestamp_date").alias("minute"),
# MAGIC     second("timestamp_date").alias("second")
# MAGIC )
# MAGIC df_with_calendar_data.show()
# MAGIC ```
# MAGIC
# MAGIC この場合、`select`メソッドと合わせて`alias`メソッドを使用することで抽出カラムの名前を指定している。これにより、元のカラムは新しい名前（エイリアス）で指定されてDataFrameに含まれる。
# MAGIC
# MAGIC #### 使い分けのポイント
# MAGIC
# MAGIC - **`withColumn`を使用する場合**：
# MAGIC   - 新しいカラムを追加したり、既存のカラムを更新したりする場合。
# MAGIC   - 「段階的に」新しいカラムを追加する場合。
# MAGIC
# MAGIC - **`alias`を使用する場合**：
# MAGIC   - `select`や`selectExpr`メソッド内でカラムに一時的な名前を付けたい場合。
# MAGIC   - カラムの抽出と同時に名前を変更したい場合（よりコンパクトなコーディングスタイル）。
# MAGIC
# MAGIC この使い分けにより、コードが意図した操作を明確に反映しやすくなり、DataFrame操作がパフォーマンス的にもコードの読みやすさの観点でも最適化される。
