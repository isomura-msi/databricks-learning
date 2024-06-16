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

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 既存の文字列列から特定のパターンを抽出する。
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 正規表現によるパターン抽出
# MAGIC 特定のパターンを既存の文字列列から抽出する方法として、正規表現を用いる。Databricksにおいては、SQLおよびPythonの双方で以下のように実現できる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC Databricks SQLでは`regexp_extract`関数を使用する。これは正規表現パターンに一致する部分文字列を抽出するための関数である。
# MAGIC
# MAGIC 以下に、確認用のテーブル・データ作成用のコードとともに、特定のパターン(例: emailアドレス)を抽出する方法を示す。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの作成とデータの投入
# MAGIC CREATE OR REPLACE TEMP VIEW example_table AS
# MAGIC SELECT 'John Doe <john.doe@example.com>' AS info
# MAGIC UNION ALL
# MAGIC SELECT 'Jane Smith <jane.smith@example.net>' AS info
# MAGIC UNION ALL
# MAGIC SELECT 'Alice Wonderland <alice.w@wonderland.org>' AS info;
# MAGIC
# MAGIC -- emailアドレスの抽出
# MAGIC SELECT info, 
# MAGIC        regexp_extract(info, '<(.*?)>', 1) AS email_address
# MAGIC FROM example_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC DatabricksのPythonノートブックでは、PySparkの`regexp_extract`関数を用いる。これはDataFrame APIの一部であり、SQLと同様に正規表現パターンに一致する部分文字列を抽出できる。
# MAGIC
# MAGIC 以下に、確認用のテーブル・データ作成用のコードとともに、特定のパターン(例: emailアドレス)を抽出する方法を示す。
# MAGIC

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract

# SparkSessionの作成
spark = SparkSession.builder.appName("Example").getOrCreate()

# データの作成
data = [
    ('John Doe <john.doe@example.com>',),
    ('Jane Smith <jane.smith@example.net>',),
    ('Alice Wonderland <alice.w@wonderland.org>',)
]

# DataFrameの作成
columns = ['info']
df = spark.createDataFrame(data, columns)

# emailアドレスの抽出
df = df.withColumn('email_address', regexp_extract('info', '<(.*?)>', 1))

# 結果の表示
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 正規表現によるその他の応用例
# MAGIC 他にもパターン抽出を使う例として、電話番号や日付の抽出がある。以下にそれらの例を示す。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 電話番号の抽出

# COMMAND ----------

# MAGIC %md
# MAGIC ##### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの作成とデータの投入
# MAGIC CREATE OR REPLACE TEMP VIEW phone_table AS
# MAGIC SELECT 'Customer service: +1-800-555-1234' AS info
# MAGIC UNION ALL
# MAGIC SELECT 'Sales: +44-20-7946-0958' AS info;
# MAGIC
# MAGIC -- 電話番号の抽出
# MAGIC SELECT info, 
# MAGIC        regexp_extract(info, '\\+[0-9-]+', 0) AS phone_number
# MAGIC FROM phone_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Python

# COMMAND ----------


# データの作成
phone_data = [
    ('Customer service: +1-800-555-1234',),
    ('Sales: +44-20-7946-0958',)
]

# DataFrameの作成
phone_columns = ['info']
phone_df = spark.createDataFrame(phone_data, phone_columns)

# 電話番号の抽出
phone_df = phone_df.withColumn('phone_number', regexp_extract('info', '\\+[0-9-]+', 0))

# 結果の表示
display(phone_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 日付の抽出

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ##### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの作成とデータの投入
# MAGIC CREATE OR REPLACE TEMP VIEW date_table AS
# MAGIC SELECT 'Meeting on 2023-10-04 at conference room' AS info
# MAGIC UNION ALL
# MAGIC SELECT 'Deadline: 2023-12-31' AS info;
# MAGIC
# MAGIC -- 日付の抽出
# MAGIC SELECT info, 
# MAGIC        regexp_extract(info, '\\d{4}-\\d{2}-\\d{2}', 0) AS date
# MAGIC FROM date_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Python

# COMMAND ----------


# データの作成
date_data = [
    ('Meeting on 2023-10-04 at conference room',),
    ('Deadline: 2023-12-31',)
]

# DataFrameの作成
date_columns = ['info']
date_df = spark.createDataFrame(date_data, date_columns)

# 日付の抽出
date_df = date_df.withColumn('date', regexp_extract('info', '\\d{4}-\\d{2}-\\d{2}', 0))

# 結果の表示
display(date_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 文字列操作関数を駆使する方法
# MAGIC 特定のサブストリングを抽出するために、標準的な文字列操作関数（例えば、`substring`, `split`, `instr`など）を組み合わせる方法。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC SQLにおいて、`substring`や`split`, `instr`を用いて特定のパターンを抽出する例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの作成とデータの投入
# MAGIC CREATE OR REPLACE TEMP VIEW example_table AS
# MAGIC SELECT 'John Doe <john.doe@example.com>' AS info
# MAGIC UNION ALL
# MAGIC SELECT 'Jane Smith <jane.smith@example.net>' AS info
# MAGIC UNION ALL
# MAGIC SELECT 'Alice Wonderland <alice.w@wonderland.org>' AS info;
# MAGIC
# MAGIC -- 角括弧内のemailアドレスの抽出
# MAGIC SELECT info, 
# MAGIC        substring(info, instr(info, '<') + 1, instr(info, '>') - instr(info, '<') - 1) AS email_address
# MAGIC FROM example_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonにおいて、PySparkの`substr`, `split`, `instr`を用いて特定のパターンを抽出する例を示す。

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import expr

# SparkSessionの作成
spark = SparkSession.builder.appName("Example").getOrCreate()

# データの作成
data = [
    ('John Doe <john.doe@example.com>',),
    ('Jane Smith <jane.smith@example.net>',),
    ('Alice Wonderland <alice.w@wonderland.org>',)
]

# DataFrameの作成
columns = ['info']
df = spark.createDataFrame(data, columns)

# emailアドレスの抽出
# ※exprに渡している引数は、SQL 版と同様
df = df.withColumn('email_address', expr("substring(info, instr(info, '<') + 1, instr(info, '>') - instr(info, '<') - 1)"))

# 結果の表示
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### UDF (User-Defined Function) を使用する方法
# MAGIC ユーザーが独自に定義した関数（UDF）を用いて特定のパターンを抽出する方法。これは、より複雑な抽出ロジックを実装できるという利点がある。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC PySparkにおいて、UDFを用いて特定のパターンを抽出する例を示す。

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import re

# SparkSessionの作成
spark = SparkSession.builder.appName("Example").getOrCreate()

# データの作成
data = [
    ('John Doe <john.doe@example.com>',),
    ('Jane Smith <jane.smith@example.net>',),
    ('Alice Wonderland <alice.w@wonderland.org>',)
]

# DataFrameの作成
columns = ['info']
df = spark.createDataFrame(data, columns)

# UDFの定義
def extract_email(text):
    match = re.search(r'<(.*?)>', text)
    if match:
        return match.group(1)
    return None

extract_email_udf = udf(extract_email, StringType())

# emailアドレスの抽出
df = df.withColumn('email_address', extract_email_udf(df['info']))

# 結果の表示
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● ドット構文を利用して、ネストされたデータフィールドを抽出する。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ネストされたデータフィールドの抽出
# MAGIC ネストされたデータフィールドは、JSONデータや構造化データを扱う際に頻繁に利用され、特定のフィールドをキーを用いてアクセスする。Databricksでは、ドット（.）構文およびコロン（:）構文を使用してこれらのフィールドにアクセスすることが可能である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC Databricks SQLでは、ドット構文を利用してネストされたデータフィールドを抽出することができる。以下に例を示す。
# MAGIC
# MAGIC データテーブルとデータの作成：

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブル作成
# MAGIC CREATE OR REPLACE TEMPORARY VIEW events_strings AS
# MAGIC SELECT string(key) key, string(value) value 
# MAGIC FROM VALUES
# MAGIC   ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
# MAGIC   ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}')
# MAGIC AS events_raw(key, value);
# MAGIC
# MAGIC
# MAGIC -- データの確認
# MAGIC SELECT * FROM events_strings;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ドット構文を利用してネストされたフィールドを取得
# MAGIC SELECT 
# MAGIC   key,
# MAGIC   get_json_object(value, '$.device') AS device,
# MAGIC   get_json_object(value, '$.geo.city') AS city,
# MAGIC   get_json_object(value, '$.geo.state') AS state
# MAGIC FROM 
# MAGIC     events_strings;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC DatabricksのPython Notebookでも、ドット構文を用いてネストされたデータフィールドを抽出することができる。以下に例を示す。
# MAGIC

# COMMAND ----------


# DatabricksのNotebookにて利用
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import get_json_object

# Sparkセッションの作成
spark = SparkSession.builder.appName("NestedDataExample").getOrCreate()

# データの作成
data = [
    ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
    ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}')
]

# DataFrameの作成
columns = ["key", "value"]
df = spark.createDataFrame(data, schema=columns)
display(df)

# COMMAND ----------


# ネストされたデータフィールドの抽出
result_df = df.select(
    "key",
    get_json_object("value", "$.device").alias("device"),
    get_json_object("value", "$.geo.city").alias("city"),
    get_json_object("value", "$.geo.state").alias("state")
)

# 結果の表示
display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### コロン構文の例
# MAGIC コロン構文もネストされたデータフィールドの抽出に利用できる。それぞれの構文は、特定のケースにおける利便性に応じて選択される。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC コロン構文を使用する場合の例示：

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- コロン構文
# MAGIC SELECT 
# MAGIC   key,
# MAGIC   get_json_object(value, '$:device') AS device,
# MAGIC   get_json_object(value, '$:geo.city') AS city,
# MAGIC   get_json_object(value, '$:geo.state') AS state
# MAGIC FROM 
# MAGIC     events_strings;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- コロン構文
# MAGIC SELECT 
# MAGIC   key,
# MAGIC   value:device AS device,
# MAGIC   value:city AS city,
# MAGIC   value:state AS state
# MAGIC FROM 
# MAGIC     events_strings;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonの場合、コロン構文を利用してフィールドを抽出する方法もサポートされている。

# COMMAND ----------


result_df = df.select(
    "key",
    get_json_object("value", "$:device").alias("device"),
    get_json_object("value", "$:geo.city").alias("city"),
    get_json_object("value", "$:geo.state").alias("state")
)

display(result_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ネストされたデータフィールドの絞り込みとソート
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC Databricks SQLでは、コロン構文を利用してネストされたデータフィールドを抽出し、フィルタリングおよびソートする方法の追加例を以下に示す。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- コロン構文を利用してフィルタリングとソートを行い、ネストされたフィールドを取得
# MAGIC SELECT 
# MAGIC   * 
# MAGIC FROM 
# MAGIC   events_strings 
# MAGIC WHERE 
# MAGIC   -- get_json_object(value, '$.event_name') = 'checkout' 
# MAGIC   value:event_name = 'checkout' 
# MAGIC ORDER BY 
# MAGIC   key 
# MAGIC LIMIT 
# MAGIC   1;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- スキーマを使用してJSONを構造体に変換
# MAGIC CREATE OR REPLACE TEMPORARY VIEW events_strings AS
# MAGIC SELECT 
# MAGIC   string(key) key, 
# MAGIC   from_json(string(value), 'STRUCT<device: STRING, ecommerce: STRUCT<>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: INT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') value 
# MAGIC FROM VALUES
# MAGIC   ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
# MAGIC   ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}')
# MAGIC AS events_raw(key, value);
# MAGIC
# MAGIC -- データの確認
# MAGIC SELECT * FROM events_strings;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- ネストされたフィールドの直接抽出
# MAGIC SELECT 
# MAGIC   key,
# MAGIC   value.device AS device,
# MAGIC   value.geo.city AS city,
# MAGIC   value.geo.state AS state
# MAGIC FROM 
# MAGIC   events_strings
# MAGIC WHERE
# MAGIC   value.device = 'macOS'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC DatabricksのPython Notebookでも、`.where()` メソッドや `.orderBy()`, `.limit()` メソッドを用いて同様の操作が可能である。以下にPythonの追加例を示す。
# MAGIC

# COMMAND ----------


# DatabricksのNotebookにて利用
from pyspark.sql.functions import get_json_object

# DataFrameの作成（前述と同じ）
data = [
    ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
    ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}')
]

columns = ["key", "value"]
df = spark.createDataFrame(data, schema=columns)
display(df)


# COMMAND ----------


# ネストされたデータフィールドの絞り込みとソート
# result_df = df.where(get_json_object("value", "$.event_name") == 'checkout') \
result_df = df.where("value:event_name == 'checkout'") \
              .orderBy("key") \
              .limit(1)

# 結果の表示
display(result_df)

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, ArrayType

# Sparkセッションの作成
spark = SparkSession.builder.appName("NestedDataExample").getOrCreate()

# スキーマの定義
schema = StructType([
    StructField("device", StringType(), True),
    StructField("ecommerce", StructType([]), True),
    StructField("event_name", StringType(), True),
    StructField("event_previous_timestamp", LongType(), True),
    StructField("event_timestamp", LongType(), True),
    StructField("geo", StructType([
        StructField("city", StringType(), True),
        StructField("state", StringType(), True)
    ]), True),
    StructField("items", ArrayType(StructType([
        StructField("coupon", StringType(), True),
        StructField("item_id", StringType(), True),
        StructField("item_name", StringType(), True),
        StructField("item_revenue_in_usd", DoubleType(), True),
        StructField("price_in_usd", DoubleType(), True),
        StructField("quantity", IntegerType(), True)
    ])), True),
    StructField("traffic_source", StringType(), True),
    StructField("user_first_touch_timestamp", LongType(), True),
    StructField("user_id", StringType(), True)
])

# データの作成
data = [
    ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
    ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}')
]

columns = ["key", "value"]
df = spark.createDataFrame(data, schema=columns)


display(df)


# COMMAND ----------

# JSONをパースして構造体に変換
df = df.withColumn("value", from_json(col("value"), schema))

# 結果の表示
display(df)

# COMMAND ----------

# ネストされたフィールドの抽出
result_df = df.select(
    "key",
    col("value.device").alias("device"),
    col("value.geo.city").alias("city"),
    col("value.geo.state").alias("state")
)

# 絞り込み
result_df_filtered = result_df.where(col("device") == 'Windows')

# 結果の表示
display(result_df_filtered)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● JSON 文字列を解析して構造体にする。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 概要
# MAGIC Databricksにおいて、JSON文字列を解析して構造体に変換する方法は、`schema_of_json`および`from_json`関数を利用することで実現できる。この手法を用いることで、ネストされたデータを効率的に処理し、必要なフィールドを抽出することが可能となる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 使用する関数:
# MAGIC - **`schema_of_json`**: JSON文字列の例から派生したスキーマを返す。
# MAGIC - **`from_json`**: 指定されたスキーマを使用して、JSON文字列を含む列を構造体型に解析する。
# MAGIC
# MAGIC 以下に、SQLとPythonの例を示す。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### SQL
# MAGIC まず、Databricks SQLを使用して、JSON文字列を構造体に変換する方法を示す。これには、テーブルを作成し、JSONデータのスキーマを取得し、そのスキーマを使用して解析を行う例が含まれる。
# MAGIC
# MAGIC データテーブルとデータの作成：

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- スキーマを定義してテーブルを作成
# MAGIC CREATE OR REPLACE TEMPORARY VIEW events_strings AS
# MAGIC SELECT 
# MAGIC   string(key) key, 
# MAGIC   string(value) value 
# MAGIC FROM VALUES
# MAGIC   ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
# MAGIC   ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}'),
# MAGIC   ('UA000000107389999', '{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}'),
# MAGIC   ('UA000000108501234', '{"device":"Android","ecommerce":{"purchase_revenue_in_usd":999.9,"total_item_quantity":2,"unique_items":2},"event_name":"purchase","event_previous_timestamp":1627819284000,"event_timestamp":1627819384000,"geo":{"city":"San Francisco","state":"CA"},"items":[{"coupon":"SUMMER21","item_id":"S_MAT_Q","item_name":"Summer Queen Mattress","item_revenue_in_usd":599.9,"price_in_usd":750.0,"quantity":1},{"coupon":"SUMMER21","item_id":"P_SUMMER","item_name":"Summer Pillow","item_revenue_in_usd":400.0,"price_in_usd":500.0,"quantity":1}],"traffic_source":"organic","user_first_touch_timestamp":1627800000000,"user_id":"UA000000109999999"}'),
# MAGIC   ('UA000000105678345', '{"device":"iOS","ecommerce":{"purchase_revenue_in_usd":850.0,"total_item_quantity":1,"unique_items":1},"event_name":"purchase","event_previous_timestamp":1612319284000,"event_timestamp":1612319384000,"geo":{"city":"New York","state":"NY"},"items":[{"coupon":"SPRING21","item_id":"S_MAT_T","item_name":"Spring Twin Mattress","item_revenue_in_usd":850.0,"price_in_usd":900.0,"quantity":1}],"traffic_source":"organic","user_first_touch_timestamp":1612300000000,"user_id":"UA000000107776543"}')
# MAGIC AS events_raw(key, value);
# MAGIC
# MAGIC -- データの確認
# MAGIC SELECT * FROM events_strings;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- スキーマを取得
# MAGIC SELECT schema_of_json('{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}') AS schema;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- JSON文字列を解析して構造体として展開
# MAGIC CREATE OR REPLACE TEMP VIEW parsed_events AS 
# MAGIC SELECT json.* 
# MAGIC FROM (
# MAGIC   SELECT from_json(value, 'STRUCT<device: STRING, ecommerce: STRUCT<purchase_revenue_in_usd: DOUBLE, total_item_quantity: BIGINT, unique_items: BIGINT>, event_name: STRING, event_previous_timestamp: BIGINT, event_timestamp: BIGINT, geo: STRUCT<city: STRING, state: STRING>, items: ARRAY<STRUCT<coupon: STRING, item_id: STRING, item_name: STRING, item_revenue_in_usd: DOUBLE, price_in_usd: DOUBLE, quantity: BIGINT>>, traffic_source: STRING, user_first_touch_timestamp: BIGINT, user_id: STRING>') AS json 
# MAGIC   FROM events_strings
# MAGIC );
# MAGIC
# MAGIC -- フラット化されたデータの確認
# MAGIC SELECT * FROM parsed_events;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC 次に、Pythonを使用して同じ操作を行うアプローチを示す。DatabricksのNotebookで実行できるように、テーブルの作成およびデータの解析方法を示す。
# MAGIC
# MAGIC データの作成：

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, schema_of_json, col

# Sparkセッションの作成
spark = SparkSession.builder.appName("NestedDataExample").getOrCreate()

# データの作成
data = [
    ('UA000000107384205', '{"device":"macOS","ecommerce":{},"event_name":"checkout","event_previous_timestamp":1593880801027797,"event_timestamp":1593880822506642,"geo":{"city":"Traverse City","state":"MI"},"items":[{"item_id":"M_STAN_T","item_name":"Standard Twin Mattress","item_revenue_in_usd":595.0,"price_in_usd":595.0,"quantity":1}],"traffic_source":"google","user_first_touch_timestamp":1593879413256859,"user_id":"UA000000107384208"}'),
    ('UA000000107388621', '{"device":"Windows","ecommerce":{},"event_name":"email_coupon","event_previous_timestamp":1593880770092554,"event_timestamp":1593880829320848,"geo":{"city":"Hickory","state":"NC"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_F","item_name":"Standard Full Mattress","item_revenue_in_usd":850.5,"price_in_usd":945.0,"quantity":1}],"traffic_source":"direct","user_first_touch_timestamp":159387985037719,"user_id":"UA000000107388621"}'),
    ('UA000000107389999', '{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}'),
    ('UA000000108501234', '{"device":"Android","ecommerce":{"purchase_revenue_in_usd":999.9,"total_item_quantity":2,"unique_items":2},"event_name":"purchase","event_previous_timestamp":1627819284000,"event_timestamp":1627819384000,"geo":{"city":"San Francisco","state":"CA"},"items":[{"coupon":"SUMMER21","item_id":"S_MAT_Q","item_name":"Summer Queen Mattress","item_revenue_in_usd":599.9,"price_in_usd":750.0,"quantity":1},{"coupon":"SUMMER21","item_id":"P_SUMMER","item_name":"Summer Pillow","item_revenue_in_usd":400.0,"price_in_usd":500.0,"quantity":1}],"traffic_source":"organic","user_first_touch_timestamp":1627800000000,"user_id":"UA000000109999999"}'),
    ('UA000000105678345', '{"device":"iOS","ecommerce":{"purchase_revenue_in_usd":850.0,"total_item_quantity":1,"unique_items":1},"event_name":"purchase","event_previous_timestamp":1612319284000,"event_timestamp":1612319384000,"geo":{"city":"New York","state":"NY"},"items":[{"coupon":"SPRING21","item_id":"S_MAT_T","item_name":"Spring Twin Mattress","item_revenue_in_usd":850.0,"price_in_usd":900.0,"quantity":1}],"traffic_source":"organic","user_first_touch_timestamp":1612300000000,"user_id":"UA000000107776543"}')
]

columns = ["key", "value"]
df = spark.createDataFrame(data, schema=columns)
display(df)


# COMMAND ----------


# スキーマの取得
json_string = '{"device":"Linux","ecommerce":{"purchase_revenue_in_usd":1075.5,"total_item_quantity":1,"unique_items":1},"event_name":"finalize","event_previous_timestamp":1593879231210816,"event_timestamp":1593879335779563,"geo":{"city":"Houston","state":"TX"},"items":[{"coupon":"NEWBED10","item_id":"M_STAN_K","item_name":"Standard King Mattress","item_revenue_in_usd":1075.5,"price_in_usd":1195.0,"quantity":1}],"traffic_source":"email","user_first_touch_timestamp":1593454417513109,"user_id":"UA000000106116176"}'
schema = schema_of_json(json_string)

# JSONデータの解析および構造体への変換
parsed_df = df.select(from_json(col("value"), schema).alias("json")).select("json.*")

# 結果の表示
display(parsed_df)
