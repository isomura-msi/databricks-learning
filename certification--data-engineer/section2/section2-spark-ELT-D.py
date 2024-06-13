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
