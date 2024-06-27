# Databricks notebook source
# MAGIC %md
# MAGIC ## ● DatabricksにおけるDelta Lakeのバージョニング操作
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 前のバージョンのテーブルを記述した人物を特定する
# MAGIC Delta Lakeテーブルの変更はすべてトランザクションログに記録されるため、どの人物が特定のバージョンのテーブルを記述したかを突き止めることができる。**`DESCRIBE HISTORY`** コマンドを用いると、この情報が簡単に確認可能である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC テーブルの履歴を確認するSQLクエリ例。

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE students;
# MAGIC -- データを確認するためのテーブルの作成
# MAGIC CREATE TABLE students
# MAGIC   (id INT, name STRING, value DOUBLE);
# MAGIC
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC INSERT INTO students VALUES (4, "Ted", 4.7);
# MAGIC INSERT INTO students VALUES (5, "Tiffany", 5.5);
# MAGIC INSERT INTO students VALUES (6, "Vini", 6.3);
# MAGIC INSERT INTO students VALUES (7, "Nanana", 7.3), (8, "VinVvivi", 2.3), (9, "Numnum", 2.9);
# MAGIC
# MAGIC -- テーブル履歴を確認
# MAGIC DESCRIBE HISTORY students

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC テーブルの履歴を確認するPythonコード例。

# COMMAND ----------


from pyspark.sql import SparkSession

# SparkSessionの生成
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()
spark.sql("DROP TABLE students_py")

# データの準備
data = [
    (1, "Yve", 1.0),
    (2, "Omar", 2.5),
    (3, "Elia", 3.3),
    (4, "Ted", 4.7),
    (5, "Tiffany", 5.5),
    (6, "Vini", 6.3)
]

df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("overwrite").format("delta").saveAsTable("students_py")
table_df = spark.sql("SELECT * FROM students_py")
display(table_df)

data = [
    (7, "Yve", 1.0),
    (8, "Omar", 2.5),
    (9, "Elia", 3.3),
    (10, "Ted", 4.7),
    (11, "Tiffany", 5.5),
    (12, "Vini", 6.3)
]

df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("append").format("delta").saveAsTable("students_py")  # ★append
table_df = spark.sql("SELECT * FROM students_py")
display(table_df)

data = [
    (13, "Yve", 1.0),
    (14, "Omar", 2.5),
    (15, "Elia", 3.3),
    (16, "Ted", 4.7),
    (17, "Tiffany", 5.5),
    (18, "Vini", 6.3)
]

df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("append").format("delta").saveAsTable("students_py")  # ★append
table_df = spark.sql("SELECT * FROM students_py")
display(table_df)

# テーブル履歴を表示
history_df = spark.sql("DESCRIBE HISTORY students_py")
display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 特定のバージョンのテーブルをクエリーする
# MAGIC Delta Lakeのタイムトラベル機能を使って特定のバージョンのテーブルをクエリーすることが可能である。具体的には **`VERSION AS OF`** 句を使う。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC 以下に特定のバージョンのテーブルをクエリーするSQLの例を示す。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 特定のバージョン3をクエリー
# MAGIC SELECT * 
# MAGIC FROM students VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonを使った特定のバージョンのテーブルをクエリーする例。

# COMMAND ----------


# 特定のバージョン2をクエリーする
version_df = spark.read.format("delta").option("versionAsOf", 2).table("students_py")
display(version_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### テーブルトランザクションの履歴を確認する
# MAGIC **`DESCRIBE HISTORY`**コマンドを使って、テーブルの全トランザクション履歴を確認できる。これには各バージョンの作成者やタイムスタンプも含まれる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC 以下にテーブルトランザクションの履歴を確認するSQLの例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの履歴を確認
# MAGIC DESCRIBE HISTORY students;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonを使ってテーブルトランザクションの履歴を確認する例。

# COMMAND ----------


# テーブルの履歴を取得
history_df = spark.sql("DESCRIBE HISTORY students_py")
display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### テーブルを前のバージョンにロールバックする
# MAGIC **`RESTORE TABLE`** 機能を活用することで、簡単に前のバージョンにロールバックすることができる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC 以下に特定のバージョン8にロールバックするSQLの例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルをバージョン3にロールバック
# MAGIC RESTORE TABLE students TO VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM students;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonを使ってテーブルを前のバージョンにロールバックする例。
# MAGIC

# COMMAND ----------


# テーブルをバージョン1にロールバック
spark.sql("RESTORE TABLE students_py TO VERSION AS OF 1")
df = spark.sql("SELECT * FROM students_py")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### テーブルを前のバージョンにロールバックできることを特定する
# MAGIC テーブルの状態を前のバージョンに戻すことが可能かどうかは、**`RESTORE`** コマンドを使って確認する。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC 以下にテーブルがバージョン4にロールバックできることを確認するSQLの例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- バージョンを確認する
# MAGIC DESCRIBE HISTORY students;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students VERSION AS OF 4;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonを使ってテーブルが前のバージョンにロールバックできることを確認する例。

# COMMAND ----------


# 履歴を確認してバージョンが存在するか確かめる
history_df = spark.sql("DESCRIBE HISTORY students_py")
display(history_df)

# バージョン1を確認する
version_df = spark.read.format("delta").option("versionAsOf", 1).table("students_py")
display(version_df)
