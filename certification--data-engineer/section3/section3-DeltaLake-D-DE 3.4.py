# Databricks notebook source
# MAGIC %md
# MAGIC ## ● データ準備

# COMMAND ----------

# 現在のノートブックのパスを取得
import os

# 現在のノートブックのフルパスを取得
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(notebook_path)
# 親フォルダのパスを取得
csv_dir_full_path = os.path.join(os.path.dirname(notebook_path), "data")

print(csv_dir_full_path)
spark.conf.set("spark.sql.csvDirFullPath", csv_dir_full_path)

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"file:/Workspace/{csv_dir_full_path}/sales-historical.csv")
display(df)

# dbutils.fs.rm("dbfs:/mnt/my-external-data/external_table/raw/sales-historical", recurse=True)
dbutils.fs.rm("dbfs:/mnt/my-external-data/external_table", recurse=True)
df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/sales-historical")
# df.write.format("delta") \
#     .mode("overwrite") \
#     .save("dbfs:/mnt/my-external-data/external_table/raw/sales-historical")

df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/sales-historical")
display(df_parquet)

# ----

df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(f"file:/Workspace/{csv_dir_full_path}/users-historical.csv")
display(df)

df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/users-historical")
df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/users-historical")
display(df_parquet)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● CTAS (Create Table as Select) 文の概要
# MAGIC CREATE TABLE AS SELECT (CTAS) 文は、入力クエリから取得したデータを使用して新しいテーブルを作成し、そのデータをすぐに挿入するためのSQLコマンドである。CTAS文は、データのレプリケーションや変換、サブセットの抽出など、さまざまなデータ操作に利用される。手動のスキーマ宣言はサポートしないが、クエリの結果からスキーマを自動的に推測できる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブル作成とデータ挿入
# MAGIC CREATE OR REPLACE TABLE sales AS
# MAGIC SELECT * FROM parquet.`/mnt/my-external-data/external_table/raw/sales-historical`;
# MAGIC
# MAGIC -- テーブルの確認
# MAGIC DESCRIBE EXTENDED sales;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- データの表示
# MAGIC SELECT * FROM sales LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("CTAS Example").getOrCreate()

# データフレームを作成しDelta形式で保存
df = spark.read.parquet("/mnt/my-external-data/external_table/raw/sales-historical")
spark.sql("DROP TABLE sales_py")
df.write.format("delta").saveAsTable("sales_py")

# テーブル情報を表示
df_explained = spark.sql("DESCRIBE EXTENDED sales_py")
display(df_explained)

# データを表示
df_sales = spark.sql("SELECT * FROM sales_py LIMIT 10")
display(df_sales)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● CTAS文の制約
# MAGIC CTAS文は、スキーマ宣言をサポートせず、追加ファイルオプションの指定もできない。そのため、CSVファイルなどからデータを取り込む際に制約がある。具体的には、CSVファイルからデータを取り込む際にカラムのデータ型やオプション（ヘッダー行やデリミタなど）が指定できない。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CTASでCSVファイルを取り込む際の問題点
# MAGIC CREATE OR REPLACE TABLE sales_unparsed AS
# MAGIC SELECT * FROM csv.`file:/Workspace/${spark.sql.csvDirFullPath}/sales-historical.csv`;
# MAGIC
# MAGIC -- データの表示
# MAGIC SELECT * FROM sales_unparsed LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● CTAS をソリューションとして使う方法
# MAGIC
# MAGIC CTAS文を用いる前に、まずCSVファイルなどからオプションを指定してデータを取り込みテンポラリビューを作成し、そのテンポラリビューを利用して新しいテーブルを正確に作成することができる。そうすることでDeltaテーブルに変換することができる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テンポラリビューを使用したCTASの改善例
# MAGIC CREATE OR REPLACE TEMP VIEW sales_tmp_vw
# MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
# MAGIC USING CSV
# MAGIC OPTIONS (
# MAGIC   path = "file:/Workspace/${spark.sql.csvDirFullPath}/raw-sales.csv",
# MAGIC   header = "true",
# MAGIC   delimiter = "|"
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM sales_tmp_vw LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テンポラリビューを利用してDeltaテーブルを作成
# MAGIC DROP TABLE sales_delta;
# MAGIC CREATE TABLE sales_delta AS
# MAGIC   SELECT * FROM sales_tmp_vw;
# MAGIC
# MAGIC -- データの表示
# MAGIC SELECT * FROM sales_delta LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python

# COMMAND ----------


# テンポラリビューを使用したCSVファイルの読み込み
df_tmp = spark.read.option("header", "true").option("delimiter", "|").csv(f"file:/Workspace/{csv_dir_full_path}/raw-sales.csv")
df_tmp.createOrReplaceTempView("sales_tmp_vw")

# テンポラリビューを利用してDeltaテーブルを作成
df_tmp_sql = spark.sql("SELECT * FROM sales_tmp_vw")
spark.sql("DROP TABLE sales_delta_py")
df_tmp_sql.write.format("delta").saveAsTable("sales_delta_py")

# データを表示
df_sales_delta = spark.sql("SELECT * FROM sales_delta_py LIMIT 10")
display(df_sales_delta)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 自動生成列を含むスキーマを宣言する方法
# MAGIC `GENERATED ALWAYS AS`で定義された列は、ユーザーが指定した関数に基づいてDeltaテーブルの他の列から値が自動的に生成される特別な列である。Databricksでは、`GENERATED ALWAYS AS`の列を宣言することによって、自動的に計算された値を保存できる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL
# MAGIC 以下のコードは、`GENERATED ALWAYS AS`の列を含むテーブルを作成し、その列に対して指定された関数を用いて値を生成する方法である。ここでは、Unixタイムスタンプを日付に変換する例を説明する。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE purchase_dates (
# MAGIC   id STRING, 
# MAGIC   transaction_timestamp STRING, 
# MAGIC   price STRING,
# MAGIC   date DATE GENERATED ALWAYS AS (
# MAGIC     cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
# MAGIC     COMMENT "generated based on `transactions_timestamp` column");
# MAGIC
# MAGIC -- テーブルの生成
# MAGIC SELECT * FROM purchase_dates LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC 以下のPythonコードは、Databricks上で生成された列を持つテーブルを作成し、ソーステーブルからデータをマージする方法を示している。
# MAGIC

# COMMAND ----------


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Generated Columns Example").getOrCreate()

# 必要な設定
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# テーブルの作成
spark.sql("""
CREATE OR REPLACE TABLE purchase_dates (
  id STRING, 
  transaction_timestamp STRING, 
  price STRING,
  date DATE GENERATED ALWAYS AS (
    cast(cast(transaction_timestamp/1e6 AS TIMESTAMP) AS DATE))
    COMMENT "generated based on `transactions_timestamp` column")
""")

# データの表示
df = spark.sql("SELECT * FROM purchase_dates LIMIT 10")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● 自動生成列に値を挿入
# MAGIC `GENERATED ALWAYS AS`の列を持つテーブルに対して、値を挿入する過程では、その`GENERATED ALWAYS AS`の列の値を明示的に指定する必要がない。Databricksは自動的に列の生成ロジックに基づいて値を計算する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC 以下のコードでは、生成された列を持つテーブルに他のテーブルからデータをマージする方法を示している。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE purchases AS
# MAGIC SELECT order_id AS id, transaction_timestamp, purchase_revenue_in_usd AS price
# MAGIC FROM sales;
# MAGIC
# MAGIC SELECT * FROM purchases

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.schema.autoMerge.enabled=true; 
# MAGIC
# MAGIC MERGE INTO purchase_dates a
# MAGIC USING purchases b
# MAGIC ON a.id = b.id
# MAGIC WHEN NOT MATCHED THEN
# MAGIC   INSERT *;
# MAGIC
# MAGIC -- データの表示
# MAGIC SELECT * FROM purchase_dates LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python
# MAGIC 以下のPythonコードは、MERGE文を使用して`GENERATED ALWAYS AS`の列を持つテーブルにデータを挿入する方法を示している。
# MAGIC

# COMMAND ----------


# データの挿入
spark.sql("""
MERGE INTO purchase_dates a
USING purchases b
ON a.id = b.id
WHEN NOT MATCHED THEN
  INSERT *
""")

# データの表示
df = spark.sql("SELECT * FROM purchase_dates LIMIT 10")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC `GENERATED ALWAYS AS`の列の値は、テーブルへの挿入時に自動的に計算される。必要に応じて、テーブルの制約（例えば、CHECK制約）を追加することで、データの品質と整合性を強制することも可能である。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● テーブルの制約を追加する
# MAGIC
# MAGIC Databricks では、データの品質と整合性を確保するために、Delta Lake に対して標準SQLの制約管理の句を使用して制約を追加できる。現在サポートされている制約は **`NOT NULL`** 制約と **`CHECK`** 制約である。制約を定義する前に、制約に違反するデータが既にテーブルに存在しないことを確認する必要がある。以下に、各制約の説明とサンプルコードを示す。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### NOT NULL 制約
# MAGIC
# MAGIC **`NOT NULL`** 制約は、指定された列が NULL 値を持つことを禁止する制約である。この制約により、データの一貫性を保つことが可能である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC
# MAGIC まず、サンプルデータ用のテーブルを作成し、データを挿入する。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE employees (
# MAGIC     id INT,
# MAGIC     name STRING,
# MAGIC     department STRING
# MAGIC );
# MAGIC
# MAGIC INSERT INTO employees VALUES
# MAGIC (1, 'Alice', 'Engineering'),
# MAGIC (2, 'Bob', 'HR'),
# MAGIC (3, 'Charlie', 'Sales');
# MAGIC
# MAGIC -- テーブル内容を確認
# MAGIC SELECT * FROM employees LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 次に、 **`department`** 列に対して **`NOT NULL`** 制約を追加する。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE employees ALTER COLUMN department SET NOT NULL;
# MAGIC
# MAGIC -- 制約の確認
# MAGIC -- テーブル作成SQLを表示
# MAGIC SHOW CREATE TABLE employees;

# COMMAND ----------

# MAGIC %md
# MAGIC 下記のコードを有効化し、実際に NULL の値を入れて制約違反となることを確認する。
# MAGIC
# MAGIC `com.databricks.sql.transaction.tahoe.schema.DeltaInvariantViolationException: NOT NULL constraint violated for column: department.` となるはずである。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO employees VALUES
# MAGIC -- (4, 'Alice2', 'Engineering'),
# MAGIC -- (5, 'Bob2', NULL);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC
# MAGIC Python の場合も同様のプロセスで制約を追加する。

# COMMAND ----------

from pyspark.sql import SparkSession

# スパークセッションの開始
spark = SparkSession.builder.appName("DatabricksExample").getOrCreate()

# サンプルデータの生成
data = [
    (1, 'Alice', 'Engineering'),
    (2, 'Bob', 'HR'),
    (3, 'Charlie', 'Sales'),
]
columns = ["id", "name", "department"]

# データを含む DataFrame の作成
df = spark.createDataFrame(data, columns)

# テーブルとしてデータを保存
df.write.format("delta").saveAsTable("employees_py")

# テーブル内容を表示
display(spark.sql("SELECT * FROM employees_py"))

# department 列に対して NOT NULL 制約を追加
spark.sql("ALTER TABLE employees_py ALTER COLUMN department SET NOT NULL")

# 制約の確認
# テーブル作成SQLを表示
display(spark.sql("SHOW CREATE TABLE employees_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC 下記のコードを有効化し、実際に NULL（None） の値を入れて制約違反となることを確認する。
# MAGIC
# MAGIC `com.databricks.sql.transaction.tahoe.schema.DeltaInvariantViolationException: NOT NULL constraint violated for column: department.` となるはずである。

# COMMAND ----------

# 追加のデータを insert
# additional_data = [
#     (4, 'David', 'Marketing'),
#     (5, 'Eve', None)
# ]
# additional_df = spark.createDataFrame(additional_data, columns)
# additional_df.write.format("delta").mode("append").saveAsTable("employees_py")

# COMMAND ----------

from delta.tables import DeltaTable

# UPDATE の例
# DeltaTable オブジェクトの作成
delta_table = DeltaTable.forName(spark, "employees_py")

# # 一部のデータを update
# delta_table.update(
#     condition="name = 'Alice'",
#     set={"department": None }
# )
# 一部のデータを None に更新して制約違反を発生させる
try:
    delta_table.update(
        condition="name = 'Alice'",
        set={"department": "NULL"}  # 意図的に制約違反を発生させる・・・が期待通りに行っていない。しかも Job が4回も実行されているようである。
    )
except Exception as e:
    print("An error occurred during the update:")
    print(e)

# COMMAND ----------

display(spark.sql("SELECT * FROM employees_py"))


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### CHECK 制約
# MAGIC
# MAGIC **`CHECK`** 制約は、指定された条件が満たされる場合にのみデータの挿入や更新を許可する制約である。以下では、テーブルの **`date`** 列に **`CHECK`** 制約を追加する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC
# MAGIC まず、サンプルデータ用のテーブルを作成し、データを挿入する。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE purchase_dates (
# MAGIC     id INT,
# MAGIC     date DATE
# MAGIC );
# MAGIC
# MAGIC INSERT INTO purchase_dates VALUES
# MAGIC (1, '2021-01-10'),
# MAGIC (2, '2021-02-15'),
# MAGIC (3, '2019-12-25');
# MAGIC
# MAGIC -- テーブル内容を確認
# MAGIC SELECT * FROM purchase_dates LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 次に、 **`date`** 列に対して **`CHECK`** 制約を追加する。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC ALTER TABLE purchase_dates ADD CONSTRAINT valid_date CHECK (date > '2018-01-01');
# MAGIC
# MAGIC -- 制約の確認
# MAGIC -- DESCRIBE EXTENDED purchase_dates;
# MAGIC SHOW CREATE TABLE purchase_dates;

# COMMAND ----------

# MAGIC %md
# MAGIC 下記のコードを有効化し、実際に制約違反の値を入れて制約違反となることを確認する。
# MAGIC
# MAGIC `com.databricks.sql.transaction.tahoe.schema.DeltaInvariantViolationException: CHECK constraint valid_date (date > '2018-01-01') violated by row with values:` となるはずである。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- INSERT INTO purchase_dates VALUES
# MAGIC -- (4, '2021-01-10'),
# MAGIC -- (5, '2021-02-15'),
# MAGIC -- (6, '2017-12-25');

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC
# MAGIC Python の場合も同様のプロセスで制約を追加する。

# COMMAND ----------


from pyspark.sql import SparkSession
from datetime import date

# スパークセッションの開始
spark = SparkSession.builder.appName("DatabricksExample").getOrCreate()

# サンプルデータの生成
data = [
    (1, date(2021, 1, 10)),
    (2, date(2021, 2, 15)),
    (3, date(2019, 12, 25)),
]
columns = ["id", "date"]

# データを含む DataFrame の作成
df = spark.createDataFrame(data, columns)

# テーブルとしてデータを保存
spark.sql("DROP TABLE purchase_dates_py")
df.write.format("delta").saveAsTable("purchase_dates_py")

# テーブル内容を表示
display(spark.sql("SELECT * FROM purchase_dates_py"))

# date 列に対して CHECK 制約を追加
spark.sql("ALTER TABLE purchase_dates_py ADD CONSTRAINT valid_date CHECK (date > '2019-01-01')")

# 制約の確認
display(spark.sql("DESCRIBE EXTENDED purchase_dates_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC 下記のコードを有効化し、実際に制約違反の値を入れて制約違反となることを確認する。
# MAGIC
# MAGIC `com.databricks.sql.transaction.tahoe.schema.DeltaInvariantViolationException: CHECK constraint valid_date (date > '2019-01-01') violated by row with values:` となるはずである。

# COMMAND ----------

# # 追加のデータを insert
# # サンプルデータの生成
# data = [
#     (4, date(2021, 1, 10)),
#     (5, date(2021, 2, 15)),
#     (6, date(2016, 12, 25)),
# ]
# columns = ["id", "date"]

# # データを含む DataFrame の作成
# df = spark.createDataFrame(data, columns)

# # テーブルとしてデータを保存
# df.write.format("delta").mode("append").saveAsTable("purchase_dates_py")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● オプションとメタデータを追加してテーブルをエンリッチ化する
# MAGIC
# MAGIC Delta Lakeテーブルをエンリッチ化するには、テーブルの作成時に追加のオプションやメタデータを指定することが重要である。これにより、テーブルに関する詳細な情報を保持し、トラブルシューティングやデータの管理を容易にすることができる。以下では、具体的な実装例として、コメントの追加、データの位置指定、日付列によるパーティショニングを含むテーブル作成方法を示す。また、SQLとPythonの両方の例を提供する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### メタデータとオプションの追加
# MAGIC
# MAGIC 追加のメタデータとオプションを含めることで、テーブルをより詳しく管理できる。以下の例では、`CREATE TABLE` ステートメントにコメントとロケーションを追加し、日付列でパーティショニングを行う。また、`SELECT` 句を使用してタイムスタンプとソースファイル名の列を追加する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC
# MAGIC 1. **テーブルを作成し、メタデータを追加するSQL文**
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの作成とデータのinsert
# MAGIC CREATE OR REPLACE TABLE users_pii
# MAGIC COMMENT "Contains PII"
# MAGIC LOCATION "/mnt/delta/tmp/users_pii"
# MAGIC PARTITIONED BY (first_touch_date)
# MAGIC AS
# MAGIC   SELECT *, 
# MAGIC     cast(cast(user_first_touch_timestamp/1e6 AS TIMESTAMP) AS DATE) gen_first_touch_date, 
# MAGIC     current_timestamp() updatedAt,
# MAGIC     input_file_name() AS source_file_renamed
# MAGIC   FROM parquet.`/mnt/my-external-data/external_table/raw/users-historical`;
# MAGIC
# MAGIC -- 作成されたテーブルの内容を確認
# MAGIC SELECT * FROM users_pii LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC 2. **テーブルプロパティの確認**
# MAGIC
# MAGIC "Contains PII" という内容が見られる。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED users_pii;

# COMMAND ----------

# MAGIC %md
# MAGIC 3. **パーティショニング場所の確認**

# COMMAND ----------

    # パーティションディレクトリの確認
    print("Partitions in users_pii:")
    partitions = dbutils.fs.ls("/mnt/delta/tmp/users_pii")
    display(partitions)

    # 特定のパーティションディレクトリ内容の確認
    print("Contents of a specific partition:")
    partition_files = dbutils.fs.ls("/mnt/delta/tmp/users_pii/first_touch_date=2020-06-15/")
    display(partition_files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC
# MAGIC 1. **テーブルを作成し、メタデータを追加するPythonコード**

# COMMAND ----------


    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp, input_file_name

    # Spark Sessionの開始
    spark = SparkSession.builder.appName("DatabricksExample").getOrCreate()

    # データのパス
    data_path = "/mnt/my-external-data/external_table/raw/users-historical/"

    # テーブルの作成
    df = spark.read.parquet(data_path)
    df_with_metadata = df.withColumn("gen_first_touch_date", (df["user_first_touch_timestamp"] / 1e6).cast("timestamp").cast("date")) \
                         .withColumn("gen_updated", current_timestamp()) \
                         .withColumn("gen_source_file", input_file_name())

    df_with_metadata.write \
        .option("comment", "Contains PII") \
        .option("path", "/mnt/delta/tmp/users_pii_py") \
        .partitionBy("first_touch_date") \
        .format("delta") \
        .saveAsTable("users_pii_py")

    # 作成されたテーブルの内容を表示
    print("Initial data in users_pii_py:")
    display(spark.sql("SELECT * FROM users_pii_py LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 2. **テーブルプロパティの確認**

# COMMAND ----------

# テーブルプロパティの確認
describe_df = spark.sql("DESCRIBE EXTENDED users_pii_py")
print("Metadata of users_pii_py:")
display(describe_df)


# COMMAND ----------

# MAGIC %md
# MAGIC 3. **パーティショニング場所の確認**

# COMMAND ----------

# パーティションディレクトリの確認
print("Partitions in users_pii_py:")
partitions = dbutils.fs.ls("/mnt/delta/tmp/users_pii_py")
display(partitions)

# 特定のパーティションディレクトリ内容の確認
print("Contents of a specific partition:")
partition_files = dbutils.fs.ls("/mnt/delta/tmp/users_pii_py/first_touch_date=2020-06-15/")
display(partition_files)


# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Delta Lakeテーブルの複製
# MAGIC
# MAGIC Delta Lakeテーブルを効率的に複製するために、Delta Lakeには **`DEEP CLONE`** と **`SHALLOW CLONE`** の2つの方法がある。これらの方法は、異なる用途や要件に応じてテーブルの複製を簡単かつ効果的に行うことを可能にする。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DEEP CLONE
# MAGIC `DEEP CLONE` は、ソーステーブルからターゲットテーブルにデータとメタデータを完全にコピーする。このコピーはインクリメンタルに行われるため、再度実行するとソースからターゲットの場所に変更を同期することができる。ただし、すべてのデータファイルをコピーする必要があるため、大きなデータセットの場合、この処理は長時間がかかる可能性がある。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### インクリメンタルなコピーと再同期についての補足説明
# MAGIC
# MAGIC **インクリメンタルなコピー**という概念は、データの完全なコピーではなく、差分のみをコピーすることで効率的にデータの同期を行う方法を指す。ここで言及されている「このコピーはインクリメンタルに行われるため、再度実行するとソースからターゲットの場所に変更を同期することができる」とは具体的に次のような意味である。
# MAGIC
# MAGIC ##### インクリメンタルコピーの概要
# MAGIC
# MAGIC - **初回のDEEP CLONE**: 初めて `DEEP CLONE` を実行すると、ソーステーブル内の全データとメタデータがターゲットテーブルに完全にコピーされる。これにはすべてのファイルが含まれるため、特に大きなテーブルの場合は時間がかかる可能性がある。
# MAGIC
# MAGIC - **再度のDEEP CLONE**: ソーステーブルに変更が加わった後に再び `DEEP CLONE` を実行すると、初回のコピー以降に行われた変更、つまり追加・更新・削除されたデータや変更されたメタデータのみがターゲットテーブルに同期される。これにより、再度のコピーは初回に比べて効率的かつ高速になる。
# MAGIC
# MAGIC ##### 再同期の実例
# MAGIC
# MAGIC 例えば、次のようなシナリオを考える：
# MAGIC
# MAGIC 1. **初回のDEEP CLONE**:
# MAGIC     ソーステーブル `purchases` には10万行のレコードが存在する。これを `DEEP CLONE` してターゲットテーブル `purchases_clone` を作成すると、10万行すべてがコピーされる。
# MAGIC
# MAGIC 2. **ソーステーブルの更新**:
# MAGIC     次に、ソーステーブル `purchases` に1万行の新しいレコードを追加し、既存のいくつかのレコードを更新したとする。
# MAGIC
# MAGIC 3. **再度のDEEP CLONE**:
# MAGIC     再び `DEEP CLONE` を実行すると、初回クローン以降に追加された1万行と他の更新された部分のみがターゲットテーブル `purchases_clone` にコピーされる。
# MAGIC
# MAGIC このように、インクリメンタルコピーは効率的なデータ管理手法であり、特に大規模データセットの同期が頻繁に必要な場合に有効である。再度のクローン操作で全データを再コピーするのではなく、変更された部分のみを同期することで、必要な時間とリソースを大幅に削減できる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC 以下は `DEEP CLONE` を使用してテーブルを複製する際のSQLの例である。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- オリジナルのテーブルを作成
# MAGIC CREATE OR REPLACE TABLE purchases (
# MAGIC   id INT,
# MAGIC   amount DECIMAL(10, 2),
# MAGIC   purchase_date DATE
# MAGIC );
# MAGIC
# MAGIC -- データを挿入
# MAGIC INSERT INTO purchases VALUES
# MAGIC (1, 100.50, '2021-01-01'),
# MAGIC (2, 200.75, '2021-02-15'),
# MAGIC (3, 50.30, '2021-03-10'),
# MAGIC (4, 300.00, '2021-04-05'),
# MAGIC (5, 120.20, '2021-05-12');
# MAGIC
# MAGIC -- 挿入したデータを確認
# MAGIC SELECT * FROM purchases LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DEEP CLONEを作成
# MAGIC CREATE OR REPLACE TABLE purchases_clone
# MAGIC DEEP CLONE purchases;
# MAGIC
# MAGIC -- 複製されたテーブルのデータを確認
# MAGIC SELECT * FROM purchases_clone LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonでも同様に `DEEP CLONE` を使用してテーブルを複製することができる。以下にその例を示す。

# COMMAND ----------

from pyspark.sql import SparkSession

# スパークセッションの開始
spark = SparkSession.builder.appName("DatabricksDeltaCloneExample").getOrCreate()

# サンプルデータを含む DataFrame の作成
data = [
    (1, 100.50, '2021-01-01'),
    (2, 200.75, '2021-02-15'),
    (3, 50.30, '2021-03-10'),
    (4, 300.00, '2021-04-05'),
    (5, 120.20, '2021-05-12')
]
columns = ["id", "amount", "purchase_date"]

df = spark.createDataFrame(data, columns)

# テーブルとしてデータを保存
df.write.format("delta").saveAsTable("purchases_py")

# テーブル内容を表示
print("Initial data in purchases_py:")
display(spark.sql("SELECT * FROM purchases_py"))

# DEEP CLONEを作成
spark.sql("CREATE OR REPLACE TABLE purchases_clone_py DEEP CLONE purchases_py")

# 複製されたテーブルのデータを表示
print("Data in purchases_clone_py:")
display(spark.sql("SELECT * FROM purchases_clone_py"))

# COMMAND ----------

# MAGIC %md
# MAGIC 生成AIに聞いたDeep Cloneの別の方法
# MAGIC
# MAGIC ※PythonでDeltaテーブルを複製する場合、SQL文を使用せずにDelta LakeのAPIを活用する方法もあるが、現時点ではDelta LakeのAPIにおいて直接的にSHALLOW CLONEやDEEP CLONEを作成するメソッドは提供されていない。このため、SQL文を使用する方法が主なアプローチとなる。
# MAGIC
# MAGIC ただし、display の利用や spark.sql を使用しない形でのテーブル読み込みや内容表示についての代替案を提供することは可能である。以下に、SQL文を使わずにDeltaテーブルを読み込む方法と表示する方法を示す。

# COMMAND ----------

# Deltaテーブルとして保存されたテーブルをDataFrameとして読み込む
df_clone = spark.read.format("delta").table("purchases_clone_py")

# DataFrameの内容を表示
display(df_clone)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### SHALLOW CLONE
# MAGIC `SHALLOW CLONE` は、Deltaトランザクションログのみをコピーするため、データは移動しない。これにより、短時間でテーブルの複製が可能になる。`SHALLOW CLONE` は、実際のデータを持たずにデータ構造やロジックを試すために便利である。クローン元のデータが変更された場合でもクローン先にはその変更は反映されず、独立して管理される。
# MAGIC
# MAGIC セミナーメモ：  
# MAGIC >トランザクションデータは、クローン元のデータを参照している  
# MAGIC 使い所：  
# MAGIC   本番データが多いが、データ構造をちょっと変えて試験してみたいとき、本番データコピーできない場合に使う  
# MAGIC シャロークローン後、クローン元でデータ変更があった場合に、シャロークローン先ではその変更データは見えない。  
# MAGIC   シャロークローン先ではトランザクション  
# MAGIC   VACUUMすると見えなくなる

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC 以下は `SHALLOW CLONE` を使用してテーブルを複製する際のSQLの例である。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- オリジナルのテーブルを基に SHALLOW CLONE を作成
# MAGIC CREATE OR REPLACE TABLE purchases_shallow_clone
# MAGIC SHALLOW CLONE purchases;
# MAGIC
# MAGIC -- 複製されたテーブルのデータを確認
# MAGIC SELECT * FROM purchases_shallow_clone LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC Pythonでも同様に `SHALLOW CLONE` を使用してテーブルを複製することができる。以下にその例を示す。

# COMMAND ----------


# SHALLOW CLONEを作成
spark.sql("CREATE OR REPLACE TABLE purchases_shallow_clone_py SHALLOW CLONE purchases_py")

# 複製されたテーブルのデータを表示
print("Data in purchases_shallow_clone_py:")
display(spark.sql("SELECT * FROM purchases_shallow_clone_py"))
