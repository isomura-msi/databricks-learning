# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-b24dbbe7-205a-4a6a-8b35-ef14ee51f01c
# MAGIC %md
# MAGIC # データの取り込みラボ (Ingesting Data Lab)
# MAGIC
# MAGIC 製品データを含む CSV ファイルを読み込みます。
# MAGIC
# MAGIC ##### タスク (Tasks)
# MAGIC 1. スキーマを推測して読み取る
# MAGIC 2. ユーザー定義スキーマで読み取る
# MAGIC 3. スキーマをDDL形式の文字列として読み取る
# MAGIC 4. Delta形式で書き込む

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.08L

# COMMAND ----------

# DBTITLE 0,--i18n-13d2d883-def7-49df-b634-910428adc5a2
# MAGIC %md
# MAGIC ### 1. スキーマを推測して読み取る (Read with infer schema)
# MAGIC - DBUtils メソッド **`fs.head`** を変数 **`single_product_cs_file_path`** で指定されたファイルパスで使用して、最初のCSVファイルを表示します
# MAGIC - 変数 **`products_csv_path`** で提供されるファイルパスにあるCSVファイルから読み取って **`products_df`** を作成します
# MAGIC    - 最初の行をヘッダーとして使用し、スキーマを推測するオプションを構成します。

# COMMAND ----------

# ANSWER
single_product_csv_file_path = f"{DA.paths.products_csv}/part-00000-tid-1663954264736839188-daf30e86-5967-4173-b9ae-d1481d3506db-2367-1-c000.csv"
print(dbutils.fs.head(single_product_csv_file_path))

products_csv_path = DA.paths.products_csv
products_df = (spark
               .read
               .option("header", True)
               .option("inferSchema", True)
               .csv(products_csv_path)
              )

products_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-3ca1ef04-e7a4-4e9b-a49e-adba3180d9a4
# MAGIC %md
# MAGIC **1.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

assert(products_df.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-d7f8541b-1691-4565-af41-6c8f36454e95
# MAGIC %md
# MAGIC ### 2. ユーザー定義のスキーマで読み取る (Read with user-defined schema)
# MAGIC カラム名とデータ型で **`StructType`** を作成してスキーマを定義します

# COMMAND ----------

# ANSWER
from pyspark.sql.types import DoubleType, StringType, StructType, StructField

user_defined_schema = StructType([
    StructField("item_id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("price", DoubleType(), True)
])

products_df2 = (spark
                .read
                .option("header", True)
                .schema(user_defined_schema)
                .csv(products_csv_path)
               )

# COMMAND ----------

# DBTITLE 0,--i18n-af9a4134-5b88-4c6b-a3bb-8a1ae7b00e53
# MAGIC %md
# MAGIC **2.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

assert(user_defined_schema.fieldNames() == ["item_id", "name", "price"])
print("All test pass")

# COMMAND ----------

from pyspark.sql import Row

expected1 = Row(item_id="M_STAN_Q", name="Standard Queen Mattress", price=1045.0)
result1 = products_df2.first()

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0a52c971-8cff-4d89-b9fb-375e1c48364d
# MAGIC %md
# MAGIC ### 3. DDL形式の文字列を使って読み取る (Read with DDL formatted string)

# COMMAND ----------

# ANSWER
ddl_schema = "`item_id` STRING,`name` STRING,`price` DOUBLE"

products_df3 = (spark
                .read
                .option("header", True)
                .schema(ddl_schema)
                .csv(products_csv_path)
               )

# COMMAND ----------

# DBTITLE 0,--i18n-733b1e61-b319-4da6-9626-3f806435eec5
# MAGIC %md
# MAGIC **3.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

assert(products_df3.count() == 12)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-0b7e2b59-3fb1-4179-9793-bddef0159c89
# MAGIC %md
# MAGIC ### 4. Deltaに書き込み (Write to Delta)
# MAGIC 変数 **`products output_path`** で提供されるファイルパスに **`products_df`** を書き込みます。

# COMMAND ----------

# ANSWER
products_output_path = DA.paths.working_dir + "/delta/products"
(products_df
 .write
 .format("delta")
 .mode("overwrite")
 .save(products_output_path)
)

# COMMAND ----------

# DBTITLE 0,--i18n-fb47127c-018d-4da0-8d6d-8584771ccd64
# MAGIC %md
# MAGIC **4.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

verify_files = dbutils.fs.ls(products_output_path)
verify_delta_format = False
verify_num_data_files = 0
for f in verify_files:
    if f.name == "_delta_log/":
        verify_delta_format = True
    elif f.name.endswith(".parquet"):
        verify_num_data_files += 1

assert verify_delta_format, "Data not written in Delta format"
assert verify_num_data_files > 0, "No data written"
del verify_files, verify_delta_format, verify_num_data_files
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-42fb4bd4-287f-4863-b6ea-f635f315d8ec
# MAGIC %md
# MAGIC ### クラスルームで使ったリソースの削除 (Clean up classroom)
# MAGIC このレッスンで作成された一時ファイル、テーブル、およびデータベースをクリーンアップします。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
