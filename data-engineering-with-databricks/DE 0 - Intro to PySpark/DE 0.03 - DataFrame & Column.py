# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-ab2602b5-4183-4f33-8063-cfc03fcb1425
# MAGIC %md
# MAGIC # DataFrame & カラム (DataFrame & Column)
# MAGIC ##### 目的 (Objectives)
# MAGIC 1. カラムの作成
# MAGIC 1. カラムのサブセット化
# MAGIC 1. カラムの追加または置換
# MAGIC 1. 行のサブセット化
# MAGIC 1. 行の並び替え
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`select`**, **`selectExpr`**, **`drop`**, **`withColumn`**, **`withColumnRenamed`**, **`filter`**, **`distinct`**, **`limit`**, **`sort`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">カラム</a>: **`alias`**, **`isin`**, **`cast`**, **`isNotNull`**, **`desc`**, operators

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.03

# COMMAND ----------

# DBTITLE 0,--i18n-ef990348-e991-4edb-bf45-84de46a34759
# MAGIC %md
# MAGIC BedBricksのイベントデータセットを使ってみましょう。

# COMMAND ----------

events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4ea9a278-1eb6-45ad-9f96-34e0fd0da553
# MAGIC %md
# MAGIC ## カラムの式 (Column Expressions)
# MAGIC
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">カラム</a>は式を使用してDataFrameのデータに基づいて計算される論理構造です。
# MAGIC
# MAGIC DataFrame内に存在する入力カラムに基づいて新しいカラムを作成します。

# COMMAND ----------

from pyspark.sql.functions import col

print(events_df.device)
print(events_df["device"])
print(col("device"))

# COMMAND ----------

# DBTITLE 0,--i18n-d87b8303-8f78-416e-99b0-b037caf2107a
# MAGIC %md
# MAGIC Scala は、DataFrame 内の既存のカラムに基づいて新しいカラムを作成するための追加の構文をサポートしています。

# COMMAND ----------

# MAGIC %scala
# MAGIC $"device"

# COMMAND ----------

# DBTITLE 0,--i18n-64238a77-0877-4bd4-af46-a9a8bd4763c6
# MAGIC %md
# MAGIC ### カラム演算子とメソッド (Column Operators and Methods)
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | \*, + , <, >= | 数学および比較演算子 |
# MAGIC | ==, != | 等式と不等式のテスト (Scala 演算子は **`===`** と **`=!=`** です) |
# MAGIC | alias | カラムに別名を付けます |
# MAGIC | cast, astype | カラムを別のデータ型にキャストします |
# MAGIC | isNull, isNotNull, isNan | null である、null でない、NaN である |
# MAGIC | asc, desc | カラムの昇順/降順に基づいてソート式を返します |

# COMMAND ----------

# DBTITLE 0,--i18n-6d68007e-3dbf-4f18-bde4-6990299ef086
# MAGIC %md
# MAGIC 既存のカラム、演算子、およびメソッドを使用して複雑な式を作成します。

# COMMAND ----------

col("ecommerce.purchase_revenue_in_usd") + col("ecommerce.total_item_quantity")
col("event_timestamp").desc()
(col("ecommerce.purchase_revenue_in_usd") * 100).cast("int")

# COMMAND ----------

# DBTITLE 0,--i18n-7c1c0688-8f9f-4247-b8b8-bb869414b276
# MAGIC %md
# MAGIC 以下の例で、DataFrameに対してカラムのメソッドを示します。

# COMMAND ----------

rev_df = (events_df
         .filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
         .withColumn("purchase_revenue", (col("ecommerce.purchase_revenue_in_usd") * 100).cast("int"))
         .withColumn("avg_purchase_revenue", col("ecommerce.purchase_revenue_in_usd") / col("ecommerce.total_item_quantity"))
         .sort(col("avg_purchase_revenue").desc())
        )

display(rev_df)

# COMMAND ----------

# DBTITLE 0,--i18n-7ba60230-ecd3-49dd-a4c8-d964addc6692
# MAGIC %md
# MAGIC ## DataFrameのトランスフォーメーションメソッド
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | **`select`** | 各要素に対して与えられた式を計算して新しい DataFrame を返します |
# MAGIC | **`drop`** | カラムが削除された新しい DataFrame を返します |
# MAGIC | **`withColumnRenamed`** | カラムの名前が変更された新しい DataFrame を返します |
# MAGIC | **`withColumn`** | カラムを追加するか、同じ名前の既存のカラムを置き換えて、新しい DataFrame を返します |
# MAGIC | **`filter`**, **`where`** | 指定された条件を使用して行をフィルタリングする |
# MAGIC | **`sort`**, **`orderBy`** | 指定された式でソートされた新しい DataFrame を返します |
# MAGIC | **`dropDuplicates`**, **`distinct`** | 重複する行が削除された新しい DataFrame を返します |
# MAGIC | **`limit`** | 最初の n 行を取って新しい DataFrame を返します |
# MAGIC | **`groupBy`** | 指定されたカラムを使用して DataFrame をグループ化し、集計を実行できるようにします |

# COMMAND ----------

# DBTITLE 0,--i18n-3e95eb92-30e4-44aa-8ee0-46de94c2855e
# MAGIC %md
# MAGIC ### カラムのサブセット化
# MAGIC DataFrameトランスフォーメーションを使用してカラムをサブセット化します。

# COMMAND ----------

# DBTITLE 0,--i18n-987cfd99-8e06-447f-b1c7-5f104cd5ed2f
# MAGIC %md
# MAGIC #### **`select()`**
# MAGIC カラムまたはカラムベースの式のセットを抽出します。

# COMMAND ----------

devices_df = events_df.select("user_id", "device")
display(devices_df)

# COMMAND ----------

from pyspark.sql.functions import col

locations_df = events_df.select(
    "user_id", 
    col("geo.city").alias("city"), 
    col("geo.state").alias("state")
)
display(locations_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8d556f84-bfcd-436a-a3dd-893143ce620e
# MAGIC %md
# MAGIC #### **`selectExpr()`**
# MAGIC SQL式のセットを選択します。

# COMMAND ----------

apple_df = events_df.selectExpr("user_id", "device in ('macOS', 'iOS') as apple_user")
display(apple_df)

# COMMAND ----------

# DBTITLE 0,--i18n-452f7fb3-3866-4835-827f-6d359f364046
# MAGIC %md
# MAGIC #### **`drop()`**
# MAGIC 文字列かColumnオブジェクトで、指定したカラムを削除した後、新しいDataFrameを返します。
# MAGIC
# MAGIC 複数のカラムを指定するために文字列を使用します。

# COMMAND ----------

anonymous_df = events_df.drop("user_id", "geo", "device")
display(anonymous_df)

# COMMAND ----------

no_sales_df = events_df.drop(col("ecommerce"))
display(no_sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-b11609a3-11d5-453b-b713-15131b277066
# MAGIC %md
# MAGIC ### カラムの追加または置換 (Add or replace columns)
# MAGIC DataFrameトランスフォーメーションを使用して、カラムの追加や置換を行います。

# COMMAND ----------

# DBTITLE 0,--i18n-f29a47d9-9567-40e5-910b-73c640cc61ca
# MAGIC %md
# MAGIC #### **`withColumn()`**
# MAGIC カラムを追加したり、同じ名前を持つ既存のカラムを置き換えたりして、新しいDataFrameを返します。

# COMMAND ----------

mobile_df = events_df.withColumn("mobile", col("device").isin("iOS", "Android"))
display(mobile_df)

# COMMAND ----------

purchase_quantity_df = events_df.withColumn("purchase_quantity", col("ecommerce.total_item_quantity").cast("int"))
purchase_quantity_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-969c0d9f-202f-405a-8c66-ef29076b48fc
# MAGIC %md
# MAGIC #### **`withColumnRenamed()`**
# MAGIC この関数は、カラムの名前を変更した新しいDataFrameを返します。

# COMMAND ----------

location_df = events_df.withColumnRenamed("geo", "location")
display(location_df)

# COMMAND ----------

# DBTITLE 0,--i18n-23b0a9ef-58d5-4973-a610-93068a998d5e
# MAGIC %md
# MAGIC ### 行のサブセット化
# MAGIC DataFrame変換を使用して行をサブセット化します。

# COMMAND ----------

# DBTITLE 0,--i18n-4ada6444-7345-41f7-aaa2-1de2d729483f
# MAGIC %md
# MAGIC #### **`filter()`**
# MAGIC 指定されたSQL式またはカラムベースの条件で行をフィルタリングします。
# MAGIC
# MAGIC ##### 別名: **`where`**

# COMMAND ----------

purchases_df = events_df.filter("ecommerce.total_item_quantity > 0")
display(purchases_df)

# COMMAND ----------

revenue_df = events_df.filter(col("ecommerce.purchase_revenue_in_usd").isNotNull())
display(revenue_df)

# COMMAND ----------

android_df = events_df.filter((col("traffic_source") != "direct") & (col("device") == "Android"))
display(android_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4d6a79eb-3989-43e1-8c28-5b976a513f5f
# MAGIC %md
# MAGIC #### **`dropDuplicates()`**
# MAGIC 重複した行を削除した新しいDataFrameを返します。オプションとして、カラムのサブセットのみを考慮することもできます。
# MAGIC
# MAGIC ##### 別名: **`distinct`**

# COMMAND ----------

display(events_df.distinct())

# COMMAND ----------

distinct_users_df = events_df.dropDuplicates(["user_id"])
display(distinct_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-433c57f4-ce40-48c9-8d04-d3a13c398082
# MAGIC %md
# MAGIC #### **`limit()`**
# MAGIC 最初のn行を取得して、新しいDataFrameを返します。

# COMMAND ----------

limit_df = events_df.limit(100)
display(limit_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d4117305-e742-497e-964d-27a7b0c395cd
# MAGIC %md
# MAGIC ### 行の並び替え
# MAGIC DataFrameトランスフォーメーションを使用して行を並び替えます。

# COMMAND ----------

# DBTITLE 0,--i18n-16b3c7fe-b5f2-4564-9e8e-4f677777c50c
# MAGIC %md
# MAGIC #### **`sort()`**
# MAGIC 指定したカラムや式で並び替えた新しいDataFrameを返します。
# MAGIC
# MAGIC ##### 別名: **`orderBy`**

# COMMAND ----------

increase_timestamps_df = events_df.sort("event_timestamp")
display(increase_timestamps_df)

# COMMAND ----------

decrease_timestamp_df = events_df.sort(col("event_timestamp").desc())
display(decrease_timestamp_df)

# COMMAND ----------

increase_sessions_df = events_df.orderBy(["user_first_touch_timestamp", "event_timestamp"])
display(increase_sessions_df)

# COMMAND ----------

decrease_sessions_df = events_df.sort(col("user_first_touch_timestamp").desc(), col("event_timestamp"))
display(decrease_sessions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-555c663e-3f62-4478-9d76-c9ee090beca1
# MAGIC %md
# MAGIC ### クラスルームで使ったリソースの削除 (Clean up classroom)

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
