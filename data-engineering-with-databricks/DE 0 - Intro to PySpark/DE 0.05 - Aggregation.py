# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-3fbfc7bd-6ef2-4fea-b8a2-7f949cd84044
# MAGIC %md
# MAGIC # 集約 (Aggregation)
# MAGIC
# MAGIC ##### 目的 (Objectives)
# MAGIC 1. 特定のカラムによるデータのグループ化
# MAGIC 1. グループ化データメソッド （Grouped data methods）
# MAGIC 1. 組み込み関数
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank" target="_blank">グループ化データ</a>: **`agg`**, **`avg`**, **`count`**, **`max`**, **`sum`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数</a>: **`approx_count_distinct`**, **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.05

# COMMAND ----------

# DBTITLE 0,--i18n-88095892-40a1-46dd-a809-19186953d968
# MAGIC %md
# MAGIC BedBricksのイベントデータセットを使ってみましょう。

# COMMAND ----------

df = spark.table("events")
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-a04aa8bd-35f0-43df-b137-6e34aebcded1
# MAGIC %md
# MAGIC ### データのグループ化 (Grouping data)
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/aspwd/aggregation_groupby.png" width="60%" />

# COMMAND ----------

# DBTITLE 0,--i18n-cd0936f7-cd8a-4277-bbaf-d3a6ca2c29ec
# MAGIC %md
# MAGIC ### groupBy
# MAGIC グループ化データオブジェクトを作成するためには、DataFrameの<strong>`groupBy`</strong>メソッドを使用します。
# MAGIC
# MAGIC このグループ化データオブジェクトは、Scalaでは<strong>`RelationalGroupedDataset`</strong>、　Pythonでは<strong>`GroupedData`</strong>と呼ばれています。

# COMMAND ----------

df.groupBy("event_name")

# COMMAND ----------

df.groupBy("geo.state", "geo.city")

# COMMAND ----------

# DBTITLE 0,--i18n-7918f032-d001-4e38-bd75-51eb68c41ffa
# MAGIC %md
# MAGIC ### グループ化データメソッド (Grouped data methods)
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/grouping.html" target="_blank">GroupedData</a>オブジェクトに対しては、様々な集計メソッド(aggregation methods)が利用可能です。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | agg | 集計対象カラムについて、集約処理を計算 |
# MAGIC | avg | 各グループの各数値カラムに対して平均値を計算 |
# MAGIC | count | 各グループの行数をカウント |
# MAGIC | max | 各グループの各数値カラムに対して最大値を計算　|
# MAGIC | mean | 各グループの各数値カラムに対して平均値を計算 |
# MAGIC | min | 各グループの各数値カラムの最小値を計算　|
# MAGIC | pivot | 現在のDataFrameのカラムをピボッドし、指定された集約関数（Aggregation method）を適用 |
# MAGIC | sum | 各グループの各数値カラムの合計を計算|

# COMMAND ----------

event_counts_df = df.groupBy("event_name").count()
display(event_counts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-bf63efea-c4f7-4ff9-9d42-4de245617d97
# MAGIC %md
# MAGIC それぞれの平均購入売上高（Purchase revenue）の計算をします。

# COMMAND ----------

avg_state_purchases_df = df.groupBy("geo.state").avg("ecommerce.purchase_revenue_in_usd")
display(avg_state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-b11167f4-c270-4f7b-b967-75538237c915
# MAGIC %md
# MAGIC そしてこれが、各州と都市の組み合わせに対して、それぞれの平均購入売上高の数量(Quantity)と売上高(Purchase revenue)です。

# COMMAND ----------

city_purchase_quantities_df = df.groupBy("geo.state", "geo.city").sum("ecommerce.total_item_quantity", "ecommerce.purchase_revenue_in_usd")
display(city_purchase_quantities_df)

# COMMAND ----------

# DBTITLE 0,--i18n-62a4e852-249a-4dbf-b47a-e85a64cbc258
# MAGIC %md
# MAGIC ## 組み込み関数 (Built-In Functions)
# MAGIC DataFrameやColumnの変換メソッドに加え、Sparkの <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-functions-builtin.html" target="_blank">SQL functions</a> モジュールには便利な関数（Functions）がたくさんあります。
# MAGIC
# MAGIC Scalaにおいては, こちらの <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/functions$.html" target="_blank">**`org.apache.spark.sql.functions`**</a>を、そしてPythonにおいては <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#functions" target="_blank">**`pyspark.sql.functions`**</a>を参照してください。
# MAGIC なお、このモジュールにある関数を利用する場合は、このモジュールを必ずimportしてください。

# COMMAND ----------

# DBTITLE 0,--i18n-68f06736-e457-4893-8c0d-be83c818bd91
# MAGIC %md
# MAGIC ### 集約関数 (Aggregate Functions)
# MAGIC
# MAGIC いくつかの集約（Aggregation）に関する組み込み関数が使用可能です。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | approx_count_distinct | グループ内の一意な項目数の近似数を返す |
# MAGIC | avg | グループ内の値の平均値を計算 |
# MAGIC | collect_list | 重複を含むオブジェクトのリストを返す |
# MAGIC | corr | 2つの列のピアソン相関係数を計算 |
# MAGIC | max | 各グループの各数値カラムの最大値を計算 |
# MAGIC | mean | 各グループの各数値カラムの平均値を計算 |
# MAGIC | stddev_samp | グループ内の式の標本標準偏差を計算 |
# MAGIC | sumDistinct | 特定の式内での一意な値の合計値を計算 |
# MAGIC | var_pop | グループ内の値の母集団分散を計算 |
# MAGIC
# MAGIC 組み込み集約関数（Aggregate functions）を適用するためには、グループ化データメソッドにおいて、
# MAGIC  <a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.GroupedData.agg.html#pyspark.sql.GroupedData.agg" target="_blank">**`agg`**</a> を使用してください。
# MAGIC
# MAGIC これにより結果のカラムに、<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.Column.alias.html" target="_blank">**`alias`**</a>　のような、他の変換処理を適応することができます。

# COMMAND ----------

from pyspark.sql.functions import sum

state_purchases_df = df.groupBy("geo.state").agg(sum("ecommerce.total_item_quantity").alias("total_purchases"))
display(state_purchases_df)

# COMMAND ----------

# DBTITLE 0,--i18n-875e6ef8-fec3-4468-ab86-f3f6946b281f
# MAGIC %md
# MAGIC グループ化されたデータに、複数の集約関数（Aggregate functions）を適用

# COMMAND ----------

from pyspark.sql.functions import avg, approx_count_distinct

state_aggregates_df = (df
                       .groupBy("geo.state")
                       .agg(avg("ecommerce.total_item_quantity").alias("avg_quantity"),
                            approx_count_distinct("user_id").alias("distinct_users"))
                      )

display(state_aggregates_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6bb4a15f-4f5d-4f70-bf50-4be00167d9fa
# MAGIC %md
# MAGIC ### 数学関数 (Math Functions)
# MAGIC いくつかの数学演算に使用できる組み込み関数が使用可能です。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | ceil | 指定したカラムの切り上げ値を計算 |
# MAGIC | cos | 指定した値の余弦（Cosine）値を計算 |
# MAGIC | log | 指定下値の自然対数（the natural logarithm）を計算 |
# MAGIC | round | HALF_UPの丸めモードで、カラムeの値を小数点以下0桁に丸めた値を返す　|
# MAGIC | sqrt | 指定された浮動小数点数の平方根（the square root）を計算 |

# COMMAND ----------

from pyspark.sql.functions import cos, sqrt

display(spark.range(10)  # Create a DataFrame with a single column called "id" with a range of integer values
        .withColumn("sqrt", sqrt("id"))
        .withColumn("cos", cos("id"))
       )

# COMMAND ----------

# DBTITLE 0,--i18n-d03fb77f-5e4c-43b8-a293-884cd7cb174c
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
