# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-8382e200-81c0-4bc3-9bdb-6aee604b0a8c
# MAGIC %md
# MAGIC # トラフィックソースの売上高に関するラボ（Revenue by Traffic Lab）
# MAGIC
# MAGIC 総売上高が上位3位のトラフィックソースを取得
# MAGIC 1. トラフィックソースによる売上高のデータ集約
# MAGIC 2. 総売上高でトップ3のトラフィックソースを取得
# MAGIC 3. 小数点以下2桁を持つように売上高の列を修正
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>: **`groupBy`**, **`sort`**, **`limit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">カラム</a>: **`alias`**, **`desc`**, **`cast`**, **`operators`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数 (Built-in Functions)</a>: **`avg`**, **`sum`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.06L

# COMMAND ----------

# DBTITLE 0,--i18n-b6ac5716-1668-4b34-8343-ee2d5c77cfad
# MAGIC %md
# MAGIC ### セットアップ (Setup)
# MAGIC 以下のセルを実行して、最初のDataframe<strong>`df`</strong>を作成します。

# COMMAND ----------

from pyspark.sql.functions import col

# Purchase events logged on the BedBricks website
df = (spark.table("events")
      .withColumn("revenue", col("ecommerce.purchase_revenue_in_usd"))
      .filter(col("revenue").isNotNull())
      .drop("event_name")
     )

display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-78acde42-f2b7-4b0f-9760-65fba886ef5b
# MAGIC %md
# MAGIC ### 1. トラフィックソース別の売上高の集計 (Aggregate revenue by traffic source)
# MAGIC - **`traffic_source`** にてグループ化してください。
# MAGIC - **`revenue`** の合計を **`total_rev`** として取得してください。また10の桁で四捨五入してください（例： nnnnn.n）。
# MAGIC - **`avg_rev`** として **`revenue`** の平均を取得してください。
# MAGIC
# MAGIC 必要な組み込み関数（built-in functions）を忘れずにインポートしてください。

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import avg, col, sum

traffic_df = (df
              .groupBy("traffic_source")
              .agg(sum(col("revenue")).alias("total_rev"),
                   avg(col("revenue")).alias("avg_rev"))
             )

display(traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0ef1149d-9690-49a3-b717-ae2c38a166ed
# MAGIC %md
# MAGIC **1.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

from pyspark.sql.functions import round

expected1 = [(620096.0, 1049.2318), (4026578.5, 986.1814), (1200591.0, 1067.192), (2322856.0, 1093.1087), (826921.0, 1086.6242), (404911.0, 1091.4043)]
test_df = traffic_df.sort("traffic_source").select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result1 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected1 == result1)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f5c20afb-2891-4fa2-8090-cca7e313354d
# MAGIC %md
# MAGIC ### 2. 総売上高でトップ3のトラフィックソースを取得 (Get top three traffic sources by total revenue)
# MAGIC - **`total_rev`** の降順で並べ替えてください。
# MAGIC - 最初の3行に制限してください。

# COMMAND ----------

# ANSWER
top_traffic_df = traffic_df.sort(col("total_rev").desc()).limit(3)
display(top_traffic_df)

# COMMAND ----------

# DBTITLE 0,--i18n-2ef23b0a-fc13-48ca-b1f3-9bc425023024
# MAGIC %md
# MAGIC **2.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

expected2 = [(4026578.5, 986.1814), (2322856.0, 1093.1087), (1200591.0, 1067.192)]
test_df = top_traffic_df.select(round("total_rev", 4).alias("total_rev"), round("avg_rev", 4).alias("avg_rev"))
result2 = [(row.total_rev, row.avg_rev) for row in test_df.collect()]

assert(expected2 == result2)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-04399ae3-d2ab-4ada-9a51-9f8cc21cc45a
# MAGIC %md
# MAGIC ### 3. 総売上高の列を小数点以下2桁に制限 (Limit revenue columns to two decimal places)
# MAGIC - 小数点以下2桁の数値を含むように **`avg_rev`** と **`total_rev`** 列を修正してください。
# MAGIC  - **`withColumn()`** を同じ名前で使用して、これらの列を置換してください。
# MAGIC  - 小数点以下2桁に制限するには、各列に100を掛け、longにキャストしてから、100で割ってください。

# COMMAND ----------

# ANSWER
final_df = (top_traffic_df
            .withColumn("avg_rev", (col("avg_rev") * 100).cast("long") / 100)
            .withColumn("total_rev", (col("total_rev") * 100).cast("long") / 100)
           )

display(final_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d28b2d3a-6db6-4ba0-8a2c-a773635a69a4
# MAGIC %md
# MAGIC **3.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

expected3 = [(4026578.5, 986.18), (2322856.0, 1093.1), (1200591.0, 1067.19)]
result3 = [(row.total_rev, row.avg_rev) for row in final_df.collect()]

assert(expected3 == result3)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-4e2d3b62-bee6-497e-b6af-44064f759451
# MAGIC %md
# MAGIC ### 4. 補講: 組み込み数学関数を使って書き換え (Bonus: Rewrite using a built-in math function)
# MAGIC 指定した小数点以下の桁数に四捨五入する組み込みの数学関数を探してください。

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import round

bonus_df = (top_traffic_df
            .withColumn("avg_rev", round("avg_rev", 2))
            .withColumn("total_rev", round("total_rev", 2))
           )

display(bonus_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6514f89e-1920-4804-96e4-a73998026023
# MAGIC %md
# MAGIC **4.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

expected4 = [(4026578.5, 986.18), (2322856.0, 1093.11), (1200591.0, 1067.19)]
result4 = [(row.total_rev, row.avg_rev) for row in bonus_df.collect()]

assert(expected4 == result4)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-8f19689f-4cf7-4031-bc4c-eb2ece7cb56d
# MAGIC %md
# MAGIC ### 5. ここまでの全ての手順を連結してください (Chain all the steps above)

# COMMAND ----------

# ANSWER
chain_df = (df
            .groupBy("traffic_source")
            .agg(sum(col("revenue")).alias("total_rev"),
                 avg(col("revenue")).alias("avg_rev"))
            .sort(col("total_rev").desc())
            .limit(3)
            .withColumn("avg_rev", round("avg_rev", 2))
            .withColumn("total_rev", round("total_rev", 2))
           )

display(chain_df)

# COMMAND ----------

# DBTITLE 0,--i18n-53c0b070-d2bc-45e9-a3ea-da25a375d6f3
# MAGIC %md
# MAGIC **5.1: 作業結果の確認 (CHECK YOUR WORK)**

# COMMAND ----------

expected5 = [(4026578.5, 986.18), (2322856.0, 1093.11), (1200591.0, 1067.19)]
result5 = [(row.total_rev, row.avg_rev) for row in chain_df.collect()]

assert(expected5 == result5)
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f8095ac2-c3cf-4bbb-b20b-eed1891489e0
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
