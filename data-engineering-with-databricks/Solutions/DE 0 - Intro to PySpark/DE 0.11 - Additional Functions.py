# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-df421470-173e-44c6-a85a-1d48d8a14d42
# MAGIC %md
# MAGIC # その他の関数 (Additional Functions)
# MAGIC
# MAGIC ##### 目的 (Objectives)
# MAGIC 1. 組み込み関数を適用して新しい列にデータを生成する
# MAGIC 1. データフレームのNA関数を適用してnull値を扱う
# MAGIC 1. データフレームを結合する
# MAGIC
# MAGIC ##### Methods
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame Methods </a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**, **`drop`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">Built-In Functions</a>:
# MAGIC   - 集約関数 (Aggregate): **`collect_set`**
# MAGIC   - コレクション (Collection): **`explode`**
# MAGIC   - 集約以外やその他 (Non-aggregate and miscellaneous): **`col`**, **`lit`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.11

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c80fc2ec-34e6-459f-b5eb-afa660db9491
# MAGIC %md
# MAGIC ### 集約以外やその他の関数 (Non-aggregate and Miscellaneous Functions)
# MAGIC これらは集約以外やその他の組み込み関数のいくつかのものです。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | col / column | 与えられた列名に基づいてColumnを返します |
# MAGIC | lit | リテラル値からなるColumnを作成します |
# MAGIC | isnull | 列がnullの場合trueを返します |
# MAGIC | rand | [0.0, 1.0)の区間で均一で独立同分布なランダムな値の列を生成します |

# COMMAND ----------

# DBTITLE 0,--i18n-bae51fa6-6275-46ec-854d-40ba81788bac
# MAGIC %md
# MAGIC <strong>`col`</strong>関数を使って特定のカラムを選択できます。

# COMMAND ----------

gmail_accounts = sales_df.filter(col("email").endswith("gmail.com"))

display(gmail_accounts)

# COMMAND ----------

# DBTITLE 0,--i18n-a88d37a6-5e98-40ad-9045-bb8fd4d36331
# MAGIC %md
# MAGIC <strong>`lit`</strong>は値からカラムを作るのに使うことができ、カラムの追加に役立ちます。

# COMMAND ----------

display(gmail_accounts.select("email", lit(True).alias("gmail user")))

# COMMAND ----------

# DBTITLE 0,--i18n-d7436832-7254-4bfa-845b-2ab10170f171
# MAGIC %md
# MAGIC ### DataFrameNaFunctions
# MAGIC <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>はデータフレームのサブモジュールで、null値を扱うメソッドを備えています。データフレームのna属性にアクセスすることでDataFrameNaFunctionsのインスタンスを取得できます。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | drop | null値が1つでもある(any)、全てnull値(all)、あるいは特定の数以上null値がある行を除いた新たなデータフレームを返す。オプションで特定の列のサブセットだけを考慮することも可能 |
# MAGIC | fill | null値を指定した値で置換する。オプションで特定の列のサブセットを指定することも可能 |
# MAGIC | replace | 値を別の値で置換した新しいデータフレームを返す。オプションで特定の列のサブセットを指定することも可能 |

# COMMAND ----------

# DBTITLE 0,--i18n-da0ccd36-e2b5-4f79-ae61-cb8252e5da7c
# MAGIC %md
# MAGIC 以下でnull/NA値の行を削除する前後の行数を確認できます。

# COMMAND ----------

print(sales_df.count())
print(sales_df.na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-aef560b8-7bb6-4985-a43d-38541ba78d33
# MAGIC %md
# MAGIC 行数が同じなので、nullの列がないことがわかります。itemsの1レコードを複数行にして（explode)、items.couponのようなカラムでnullのものを見つける必要があります。

# COMMAND ----------

sales_exploded_df = sales_df.withColumn("items", explode(col("items")))
display(sales_exploded_df.select("items.coupon"))
print(sales_exploded_df.select("items.coupon").count())
print(sales_exploded_df.select("items.coupon").na.drop().count())

# COMMAND ----------

# DBTITLE 0,--i18n-4c01038b-2afa-41d8-a390-fad45d1facfe
# MAGIC %md
# MAGIC <strong>`na.fill`</strong>を使ってクーポンコードが無いところを補完することができます。

# COMMAND ----------

display(sales_exploded_df.select("items.coupon").na.fill("NO COUPON"))

# COMMAND ----------

# DBTITLE 0,--i18n-8a65ceb6-7bc2-4147-be66-71b63ae374a1
# MAGIC %md
# MAGIC ### データフレームの結合 (Joining DataFrames)
# MAGIC データフレームの<a href="https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.sql.DataFrame.join.html?highlight=join#pyspark.sql.DataFrame.join" target="_blank"><strong>`join`</strong></a>メソッドにより、与えられた結合条件に基づいて2つのデータフレームを結合することができます。
# MAGIC
# MAGIC いくつかの異なるタイプのjoinがサポートされています:
# MAGIC
# MAGIC "name"という共通の列の値が等しい場合に内部結合(inner join)する(つまり、等結合)<br/>
# MAGIC **`df1.join(df2, "name")`**
# MAGIC
# MAGIC "name"と"age"という共通の列の値が等しい場合に内部結合する<br/>
# MAGIC **`df1.join(df2, ["name", "age"])`**
# MAGIC
# MAGIC "name"という共通の列の値が等しい場合に完全外部結合（Full outer join)する<br/>
# MAGIC **`df1.join(df2, "name", "outer")`**
# MAGIC
# MAGIC 明示的な列の等式に基づいて左外部結合(Left outer join)する<br/>
# MAGIC **`df1.join(df2, df1["customer_name"] == df2["account_name"], "left_outer")`**

# COMMAND ----------

# DBTITLE 0,--i18n-67001b92-91e6-4137-9138-b7f00950b450
# MAGIC %md
# MAGIC 上で得られたgmail_accountsと結合するためにユーザーデータを読み込みます。

# COMMAND ----------

users_df = spark.table("users")
display(users_df)

# COMMAND ----------

joined_df = gmail_accounts.join(other=users_df, on='email', how = "inner")
display(joined_df)

# COMMAND ----------

# DBTITLE 0,--i18n-9a47f04c-ce9c-4892-b457-1a2e57176721
# MAGIC %md
# MAGIC ### クラスルームで使ったリソースの削除 (Clean up classroom)
# MAGIC
# MAGIC 最後に、クラスルームのクリーンアップを行います。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
