# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-c52349f9-afd2-4532-804b-2d50a67839fa
# MAGIC %md
# MAGIC # カゴ落ちラボ (Abandoned Carts Lab)
# MAGIC 購入しなかったユーザーのEメールごとに、カゴ落ちした商品を取得しましょう。
# MAGIC 1. トランザクションからのコンバージョンしたユーザーのEメールの取得
# MAGIC 2. ユーザーIDとEメールの結合
# MAGIC 3. 各ユーザーのカートの商品履歴の取得
# MAGIC 4. カートの商品履歴とEメールの結合
# MAGIC 5. カゴ落ち商品とひもづくEメールの抽出
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.join.html#pyspark.sql.DataFrame.join" target="_blank">DataFrame</a>: **`join`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html" target="_blank">組み込み関数</a>: **`collect_set`**, **`explode`**, **`lit`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrameNaFunctions.html#pyspark.sql.DataFrameNaFunctions" target="_blank">DataFrameNaFunctions</a>: **`fill`**

# COMMAND ----------

# DBTITLE 0,--i18n-f1f59329-e456-4268-836a-898d3736f378
# MAGIC %md
# MAGIC ### セットアップ (Setup)
# MAGIC 以下のセルを実行して<strong>`sales_df`</strong>、<strong>`users_df`</strong>、<strong>`events_df`</strong>のデータフレームを作成します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.12L

# COMMAND ----------

# sale transactions at BedBricks
sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# user IDs and emails at BedBricks
users_df = spark.table("users")
display(users_df)

# COMMAND ----------

# events logged on the BedBricks website
events_df = spark.table("events")
display(events_df)

# COMMAND ----------

# DBTITLE 0,--i18n-c2065783-4c56-4d12-bdf8-2e0fce22016f
# MAGIC %md
# MAGIC ### 1: トランザクションからのコンバージョンしたユーザーのEメールの取得 (Get emails of converted users from transactions)
# MAGIC - <strong>`salesDF`</strong>の <strong>`email`</strong>列を抽出し、重複を削除してください
# MAGIC - 全ての行に<strong>`True`</strong>の値を持つ新しい列<strong>`converted`</strong>を追加してください
# MAGIC
# MAGIC 結果を<strong>`converted_users_df`</strong>として保存してください。

# COMMAND ----------

# ANSWER
from pyspark.sql.functions import *

converted_users_df = (sales_df
                      .select("email")
                      .distinct()
                      .withColumn("converted", lit(True))
                     )
display(converted_users_df)

# COMMAND ----------

# DBTITLE 0,--i18n-4becd415-94d5-4e58-a995-d17ef1be87d0
# MAGIC %md
# MAGIC #### 1.1: 作業結果の確認 (Check Your Work)
# MAGIC
# MAGIC 作成したソリューションが正しいかどうか確認するために以下のセルを実行してください:

# COMMAND ----------

expected_columns = ["email", "converted"]

expected_count = 10510

assert converted_users_df.columns == expected_columns, "converted_users_df does not have the correct columns"

assert converted_users_df.count() == expected_count, "converted_users_df does not have the correct number of rows"

assert converted_users_df.select(col("converted")).first()[0] == True, "converted column not correct"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-72c8bd3a-58ae-4c30-8e3e-007d9608aea3
# MAGIC %md
# MAGIC ### 2: ユーザーIDとEメールの結合 (Join emails with user IDs)
# MAGIC - <strong>`converted_users_df`</strong>と<strong>`users_df`</strong>を<strong>`email`</strong>列を使って外部結合してください
# MAGIC - <strong>`email`</strong>がnullではないユーザーを抽出してください
# MAGIC - <strong>`converted`</strong>のnull値を<strong>`False`</strong>で置換してください
# MAGIC
# MAGIC 結果を<strong>`conversions_df`</strong>として保存してください。

# COMMAND ----------

# ANSWER
conversions_df = (users_df
                  .join(converted_users_df, "email", "outer")
                  .filter(col("email").isNotNull())
                  .na.fill(False)
                 )
display(conversions_df)

# COMMAND ----------

# DBTITLE 0,--i18n-48691094-e17f-405d-8f91-286a42ce55d7
# MAGIC %md
# MAGIC #### 2.1: 作業結果の確認 (Check Your Work)
# MAGIC
# MAGIC 作成したソリューションが正しいかどうか確認するために以下のセルを実行してください:

# COMMAND ----------

expected_columns = ['email', 'user_id', 'user_first_touch_timestamp', 'updated', 'converted']

expected_count = 38939

expected_false_count = 28429

assert conversions_df.columns == expected_columns, "Columns are not correct"

assert conversions_df.filter(col("email").isNull()).count() == 0, "Email column contains null"

assert conversions_df.count() == expected_count, "There is an incorrect number of rows"

assert conversions_df.filter(col("converted") == False).count() == expected_false_count, "There is an incorrect number of false entries in converted column"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-8a92bfe3-cad0-40af-a283-3241b810fc20
# MAGIC %md
# MAGIC ### 3: 各ユーザーのカートの商品履歴の取得 (Get cart item history for each user)
# MAGIC - <strong>`events_df`</strong>の<strong>`items`</strong>フィールドの1レコードを複数行（explode)にして既存の<strong>`items`</strong>フィールドを置き換えてください
# MAGIC - <strong>`user_id`</strong>でグループ化してください
# MAGIC   - 各ユーザーの全ての<strong>`items.item_id`</strong>をセットとして集めて(collect a set)、"cart"という列にしてください
# MAGIC
# MAGIC 結果を<strong>`carts_df`</strong>として保存してください。

# COMMAND ----------

# ANSWER
carts_df = (events_df
            .withColumn("items", explode("items"))
            .groupBy("user_id").agg(collect_set("items.item_id").alias("cart"))
           )
display(carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-d0ed8434-7016-44c0-a865-4b55a5001194
# MAGIC %md
# MAGIC #### 3.1: 作業結果の確認 (Check Your Work)
# MAGIC
# MAGIC 作成したソリューションが正しいかどうか確認するために以下のセルを実行してください:

# COMMAND ----------

expected_columns = ["user_id", "cart"]

expected_count = 24574

assert carts_df.columns == expected_columns, "Incorrect columns"

assert carts_df.count() == expected_count, "Incorrect number of rows"

assert carts_df.select(col("user_id")).drop_duplicates().count() == expected_count, "Duplicate user_ids present"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-97b01bbb-ada0-4c4f-a253-7c578edadaf9
# MAGIC %md
# MAGIC ### 4: カートの商品履歴とEメールの結合 (Join cart item history with emails)
# MAGIC - <strong>`conversions_df`</strong>と<strong>`carts_df`</strong>を<strong>`user_id`</strong>フィールドで左外部結合してください
# MAGIC
# MAGIC 結果を<strong>`email_carts_df`</strong>として保存してください。

# COMMAND ----------

# ANSWER
email_carts_df = conversions_df.join(carts_df, "user_id", "left")
display(email_carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-0cf80000-eb4c-4f0f-a3f8-7e938b99f1ef
# MAGIC %md
# MAGIC #### 4.1: 作業結果の確認 (Check Your Work)
# MAGIC
# MAGIC 作成したソリューションが正しいかどうか確認するために以下のセルを実行してください:

# COMMAND ----------

email_carts_df.filter(col("cart").isNull()).count()

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 38939

expected_cart_null_count = 19671

assert email_carts_df.columns == expected_columns, "Columns do not match"

assert email_carts_df.count() == expected_count, "Counts do not match"

assert email_carts_df.filter(col("cart").isNull()).count() == expected_cart_null_count, "Cart null counts incorrect from join"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-380e3eda-3543-4f67-ae11-b883e5201dba
# MAGIC %md
# MAGIC ### 5: カゴ落ち商品とひも付いたEメールの抽出 （Filter for emails with abandoned cart items)
# MAGIC - <strong>`email_carts_df`</strong>から<strong>`converted`</strong>がFalseであるユーザーを抽出してください
# MAGIC - カートがnullではないユーザーを抽出してください
# MAGIC
# MAGIC 結果を<strong>`abandoned_carts_df`</strong>として保存してください。

# COMMAND ----------

# ANSWER
abandoned_carts_df = (email_carts_df
                      .filter(col("converted") == False)
                      .filter(col("cart").isNotNull())
                     )
display(abandoned_carts_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05ff2599-c1e6-404f-a38b-262fb0a055fa
# MAGIC %md
# MAGIC #### 5.1: 作業結果の確認 (Check Your Work)
# MAGIC
# MAGIC 作成したソリューションが正しいかどうか確認するために以下のセルを実行してください:

# COMMAND ----------

expected_columns = ["user_id", "email", "user_first_touch_timestamp", "updated", "converted", "cart"]

expected_count = 10212

assert abandoned_carts_df.columns == expected_columns, "Columns do not match"

assert abandoned_carts_df.count() == expected_count, "Counts do not match"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-e2f48480-5c42-490a-9f14-9b92d29a9823
# MAGIC %md
# MAGIC ### ボーナスアクティビティ (Bonus Activity)
# MAGIC 商品単位でカゴ落ちした数をプロットしてください

# COMMAND ----------

# ANSWER
abandoned_items_df = (abandoned_carts_df
                      .withColumn("items", explode("cart"))
                      .groupBy("items")
                      .count()
                      .sort("items")
                     )
display(abandoned_items_df)

# COMMAND ----------

# DBTITLE 0,--i18n-a08b8c26-6a94-4a20-a096-e74721824eac
# MAGIC %md
# MAGIC #### 6.1: 作業結果の確認 (Check Your Work)
# MAGIC
# MAGIC 作成したソリューションが正しいかどうか確認するために以下のセルを実行してください:

# COMMAND ----------

expected_columns = ["items", "count"]

expected_count = 12

assert abandoned_items_df.count() == expected_count, "Counts do not match"

assert abandoned_items_df.columns == expected_columns, "Columns do not match"
print("All test pass")

# COMMAND ----------

# DBTITLE 0,--i18n-f2f44609-5139-465a-ad7d-d87f3f06a380
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
