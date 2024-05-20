# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-0eeddecf-4f2c-4599-960f-8fefe777281f
# MAGIC %md
# MAGIC # 日付時刻関数 (Datetime Functions)
# MAGIC
# MAGIC ##### 目的 (Objectives)
# MAGIC 1. タイムスタンプ型(timestamp)への型変換
# MAGIC 2. 日時型（datetimes）のフォーマット
# MAGIC 3. タイムスタンプ型(timestamp)からの抽出
# MAGIC 4. 日付型(date)への変換
# MAGIC 5. 日時型(datetimes)の操作
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/column.html" target="_blank">Column</a>: **`cast`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/functions.html#datetime-functions" target="_blank">組み込み関数 (Built-In Functions)</a>: **`date_format`**, **`to_date`**, **`date_add`**, **`year`**, **`month`**, **`dayofweek`**, **`minute`**, **`second`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.09

# COMMAND ----------

# DBTITLE 0,--i18n-6d2e9c6a-0561-4426-ae88-a8ebca06c61b
# MAGIC %md
# MAGIC BedBricksのイベントデータセットのサブセットを使用して、日付や時間などのデータ型を操作する練習をしてみましょう。

# COMMAND ----------

from pyspark.sql.functions import col

df = spark.table("events").select("user_id", col("event_timestamp").alias("timestamp"))
display(df)

# COMMAND ----------

# DBTITLE 0,--i18n-34540d5e-7a9b-496d-b9d7-1cf7de580f23
# MAGIC %md
# MAGIC ### 組み込み関数：日付や時刻の関数 (Built-In Functions: Date Time Functions)
# MAGIC これらはSparkの日付と時刻を操作する組み込み関数のうちのいくつかのものです。
# MAGIC
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | **`add_months`** | startDateからnumMonths後の日付を返す |
# MAGIC | **`current_timestamp`** | クエリの評価開始時の現在のタイムスタンプをタイムスタンプ型の列として返す |
# MAGIC | **`date_format`** | 2番目の引数で指定された日付形式に従って、日付/タイムスタンプ/文字列を文字列の値に変換 |
# MAGIC | **`dayofweek`** | 指定された日付/タイムスタンプ/文字列から曜日を整数として抽出 |
# MAGIC | **`from_unixtime`** | UNIXエポック（1970年01月01日00時00分00秒UTC）からの秒数を、現在のシステム時刻のタイムスタンプを表す文字列に変換し、yyyy-MM-dd HH:mm:ss形式で返す |
# MAGIC | **`minute`** | 指定された日付/タイムスタンプ/文字列から分を整数として抽出 |
# MAGIC | **`unix_timestamp`** | 指定されたパターンの時間文字列をUNIXタイムスタンプ（秒単位）に変換 |

# COMMAND ----------

# DBTITLE 0,--i18n-fa5f62a7-e690-48c8-afa0-b446d3bc7aa6
# MAGIC %md
# MAGIC ### タイムスタンプ型(timestamp)への型変換 (Cast to Timestamp)
# MAGIC
# MAGIC #### **`cast()`**
# MAGIC 文字列表現またはDataTypeを使用して特定された、異なるデータ型（DataType）へと列を型変換します。

# COMMAND ----------

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast("timestamp"))
display(timestamp_df)

# COMMAND ----------

from pyspark.sql.types import TimestampType

timestamp_df = df.withColumn("timestamp", (col("timestamp") / 1e6).cast(TimestampType()))
display(timestamp_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6c9cb2b0-ef18-48c4-b1ed-3fad453172c1
# MAGIC %md
# MAGIC ### Datetimes
# MAGIC
# MAGIC Sparkでdatetimeを使用する場合、いくつかの一般的なシナリオがあります：
# MAGIC
# MAGIC - CSV/JSONデータソースは、datetimeコンテンツの解析（Parse）と書式設定（Format）にパターン文字列を使用します。
# MAGIC - Datetime関数は、StringTypeをDateTypeやTimestampTypeに変換操作に関連します、例えば **`unix_timestamp`** , **`date_format`** , **`from_unixtime`** , **`to_date`** , **`to_timestamp`** など。
# MAGIC
# MAGIC #### 形式設定と解析のための日付と時刻のパターン （Datetime Patterns for Formatting and Parsing）
# MAGIC Sparkは<a href="https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html" target="_blank">パースとフォーマットのための日付時刻パターン</a>に従います。これらパターンのサブセットは以下の通りです。 
# MAGIC
# MAGIC | 記号 | 意味         | 表現 | 例               |
# MAGIC | ------ | --------------- | ------------ | ---------------------- |
# MAGIC | G      | era             | text         | AD; Anno Domini        |
# MAGIC | y      | year            | year         | 2020; 20               |
# MAGIC | D      | day-of-year     | number(3)    | 189                    |
# MAGIC | M/L    | month-of-year   | month        | 7; 07; Jul; July       |
# MAGIC | d      | day-of-month    | number(3)    | 28                     |
# MAGIC | Q/q    | quarter-of-year | number/text  | 3; 03; Q3; 3rd quarter |
# MAGIC | E      | day-of-week     | text         | Tue; Tuesday           |
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Spark の日付とタイムスタンプの扱いがバージョン 3.0 で変更され、これらの値の解析と書式設定に使用するパターンも変わりました。
# MAGIC これらの変更点については、 <a href="https://databricks.com/blog/2020/07/22/a-comprehensive-look-at-dates-and-timestamps-in-apache-spark-3-0.html" target="_blank">こちらDatabricks記事</a>を参照してください。

# COMMAND ----------

# DBTITLE 0,--i18n-6bc9e089-fc58-4d8f-b118-d5162b747dc6
# MAGIC %md
# MAGIC #### 日付のフォーマット （Format date）
# MAGIC
# MAGIC #### **`date_format()`**
# MAGIC
# MAGIC 指定された日付パターンで、日付/タイムスタンプ/文字列（date/timestamp/string)から文字列（String）へ変換します。

# COMMAND ----------

from pyspark.sql.functions import date_format

formatted_df = (timestamp_df
                .withColumn("date string", date_format("timestamp", "MMMM dd, yyyy"))
                .withColumn("time string", date_format("timestamp", "HH:mm:ss.SSSSSS"))
               )
display(formatted_df)

# COMMAND ----------

# DBTITLE 0,--i18n-adc065e9-e241-424e-ad6e-db1e2cb9b1e6
# MAGIC %md
# MAGIC #### タイムスタンプ（timestamp）から日付（datetime）への抽出 （Extract datetime attribute from timestamp）
# MAGIC
# MAGIC #### **`year`**
# MAGIC 指定された日付/タイムスタンプ/文字列（date/timestamp/string）から整数（Integer）としての年数の抽出
# MAGIC
# MAGIC ##### 関連するメソッド（Similar methods）: **`month`**, **`dayofweek`**, **`minute`**, **`second`**　など。

# COMMAND ----------

from pyspark.sql.functions import year, month, dayofweek, minute, second

datetime_df = (timestamp_df
               .withColumn("year", year(col("timestamp")))
               .withColumn("month", month(col("timestamp")))
               .withColumn("dayofweek", dayofweek(col("timestamp")))
               .withColumn("minute", minute(col("timestamp")))
               .withColumn("second", second(col("timestamp")))
              )
display(datetime_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f06bd91b-c4f4-4909-98dd-680fbfdf56cd
# MAGIC %md
# MAGIC #### 日付への変換　（Convert to Date）
# MAGIC
# MAGIC #### **`to_date`**
# MAGIC 日付型（DataType）への変換ルール（Casting rules）によって、カラムを日付型（DateType）へ変換する

# COMMAND ----------

from pyspark.sql.functions import to_date

date_df = timestamp_df.withColumn("date", to_date(col("timestamp")))
display(date_df)

# COMMAND ----------

# DBTITLE 0,--i18n-8367af41-fc35-44ba-8ab1-df721452e6f3
# MAGIC %md
# MAGIC ### 日時型の操作 （Manipulate Datetimes）
# MAGIC #### **`date_add`**
# MAGIC 開始日から指定された日数後の日付を返す

# COMMAND ----------

from pyspark.sql.functions import date_add

plus_2_df = timestamp_df.withColumn("plus_two_days", date_add(col("timestamp"), 2))
display(plus_2_df)

# COMMAND ----------

# DBTITLE 0,--i18n-3669ec6f-2f26-4607-9f58-656d463308b5
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
