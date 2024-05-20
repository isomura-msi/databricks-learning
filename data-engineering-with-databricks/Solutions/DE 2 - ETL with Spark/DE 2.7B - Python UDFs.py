# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-7c0e5ecf-c2e4-4a89-a418-76faa15ce226
# MAGIC %md
# MAGIC # Python ユーザー定義関数(Python User-Defined Functions)
# MAGIC
# MAGIC ##### 目的
# MAGIC 1. 関数を定義する
# MAGIC 1. UDF を作成して適用する
# MAGIC 1. Python デコレーター構文で UDF を作成して登録する
# MAGIC 1. Pandas (ベクトル化された) UDF を作成して適用する
# MAGIC
# MAGIC ##### メソッド
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.udf.html" target="_blank">Python UDF Decorator</a>: **`@udf`**
# MAGIC - <a href="https://spark.apache.org/docs/3.1.3/api/python/reference/api/pyspark.sql.functions.pandas_udf.html" target="_blank">Pandas UDF Decorator</a>: **`@pandas_udf`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.7B

# COMMAND ----------

# DBTITLE 0,--i18n-1e94c419-dd84-4f8d-917a-019b15fc6700
# MAGIC %md
# MAGIC ### ユーザー定義関数 (UDF)
# MAGIC
# MAGIC カスタムカラム変換関数
# MAGIC
# MAGIC - Catalyst Optimizer で最適化できません
# MAGIC - 関数はシリアル化され、エグゼキュータに送信されます
# MAGIC - 行データは、Spark のネイティブ バイナリ形式から逆シリアル化されて UDF に渡され、結果は Spark のネイティブ形式にシリアル化されます。
# MAGIC - Python UDF の場合、エグゼキューターと各ワーカー ノードで実行されている Python インタープリターとの間の追加のプロセス間通信オーバーヘッド

# COMMAND ----------

# DBTITLE 0,--i18n-4d1eb639-23fb-42fa-9b62-c407a0ccde2d
# MAGIC %md
# MAGIC このデモでは、販売データを使用します。

# COMMAND ----------

sales_df = spark.table("sales")
display(sales_df)

# COMMAND ----------

# DBTITLE 0,--i18n-05043672-b02a-4194-ba44-75d544f6af07
# MAGIC %md
# MAGIC ### 関数を定義する
# MAGIC
# MAGIC **`email`** フィールドから文字列の最初の文字を取得する関数を (ドライバーで) 定義します。

# COMMAND ----------

def first_letter_function(email):
    return email[0]

first_letter_function("annagray@kaufman.com")

# COMMAND ----------

# DBTITLE 0,--i18n-17f25aa9-c20f-41da-bac5-95ebb413dcd4
# MAGIC %md
# MAGIC ### UDF の作成と適用
# MAGIC
# MAGIC 関数を UDF として登録します。 これにより、関数がシリアル化され、DataFrame レコードを変換できるようにエグゼキューターに送信されます。

# COMMAND ----------

first_letter_udf = udf(first_letter_function)

# COMMAND ----------

# DBTITLE 0,--i18n-75abb6ee-291b-412f-919d-be646cf1a580
# MAGIC %md
# MAGIC **`email`** 列に UDF を適用します。

# COMMAND ----------

from pyspark.sql.functions import col

display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-26f93012-a994-4b6a-985e-01720dbecc25
# MAGIC %md
# MAGIC ### デコレーター構文を使用する (Python のみ)
# MAGIC
# MAGIC または、<a href="https://realpython.com/primer-on-python-decorators/" target="_blank">Python デコレータ構文</a>を使用して UDF を定義および登録することもできます。**`@udf`** デコレータ パラメータは、関数が返す列のデータ型です。
# MAGIC
# MAGIC ローカルの Python 関数を呼び出すことはできなくなります (つまり、 **`first_letter_udf("annagray@kaufman.com")`** は機能しません)。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> 
# MAGIC この例ではPython 3.5で導入された <a href="https://docs.python.org/3/library/typing.html" target="_blank">Pythonタイプヒント</a>を使います. この例では型ヒントは必須ではありませんが、代わりに、開発者が関数を正しく使用するのに役立つ "ドキュメント" として機能します。 この例では、UDF が一度に 1 つのレコードを処理し、単一の **`str`** 引数を取り、**`str`** 値を返すことを強調するために使用されています。

# COMMAND ----------

# Our input/output is a string
@udf("string")
def first_letter_udf(email: str) -> str:
    return email[0]

# COMMAND ----------

# DBTITLE 0,--i18n-4d628fe1-2d94-4d86-888d-7b9df4107dba
# MAGIC %md
# MAGIC ここでデコレータ UDF を使用しましょう。

# COMMAND ----------

from pyspark.sql.functions import col

sales_df = spark.table("sales")
display(sales_df.select(first_letter_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-3ae354c0-0b10-4e8c-8cf6-da68e8fba9f2
# MAGIC %md
# MAGIC ### Pandas/ベクトル化された UDF
# MAGIC
# MAGIC Pandas UDF は、UDF の効率を向上させるために Python で使用できます。 Pandas UDF は Apache Arrow を利用して計算を高速化します。
# MAGIC
# MAGIC * <a href="https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html" target="_blank">ブログ投稿</a>
# MAGIC * <a href="https://spark.apache.org/docs/latest/api/python/user_guide/sql/arrow_pandas.html?highlight=arrow" target="_blank">ドキュメント</a>
# MAGIC
# MAGIC <img src="https://databricks.com/wp-content/uploads/2017/10/image1-4.png" alt="ベンチマーク" width="500" height="1500">
# MAGIC
# MAGIC
# MAGIC ユーザー定義関数は、次を使用して実行されます。
# MAGIC * <a href="https://arrow.apache.org/" target="_blank">Apache Arrow</a>：JVM と Python の間でデータを効率的に転送するために Spark で使用されるインメモリカラム式データ形式 (シリアル化コストがゼロに近い)
# MAGIC * 関数内のPandas: Pandasインスタンスと API を操作するための
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" alt="Warning"> Spark 3.0 以降、Python 型ヒントを使用して Pandas UDF を **常に** 定義する必要があります。

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

# We have a string input/output
@pandas_udf("string")
def vectorized_udf(email: pd.Series) -> pd.Series:
    return email.str[0]

# Alternatively
# def vectorized_udf(email: pd.Series) -> pd.Series:
#     return email.str[0]
# vectorized_udf = pandas_udf(vectorized_udf, "string")

# COMMAND ----------

display(sales_df.select(vectorized_udf(col("email"))))

# COMMAND ----------

# DBTITLE 0,--i18n-9a2fb1b1-8060-4e50-a759-f30dc73ce1a1
# MAGIC %md
# MAGIC これらの Pandas UDF を SQL 名前空間に登録できます。

# COMMAND ----------

spark.udf.register("sql_vectorized_udf", vectorized_udf)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Use the Pandas UDF from SQL
# MAGIC SELECT sql_vectorized_udf(email) AS firstLetter FROM sales

# COMMAND ----------

# DBTITLE 0,--i18n-5e506b8d-a488-4373-af9a-9ebb14834b1b
# MAGIC %md
# MAGIC ### クリーンアップ

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
