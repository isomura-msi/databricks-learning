# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-ad7af192-ab00-41a3-b683-5de4856cacb0
# MAGIC %md
# MAGIC # Spark SQL
# MAGIC
# MAGIC DataFrame API を使用して、Spark SQL の基本的な概念を把握します。
# MAGIC
# MAGIC ##### 目的 (Objectives)
# MAGIC 1. SQL クエリの実行
# MAGIC 1. テーブルからDataFrameを作成
# MAGIC 1. DataFrameトランスフォーメーションにより同等のクエリを書く
# MAGIC 1. DataFrameアクションを使用して計算の実行
# MAGIC 1. DataFramesとSQL間の変換
# MAGIC
# MAGIC ##### メソッド (Methods)
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">SparkSession</a>: **`sql`**, **`table`**
# MAGIC - <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/dataframe.html" target="_blank">DataFrame</a>:
# MAGIC   - トランスフォーメーション (Transformations):  **`select`**, **`where`**, **`orderBy`**
# MAGIC   - アクション (Actions): **`show`**, **`count`**, **`take`**
# MAGIC   - その他メソッド (Other methods): **`printSchema`**, **`schema`**, **`createOrReplaceTempView`**

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-00.01

# COMMAND ----------

# DBTITLE 0,--i18n-3ad6c2cb-bfa4-4af5-b637-ba001a9ef54b
# MAGIC %md
# MAGIC ## 複数のインターフェース (Multiple Interfaces)
# MAGIC Spark SQL は、複数のインターフェイスを備えた構造化データ処理用のモジュールです。
# MAGIC
# MAGIC Spark SQLモジュールとやり取りするには、次の2つの方法があります。
# MAGIC 1. SQL クエリの実行
# MAGIC 1. DataFrame APIの利用

# COMMAND ----------

# DBTITLE 0,--i18n-236a9dcf-8e89-4b08-988a-67c3ca31bb71
# MAGIC %md
# MAGIC **Method 1: SQL クエリの実行**
# MAGIC
# MAGIC これは基本的なSQLクエリです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT name, price
# MAGIC FROM products
# MAGIC WHERE price < 200
# MAGIC ORDER BY price

# COMMAND ----------

# DBTITLE 0,--i18n-58f7e711-13f5-4015-8cff-c18ec5b305c6
# MAGIC %md
# MAGIC **Method 2: DataFrame APIの利用**
# MAGIC
# MAGIC DataFrame APIを利用してSpark SQLクエリを書きます。
# MAGIC
# MAGIC 次のセルは、上記で取得したものと同じ結果を含む DataFrame を返します。

# COMMAND ----------

display(spark
        .table("products")
        .select("name", "price")
        .where("price < 200")
        .orderBy("price")
       )

# COMMAND ----------

# DBTITLE 0,--i18n-5b338899-be0c-46ae-92d9-8cfc3c2c3fb8
# MAGIC %md
# MAGIC レッスンの後半でDataFrame APIの構文について説明しますが、このビルダーデザインパターンを使用すると、SQLと似たように一連の操作を連鎖させることができます。

# COMMAND ----------

# DBTITLE 0,--i18n-bb02bfff-cf98-4639-af21-76bec5c8d95b
# MAGIC %md
# MAGIC ## クエリの実行 (Query Execution)
# MAGIC
# MAGIC 任意のインターフェイスを使用して同じクエリを表現できます。 Spark SQLエンジンは、Sparkクラスターでクエリの最適化と実行のため同じクエリプランを生成します。
# MAGIC
# MAGIC ![query execution engine](https://files.training.databricks.com/images/aspwd/spark_sql_query_execution_engine.png)
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" alt="Note"> Resilient Distributed Dataset (RDD)は、Spark クラスターによって処理されるデータセットの低レベルの表現です。 Sparkの初期のバージョンでは、<a href="https://spark.apache.org/docs/latest/rdd-programming-guide.html" target="_blank">RDD を直接操作するコード</a>を書く必要がありました。 最新バージョンのSparkでは、代わりに高レベルのDataFrame APIを使用する必要があります。Sparkは自動的に低レベルのRDD操作にコンパイルします。

# COMMAND ----------

# DBTITLE 0,--i18n-fbaea5c1-fefc-4b3b-a645-824ffa77bbd5
# MAGIC %md
# MAGIC ## Spark API ドキュメント (Spark API Documentation)
# MAGIC
# MAGIC Spark SQLでDataFrameを操作する方法を学ぶために、まずSpark APIドキュメントを見てみましょう。
# MAGIC メインのSpark<a href="https://spark.apache.org/docs/latest/" target="_blank">ドキュメント</a>ページには、Sparkの各バージョンのAPIドキュメントとガイドへのリンクが含まれています。
# MAGIC
# MAGIC
# MAGIC <a href="https://spark.apache.org/docs/latest/api/scala/org/apache/spark/index.html" target="_blank">Scala API</a>と<a href="https://spark.apache.org/docs/latest/api/python/index.html" target="_blank">Python API</a> が最も使用されており、ドキュメントを参照すると役立つことがよくあります。
# MAGIC Scalaドキュメントはより包括的である傾向があり、Pythonドキュメントはより多くのコード例を含む傾向があります。
# MAGIC
# MAGIC #### Spark SQLモジュールのドキュメントのナビゲート (Navigating Docs for the Spark SQL Module)
# MAGIC
# MAGIC Scala APIの<strong>`org.apache.spark.sql`</strong>または Python API の<strong>`pyspark.sql`</strong>に移動すると、Spark SQLモジュールのドキュメントがあります。
# MAGIC このモジュールで最初に探索してみるクラスは<strong>`SparkSession`</strong>クラスです。検索バーに「SparkSession」と入力すると、<strong>`SparkSession`</strong>を見つけられます。

# COMMAND ----------

# DBTITLE 0,--i18n-24790eda-96df-49bb-af34-b1ed839fa80a
# MAGIC %md
# MAGIC ## SparkSession
# MAGIC
# MAGIC <strong>`SparkSession`</strong>クラスは、DataFrame APIを使用するSparkのすべての機能への唯一のエントリです。
# MAGIC
# MAGIC Databricksノートブックでは、既にSparkSessionが作成され、<strong>`spark`</strong>という変数に格納されています。

# COMMAND ----------

spark

# COMMAND ----------

# DBTITLE 0,--i18n-4f5934fb-12b9-4bf2-b821-5ab17d627309
# MAGIC %md
# MAGIC このレッスンの最初の例では、SparkSessionのメソッド<strong>`table`</strong>を使用して<strong>`products`</strong>テーブルからDataFrameを作成しました。これを変数<strong>`products_df`</strong>に保存しましょう。

# COMMAND ----------

products_df = spark.table("products")

# COMMAND ----------

# DBTITLE 0,--i18n-f9968eff-ed08-4ed6-9fe7-0252b94bf50a
# MAGIC %md
# MAGIC 以下は、DataFrame を作成するために使用するいくつかのメソッドです。これらはすべて **`SparkSession`** <a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/spark_session.html" target="_blank">ドキュメント</a>にあります。
# MAGIC
# MAGIC #### **`SparkSession`** メソッド
# MAGIC | メソッド | 説明 |
# MAGIC | --- | --- |
# MAGIC | sql | 指定クエリの結果としてDataFrameを返します |
# MAGIC | table | 指定テーブルをDataFrameとして返します |
# MAGIC | read | データを DataFrame として読み取るために使用できる DataFrameReader を返します |
# MAGIC | range | ステップ値とパーティション数を使用して、開始値から終了値 (排他的) までの範囲内の要素を含む列を持つ DataFrame を作成します |
# MAGIC | createDataFrame |主にテストに使用される。タプルのリストからDataFrameを作成します |

# COMMAND ----------

# DBTITLE 0,--i18n-2277e250-91f9-489a-940b-97d17e75c7f5
# MAGIC %md
# MAGIC SparkSessionメソッドを使用してSQLを実行しましょう。

# COMMAND ----------

result_df = spark.sql("""
SELECT name, price
FROM products
WHERE price < 200
ORDER BY price
""")

display(result_df)

# COMMAND ----------

# DBTITLE 0,--i18n-f2851702-3573-4cb4-9433-ec31d4ceb0f2
# MAGIC %md
# MAGIC ## DataFrames
# MAGIC DataFrame API のメソッドを使用してクエリを書くと、DataFrameとして結果が返されることを思い出してください。これを変数<strong>`budget_df`</strong>に格納しましょう。
# MAGIC
# MAGIC <strong>DataFrame</strong>は、名前付きカラムにグループ化されたデータの分散コレクションです。

# COMMAND ----------

budget_df = (spark
             .table("products")
             .select("name", "price")
             .where("price < 200")
             .orderBy("price")
            )

# COMMAND ----------

# DBTITLE 0,--i18n-d538680a-1d7a-433c-9715-7fd975d4427b
# MAGIC %md
# MAGIC <strong>display()</strong>を使用して、DataFrameの結果を出力できます。

# COMMAND ----------

display(budget_df)

# COMMAND ----------

# DBTITLE 0,--i18n-ea532d26-a607-4860-959a-00a2eca34305
# MAGIC %md
# MAGIC <strong>`schema`</strong>は、データフレームのカラム名と型を定義します。
# MAGIC <strong>`schema`</strong>属性を使用して、DataFrameのスキーマにアクセスします。

# COMMAND ----------

budget_df.schema

# COMMAND ----------

# DBTITLE 0,--i18n-4212166a-a200-44b5-985c-f7f1b33709a3
# MAGIC %md
# MAGIC <strong>`printSchema()`</strong>メソッドを使用して、このスキーマのより適切な出力を表示します。

# COMMAND ----------

budget_df.printSchema()

# COMMAND ----------

# DBTITLE 0,--i18n-7ad577db-093a-40fb-802e-99bbc5a4435b
# MAGIC %md
# MAGIC ## トランスフォーメーション (Transformations)
# MAGIC <strong>`budget_df`</strong>を作成したとき、一連のDataFrame変換メソッドを使用しました。 例）<strong>`select`</strong>、<strong>`where`<strong>、<strong>`orderBy`<strong>
# MAGIC
# MAGIC <strong><code>products_df  
# MAGIC &nbsp;  .select("name", "price")  
# MAGIC &nbsp;  .where("price < 200")  
# MAGIC &nbsp;  .orderBy("price")  
# MAGIC </code></strong>
# MAGIC     
# MAGIC トランスフォーメーションはDataFrameを操作して返すため、トランスフォーメーションメソッドを連鎖させて新しいDataFrameを構築できます。
# MAGIC ただし、変換メソッドは<strong>遅延評価</strong>されるため、これらの操作は単独では実行できません。
# MAGIC
# MAGIC 次のセルを実行しても、計算はトリガーされません。

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price"))

# COMMAND ----------

# DBTITLE 0,--i18n-56f40b55-842f-44cf-b34a-b0fd17a962d4
# MAGIC %md
# MAGIC ## アクション (Actions)
# MAGIC 逆に、DataFrameアクションは、<strong>計算をトリガー</strong>するメソッドです。
# MAGIC DataFrame変換の実行をトリガーするには、アクションが必要です。
# MAGIC
# MAGIC <strong>`show`</strong>アクションは、次のセルのトランスフォーメーションを実行させます。

# COMMAND ----------

(products_df
  .select("name", "price")
  .where("price < 200")
  .orderBy("price")
  .show())

# COMMAND ----------

# DBTITLE 0,--i18n-6f574091-0026-4dd2-9763-c6d4c3b9c4fe
# MAGIC %md
# MAGIC 以下は<a href="https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql.html#dataframe-apis" target="_blank">DataFrame</a>アクションの例です。
# MAGIC
# MAGIC ### DataFrameのアクションメソッド (DataFrame Action Methods)
# MAGIC | メソッド |説明 |
# MAGIC |  --- | --- |
# MAGIC | show | DataFrame の上位 n 行を表形式で表示 |
# MAGIC | count |　DataFrame の行数を返します| 
# MAGIC | describe,  summary |数値列と文字列の基本統計を計算 |
# MAGIC | first, head |最初の行を返します |
# MAGIC | collect | DataFrame のすべての行を含む配列を返します |
# MAGIC | take | DataFrame の最初の n 行の配列を返します|

# COMMAND ----------

# DBTITLE 0,--i18n-7e725f41-43bc-4e56-9c44-d46becd375a0
# MAGIC %md
# MAGIC <strong>`count`</strong>はDataFrameの行数を返します。

# COMMAND ----------

budget_df.count()

# COMMAND ----------

# DBTITLE 0,--i18n-12ea69d5-587e-4953-80d9-81955eeb9d7b
# MAGIC %md
# MAGIC <strong>`collect`</strong>はDataFrameのすべての行を含む配列を返します

# COMMAND ----------

budget_df.collect()

# COMMAND ----------

# DBTITLE 0,--i18n-983f5da8-b456-42b5-b21c-b6f585c697b4
# MAGIC %md
# MAGIC ## DataFramesとSQL間の変換 (Convert between DataFrames and SQL)

# COMMAND ----------

# DBTITLE 0,--i18n-0b6ceb09-86dc-4cdd-9721-496d01e8737f
# MAGIC %md
# MAGIC <strong>`createOrReplaceTempView`</strong>は、DataFrameに基づいて一時的なビューを作成します。一時ビューの有効期間は、DataFrameの作成に使用されたSparkSessionに関連付けられています。

# COMMAND ----------

budget_df.createOrReplaceTempView("budget")

# COMMAND ----------

display(spark.sql("SELECT * FROM budget"))

# COMMAND ----------

# DBTITLE 0,--i18n-81ff52ca-2160-4bfd-a78f-b3ba2f8b4933
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
