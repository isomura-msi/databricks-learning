-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-ac3b4f33-4b00-4169-a663-000fddc1fb9d
-- MAGIC %md
-- MAGIC # 外部ソース用にオプションを指定する（Providing Options for External Sources）
-- MAGIC ファイルを直接照会することは自己記述的な形式には適していますが、多くのデータソースでは適切にレコードを取り込むためには追加の設定もしくはスキーマの宣言が必要となります。
-- MAGIC
-- MAGIC このレッスンでは、外部のデータソースを使用してテーブルを作成します。 これらのテーブルはまだDelta Lake形式では保存されません（したがってレイクハウス用には最適されません）が、この技を使用すると多様な外部システムからのデータ抽出を円滑にできます。
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC - Spark SQLを使用して外部ソースからデータを抽出するためにオプションを設定する
-- MAGIC - さまざまなファイル形式用の外部データソースに対してテーブルを作成する
-- MAGIC - 外部ソースに対して定義されたテーブルを照会する際のデフォルト動作を説明する

-- COMMAND ----------

-- DBTITLE 0,--i18n-d0bd783f-524d-4953-ac3c-3f1191d42a9e
-- MAGIC %md
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-90830ba2-82d9-413a-974e-97295b7246d0
-- MAGIC %md
-- MAGIC ## 直接的なクエリを使用できない場合 （When Direct Queries Don't Work）
-- MAGIC
-- MAGIC CSVは最も一般的なファイル形式の1つですが、これらのファイルに対する直接的なクエリを使用しても望ましい結果は滅多に得られません。

-- COMMAND ----------

SELECT * FROM csv.`${DA.paths.sales_csv}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-2de59e8e-3bc3-4609-96ad-e8985b250154
-- MAGIC %md
-- MAGIC 上記から次のことが分かります：
-- MAGIC 1. ヘッダの列がテーブルの列として抽出されています
-- MAGIC 1. すべての列が１つの列として読み込まれています
-- MAGIC 1. ファイルはパイプ（ **`|`** ）区切りを使用しています
-- MAGIC 1. 最後の列には、切り捨てられるネスト化されたデータが含まれています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-3eae3be1-134c-4f3b-b423-d081fb780914
-- MAGIC %md
-- MAGIC ## 読み取りオプションを使用して外部データに対してテーブルを登録する（Registering Tables on External Data with Read Options）
-- MAGIC
-- MAGIC Sparkはデフォルトの設定で一部の自己記述的なデータソースを効率的に抽出できますが、多くの形式ではスキーマの宣言もしくは他のオプションが必要となります。
-- MAGIC
-- MAGIC 外部ソースに対してテーブルを作成する際、多くの<a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/sql-ref-syntax-ddl-create-table-using.html" target="_blank">追加設定</a>を行えますが、以下の構文ではほとんどの形式からデータを抽出するための基本を示します。
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE table_identifier (col_name1 col_type1, ...)<br/>
-- MAGIC USING data_source<br/>
-- MAGIC OPTIONS (key1 = val1, key2 = val2, ...)<br/>
-- MAGIC LOCATION path<br/>
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC オプションは、引用符を使用しないテキストのキーおよび引用符を使用する値で渡されることにご注意ください。 Sparkは、カスタムのオプションで多くの<a href="https://docs.databricks.com/data/data-sources/index.html" target="_blank">データソース</a>をサポートーしており、外部<a href="https://docs.databricks.com/libraries/index.html" target="_blank">ライブラリ</a>を介して追加のシステムが非公式にサポートされている可能性があります。
-- MAGIC
-- MAGIC **注**：ワークスペースの設定によっては、一部のデータソースに対して、ライブラリを読み込み、必要なセキュリティ設定を行うには管理者の手伝いが必要になる場合があります。

-- COMMAND ----------

-- DBTITLE 0,--i18n-1c947c25-bb8b-4cad-acca-29651e191108
-- MAGIC %md
-- MAGIC 以下のセルでは、Spark SQL DDLを使用して、外部のCSVソースに対して次の情報を指定したテーブルを作成する方法を示します：
-- MAGIC 1. 列の名前と型
-- MAGIC 1. ファイル形式
-- MAGIC 1. フィールドを区切るための区切り文字
-- MAGIC 1. ヘッダの有無
-- MAGIC 1. このデータの保存先へのパス

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "${DA.paths.sales_csv}"

-- COMMAND ----------

-- DBTITLE 0,--i18n-4631ecfc-06b5-494a-904f-8577e345c98d
-- MAGIC %md
-- MAGIC **注**: PySparkで外部ソースに対するテーブルを作成するために、　SQLを<strong>`spark.sql()`</strong>関数で投げることができます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(f"""
-- MAGIC CREATE TABLE IF NOT EXISTS sales_csv
-- MAGIC   (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
-- MAGIC USING CSV
-- MAGIC OPTIONS (
-- MAGIC   header = "true",
-- MAGIC   delimiter = "|"
-- MAGIC )
-- MAGIC LOCATION "{DA.paths.sales_csv}"
-- MAGIC """)

-- COMMAND ----------

-- DBTITLE 0,--i18n-964019da-1d24-4a60-998a-bbf23ffc64a6
-- MAGIC %md
-- MAGIC テーブルの宣言時にはデータが移動していないことにご注意ください。 
-- MAGIC
-- MAGIC ファイルを直接クエリしてビューを作成したときと同様に、あくまで外部の場所に保存されているファイルを指しているだけです。
-- MAGIC
-- MAGIC 次のセルを実行してデータが正しく読み込まれていることを確認しましょう。

-- COMMAND ----------

SELECT * FROM sales_csv

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-d11afb5e-08d3-42c3-8904-14cdddfe5431
-- MAGIC %md
-- MAGIC テーブルの宣言時に渡されたメタデータとオプションはすべてメタストアに保持され、この場所のデータが常にこのオプションを使用して読み取られるようにします。
-- MAGIC
-- MAGIC **注**：データソースとしてCSVを扱っている場合は、ソースディレクトリにデータファイルが追加されても列の順序が変わらないようにするのが重要です。 このデータ形式には強力なスキーマ強制がないため、Sparkは、テーブルの宣言時に指定された順序で列を読み込み列の名前とデータ型を適用します。
-- MAGIC
-- MAGIC テーブルに対して **`DESCRIBE EXTENDED`** を実行すると、このテーブルの定義に関連づけられているすべてのメタデータが表示されます。

-- COMMAND ----------

DESCRIBE EXTENDED sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdbb45bc-72b3-4610-97a6-accd30ec8fec
-- MAGIC %md
-- MAGIC ## 外部のデータソースを使用したテーブルの制限（Limits of Tables with External Data Sources）
-- MAGIC
-- MAGIC Databricksで講座を受けたり弊社の文献を読んだりしたことのある方は、Delta Lakeとレイクハウスのことを聞いたことがあるかもしれません。 外部のデータソースに対してテーブルもしくはクエリを定義するとき、Delta Lakeとレイクハウスを伴うパフォーマンスの保証は期待**できません**のでご注意ください。
-- MAGIC
-- MAGIC 例えば、Delta Lakeのテーブルを使用すると、常にソースデータの最新バージョンがクエリされることが保証されますが、他のデータソースに対して登録されているテーブルを使用した場合は、キャッシュされた以前のバージョンを表している可能性があります。
-- MAGIC
-- MAGIC 以下のセルでは、テーブルの元になっているファイルを直接更新する外部システムを表しているものと考えられるロジックが実行されます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (spark.read
-- MAGIC       .option("header", "true")
-- MAGIC       .option("delimiter", "|")
-- MAGIC       .csv(DA.paths.sales_csv)
-- MAGIC       .write.mode("append")
-- MAGIC       .format("csv")
-- MAGIC       .save(DA.paths.sales_csv, header="true"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-28b2112b-4eb2-4bd4-ad76-131e010dfa44
-- MAGIC %md
-- MAGIC テーブルにある現在のレコード数を見ても、表示される数字にはこれらの新しく挿入された列は反映されません。

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-bede6aed-2b6b-4ee7-8017-dfce2217e8b4
-- MAGIC %md
-- MAGIC 以前、このデータソースを照会したとき、Sparkはその元になっているデータを自動的にローカルストレージのキャッシュに格納しました。 これにより、その後のクエリでSparkは、このローカルのキャッシュを照会するだけで最適なパフォーマンスを実現できます。
-- MAGIC
-- MAGIC この外部データソースは、このデータは更新すべきだとSparkに伝えるようには設定されていません。
-- MAGIC
-- MAGIC **`REFRESH TABLE`** コマンドを実行すると、データのキャッシュを手動で更新**できます**。

-- COMMAND ----------

REFRESH TABLE sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-656a9929-a4e5-4fb1-bfe6-6c3cc7137598
-- MAGIC %md
-- MAGIC テーブルを更新するとキャッシュが無効になるため、元のデータソースを再スキャンしてすべてのデータをまたメモリに取り込む必要が出てくることにご注意ください。
-- MAGIC
-- MAGIC 非常に大きなデータセットの場合、この処理には長時間がかかる場合があります。

-- COMMAND ----------

SELECT COUNT(*) FROM sales_csv

-- COMMAND ----------

-- DBTITLE 0,--i18n-ee1ac9ff-add1-4247-bc44-2e71f0447390
-- MAGIC %md
-- MAGIC ## SQLデータベースからデータを抽出（Extracting Data from SQL Databases）
-- MAGIC SQLデータベースは、非常に一般的なデータソースであり、Databricksにはさまざまな種類のSQLに接続するための標準JDBCドライバが備わっています。
-- MAGIC
-- MAGIC このような接続を生成する一般的な構文は次の通りです：
-- MAGIC
-- MAGIC <strong><code>
-- MAGIC CREATE TABLE <jdbcTable><br/>
-- MAGIC USING JDBC<br/>
-- MAGIC OPTIONS (<br/>
-- MAGIC &nbsp; &nbsp; url = "jdbc:{databaseServerType}://{jdbcHostname}:{jdbcPort}",<br/>
-- MAGIC &nbsp; &nbsp; dbtable = "{jdbcDatabase}.table",<br/>
-- MAGIC &nbsp; &nbsp; user = "{jdbcUsername}",<br/>
-- MAGIC &nbsp; &nbsp; password = "{jdbcPassword}"<br/>
-- MAGIC )
-- MAGIC </code></strong>
-- MAGIC
-- MAGIC 下のコードサンプルでは、<a href="https://www.sqlite.org/index.html" target="_blank">SQLite</a>を使用して接続します。
-- MAGIC
-- MAGIC **注意：**SQLiteは、ローカルファイルにデータベースを保管し、ポート、ユーザー名、パスワードを必要としません。
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_24.png" /> **要注意**：JDBCサーバのバックエンド構成は、このノートブックがシングルノードクラスタで実行されていることを前提としています。 複数のワーカーを使用したクラスタを実行している場合、エグゼキュータで実行されているクライアントはドライバに接続することができません。

-- COMMAND ----------

DROP TABLE IF EXISTS users_jdbc;

CREATE TABLE users_jdbc
USING JDBC
OPTIONS (
  url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
  dbtable = "users"
)

-- COMMAND ----------

-- DBTITLE 0,--i18n-33fb962c-707d-43b8-8a37-41ebb5d83b2f
-- MAGIC %md
-- MAGIC これで、ローカルで定義されているかのようにこのテーブルを照会できます。

-- COMMAND ----------

SELECT * FROM users_jdbc

-- COMMAND ----------

-- DBTITLE 0,--i18n-3576239e-8f73-4ef9-982e-e42542d4fc70
-- MAGIC %md
-- MAGIC テーブルのメタデータを確認すると、外部システムからスキーマの情報を取り込んだことが分かります。 （この接続に関連づけられているユーザー名とパスワードが含まれる）ストレージのプロパティは自動的に編集されます。

-- COMMAND ----------

DESCRIBE EXTENDED users_jdbc

-- COMMAND ----------

-- DBTITLE 0,--i18n-c20051d4-f3c3-483c-b4fa-ff2d356fcd5e
-- MAGIC %md
-- MAGIC 指定された場所の中身をリストしてみると、ローカルにはデータは保持されていないことがわかります。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as F
-- MAGIC
-- MAGIC location = spark.sql("DESCRIBE EXTENDED users_jdbc").filter(F.col("col_name") == "Location").first()["data_type"]
-- MAGIC print(location)
-- MAGIC
-- MAGIC files = dbutils.fs.ls(location)
-- MAGIC print(f"Found {len(files)} files")

-- COMMAND ----------

-- DBTITLE 0,--i18n-1cb11f07-755c-4fb2-a122-1eb340033712
-- MAGIC %md
-- MAGIC データウェアハウスなど、一部のSQLシステムにはカスタムのドライバがあることにご注意ください。 Sparkがさまざまな外部のデータソースと相互作用する方法は異なりますが、2つの基本的な方法は次の通り要約できます：
-- MAGIC 1. ソーステーブルを丸ごとDatabricksに移動させて、現在アクティブなクラスタでロジックを実行する
-- MAGIC 1. クエリを外部のSQLデータベースに転送して、Databricksに結果のみを返す
-- MAGIC
-- MAGIC どちらにしても、外部のSQLデータベースで非常に大きなデータセットを扱うと、次の理由のどちらかのせいで深刻なオーバーヘッドにつながってしまいます。
-- MAGIC 1. 公共インターネットですべてのデータを移動させることによるネットワーク転送の遅延
-- MAGIC 1. ビッグデータクエリに最適化されていないソースシステムでのクエリロジックの実行

-- COMMAND ----------

-- DBTITLE 0,--i18n-c973af61-2b79-4c55-8e32-f6a8176ea9e8
-- MAGIC %md
-- MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
