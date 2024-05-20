# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-8c690200-3a21-4838-8d2a-15385fda557f
# MAGIC %md
# MAGIC # Fundamentals of DLT Python Syntax
# MAGIC
# MAGIC このノートブックでは、レイクハウス上でDelta Live Tables（DLT）を使用し、オブジェクトストレージ上のJSONファイルにある生データを一連のテーブルに処理する分析ワークロードの駆動を実証します。パイプラインを流れるようにデータが段階的に変換され、エンリッチされるメダリオンアーキテクチャです。このノートブックでは、アーキテクチャよりもDLTのPython構文に焦点を当てますが、メダリオンアーキテクチャの概要を簡単に説明します：
# MAGIC
# MAGIC ブロンズテーブルには、JSONから読み込まれた生のレコードを含みます。レコードは、どのように取り込まれたかを示すデータでエンリッチされています。
# MAGIC シルバーテーブルでは、生データを検証し、関心のあるフィールドをエンリッチします。
# MAGIC ゴールドテーブルには、ビジネスインサイトとダッシュボードに使う集計データを含みます。
# MAGIC
# MAGIC ## 学習の目的(Learning Objectives)
# MAGIC このノートブックで次を学びます：
# MAGIC
# MAGIC * Delta Live Tablesの宣言
# MAGIC * Auto Loaderによるデータの取り込み
# MAGIC * DLT Pipelinesにおけるパラメータの使用
# MAGIC * 制約によるデータ品質の強制
# MAGIC * テーブルへのコメントの追加
# MAGIC * liveテーブルとstreaming live テーブルの、構文および実行の違い

# COMMAND ----------

# DBTITLE 0,--i18n-c23e126e-c267-4704-bfca-caee48728551
# MAGIC %md
# MAGIC ## DLT ライブラリノートブックについて(About DLT Library Notebooks)
# MAGIC
# MAGIC
# MAGIC DLTの構文は、ノートブックでのインタラクティブな実行を意図したものではありません。 このノートブックを適切に実行するためには、DLTパイプラインの一部としてスケジュールされる必要があります。
# MAGIC
# MAGIC 本ノートブックの作成時点では、Databricksの最新のランタイムは **`dlt`** モジュールをincludeしていません。そんとあえｍ，DLTコマンドをノートブック上で実行しようとするとエラーになります。
# MAGIC
# MAGIC DLTコードの開発およびトラブルシューティングについては、このコースの後半で説明します。
# MAGIC
# MAGIC
# MAGIC ## パラメタの設定(Parameterization)
# MAGIC
# MAGIC DLTパイプラインを設定した時に、いくつかのオプションを指定しました。 そのうちの1つが、**Configurations** フィールドに追加したキーとバリューのペアです。
# MAGIC
# MAGIC DLTパイプラインにおける Configurationは、Databricksジョブにおけるパラメータに似ていますが、実際にはSparkの設定にセットされます。
# MAGIC
# MAGIC Pythonでは、これら値に **`spark.conf.get()`** でアクセスできます。
# MAGIC
# MAGIC 本レッスンでは、 Pythonの変数 **`source`** をセットし、コードの中で必要に応じてこの変数を使用することにしています。
# MAGIC
# MAGIC
# MAGIC ## importに関する注意 (A Note on Imports)
# MAGIC
# MAGIC **`dlt`** モジュールは、Pythonノートブックライブラリに明示的にインポートする必要があります。
# MAGIC
# MAGIC ここでは、**`pyspark.sql.functions`** を **`F`** としてインポートしておくことにします。
# MAGIC
# MAGIC 開発者の中には **`*`** で全てをインポートする人もいれば、現在のノートブックで必要な機能だけをインポートする人もいます。
# MAGIC
# MAGIC 本レッスンでは、どのメソッドがこのライブラリからインポートされたものなのかが明確に分かるように、全体的に **`F`** を使用します。

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-d260ca6c-11c2-464d-8c16-f5ff6e56b8b0
# MAGIC %md
# MAGIC ## DataFrameのテーブル (Tables as DataFrames)
# MAGIC
# MAGIC DLTで作成できる永続的なテーブルには、2つの異なるタイプがあります：
# MAGIC
# MAGIC * **Liveテーブル** は、レイクハウスのマテリアライズド・ビューです。更新(refresh)のたびにクエリの現在の結果を返します。
# MAGIC * **Streaming live テーブル** は、インクリメンタルな、ほぼリアルタイムのデータ処理用に設計されています。
# MAGIC
# MAGIC これらのオブジェクトはどちらもDelta Lakeプロトコル（ACIDトランザクション、バージョニング、その他多くの利点を提供）で保存されたテーブルとして永続化されていることに注意してください。 ライブテーブルとストリーミングライブテーブルの違いについては、このノートの後半で詳しく説明します。
# MAGIC
# MAGIC Delta Live テーブルは、おなじみのPySpark APIを拡張する新しいPython関数をいくつか導入しています。
# MAGIC
# MAGIC 設計の中心は、Spark DataFrameを返すPython関数に **`@dlt.table`** というデコレーターを追加することです。
# MAGIC (注：これにはKoalas DataFrameも含まれますが、このコースでは取り上げません)。
# MAGIC
# MAGIC SparkやStructured Streamingに慣れている方なら、DLTで使われている構文の大部分にお気づきでしょう。
# MAGIC 大きな違いは、DataFrameライター用のメソッドやオプションは一切見かけないということです。
# MAGIC
# MAGIC このように、DLTテーブルの定義の基本形は次のようになります：
# MAGIC **`@dlt.table`**<br/>
# MAGIC **`def <function-name>():`**<br/>
# MAGIC **`    return (<query>)`**</br>

# COMMAND ----------

# DBTITLE 0,--i18n-969f77f0-34bf-4e23-bdc5-1f0575e99ed1
# MAGIC %md
# MAGIC ## Auto Loaderを使ったストリーミング形式のデータ取り込み (Streaming Ingestion with Auto Loader)
# MAGIC
# MAGIC Databricksは、クラウドオブジェクトストレージからDelta Lakeにデータをインクリメンタルにロードするための最適化された[Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)機能を開発しました。 DLTでAuto Loaderを使用するのは簡単です。ソースデータディレクトリを構成し、いくつかの構成設定を行い、ソースデータに対してクエリを記述するだけです。Auto Loaderは、新しいデータファイルがソースのクラウドオブジェクトストレージに到着すると自動的に検出します。無限に増えるデータセットに対して高価なスキャンや結果の再計算を行う必要なく、新しいレコードをインクリメンタルに処理します。
# MAGIC
# MAGIC **`format("cloudFiles")`** は、format("cloudFiles")設定を行うことで、Structured Streaming APIと組み合わせてDatabricks全体でデータのインクリメンタルインジェストを行うことができます。DLTでは、データの読み込みに関連する設定のみを行います。スキーマの推論や進化のための場所も、これらの設定が有効であれば自動的に設定されることに留意してください。
# MAGIC
# MAGIC 以下のクエリーは、Auto Loaderを設定することでソースからストリーミングDataFrame を返します。
# MAGIC
# MAGIC formatに**`format("cloudFiles")`** を指定することに加えて、AutoLoaderでは次を設定できます:
# MAGIC * **`cloudFiles.format`** オプションを **`json`** にする(クラウドオブジェクト上のファイルがjsonであることを示す)
# MAGIC * **`cloudFiles.inferColumnTypes`** オプションを **`True`** にする(各カラムの型を自動判別する)
# MAGIC * **`load`** メソッドでオブジェクトストレージのパスを指定する。
# MAGIC * select文でいくつかの **`pyspark.sql.functions`** を指定し、ソースデータのフィールドからデータのエンリッチを行う。
# MAGIC
# MAGIC デフォルトで、**`@dlt.table`** は関数の名前をターゲットテーブルの名前にします。

# COMMAND ----------

@dlt.table
def orders_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", True)
            .load(f"{source}/orders")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-d4d6be28-67fb-44d8-ab4a-cfae62d819ed
# MAGIC %md
# MAGIC ## 検証、エンリッチ、データ変換(Validating, Enriching, and Transforming Data)
# MAGIC DLTは、データ品質チェックの新しい機能を提供します。また、メタデータでテーブルのデータをエンリッチするいくつかのオプションを提供します。
# MAGIC
# MAGIC 以下、クエリの構文を分解してみましょう。
# MAGIC
# MAGIC ### **`@dlt.table()`**のオプション(Options for **`@dlt.table()`**)
# MAGIC
# MAGIC <a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-python-ref.html#create-table" target="_blank">様々なオプション</a>があり、テーブル作成時に設定できます。その中からデータセットに情報を付与する2つ取り上げます。
# MAGIC
# MAGIC ##### **`コメント(comment)`**
# MAGIC
# MAGIC テーブルのコメントはリレーショナル・データベースで標準的です。組織全体のユーザーに有用な情報を提供するために使用することができます。 この例では、データがどのように取り込まれ、制約付けられているか（これは他のメタデータを確認することでも得られる）を説明する、短くヒューマンリーダブルな説明を書いています。
# MAGIC
# MAGIC
# MAGIC ##### **`テーブルのプロパティ(table_properties)`**
# MAGIC
# MAGIC このフィールドを使って、データのカスタムタグ付けのために、任意の数のキー/バリュー ペアを渡すために使用できます。
# MAGIC ここでは、キー **`quality`** に バリュー **`silver`**を設定しています。
# MAGIC
# MAGIC このフィールドでは、カスタムタグを任意に設定できますが、テーブルの使い方を制御する設定にも使用されることに注意してください。テーブルの詳細を確認すると、テーブルが作成される際にデフォルトでオンになっている設定を見ることがあります。
# MAGIC
# MAGIC
# MAGIC ### データ品質に関する制約事項(Data Quality Constraints)
# MAGIC
# MAGIC Python版DLTでは、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-expectations.html#delta-live-tables-data-quality-constraints" target="_blank">データ品質の制約</A>を設定するためにデコレーター関数を使用しています。コースを通して、これらのいくつかを目にすることになります。
# MAGIC
# MAGIC DLTでは、シンプルなBooleanステートメントを使用して、データの品質強制チェックを可能にします。以下のステートメントで、次を行います:
# MAGIC * **`valid_date`**という名前の制約を宣言する。
# MAGIC * フィールド **`order_timestamp`** に2021年1月1日以上の値が含まれていることを条件とするチェックを定義する。
# MAGIC * デコレーター **`@dlt.expect_or_fail()`** を使って、制約に違反するレコードがあれば、現在のトランザクションを失敗させるように DLT に指示します。
# MAGIC
# MAGIC 各制約は複数の条件を持つことができ、1つのテーブルに対して複数の制約を設定することができます。制約違反は更新を失敗させるだけでなく、自動的にレコードを削除したり、これらの無効なレコードを処理しながら違反の数だけを記録することもできます
# MAGIC
# MAGIC
# MAGIC ### DLTによるreadメソッド(DLT Read Methods)
# MAGIC
# MAGIC Python  **`dlt`** モジュールは、DLTパイプライン中の他のテーブルやビューへの参照を簡単に設定するために **`read()`** と **`read_stream()`**メソッドを提供します。この構文では、schema(database)を参照することなくデータセットを名前で参照することができます。**`spark.table("LIVE.<table_name.")`** を使用することもできます。ここで、**`LIVE`** は DLT Pipeline で参照されるデータベースを表すキーワードに置き換えられます。

# COMMAND ----------

@dlt.table(
    comment = "Append only orders with valid timestamps",
    table_properties = {"quality": "silver"})
@dlt.expect_or_fail("valid_date", F.col("order_timestamp") > "2021-01-01")
def orders_silver():
    return (
        dlt.read_stream("orders_bronze")
            .select(
                "processing_time",
                "customer_id",
                "notifications",
                "order_id",
                F.col("order_timestamp").cast("timestamp").alias("order_timestamp")
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-e6fed8ba-7028-4d19-9310-cc705b7858e4
# MAGIC %md
# MAGIC ## ライブテーブル vs. ストリーミングライブテーブル(Live Tables vs. Streaming Live Tables)
# MAGIC これまでに紹介した2つの関数は、いずれもストリーミング・ライブ・テーブルを作成するものでした。以下では、集約されたデータのLiveテーブル（またはマテリアライズドビュー）を返す簡単な関数を紹介します。
# MAGIC
# MAGIC Sparkは歴史的に、バッチクエリとストリーミングクエリを区別してきました。LiveテーブルとStreaming Liveテーブルも同様の違いがあります。
# MAGIC
# MAGIC DLTのテーブルタイプは、PySparkとStructured Streaming APIの構文（といくつかの制限事項）を継承していることに注意してください。
# MAGIC
# MAGIC 下記は、これらのテーブルタイプの違いです:
# MAGIC
# MAGIC ### Liveテーブル(Live Tables)
# MAGIC * 常に「正しい」、つまり更新後もその内容は定義と一致する。
# MAGIC * 初回からすべてのデータを対象にテーブルが定義された場合、同じ結果を返します。
# MAGIC * DLT Pipelineの外部からの操作で変更してはならない（未定義の答えが返ってくるか、変更内容が元に戻される）。
# MAGIC
# MAGIC ### Streaming Liveテーブル(Streaming Live Tables)
# MAGIC * "Append-only"のストリーミングソースからの読み込みのみをサポートします。
# MAGIC * たとえ結合対象のテーブル次元が変わったり、クエリ定義が変わっても、ソースからの各入力バッチを1回だけ読み込みます。。
# MAGIC * マネージドDLTパイプラインの外にあるテーブルを操作できる（データの追加、GDPRの実行など）。

# COMMAND ----------

@dlt.table
def orders_by_date():
    return (
        dlt.read("orders_silver")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )

# COMMAND ----------

# DBTITLE 0,--i18n-facc7d78-16e4-4f03-a232-bb5e4036952a
# MAGIC %md
# MAGIC ## サマリ(Summary)
# MAGIC
# MAGIC 次を学習しました：
# MAGIC * デルタライブテーブルの宣言
# MAGIC * Auto Loaderによるデータの取り込み
# MAGIC * DLT Pipelinesにおけるパラメータの使用
# MAGIC * 制約によるデータ品質の強制
# MAGIC * テーブルへのコメントの追加
# MAGIC * LiveテーブルとStreaming Liveテーブルの構文と実行の違い
# MAGIC
# MAGIC 次のノートでは、これらの構文にいくつかの新しい概念を加えていきます。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
