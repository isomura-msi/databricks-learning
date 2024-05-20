-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-c457f8c6-b318-46da-a68f-36f67d8a1b9c
-- MAGIC %md
-- MAGIC # Delta Live TablesにおけるSQL構文の基礎(Fundamentals of DLT SQL Syntax)
-- MAGIC
-- MAGIC このノートブックでは、レイクハウス上でDelta Live Tables（DLT）を使用し、オブジェクトストレージ上のJSONファイルにある生データを一連のテーブルに処理する分析ワークロードの駆動を実証します。パイプラインを流れるようにデータが段階的に変換され、エンリッチされるメダリオンアーキテクチャです。このノートブックでは、アーキテクチャよりもDLTのSQL構文に焦点を当てますが、メダリオンアーキテクチャの概要を簡単に説明します：
-- MAGIC
-- MAGIC * ブロンズテーブルには、JSONから読み込まれた生のレコードを含みます。レコードは、どのように取り込まれたかを示すデータでエンリッチされています。
-- MAGIC * シルバーテーブルでは、生データを検証し、関心のあるフィールドをエンリッチします。
-- MAGIC * ゴールドテーブルには、ビジネスインサイトとダッシュボードに使う集計データを含みます。
-- MAGIC
-- MAGIC
-- MAGIC ## 学習の目的(Learning Objectives)
-- MAGIC
-- MAGIC このノートブックで次を学びます：
-- MAGIC
-- MAGIC * Delta Live Tablesの宣言
-- MAGIC * Auto Loaderによるデータの取り込み
-- MAGIC * DLT Pipelinesにおけるパラメータの使用
-- MAGIC * 制約によるデータ品質の強制
-- MAGIC * テーブルへのコメントの追加
-- MAGIC * liveテーブルとstreaming live テーブルの、構文および実行の違い

-- COMMAND ----------

-- DBTITLE 0,--i18n-736da23b-5e31-4b7c-9f58-b56f3a133714
-- MAGIC %md
-- MAGIC ## DLT ライブラリノートブックについて(About DLT Library Notebooks)
-- MAGIC
-- MAGIC DLTの構文は、ノートブックでのインタラクティブな実行を意図したものではありません。
-- MAGIC このノートブックを適切に実行するためには、DLTパイプラインの一部としてスケジュールされる必要があります。
-- MAGIC
-- MAGIC DLT ノートブックセルを対話的に実行すると、文が構文的に有効であることを示すメッセージが表示されるはずです。このメッセージを返す前にいくつかの構文チェックが行われますが、クエリが期待通りに実行されることを保証するものではないことに注意してください。DLTコードの開発およびトラブルシューティングについては、このコースの後半で説明します。
-- MAGIC
-- MAGIC ## パラメタの設定(Parameterization)
-- MAGIC
-- MAGIC DLTパイプラインを設定した時に、いくつかのオプションを指定しました。
-- MAGIC そのうちの1つが、**Configurations**フィールドに追加したキーとバリューのペアです。
-- MAGIC
-- MAGIC DLTパイプラインにおける Configurationは、DatabricksジョブにおけるパラメータやDatabricksノートブックにおけるウィジェットに似ています。
-- MAGIC
-- MAGIC このレッスンでは、 **`${source}`** を使用して、設定したファイルパスをSQLクエリに文字列置換します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-e47d07ed-d184-4108-bc22-53fd34678a67
-- MAGIC %md
-- MAGIC ## Query結果としてのテーブル(Tables as Query Results)
-- MAGIC
-- MAGIC Delta Live Tables は、標準的なSQLクエリを適応し、DDL（データ定義言語 : Data Definition Language）と DML（データ操作言語 Data Manipulation Language）を統合した宣言的の構文です。
-- MAGIC
-- MAGIC DLTで作成できる永続的なテーブルには、2つの異なるタイプがあります：
-- MAGIC
-- MAGIC * **Live テーブル** は、レイクハウスのマテリアライズド・ビューです。更新(refresh)のたびにクエリの現在の結果を返します。
-- MAGIC
-- MAGIC * **Streaming Live テーブル**は、インクリメンタルな、ほぼリアルタイムのデータ処理用に設計されています。
-- MAGIC
-- MAGIC これらのオブジェクトはどちらもDelta Lakeプロトコル（ACIDトランザクション、バージョニング、その他多くの利点を提供）で保存されたテーブルとして永続化されていることに注意してください。
-- MAGIC ライブテーブルとストリーミングライブテーブルの違いについては、このノートの後半で詳しく説明します。
-- MAGIC
-- MAGIC どちらの種類のテーブルに対しても、DLTはCTAS（create table as select）ステートメントを少し修正したようなアプローチをとります。エンジニアは、データを変換するクエリを書くことだけを気にすればよく、あとはDLTが処理します。
-- MAGIC
-- MAGIC SQL DLTクエリの基本構文は以下の通りです：
-- MAGIC
-- MAGIC **`CREATE OR REFRESH [STREAMING] LIVE TABLE table_name`**<br/>
-- MAGIC **`AS select_statement`**<br/>

-- COMMAND ----------

-- DBTITLE 0,--i18n-32b81589-fb23-45c0-8317-977a7d4c722a
-- MAGIC %md
-- MAGIC ## Auto Loaderを使ったストリーミング形式のデータ取り込み (Streaming Ingestion with Auto Loader)
-- MAGIC
-- MAGIC Databricksは、クラウドオブジェクトストレージからDelta Lakeにデータをインクリメンタルにロードするための最適化された[Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)機能を開発しました。
-- MAGIC DLTでAuto Loaderを使用するのは簡単です。ソースデータディレクトリを構成し、いくつかの構成設定を行い、ソースデータに対してクエリを記述するだけです。Auto Loaderは、新しいデータファイルがソースのクラウドオブジェクトストレージに到着すると自動的に検出します。無限に増えるデータセットに対して高価なスキャンや結果の再計算を行う必要なく、新しいレコードをインクリメンタルに処理します。
-- MAGIC
-- MAGIC **`cloud_files()`** メソッドにより、Auto Loader をSQLでネイティブに使用することができます。
-- MAGIC このメソッドは、以下の位置パラメータを受け取ります：
-- MAGIC
-- MAGIC * ソースの場所（クラウドベースのオブジェクトストレージであるべき)
-- MAGIC * ソースデータの形式（この場合はJSON）。
-- MAGIC * 任意の量でカンマ区切りの、readerオプションのリスト。この場合、**`cloudFiles.inferColumnTypes`**を **`true`** に設定しています。
-- MAGIC
-- MAGIC 以下のクエリでは、ソースに含まれるフィールドに加えて、 **`Spark SQL`** 関数の **`current_timestamp()`** と  **`input_file_name()`** を使用して、レコードがいつ取り込まれたのか、各レコードのファイルソースについての情報を取得しています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-4be3b288-fd3b-4380-b32a-29fdb0d499ac
-- MAGIC %md
-- MAGIC ## 検証、エンリッチ、データ変換(Validating, Enriching, and Transforming Data)
-- MAGIC
-- MAGIC DLTは、Sparkによる標準的な変換の結果を使って、簡単にテーブルを宣言できるようにします。
-- MAGIC DLTは、Spark SQLで使用されている機能を活用し、データ品質チェックのための新しい機能を追加しています。
-- MAGIC
-- MAGIC 以下、クエリの構文を分解してみましょう。
-- MAGIC
-- MAGIC
-- MAGIC ### Select文(The Select Statement)
-- MAGIC
-- MAGIC select文には、クエリのコアロジックが含まれています。この例では、私たちは次を行います:
-- MAGIC * フィールド **`order_timestamp`** を timestamp 型にキャストする。
-- MAGIC * 残りのフィールドをすべて選択する（元の **`order_timestamp`**を含む、関心のない3つのフィールドのリストを除く）。
-- MAGIC
-- MAGIC **`FROM`** 句には、あまり馴染みのない2つの構文があることに注意してください：
-- MAGIC * **`LIVE`** キーワードは、スキーマ名の代わりに、現在のDLTパイプラインに設定されているターゲットスキーマを参照するために使用されます。
-- MAGIC * **`STREAM`** メソッドにより、SQLクエリ用のストリーミングデータソースを宣言できます
-- MAGIC
-- MAGIC パイプラインの設定時にターゲットスキーマが宣言されていない場合、テーブルは公開されません（つまり、メタストアに登録されず、他の場所でクエリに使用できるようになりません）のでご注意ください。ターゲットスキーマは、異なる実行環境間で移動する際に簡単に変更できます。つまり、スキーマ名をハードコーディングすることなく、同じコードを別の地域ワークロードに展開したり、開発環境から本番環境に昇格させたりすることができます。
-- MAGIC
-- MAGIC
-- MAGIC ### データ品質の制約(Data Quality Constraints)
-- MAGIC
-- MAGIC DLTでは、シンプルなbooleanを返すステートメントを使用して、データの品質チェックを可能にします。以下のステートメントで次を行います：
-- MAGIC * **`valid_date`**という名前の制約を宣言する。
-- MAGIC * フィールド **`order_timestamp`** に2021年1月1日以上の値が含まれている条件チェックを定義する。
-- MAGIC * 制約に違反するレコードがある場合、現在のトランザクションを失敗させるようDLTに指示する。
-- MAGIC
-- MAGIC 各制約は複数の条件を持つことができ、1つのテーブルに対して複数の制約を設定することができます。
-- MAGIC 制約違反は更新を失敗させるだけでなく、自動的にレコードを削除したり、これらの無効なレコードを処理しながら違反の数だけを記録することもできます。
-- MAGIC
-- MAGIC ### テーブルのコメント(Table Comments)
-- MAGIC
-- MAGIC テーブルのコメントはSQLの標準であり、組織全体のユーザーに有用な情報を提供するために使用することができます。
-- MAGIC この例では、データがどのように取り込まれ、制約付けられているか（これは他のメタデータを確認することでも得られる）を説明する、短くヒューマンリーダブルな説明を書いています。
-- MAGIC
-- MAGIC ### テーブルのプロパティ(Table Properties)
-- MAGIC
-- MAGIC **`TBLPROPERTIES`** フィールドは、データのカスタムタグ付けのために、任意の数の キー/バリュー ペアを渡すために使用できます。ここでは、キー **`quality`** に バリュー **`silver`**を設定しています。
-- MAGIC
-- MAGIC このフィールドでは、カスタムタグを任意に設定できますが、テーブルの使い方を制御する設定にも使用されることに注意してください。テーブルの詳細を確認すると、テーブルが作成される際にデフォルトでオンになっている設定を見ることがあります。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_silver
(CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
COMMENT "Append only orders with valid timestamps"
TBLPROPERTIES ("quality" = "silver")
AS SELECT timestamp(order_timestamp) AS order_timestamp, * EXCEPT (order_timestamp, source_file, _rescued_data)
FROM STREAM(LIVE.orders_bronze)

-- COMMAND ----------

-- DBTITLE 0,--i18n-c88a50db-2c24-4117-be3f-193da33e4a5b
-- MAGIC %md
-- MAGIC ## Live テーブル と Streaming Live テーブル (Live Tables vs. Streaming Live Tables)
-- MAGIC
-- MAGIC これまでの2つのクエリでは、どちらもStreaming Live Tableを作成しました。以下では、集約されたデータのLive Table（またはマテリアライズド・ビュー）を返す簡単なクエリを紹介します。
-- MAGIC
-- MAGIC Sparkは歴史的に、バッチクエリとストリーミングクエリを区別してきました。LiveテーブルとStreaming Liveテーブルも同様の違いがあります。
-- MAGIC
-- MAGIC Streaming LiveテーブルとLiveテーブルの構文の違いは、create句にSTREAMINGキーワードがないことのみです。STREAM()メソッドでソーステーブルをラッピングしないことに注意してください。
-- MAGIC
-- MAGIC 以下、これらのタイプのテーブルの違いを紹介します。
-- MAGIC
-- MAGIC ### Liveテーブル(Live Tables)
-- MAGIC
-- MAGIC * 常に「正しい」、つまり更新後もその内容は定義と一致する。
-- MAGIC * すべてのデータを対象にテーブルが定義されたときと同じ結果を返します。
-- MAGIC * DLT Pipelineの外部からの操作で変更してはならない（未定義の答えが返ってくるか、変更内容が元に戻される）。
-- MAGIC
-- MAGIC
-- MAGIC ### Streaming Liveテーブル(Streaming Live Tables)
-- MAGIC
-- MAGIC * "Append-only"のストリーミングソースからの読み込みのみをサポートします。
-- MAGIC * たとえ結合対象のテーブル次元が変わったり、クエリ定義が変わっても、ソースからの各入力バッチを1回だけ読み込みます。。
-- MAGIC * マネージドDLTパイプラインの外にあるテーブルを操作できる（データの追加、GDPRの実行など）。

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE orders_by_date
AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
FROM LIVE.orders_silver
GROUP BY date(order_timestamp)

-- COMMAND ----------

-- DBTITLE 0,--i18n-e15b4f61-b33a-4ac5-8b81-c6e7578ce28f
-- MAGIC %md
-- MAGIC ## サマリ(Summary)
-- MAGIC
-- MAGIC 次を学習しました：
-- MAGIC * デルタライブテーブルの宣言
-- MAGIC * Auto Loaderによるデータの取り込み
-- MAGIC * DLT Pipelinesにおけるパラメータの使用
-- MAGIC * 制約によるデータ品質の強制
-- MAGIC * テーブルへのコメントの追加
-- MAGIC * LiveテーブルとStreaming Liveテーブルの構文と実行の違い
-- MAGIC
-- MAGIC
-- MAGIC 次のノートでは、これらの構文にいくつかの新しい概念を加えていきます。

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
