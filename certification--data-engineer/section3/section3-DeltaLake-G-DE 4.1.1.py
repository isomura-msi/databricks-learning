# Databricks notebook source
# MAGIC %md
# MAGIC ## ● メダリオンアーキテクチャの概要
# MAGIC
# MAGIC メダリオンアーキテクチャは、データレイクハウス上でのデータ処理と分析を段階的に行うフレームワークである。データはブロンズ、シルバー、ゴールドの各テーブルを通じて変換およびエンリッチされる。このアーキテクチャにより、データの品質と信頼性が向上し、最終的にはビジネスインサイトのための高品質なデータセットが得られる。データの取り込みから加工、分析に至るまでのプロセスを体系的に管理するために設計されている。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ブロンズテーブル
# MAGIC ブロンズテーブルは、生データをそのまま取り込む層である。データはオブジェクトストレージから直接読み込まれ、元の形式で保存される。ここではデータの完全性が重要であり、データがどのように取り込まれたかを示すメタデータ（取り込み時のタイムスタンプやソース情報など）も保持される。
# MAGIC
# MAGIC ※実装サンプル参照: `certification--data-engineer/section3/src/pipeline-sql-1.sql`
# MAGIC
# MAGIC #### 具体例
# MAGIC - IoTデバイスからのセンサーデータ
# MAGIC - ソーシャルメディアからのストリームデータ
# MAGIC - トランザクションシステムからのログデータ
# MAGIC
# MAGIC #### 注意点
# MAGIC - データは加工されていないため、品質が不均一である可能性がある。
# MAGIC - 大量のデータが蓄積されるため、ストレージの管理が重要である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### シルバーテーブル
# MAGIC
# MAGIC シルバーテーブルは、ブロンズテーブルから取り込まれたデータをクレンジング、検証、加工するレイヤーである。このレイヤーでは、データの品質を向上させ、分析やレポーティングに適した形式に変換する。
# MAGIC
# MAGIC ※実装サンプル参照: `certification--data-engineer/section3/src/pipeline-sql-1.sql`
# MAGIC
# MAGIC #### 具体例
# MAGIC - 住所情報の正規化
# MAGIC - 重複レコードの削除
# MAGIC - 必要なフィールドの抽出と変換
# MAGIC
# MAGIC #### 注意点
# MAGIC - データのクレンジングと検証は、後続の分析の品質に直接影響するため、慎重に行う必要がある。
# MAGIC - 変換プロセスは、データの意味を変えないように注意する必要がある。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ゴールドテーブル
# MAGIC
# MAGIC ゴールドテーブルは、ビジネスインサイトの抽出やダッシュボードの作成に使用される、集計されたデータや分析結果を保存するレイヤーである。このレイヤーのデータは、最終的な意思決定支援に利用される。
# MAGIC
# MAGIC ※実装サンプル参照: `certification--data-engineer/section3/src/pipeline-sql-1.sql`
# MAGIC
# MAGIC #### 具体例
# MAGIC - 顧客セグメントごとの売上高
# MAGIC - 月次のパフォーマンス指標
# MAGIC - 製品別の利益率分析
# MAGIC
# MAGIC #### 注意点
# MAGIC - ゴールドテーブルのデータは、ビジネスの意思決定に直接影響を与えるため、データの正確性と信頼性が非常に重要である。
# MAGIC - データの集計や分析には、ビジネスの要件を正確に理解し、適切な指標を選択する必要がある。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● DLT ライブラリノートブックについて
# MAGIC
# MAGIC Delta Live Tables（DLT）の構文は、データ処理パイプラインの宣言的定義を可能にするために設計されている。ユーザーはデータの変換方法を宣言し、DLTはタスクのオーケストレーション、クラスタ管理、モニタリング、データ品質の保証、エラーハンドリングを管理する。このフレームワークは、信頼性が高く、保守が容易で、テスト可能なデータ処理パイプラインの構築を目的としている。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC DLTパイプラインは、Databricksノートブック内で定義されるが、ノートブックのセルをインタラクティブに実行することは想定されていない。DLTクエリを含むセルを実行すると、構文が有効であることを示すメッセージが表示される(※)が、これはクエリが実行環境で期待通りに機能することを意味しない。実際のデータ処理は、DLTパイプラインがDatabricksのジョブとしてスケジュールされ、実行されたときにのみ行われる。
# MAGIC
# MAGIC ※そのメッセージは `This Delta Live Tables query is syntactically valid, but you must create a pipeline in order to define and populate your table.` というもの。Delta Live Tables（DLT）クエリの構文が正しいが、テーブルを定義してデータを格納するためには、DLTパイプラインを作成する必要があることを意味している。
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC DLTパイプラインの開発プロセスでは、パイプラインの定義をノートブックに記述し、そのノートブックをパイプラインのソースコードとして使用する。パイプラインが実行されると、DLTはノートブック内のすべてのコードを評価し、定義されたデータ変換を実行する。このプロセスは、データの品質を維持しながら、効率的かつ効果的にデータを処理するためのものである。
# MAGIC
# MAGIC したがって、DLTパイプラインの開発とトラブルシューティングは、パイプラインがスケジュールされ、実行される環境で行う必要がある。このアプローチにより、データ処理のロジックが正確に実装され、期待される結果が得られることが保証される。
# MAGIC
# MAGIC
# MAGIC ## ● パラメタの設定(Parameterization)
# MAGIC
# MAGIC 結論から言うと、この機能を使うと実行時に動的にコードを変えられるということである。
# MAGIC
# MAGIC
# MAGIC DLTパイプラインを設定した時に、いくつかのオプションを指定した。そのうちの1つが、**Configurations**フィールドに追加したキーとバリューのペアである。
# MAGIC
# MAGIC DLTパイプラインにおけるConfigurationは、DatabricksジョブにおけるパラメータやDatabricksノートブックにおけるウィジェットに似ている。
# MAGIC
# MAGIC 下記サンプルでは、 **`${source}`** を使用して、設定したファイルパスをSQLクエリに文字列置換する。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze
# MAGIC AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
# MAGIC FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))
# MAGIC ```
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Query結果としてのテーブル
# MAGIC
# MAGIC Delta Live Tables（DLT）は、データ変換と管理を自動化するための宣言的なフレームワークである。このフレームワークは、データエンジニアがデータパイプラインを簡単に構築、管理、テストできるように設計されている。DLTは、データの取り込みから変換、集約、および最終的な分析用データセットの作成まで、データパイプラインの各ステップを自動的に管理する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Live テーブル
# MAGIC
# MAGIC Live テーブルは、データのバッチ処理に適しており、データの更新があるたびにテーブルの内容がリフレッシュされる。これは、データウェアハウス内で定期的に更新されるデータセットに適している。
# MAGIC
# MAGIC #### 具体例
# MAGIC - 毎日更新される売上データの集計
# MAGIC - 定期的に更新される在庫管理データ
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Live テーブル
# MAGIC
# MAGIC Streaming Live テーブルは、リアルタイムデータ処理に特化しており、データがストリームとして到着すると即座に処理される。これは、イベントデータやログデータのリアルタイム分析に適している。
# MAGIC
# MAGIC #### 具体例
# MAGIC - ソーシャルメディアからのリアルタイムイベントストリーム
# MAGIC - IoTデバイスからのセンサーデータストリーム
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Delta Lakeプロトコルの利点
# MAGIC
# MAGIC Delta Lakeプロトコルは、ACIDトランザクション、スキーマの進化、監査履歴、およびデータのバージョニングをサポートする。これにより、データの整合性が保たれ、時間を遡ってデータの状態を確認することが可能になる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### DLTのCTASステートメント
# MAGIC
# MAGIC DLTでは、CTAS（CREATE TABLE AS SELECT）ステートメントを使用して、データ変換のロジックを定義する。このアプローチにより、データエンジニアはデータの変換方法を宣言的に記述でき、DLTがその実行を管理する。
# MAGIC
# MAGIC SQL DLTクエリの基本構文：
# MAGIC ```sql
# MAGIC CREATE OR REFRESH [STREAMING] LIVE TABLE table_name
# MAGIC AS select_statement
# MAGIC ```
# MAGIC
# MAGIC 例:
# MAGIC ```sql
# MAGIC sql CREATE OR REFRESH LIVE TABLE sales_summary 
# MAGIC AS SELECT date, SUM(sales) AS total_sales FROM transactions GROUP BY date
# MAGIC ```
# MAGIC
# MAGIC この例では、`transactions`テーブルから日付ごとの売上合計を計算し、`sales_summary`という新しいLive テーブルを作成する。
# MAGIC
# MAGIC Python の基本構文:
# MAGIC ```python
# MAGIC @dlt.table
# MAGIC def <function-name>():
# MAGIC   return (<query>)
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Auto Loaderを使ったストリーミング形式のデータ取り込み
# MAGIC
# MAGIC Databricksは、クラウドオブジェクトストレージからDelta Lakeにデータをインクリメンタルにロードするための最適化されたAuto Loader機能を開発した。この機能を使用することで、DLTでのAuto Loaderの利用が容易になる。ソースデータディレクトリを構成し、いくつかの構成設定を行い、ソースデータに対してクエリを記述するだけで、Auto Loaderは新しいデータファイルがソースのクラウドオブジェクトストレージに到着すると自動的に検出する。これにより、データセットが無限に増えても、高価なスキャンや結果の再計算を行う必要なく、新しいレコードをインクリメンタルに処理することが可能となる。
# MAGIC
# MAGIC Auto LoaderをSQLでネイティブに使用するためには、`cloud_files()`メソッドを使用する。このメソッドは、以下の位置パラメータを受け取る：
# MAGIC
# MAGIC 1. ソースの場所（クラウドベースのオブジェクトストレージであるべき）
# MAGIC 2. ソースデータの形式（JSON, CSV）
# MAGIC 3. readerオプションのリスト（任意の量でカンマ区切り）。この場合、`cloudFiles.inferColumnTypes`を`true`に設定している。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 具体例
# MAGIC
# MAGIC ストリーミングデータの取り込みにAuto Loaderを使用する場合、以下のようなSQLクエリが使用される。
# MAGIC
# MAGIC ```sql
# MAGIC sql CREATE OR REFRESH STREAMING LIVE TABLE orders_bronze 
# MAGIC AS SELECT current_timestamp() AS processing_time, input_file_name() AS source_file, * 
# MAGIC FROM cloud_files("${source}/orders", "json", map("cloudFiles.inferColumnTypes", "true"))
# MAGIC ```
# MAGIC
# MAGIC このクエリは、指定されたソースディレクトリからJSON形式のファイルを読み込み、各レコードに処理時刻とソースファイル名を追加して、`orders_bronze`というストリーミングLIVEテーブルを作成する。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python版
# MAGIC

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

# MAGIC %md
# MAGIC ### 注意点
# MAGIC
# MAGIC - Auto Loaderを使用する際は、ソースデータの形式やパスを正確に指定する必要がある。
# MAGIC - `cloudFiles.inferColumnTypes`オプションを`true`に設定することで、Auto Loaderはファイル内のデータ型を自動的に推測し、適切なスキーマを適用する。
# MAGIC - ストリーミングデータの取り込みでは、データの到着に応じてテーブルが更新されるため、データのリアルタイム分析が可能になる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 使い所
# MAGIC
# MAGIC - リアルタイムでのデータモニタリングや分析が必要な場合。
# MAGIC - 大量のデータが継続的に生成され、それを効率的に取り込みたい場合。
# MAGIC - データの取り込みから分析までのレイテンシーを最小限に抑えたい場合。
# MAGIC
# MAGIC Auto Loaderを使用することで、データ取り込みプロセスを簡素化し、データパイプラインの効率を大幅に向上させることができる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 検証、エンリッチ、データ変換
# MAGIC
# MAGIC DLTは、Sparkによる標準的な変換の結果を使って、簡単にテーブルを宣言できるようにする。DLTは、Spark SQLで使用されている機能を活用し、データ品質チェックのための新しい機能を追加している。
# MAGIC
# MAGIC 以下、実例提示とクエリの構文を分解する。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE orders_silver
# MAGIC (CONSTRAINT valid_date EXPECT (order_timestamp > "2021-01-01") ON VIOLATION FAIL UPDATE)
# MAGIC COMMENT "Append only orders with valid timestamps"
# MAGIC TBLPROPERTIES ("quality" = "silver")
# MAGIC AS SELECT timestamp(order_timestamp) AS order_timestamp, * EXCEPT (order_timestamp, source_file, _rescued_data)
# MAGIC FROM STREAM(LIVE.orders_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python版

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

# MAGIC %md
# MAGIC ### Select文
# MAGIC
# MAGIC select文には、クエリのコアロジックが含まれている。この例では、次を行う：
# MAGIC * フィールド **`order_timestamp`** をtimestamp型にキャストする。
# MAGIC * 残りのフィールドをすべて選択する（元の **`order_timestamp`**を含む、関心のない3つのフィールドのリストを除く）。
# MAGIC
# MAGIC **`FROM`** 句には、あまり馴染みのない2つの構文があることに注意する：
# MAGIC * **`LIVE`** キーワードは、スキーマ名の代わりに、現在のDLTパイプラインに設定されているターゲットスキーマを参照するために使用される。
# MAGIC * **`STREAM`** メソッドにより、SQLクエリ用のストリーミングデータソースを宣言できる。
# MAGIC
# MAGIC パイプラインの設定時にターゲットスキーマが宣言されていない場合、テーブルは公開されない（つまり、メタストアに登録されず、他の場所でクエリに使用できるようにならない）ので注意が必要である。ターゲットスキーマは、異なる実行環境間で移動する際に簡単に変更できる。つまり、スキーマ名をハードコーディングすることなく、同じコードを別の地域ワークロードに展開したり、開発環境から本番環境に昇格させたりすることが可能である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### データ品質の制約
# MAGIC
# MAGIC DLTでは、シンプルなbooleanを返すステートメントを使用して、データの品質チェックを可能にする。以下のステートメントで次を行う：
# MAGIC * **`valid_date`** という名前の制約を宣言する。
# MAGIC * フィールド **`order_timestamp`** に2021年1月1日以上の値が含まれている条件チェックを定義する。
# MAGIC * 制約に違反するレコードがある場合、現在のトランザクションを失敗させるようDLTに指示する。
# MAGIC
# MAGIC 各制約は複数の条件を持つことができ、1つのテーブルに対して複数の制約を設定することが可能である。制約違反は更新を失敗させるだけでなく、自動的にレコードを削除したり、これらの無効なレコードを処理しながら違反の数だけを記録することもできる。
# MAGIC
# MAGIC 処理オプション
# MAGIC - **`FAIL UPDATE`** : 制約違反が発生した場合に、トランザクションを失敗させる。これにより、データの品質が保証される。
# MAGIC - **`DROP`** : 違反したレコードを自動的に削除する。
# MAGIC - **`LOG`** : 違反の数を記録するが、レコードは保持される。これにより、後で違反したレコードを分析することができる。

# COMMAND ----------

# MAGIC %md
# MAGIC ### テーブルのコメント
# MAGIC
# MAGIC テーブルのコメントはSQLの標準であり、組織全体のユーザーに有用な情報を提供するために使用できる。この例では、データがどのように取り込まれ、制約付けられているか（これは他のメタデータを確認することでも得られる）を説明する、短くヒューマンリーダブルな説明を書いている。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### テーブルのプロパティ
# MAGIC
# MAGIC **`TBLPROPERTIES`** フィールドは、データのカスタムタグ付けのために、任意の数のキー/バリュー ペアを渡すために使用できる。ここでは、キー **`quality`** にバリュー **`silver`** を設定している。
# MAGIC
# MAGIC このフィールドでは、カスタムタグを任意に設定できるが、テーブルの使い方を制御する設定にも使用されることに注意する。テーブルの詳細を確認すると、テーブルが作成される際にデフォルトでオンになっている設定を見ることができる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Live テーブル と Streaming Live テーブル
# MAGIC Delta Live Tables (DLT) は、データ変換と管理を自動化するための宣言的なフレームワークを提供する。このフレームワーク内で、Live テーブルと Streaming Live テーブルは、データの取り扱い方において重要な違いを持つ。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Liveテーブル
# MAGIC Live テーブルは、マテリアライズド・ビューとして機能し、定義されたクエリに基づいてデータを集約または変換し、その結果を保存する。これらのテーブルは、データのバッチ処理に適しており、データの一貫性と正確性を保証する。
# MAGIC
# MAGIC 特徴:
# MAGIC - 更新後もテーブルの内容は定義と一致し続ける。
# MAGIC   - Liveテーブルは、データパイプラインにおける変換処理の結果を反映する。たとえば、あるテーブルが特定のフィルタリングと集約を通じて生成される場合、その定義に基づいてデータが更新されるたびに、テーブルの内容は自動的に再計算され、最新の状態を保持する。これにより、テーブルの内容は常にその定義と一致し続ける。
# MAGIC
# MAGIC - テーブルが定義されたときと同じデータを対象に、同じ結果を返す。
# MAGIC   - Liveテーブルは、定義されたクエリに基づいてデータを処理する。例えば、あるテーブルが過去1年間の販売データを集約するために定義されている場合、このテーブルは常に過去1年間のデータを対象に集約を行い、同じデータセットに対しては同じ結果を返す。これにより、データの一貫性が保証される。
# MAGIC - DLT Pipelineの外部からの操作による変更は許されず、変更が試みられた場合は元に戻されるか、未定義の結果が返される。
# MAGIC   - DLT環境では、Liveテーブルへの直接的なデータ挿入や更新は許されていない。これは、データの一貫性と信頼性を保つためである。例えば、ユーザーが直接Liveテーブルにデータを挿入しようとした場合、その操作は拒否されるか、もしくは実行されたとしても次のデータ更新時に元の定義に基づいた状態に戻される。これにより、DLT Pipeline外部からの不正な操作によるデータの不整合を防ぐことができる

# COMMAND ----------

# MAGIC %md
# MAGIC ### Streaming Liveテーブル
# MAGIC Streaming Live テーブルは、リアルタイムデータ処理に特化しており、データがストリームとして到着すると即座に処理される。これは、イベントデータやログデータのリアルタイム分析に適している。
# MAGIC
# MAGIC 特徴:
# MAGIC - "Append-only"のストリーミングソースからの読み込みをサポートする。
# MAGIC   - Streaming Liveテーブルは、データが継続的に追加されるストリーミングソースからの読み込みをサポートしている。これは、ログデータやイベントデータなど、時間とともに新しいレコードが追加されるデータソースに特に有効である。
# MAGIC   - 具体例: ウェブサイトのクリックストリームデータや、IoTデバイスからのセンサーデータストリームが該当する。これらのデータソースは、新しいイベントが発生するたびにデータが追加されるため、"Append-only"の特性を持つ。
# MAGIC
# MAGIC - ソースからの各入力バッチを1回だけ読み込み、たとえ結合対象のテーブル次元が変わったり、クエリ定義が変わっても、結果は一貫している。
# MAGIC   - Streaming Liveテーブルは、ソースデータからの各入力バッチを一度だけ処理し、その結果を保存する。これにより、データの一貫性が保たれ、クエリの定義や結合対象のテーブルの構造が変更された場合でも、過去のデータに基づく結果は変わらない。
# MAGIC   - 具体例: リアルタイムで収集される販売データに基づいて、時間帯別の売上高を集計する場合、新しい販売データがストリームとして到着するたびに、そのバッチのデータだけが処理され、集計結果が更新される。
# MAGIC
# MAGIC - マネージドDLTパイプラインの外にあるテーブルを操作でき、データの追加やGDPRの実行などが可能である。
# MAGIC   - Streaming Liveテーブルは、DLTパイプラインの外部にあるテーブルに対しても操作を行うことができる。これにより、データの追加や、GDPRなどの規制に基づくデータの削除など、柔軟なデータ管理が可能になる。
# MAGIC   - 具体例: 顧客データを管理するテーブルに対して、顧客からの要請に基づいて特定のデータレコードを削除する操作が該当する。この操作は、DLTパイプラインの外部から実行されるが、Streaming Liveテーブルを通じて、リアルタイムでのデータ処理と管理が可能である。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ゴールドテーブル SQL 版

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REFRESH LIVE TABLE orders_by_date
# MAGIC AS SELECT date(order_timestamp) AS order_date, count(*) AS total_daily_orders
# MAGIC FROM LIVE.orders_silver
# MAGIC GROUP BY date(order_timestamp)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ゴールドテーブル Python 版

# COMMAND ----------

@dlt.table
def orders_by_date():
    return (
        dlt.read("orders_silver")
            .groupBy(F.col("order_timestamp").cast("date").alias("order_date"))
            .agg(F.count("*").alias("total_daily_orders"))
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 比較
# MAGIC Live テーブルと Streaming Live テーブルの主な違いは、データの取り扱い方にある。Live テーブルは、データの一貫性と正確性を重視し、定期的なバッチ処理に適している。一方、Streaming Live テーブルは、リアルタイムでのデータ処理と分析を可能にし、データの追加や変更に柔軟に対応する。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● AutoLoaderを使ったデータの取り込み
# MAGIC
# MAGIC スキーマを推論するAuto Loaderオプション（ `map("cloudFiles.inferColumnTypes", "true")` ）を省略していることに注意する。スキーマがない状態でJSONからデータを取り込むと、フィールドは正しい名前を持つが、すべて**STRING**型として保存される。
# MAGIC
# MAGIC 以下のコードでは、簡単なコメントと、データ取り込みの時刻と各レコードのファイル名のフィールドを追加している。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customers_bronze
# MAGIC COMMENT "Raw data from customers CDC feed"
# MAGIC AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
# MAGIC FROM cloud_files("${source}/customers", "json")
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python 版

# COMMAND ----------

@dlt.table(
    name = "customers_bronze",
    comment = "Raw data from customers CDC feed"
)
def ingest_customers_cdc():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(f"{source}/customers")
        .select(
            F.current_timestamp().alias("processing_time"),
            F.input_file_name().alias("source_file"),
            "*"
        )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC 上記では、DLTテーブル宣言の **name** オプションの使用法を示している。このオプションにより、開発者は、テーブルを定義するDataFrameを生成する関数定義とは別に、結果のテーブルの名前を指定することができる。
# MAGIC
# MAGIC このオプションを使用して、 **`<dataset-name>_<data-quality>`** というテーブルの命名規則と、その関数が何を行っているかを記述する関数の命名規則を満たしている。(このオプションを指定しなければ、テーブル名は関数から ingest_customers_cdc と推測されたであろう)。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● データ品質の制約2
# MAGIC
# MAGIC 下記クエリでは次を実証する:
# MAGIC * 制約が破られたときの3種類の動作オプション
# MAGIC * 複数の制約を持つクエリ
# MAGIC * 1つの制約条件に対して複数の条件を提供する
# MAGIC * 制約の中で組み込みSQL関数を使用する
# MAGIC
# MAGIC データソースについて:
# MAGIC * データは、 **`INSERT`** 、 **`UPDATE`** および **`DELETE`** 操作を含むCDCフィードである。
# MAGIC * 更新および挿入操作には、すべてのフィールドに有効な項目が含まれているべきである。
# MAGIC * 削除操作では、タイムスタンプ、customer_id、operationフィールド以外のすべてのフィールドにNULL値を含めるべきである。
# MAGIC
# MAGIC 良いデータだけがシルバーテーブルに入るようにするために、削除操作で期待されるNULL値を無視する一連の品質強制ルールを書く。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE STREAMING LIVE TABLE customers_bronze_clean
# MAGIC (CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
# MAGIC CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
# MAGIC CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"), -- on violation 未指定。件数をカウントするのみ。
# MAGIC CONSTRAINT valid_address EXPECT (
# MAGIC   (address IS NOT NULL and 
# MAGIC   city IS NOT NULL and 
# MAGIC   state IS NOT NULL and 
# MAGIC   zip_code IS NOT NULL) or
# MAGIC   operation = "DELETE"),
# MAGIC CONSTRAINT valid_email EXPECT (
# MAGIC   rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
# MAGIC   operation = "DELETE") ON VIOLATION DROP ROW)
# MAGIC AS SELECT *
# MAGIC   FROM STREAM(LIVE.customers_bronze)
# MAGIC ```
# MAGIC
# MAGIC 以下、それぞれの制約を分解して説明する：
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### valid_id
# MAGIC
# MAGIC `CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE`
# MAGIC
# MAGIC この制約により、レコードの **`customer_id`** フィールドにNULL値が含まれている場合、トランザクションが失敗することになる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### valid_operation
# MAGIC
# MAGIC `CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW`
# MAGIC
# MAGIC この制約により、 **`operation`** フィールドにNULL値が含まれるレコードはすべてdropする。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### valid_address
# MAGIC ```sql
# MAGIC CONSTRAINT valid_address EXPECT (
# MAGIC   (address IS NOT NULL and 
# MAGIC   city IS NOT NULL and 
# MAGIC   state IS NOT NULL and 
# MAGIC   zip_code IS NOT NULL) or
# MAGIC   operation = "DELETE")
# MAGIC ```
# MAGIC
# MAGIC この制約は、 **`operation`** フィールドが **`DELETE`** であるかどうかをチェックする。DELETEでない場合は、アドレスを構成する4つのフィールドのいずれかにNULL値があるかどうかをチェックする。無効なレコードに対する操作(`ON VIOLATION` + drop, fail)の指示がないため、違反した行はメトリクスに記録されるが、dropされることはない。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### valid_email
# MAGIC ```sql
# MAGIC CONSTRAINT valid_email EXPECT (
# MAGIC   rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
# MAGIC   operation = "DELETE") ON VIOLATION DROP ROW)
# MAGIC ```
# MAGIC
# MAGIC この制約は、正規表現によるパターン・マッチを使用して、 **`email`** フィールドの値が有効なemailアドレスであることをチェックする。 **`operation`** フィールドが **`DELETE`** であるレコードには、この制約を適用しないロジックが含まれている（これらのレコードは **`email`** フィールドにNULL値を持つことになるため）。違反したレコードはdropされる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python 版
# MAGIC

# COMMAND ----------

@dlt.table
@dlt.expect_or_fail("valid_id", "customer_id IS NOT NULL")
@dlt.expect_or_drop("valid_operation", "operation IS NOT NULL")
@dlt.expect("valid_name", "name IS NOT NULL or operation = 'DELETE'")
@dlt.expect("valid_adress", """
    (address IS NOT NULL and 
    city IS NOT NULL and 
    state IS NOT NULL and 
    zip_code IS NOT NULL) or
    operation = "DELETE"
    """)
@dlt.expect_or_drop("valid_email", """
    rlike(email, '^([a-zA-Z0-9_\\\\-\\\\.]+)@([a-zA-Z0-9_\\\\-\\\\.]+)\\\\.([a-zA-Z]{2,5})$') or 
    operation = "DELETE"
    """)
def customers_bronze_clean():
    return (
        dlt.read_stream("customers_bronze")
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● CDCデータとは
# MAGIC
# MAGIC CDCデータとは、Change Data Captureの略であり、データベースの変更（挿入、更新、削除）をリアルタイムでキャプチャし、それらの変更を別の場所に反映させるプロセスを指す。この技術は、データウェアハウス、データレイク、または他のデータベースへのデータの連続的な同期に使用される。CDCは、データの整合性を保ちながら、データシステム間でのデータの移動と同期を効率的に行うための重要な手法である。
# MAGIC
# MAGIC 例えば、オンライン販売システムのトランザクションデータベースに新しい注文が挿入された場合、CDCを使用してその注文情報をデータウェアハウスにリアルタイムで反映させることができる。これにより、データウェアハウスに保存されているデータは常に最新の状態を保ち、ビジネスインテリジェンスやレポーティングのための分析がリアルタイムで行えるようになる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● **`APPLY CHANGES INTO`** を使ったCDCデータの処理
# MAGIC
# MAGIC DLTは、CDCフィード処理を簡略化する新しい構文を導入している。
# MAGIC
# MAGIC **`APPLY CHANGES INTO`** は、以下の保証と要件を備えている：
# MAGIC
# MAGIC - CDCデータのインクリメンタル/ストリーミング取り込みを行う。
# MAGIC   
# MAGIC   CDCデータソースからの変更をリアルタイムで取り込み、Delta Lakeテーブルに反映する。例えば、Eコマースプラットフォームの注文データが更新された場合、これらの変更は即座に分析用のテーブルに適用される。
# MAGIC
# MAGIC - テーブルの主キーとして1つまたは複数のフィールドを指定するための簡単な構文を提供する。
# MAGIC   
# MAGIC   テーブルの一意性を保証するために、`KEYS`句を使用して主キーを指定する。例えば、顧客IDと注文IDの組み合わせを主キーとして使用することができる。
# MAGIC
# MAGIC - デフォルトでは、行には挿入と更新が含まれると仮定する。
# MAGIC   
# MAGIC   新しいレコードが到着すると、それが新しい行であるか、既存の行の更新であるかを自動的に判断し、適切に処理する。
# MAGIC
# MAGIC - オプションで削除を適用できる。
# MAGIC   
# MAGIC   `APPLY AS DELETE WHEN`句を使用して、特定の条件下でレコードを削除するロジックを定義する。例えば、特定のフラグが設定されたレコードを削除する。
# MAGIC
# MAGIC - ユーザー指定のシーケンスキーを使って、遅れて到着したレコードを自動的にオーダーする。
# MAGIC   
# MAGIC   `SEQUENCE BY`句を使用して、レコードの順序を指定し、遅れて到着したデータを正しい順序で処理する。
# MAGIC
# MAGIC - **`EXCEPT`** キーワードで無視する列を指定するシンプルな構文を使用する。
# MAGIC   
# MAGIC   変更の適用から特定の列を除外するために、`EXCEPT`キーワードを使用する。これにより、特定の列の変更を無視して、他の列のみを更新することができる。
# MAGIC
# MAGIC - Type 1 SCDとして変更を適用することがデフォルトとなる。
# MAGIC   
# MAGIC   変更が行われると、既存のレコードは新しい情報で更新され、履歴は保持されない。これは、最新の状態のみを追跡するシナリオに適している。
# MAGIC
# MAGIC
# MAGIC 次のコードは以下を実行する：
# MAGIC * **`customers_silver`** テーブルを事前に生成。 **`APPLY CHANGES INTO`** では、ターゲットテーブルを別のステートメントで宣言する必要がある。
# MAGIC * 変更を適用する対象として、 **`customers_silver`** テーブルを指定。
# MAGIC * ストリーミングソースとして **`customers_bronze_clean`** テーブルを指定。
# MAGIC * **`customer_id`** を主キーとして指定。
# MAGIC * **`operation`** フィールドが **`DELETE`** であるレコードを削除として適用することを指定する。
# MAGIC * 操作の適用順序を決めるために **`timestamp`** フィールドを指定する。
# MAGIC * **`operation`**, **`source_file`**, **`_rescued_data`** を除くすべてのフィールドがターゲットテーブルに追加されることを示す。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REFRESH STREAMING LIVE TABLE customers_silver;
# MAGIC
# MAGIC APPLY CHANGES INTO LIVE.customers_silver
# MAGIC   FROM STREAM(LIVE.customers_bronze_clean)
# MAGIC   KEYS (customer_id)
# MAGIC   APPLY AS DELETE WHEN operation = "DELETE"
# MAGIC   SEQUENCE BY timestamp -- operation の適用順序を決めるカラムを指定する
# MAGIC   COLUMNS * EXCEPT (operation, source_file, _rescued_data)
# MAGIC ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python 版

# COMMAND ----------

dlt.create_target_table(
    name = "customers_silver")

dlt.apply_changes(
    target = "customers_silver",
    source = "customers_bronze_clean",
    keys = ["customer_id"],
    sequence_by = F.col("timestamp"),
    apply_as_deletes = F.expr("operation = 'DELETE'"),
    except_column_list = ["operation", "source_file", "_rescued_data"])

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## ● Applied Changesを適用したテーブルへのクエリ
# MAGIC
# MAGIC **`APPLY CHANGES INTO`** はデフォルトで、タイプ1のSCDテーブルを作成する。つまり、各ユニークキーは最大1レコードを持ち、更新は元の情報を上書きすることを意味する。
# MAGIC
# MAGIC 前のセルで操作のターゲットはストリーミング・ライブ・テーブルとして定義されている。このテーブルではデータの更新と削除が行われているため、ストリーミング・ライブ・テーブル・ソースに対する追記のみの要件が破られる。そのため、下流のオペレーションでは、このテーブルに対してストリーミング・クエリを実行することはできない。
# MAGIC
# MAGIC このパターンにより、更新が順番通りに行われなかった場合でも、下流の結果が適切に再計算され、更新が反映されることが保証される。また、ソーステーブルからレコードが削除された場合、その値がパイプラインの後のテーブルに反映されないことも保証される。
# MAGIC
# MAGIC 以下では、 **`customers_silver`** テーブルのデータからライブテーブルを作成する簡単な集計クエリを定義する。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE LIVE TABLE customer_counts_state
# MAGIC   COMMENT "Total active customers per state"
# MAGIC AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
# MAGIC   FROM LIVE.customers_silver
# MAGIC   GROUP BY state
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Python 版

# COMMAND ----------

@dlt.table(
    comment="Total active customers per state")
def customer_counts_state():
    return (
        dlt.read("customers_silver")
            .groupBy("state")
            .agg( 
                F.count("*").alias("customer_count"), 
                F.first(F.current_timestamp()).alias("updated_at")
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## ● DLTビュー
# MAGIC
# MAGIC 以下のクエリは、キーワード **`TABLE`** を **`VIEW`** に置き換えて、DLTビューを定義している。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE LIVE VIEW subscribed_order_emails_v
# MAGIC   AS SELECT a.customer_id, a.order_id, b.email 
# MAGIC     FROM LIVE.orders_silver a
# MAGIC     INNER JOIN LIVE.customers_silver b
# MAGIC     ON a.customer_id = b.customer_id
# MAGIC     WHERE notifications = 'Y'
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC DLTのビューは、永続化されたテーブルとは異なり、オプションで **`STREAMING`** として定義することができる。
# MAGIC
# MAGIC ビューはライブテーブルと同じ更新保証を持つが、クエリの結果はディスクに保存されない。
# MAGIC
# MAGIC Databricksの他の場所で使用されるビューとは異なり、DLTビューはメタストアに永続化されないため、その一部であるDLTパイプライン内からしか参照できないことになる。これは、ほとんどのSQLシステムにおける一時的なビューと同様のスコープである。
# MAGIC
# MAGIC ビューは、データ品質を強化するために使用することができ、ビューのメトリクスは、テーブルの場合と同様に収集および報告される。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python 版

# COMMAND ----------

@dlt.view
def subscribed_order_emails_v():
    return (
        dlt.read("orders_silver").filter("notifications = 'Y'").alias("a")
            .join(
                dlt.read("customers_silver").alias("b"), 
                on="customer_id"
            ).select(
                "a.customer_id", 
                "a.order_id", 
                "b.email"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## ● ノートブックをまたがったジョインとテーブル参照 (Joins and Referencing Tables Across Notebook Libraries)
# MAGIC
# MAGIC これまでレビューしたコードでは、2つのソースデータセットが別々のノートブックで一連のステップを経て伝搬していく様子が描かれている。
# MAGIC
# MAGIC DLTは、1つのDLTパイプライン構成の一部として、複数のノートブックのスケジューリングをサポートしている。既存のDLTパイプラインを編集して、ノートブックを追加することができる。
# MAGIC
# MAGIC DLTパイプライン内では、どのノートブックライブラリのコードでも、他のノートブックライブラリで作成したテーブルやビューを参照することができる。
# MAGIC
# MAGIC 本来、 **`LIVE`** キーワードで参照するスキーマの範囲は、個々のノートブックではなく、DLTパイプラインレベルであると考えることができる。
# MAGIC
# MAGIC 以下のクエリでは、 **`orders`** と **`customers`** のデータセットからシルバーテーブルを結合して、新しいビューを作成している。このビューはストリーミングとして定義されていないことに注意すること。このため、各顧客の現在有効な **`email`** を常に取得し、 **`customers_silver`** テーブルから削除された顧客のレコードを自動的にdropする。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE LIVE VIEW subscribed_order_emails_v
# MAGIC   AS SELECT a.customer_id, a.order_id, b.email 
# MAGIC     FROM LIVE.orders_silver a
# MAGIC     INNER JOIN LIVE.customers_silver b
# MAGIC     ON a.customer_id = b.customer_id
# MAGIC     WHERE notifications = 'Y'
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python 版

# COMMAND ----------

@dlt.view
def subscribed_order_emails_v():
    return (
        dlt.read("orders_silver").filter("notifications = 'Y'").alias("a")
            .join(
                dlt.read("customers_silver").alias("b"), 
                on="customer_id"
            ).select(
                "a.customer_id", 
                "a.order_id", 
                "b.email"
            )
    )

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## ● DLTパイプラインへのノートブックの追加
# MAGIC
# MAGIC 既存のパイプラインにノートブックライブラリを追加することは、DLT UIで簡単にできる。
# MAGIC
# MAGIC 1. 前のコースで設定したDLTパイプラインに移動する。
# MAGIC 2. 右上の「設定 (Settings)」ボタンをクリックする。
# MAGIC 3. **Notebook Libraries** の下にある **Add notebook library** をクリックする。
# MAGIC    * ファイルピッカーを使用してこのノートブックを選択し、 **Select** をクリックする。
# MAGIC 4. **Save** ボタンをクリックすると、更新内容が保存される。
# MAGIC 5. 画面右上の青い **Start** ボタンをクリックして、パイプラインを更新し、新しいレコードを処理する。
# MAGIC
