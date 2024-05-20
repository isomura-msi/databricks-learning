# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-133f57e8-6ff4-4b56-be71-9598e1513fd3
# MAGIC %md
# MAGIC # より詳細なDLT Python構文(More DLT Python Syntax)
# MAGIC
# MAGIC DLT Pipelinesは、1つまたは複数のノートブックを使用して、複数のデータセットを単一のスケーラブルなワークロードに簡単に結合します。
# MAGIC
# MAGIC 前回のノートブックでは、クラウドオブジェクトストレージからデータを処理する際に、一連のクエリを通じて各ステップでレコードを検証し、エンリッチするDLTシンタックスの基本機能のいくつかを確認しました。このノートブックでは、同様にメダリオンアーキテクチャを踏襲しつつ、いくつかの新しい概念を導入します。
# MAGIC
# MAGIC * 生レコードは、customersに関するCDC（Change Data Capture）情報を表現します。
# MAGIC * ブロンズテーブルでは、再びAuto Loaderを使用して、クラウドオブジェクトストレージからJSONデータを取り込みます。
# MAGIC * シルバーレイヤにレコードを渡す前に、制約を課すためのテーブルを定義している
# MAGIC * **`dlt.apply_changes()`** は、自動的にCDCデータを Type 1 の <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">slowly changing dimension (SCD) テーブル]</a>として使うシルバーレイヤーに登録するために使用されます。
# MAGIC * このType1テーブルの最新バージョンから集計を計算するために、ゴールドテーブルを定義している。
# MAGIC * 別のノートブックに定義されたテーブルと結合するビューを定義している。
# MAGIC
# MAGIC ## 学習の目的(Learning Objectives)
# MAGIC
# MAGIC 次を学習します : 
# MAGIC * **`dlt.apply_changes()`** を使ったCDCデータの処理
# MAGIC * live viewの宣言
# MAGIC * live tableのジョイン
# MAGIC * DLTライブラリノートブックを組み合わせてパイプラインを作る
# MAGIC * DLTパイプラインを複数のノートブックで構成し、スケジューリングする

# COMMAND ----------

import dlt
import pyspark.sql.functions as F

source = spark.conf.get("source")

# COMMAND ----------

# DBTITLE 0,--i18n-ec15ca9a-719d-41b3-9a45-93f53bc9fb73
# MAGIC %md
# MAGIC ## AutoLoaderを使ったデータの取り込み(Ingest Data with Auto Loader)
# MAGIC
# MAGIC 前のノートブックで、AutoLoaderを使ってデータソースに対応するブロンズテーブルを定義しました。
# MAGIC
# MAGIC 以下のコードでは、スキーマを推論するAuto Loaderオプションを省略していることに注意してください。スキーマがない状態でJSONからデータを取り込むと、フィールドは正しい名前を持ちますが、すべて **`STRING`** 型として保存されます。
# MAGIC
# MAGIC ## テーブル名の指定(Specifying Table Names)
# MAGIC
# MAGIC 以下のコードは、DLTテーブル宣言の**`name`**オプションの使用法を示しています。このオプションにより、開発者は、テーブルを定義するDataFrameを生成する関数定義とは別に、結果のテーブルの名前を指定することができます。
# MAGIC
# MAGIC 以下の例では、このオプションを使用して、**`<dataset-name>_<data-quality>`** というテーブルの命名規則と、その関数が何を行っているかを記述する関数の命名規則を満たしています。(このオプションを指定しなければ、テーブル名は関数から **`ingest_customers_cdc`** と推測されたでしょう)。

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

# DBTITLE 0,--i18n-ada9c9fb-cb58-46d8-9702-9ad16c95d821
# MAGIC %md
# MAGIC ## 品質の強制の続き (Quality Enforcement Continued)
# MAGIC
# MAGIC 下記クエリでは次を実証します:
# MAGIC * 制約が破られたときの3種類の動作オプション
# MAGIC * 複数の制約を持つクエリ
# MAGIC * 1つの制約条件に対して複数の条件を提供する
# MAGIC * 制約の中で組み込みSQL関数を使用する
# MAGIC
# MAGIC データソースについて:
# MAGIC * データは、**`INSERT`** 、 **`UPDATE`** および **`DELETE`** 操作を含むCDCフィードです。
# MAGIC * 更新および挿入操作には、すべてのフィールドに有効な項目が含まれているべきです。
# MAGIC * 削除操作では、タイムスタンプ、**`customer_id`**、operationフィールド以外のすべてのフィールドに **`NULL`** 値を含めるべきです。
# MAGIC
# MAGIC 良いデータだけがシルバーテーブルに入るようにするために、削除操作で期待されるNULL値を無視する一連の品質強制ルールを書きます。
# MAGIC
# MAGIC 以下、それぞれの制約を分解して説明します：
# MAGIC
# MAGIC ##### **`valid_id`**
# MAGIC
# MAGIC この制約により、レコードの **`customer_id`** フィールドにNULL値が含まれている場合、トランザクションが失敗することになります。
# MAGIC
# MAGIC ##### **`valid_operation`**
# MAGIC
# MAGIC この制約により、**`operation`** フィールドにNULL値が含まれるレコードはすべてdropします。
# MAGIC
# MAGIC ##### **`valid_address`**
# MAGIC
# MAGIC この制約は、 **`operation`** フィールドが **`DELETE`** であるかどうかをチェックします。
# MAGIC DELETEでない場合は、アドレスを構成する4つのフィールドのいずれかにNULL値があるかどうかをチェックします。
# MAGIC 無効なレコードに対する操作(drop, fail)の指示がないため、違反した行はメトリクスに記録されるが、dropされることはありません。
# MAGIC
# MAGIC ##### **`valid_email`**
# MAGIC
# MAGIC この制約は、正規表現によるパターン・マッチを使用して、**`email`** フィールドの値が有効なemailアドレスであることをチェックします。 **`operation`** フィールドが **`DELETE`** であるレコードには、この制約を適用しないロジックが含まれています（これらのレコードは **`email`** フィールドにNULL値を持つことになるため）。違反したレコードはdropされます。

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

# DBTITLE 0,--i18n-e3093247-1329-47b7-b06d-51284be37799
# MAGIC %md
# MAGIC ## **`dlt.apply_changes()`** を使ったCDCデータの処理 (Processing CDC Data with **`dlt.apply_changes()`**)
# MAGIC
# MAGIC DLTは、CDCフィード処理を簡略化する新しい構文を導入しています。
# MAGIC
# MAGIC **`dlt.apply_changes()`** APPLY CHANGES INTOは、以下の保証と要件を備えています：
# MAGIC * CDCデータのインクリメンタル/ストリーミング取り込みを行う。
# MAGIC * テーブルの主キーとして1つまたは複数のフィールドを指定するための簡単な構文を提供します。
# MAGIC * デフォルトでは、行には挿入と更新が含まれると仮定しています。
# MAGIC * オプションで削除を適用できる
# MAGIC * ユーザー指定のシーケンス フィールドを使って、遅れて到着したレコードを自動的にオーダーします
# MAGIC * **`except_column_list`** で無視する列を指定するシンプルな構文を使用します
# MAGIC * Type 1 SCDとして変更を適用することがデフォルトとなります。
# MAGIC
# MAGIC
# MAGIC 次のコードは以下をしています:
# MAGIC *  **`customers_silver`** テーブルを事前に生成。**`dlt.apply_changes()`** では、ターゲットテーブルを別のステートメントで宣言する必要があります。
# MAGIC * 変更を適用する対象として、 **`customers_silver`** テーブルを指定。
# MAGIC * ストリーミングソースとして **`customers_bronze_clean`** テーブルを指定。
# MAGIC * **`customer_id`** を主キーとして指定。
# MAGIC *  **`operation`** フィールドが **`DELETE`** であるレコードを削除として適用することを指定する。
# MAGIC * 操作の適用順序を決めるために **`timestamp`** フィールドを指定する。
# MAGIC * **`operation`**, **`source_file`**, **`_rescued_data`** を除くすべてのフィールドがターゲットテーブルに追加されることを示す。

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

# DBTITLE 0,--i18n-43fe6e78-d4c2-46c9-9116-232b1c9bbcd9
# MAGIC %md
# MAGIC ## Applied Changesを適用したテーブルへのクエリー(Querying Tables with Applied Changes)
# MAGIC
# MAGIC **`dlt.apply_changes()`** はデフォルトで、タイプ1のSCDテーブルを作成します。つまり、各ユニークキーは最大1レコードを持ち、更新は元の情報を上書きすることを意味します。
# MAGIC
# MAGIC 前のセルで操作のターゲットはストリーミング・ライブ・テーブルとして定義されていますが、このテーブルではデータの更新と削除が行われています（そのため、ストリーミング・ライブ・テーブル・ソースに対する追記のみの要件が破られます）。そのため、下流のオペレーションでは、このテーブルに対してストリーミング・クエリーを実行することはできません。
# MAGIC
# MAGIC このパターンにより、更新が順番通りに行われなかった場合でも、下流の結果が適切に再計算され、更新が反映されることが保証されます。また、ソーステーブルからレコードが削除された場合、その値がパイプラインの後のテーブルに反映されないことも保証されます。
# MAGIC
# MAGIC 以下では、 **`customers_silver`** テーブルのデータからライブテーブルを作成する簡単な集計クエリを定義します。

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

# DBTITLE 0,--i18n-18af0840-c1e0-48b5-9f79-df03ee591aad
# MAGIC %md
# MAGIC ## DLTビュー (DLT Views)
# MAGIC
# MAGIC 以下のクエリは、**`@dlt.view`** デコレータを使うことで、DLTビューを定義しています。
# MAGIC
# MAGIC
# MAGIC DLTのビューは、永続化されたテーブルとは異なり、デコレータによってストリーミング実行することを継承できます。
# MAGIC
# MAGIC ビューはライブテーブルと同じ更新保証を持ちますが、クエリの結果はディスクに保存されません。
# MAGIC
# MAGIC Databricksの他の場所で使用されるビューとは異なり、DLTビューはメタストアに永続化されないため、その一部であるDLTパイプライン内からしか参照できないことになります。(これは、ほとんどのSQLシステムにおける一時的なビューと同様のスコープです）。
# MAGIC
# MAGIC ビューは、データ品質を強化するために使用することができ、ビューのメトリクスは、テーブルの場合と同様に収集および報告されます。
# MAGIC
# MAGIC
# MAGIC ## ノートブックをまたがったジョインとテーブル参照 (Joins and Referencing Tables Across Notebook Libraries)
# MAGIC
# MAGIC これまでレビューしたコードでは、2つのソースデータセットが別々のノートブックで一連のステップを経て伝搬していく様子が描かれています。
# MAGIC
# MAGIC DLTは、1つのDLTパイプライン構成の一部として、複数のノートブックのスケジューリングをサポートしています。
# MAGIC 既存のDLTパイプラインを編集して、ノートブックを追加することができます。
# MAGIC
# MAGIC DLT Pipeline内では、どのノートブックライブラリのコードでも、他のノートブックライブラリで作成したテーブルやビューを参照することができます。
# MAGIC
# MAGIC 本来、 **`LIVE`** キーワードで参照するスキーマの範囲は、個々のノートブックではなく、DLTパイプラインレベルであると考えることができるのです。
# MAGIC
# MAGIC 以下のクエリでは、 **`orders`** と **`customers`** のデータセットからシルバーテーブルを結合して、新しいビューを作成しています。このビューはストリーミングとして定義されていないことに注意してください。そのため、各顧客の現在有効な **`email`**  を常に取得し、 **`customers_silver`** テーブルから削除された顧客のレコードを自動的にdropします。

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

# DBTITLE 0,--i18n-3351377c-3b34-477a-8c23-150895c2ef50
# MAGIC %md
# MAGIC ## DLTパイプラインへのノートブックの追加 (Adding this Notebook to a DLT Pipeline)
# MAGIC
# MAGIC 既存のパイプラインにノートブックライブラリを追加することは、DLT UIで簡単にできます。
# MAGIC
# MAGIC 1. 前のコースで設定したDLTパイプラインに移動します
# MAGIC 1. 右上の「**設定(Settings)**」ボタンをクリックします
# MAGIC 1. **Notebook Libraries**の下にある **Add notebook library** をクリックします。
# MAGIC    * ファイルピッカーを使用してこのノートブックを選択し、**Select** をクリックします。
# MAGIC 1. **Save** ボタンをクリックすると、更新内容が保存されます
# MAGIC 1. 画面右上の青い **Start** ボタンをクリックして、パイプラインを更新し、新しいレコードを処理します。
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png">
# MAGIC このノートブックは、[DE 4.1 - DLT UI Walkthrough]($../DE 4.1 - DLT UI Walkthrough)の **Generate Pipeline Configuration** セクションにある Notebook #2 になります。

# COMMAND ----------

# DBTITLE 0,--i18n-6b3c538d-c036-40e7-8739-cb3c305c09c1
# MAGIC %md
# MAGIC ## サマリ(Summary)
# MAGIC
# MAGIC 以下を学習しました:
# MAGIC * **`APPLY CHANGES INTO`** でCDCデータを処理する。
# MAGIC * Live Viewの宣言
# MAGIC * Live Tableのジョイン
# MAGIC * DLTライブラリノートブックがパイプラインでどのように連携するか
# MAGIC * DLTパイプラインで複数のノートブックをスケジューリング
# MAGIC
# MAGIC 次のノートブックでは、パイプラインのアウトプットを探ります。そして、DLTコードの反復開発とトラブルシューティングの方法について見ていきます。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
