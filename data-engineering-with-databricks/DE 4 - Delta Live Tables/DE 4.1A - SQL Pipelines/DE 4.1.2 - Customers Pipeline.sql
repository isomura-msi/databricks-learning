-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-ff0a44e1-46a3-4191-b583-5c657053b1af
-- MAGIC %md
-- MAGIC # より詳細なDLT SQL構文(More DLT SQL Syntax)
-- MAGIC
-- MAGIC DLT Pipelinesは、1つまたは複数のノートブックを使用して、複数のデータセットを単一のスケーラブルなワークロードに簡単に結合します。
-- MAGIC
-- MAGIC 前回のノートブックでは、クラウドオブジェクトストレージからデータを処理する際に、一連のクエリを通じて各ステップでレコードを検証し、エンリッチするDLTシンタックスの基本機能のいくつかを確認しました。このノートブックでは、同様にメダリオンアーキテクチャを踏襲しつつ、いくつかの新しい概念を導入します。
-- MAGIC
-- MAGIC * 生レコードは、customersに関するCDC（Change Data Capture）情報を表現します。
-- MAGIC * ブロンズテーブルでは、再びAuto Loaderを使用して、クラウドオブジェクトストレージからJSONデータを取り込みます。
-- MAGIC * シルバーレイヤにレコードを渡す前に、制約を課すためのテーブルを定義している
-- MAGIC * **`APPLY CHANGES INTO`** は、自動的にCDCデータを Type 1 の <a href="https://en.wikipedia.org/wiki/Slowly_changing_dimension" target="_blank">slowly changing dimension (SCD)<a/> テーブルとして使うシルバーレイヤーに登録するために使用されます。
-- MAGIC * このType1テーブルの最新バージョンから集計を計算するために、ゴールドテーブルを定義している。
-- MAGIC * 別のノートブックに定義されたテーブルと結合するビューを定義している。
-- MAGIC
-- MAGIC
-- MAGIC ## 学習の目的(Learning Objectives)
-- MAGIC
-- MAGIC 次を学習します : 
-- MAGIC * **`APPLY CHANGES INTO`** を使ったCDCデータの処理
-- MAGIC * live viewの宣言
-- MAGIC * live tableのジョイン
-- MAGIC * DLTライブラリノートブックを組み合わせてパイプラインを作る
-- MAGIC * DLTパイプラインを複数のノートブックで構成し、スケジューリングする

-- COMMAND ----------

-- DBTITLE 0,--i18n-959211b8-33e7-498b-8da9-771fdfb0978b
-- MAGIC %md
-- MAGIC ## AutoLoaderを使ったデータの取り込み(Ingest Data with Auto Loader)
-- MAGIC
-- MAGIC 前のノートブックで、AutoLoaderを使ってデータソースに対応するブロンズテーブルを定義しました。
-- MAGIC
-- MAGIC 以下のコードでは、スキーマを推論するAuto Loaderオプションを省略していることに注意してください。スキーマがない状態でJSONからデータを取り込むと、フィールドは正しい名前を持ちますが、すべて**`STRING`**型として保存されます。
-- MAGIC
-- MAGIC また、以下のコードでは、簡単なコメントと、データ取り込みの時刻と各レコードのファイル名のフィールドを追加しています。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_bronze
COMMENT "Raw data from customers CDC feed"
AS SELECT current_timestamp() processing_time, input_file_name() source_file, *
FROM cloud_files("${source}/customers", "json")

-- COMMAND ----------

-- DBTITLE 0,--i18n-43d96582-8a29-49d0-b57f-0038334f6b88
-- MAGIC %md
-- MAGIC ## 品質の強制の続き (Quality Enforcement Continued)
-- MAGIC
-- MAGIC 下記クエリでは次を実証します:
-- MAGIC * 制約が破られたときの3種類の動作オプション
-- MAGIC * 複数の制約を持つクエリ
-- MAGIC * 1つの制約条件に対して複数の条件を提供する
-- MAGIC * 制約の中で組み込みSQL関数を使用する
-- MAGIC
-- MAGIC データソースについて:
-- MAGIC * データは、**`INSERT`** 、 **`UPDATE`** および **`DELETE`** 操作を含むCDCフィードです。
-- MAGIC * 更新および挿入操作には、すべてのフィールドに有効な項目が含まれているべきです。
-- MAGIC * 削除操作では、タイムスタンプ、customer_id、operationフィールド以外のすべてのフィールドにNULL値を含めるべきです。
-- MAGIC
-- MAGIC 良いデータだけがシルバーテーブルに入るようにするために、削除操作で期待されるNULL値を無視する一連の品質強制ルールを書きます。
-- MAGIC
-- MAGIC 以下、それぞれの制約を分解して説明します：
-- MAGIC
-- MAGIC ##### **`valid_id`**
-- MAGIC
-- MAGIC この制約により、レコードの **`customer_id`** フィールドにNULL値が含まれている場合、トランザクションが失敗することになります。
-- MAGIC
-- MAGIC ##### **`valid_operation`**
-- MAGIC
-- MAGIC この制約により、**`operation`** フィールドにNULL値が含まれるレコードはすべてdropします。
-- MAGIC
-- MAGIC ##### **`valid_address`**
-- MAGIC
-- MAGIC この制約は、 **`operation`** フィールドが **`DELETE`** であるかどうかをチェックします。
-- MAGIC DELETEでない場合は、アドレスを構成する4つのフィールドのいずれかにNULL値があるかどうかをチェックします。
-- MAGIC 無効なレコードに対する操作(drop, fail)の指示がないため、違反した行はメトリクスに記録されるが、dropされることはありません。
-- MAGIC
-- MAGIC ##### **`valid_email`**
-- MAGIC
-- MAGIC この制約は、正規表現によるパターン・マッチを使用して、**`email`** フィールドの値が有効なemailアドレスであることをチェックします。 **`operation`** フィールドが **`DELETE`** であるレコードには、この制約を適用しないロジックが含まれています（これらのレコードは **`email`** フィールドにNULL値を持つことになるため）。違反したレコードはdropされます。

-- COMMAND ----------

CREATE STREAMING LIVE TABLE customers_bronze_clean
(CONSTRAINT valid_id EXPECT (customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
CONSTRAINT valid_operation EXPECT (operation IS NOT NULL) ON VIOLATION DROP ROW,
CONSTRAINT valid_name EXPECT (name IS NOT NULL or operation = "DELETE"),
CONSTRAINT valid_address EXPECT (
  (address IS NOT NULL and 
  city IS NOT NULL and 
  state IS NOT NULL and 
  zip_code IS NOT NULL) or
  operation = "DELETE"),
CONSTRAINT valid_email EXPECT (
  rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$') or 
  operation = "DELETE") ON VIOLATION DROP ROW)
AS SELECT *
  FROM STREAM(LIVE.customers_bronze)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5766064f-1619-4468-ac05-2176451e11c0
-- MAGIC %md
-- MAGIC ## **`APPLY CHANGES INTO`** を使ったCDCデータの処理 (Processing CDC Data with **`APPLY CHANGES INTO`**)
-- MAGIC
-- MAGIC DLTは、CDCフィード処理を簡略化する新しい構文を導入しています。
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** APPLY CHANGES INTOは、以下の保証と要件を備えています：
-- MAGIC * CDCデータのインクリメンタル/ストリーミング取り込みを行う。
-- MAGIC * テーブルの主キーとして1つまたは複数のフィールドを指定するための簡単な構文を提供します。
-- MAGIC * デフォルトでは、行には挿入と更新が含まれると仮定しています。
-- MAGIC * オプションで削除を適用できる
-- MAGIC * ユーザー指定のシーケンス キーを使って、遅れて到着したレコードを自動的にオーダーする
-- MAGIC * **`EXCEPT`** キーワードで無視する列を指定するシンプルな構文を使用する
-- MAGIC * Type 1 SCDとして変更を適用することがデフォルトとなります。
-- MAGIC
-- MAGIC 次のコードは以下をしています:
-- MAGIC *  **`customers_silver`** テーブルを事前に生成。**`APPLY CHANGES INTO`** では、ターゲットテーブルを別のステートメントで宣言する必要があります。
-- MAGIC * 変更を適用する対象として、 **`customers_silver`** テーブルを指定。
-- MAGIC * ストリーミングソースとして **`customers_bronze_clean`** テーブルを指定。
-- MAGIC * **`customer_id`** を主キーとして指定。
-- MAGIC *  **`operation`** フィールドが **`DELETE`** であるレコードを削除として適用することを指定する。
-- MAGIC * 操作の適用順序を決めるために **`timestamp`** フィールドを指定する。
-- MAGIC * **`operation`**, **`source_file`**, **`_rescued_data`** を除くすべてのフィールドがターゲットテーブルに追加されることを示す。

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE customers_silver;

APPLY CHANGES INTO LIVE.customers_silver
  FROM STREAM(LIVE.customers_bronze_clean)
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp
  COLUMNS * EXCEPT (operation, source_file, _rescued_data)

-- COMMAND ----------

-- DBTITLE 0,--i18n-5956effc-3fb5-4473-b2e7-297e5a3e1103
-- MAGIC %md
-- MAGIC ## Applied Changesを適用したテーブルへのクエリー(Querying Tables with Applied Changes)
-- MAGIC
-- MAGIC **`APPLY CHANGES INTO`** はデフォルトで、タイプ1のSCDテーブルを作成します。つまり、各ユニークキーは最大1レコードを持ち、更新は元の情報を上書きすることを意味します。
-- MAGIC
-- MAGIC 前のセルで操作のターゲットはストリーミング・ライブ・テーブルとして定義されていますが、このテーブルではデータの更新と削除が行われています（そのため、ストリーミング・ライブ・テーブル・ソースに対する追記のみの要件が破られます）。そのため、下流のオペレーションでは、このテーブルに対してストリーミング・クエリーを実行することはできません。
-- MAGIC
-- MAGIC このパターンにより、更新が順番通りに行われなかった場合でも、下流の結果が適切に再計算され、更新が反映されることが保証されます。また、ソーステーブルからレコードが削除された場合、その値がパイプラインの後のテーブルに反映されないことも保証されます。
-- MAGIC
-- MAGIC 以下では、 **`customers_silver`** テーブルのデータからライブテーブルを作成する簡単な集計クエリを定義します。

-- COMMAND ----------

CREATE LIVE TABLE customer_counts_state
  COMMENT "Total active customers per state"
AS SELECT state, count(*) as customer_count, current_timestamp() updated_at
  FROM LIVE.customers_silver
  GROUP BY state

-- COMMAND ----------

-- DBTITLE 0,--i18n-cd430cf6-927e-4a2e-a68d-ba87b9fdf3f6
-- MAGIC %md
-- MAGIC ## DLTビュー (DLT Views)
-- MAGIC
-- MAGIC 以下のクエリは、キーワード **`TABLE`** を **`VIEW`** に置き換えて、DLTビューを定義しています。
-- MAGIC
-- MAGIC DLTのビューは、永続化されたテーブルとは異なり、オプションで **`STREAMING`** として定義することができます。
-- MAGIC
-- MAGIC ビューはライブテーブルと同じ更新保証を持ちますが、クエリの結果はディスクに保存されません。
-- MAGIC
-- MAGIC Databricksの他の場所で使用されるビューとは異なり、DLTビューはメタストアに永続化されないため、その一部であるDLTパイプライン内からしか参照できないことになります。(これは、ほとんどのSQLシステムにおける一時的なビューと同様のスコープです）。
-- MAGIC
-- MAGIC ビューは、データ品質を強化するために使用することができ、ビューのメトリクスは、テーブルの場合と同様に収集および報告されます。
-- MAGIC
-- MAGIC ## ノートブックをまたがったジョインとテーブル参照 (Joins and Referencing Tables Across Notebook Libraries)
-- MAGIC
-- MAGIC これまでレビューしたコードでは、2つのソースデータセットが別々のノートブックで一連のステップを経て伝搬していく様子が描かれています。
-- MAGIC
-- MAGIC DLTは、1つのDLTパイプライン構成の一部として、複数のノートブックのスケジューリングをサポートしています。
-- MAGIC 既存のDLTパイプラインを編集して、ノートブックを追加することができます。
-- MAGIC
-- MAGIC DLT Pipeline内では、どのノートブックライブラリのコードでも、他のノートブックライブラリで作成したテーブルやビューを参照することができます。
-- MAGIC
-- MAGIC 本来、 **`LIVE`** キーワードで参照するスキーマの範囲は、個々のノートブックではなく、DLTパイプラインレベルであると考えることができるのです。
-- MAGIC
-- MAGIC 以下のクエリでは、 **`orders`** と **`customers`** のデータセットからシルバーテーブルを結合して、新しいビューを作成しています。このビューはストリーミングとして定義されていないことに注意してください。そのため、各顧客の現在有効な **`email`**  を常に取得し、 **`customers_silver`** テーブルから削除された顧客のレコードを自動的にdropします。

-- COMMAND ----------

CREATE LIVE VIEW subscribed_order_emails_v
  AS SELECT a.customer_id, a.order_id, b.email 
    FROM LIVE.orders_silver a
    INNER JOIN LIVE.customers_silver b
    ON a.customer_id = b.customer_id
    WHERE notifications = 'Y'

-- COMMAND ----------

-- DBTITLE 0,--i18n-d4a1dd79-e8e6-4d18-bf00-c5d49cd04b04
-- MAGIC %md
-- MAGIC ## DLTパイプラインへのノートブックの追加 (Adding this Notebook to a DLT Pipeline)
-- MAGIC
-- MAGIC 既存のパイプラインにノートブックライブラリを追加することは、DLT UIで簡単にできます。
-- MAGIC
-- MAGIC 1. 前のコースで設定したDLTパイプラインに移動します
-- MAGIC 1. 右上の「設定(Settings)」ボタンをクリックします
-- MAGIC 1. **Notebook Libraries**の下にある **Add notebook library** をクリックします。
-- MAGIC    * ファイルピッカーを使用してこのノートブックを選択し、**Select** をクリックします。
-- MAGIC 1. **Save** ボタンをクリックすると、更新内容が保存されます
-- MAGIC 1. 画面右上の青い **Start** ボタンをクリックして、パイプラインを更新し、新しいレコードを処理します。
-- MAGIC
-- MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png">
-- MAGIC このノートブックは、[DE 4.1 - DLT UI Walkthrough]($../DE 4.1 - DLT UI Walkthrough)の **Generate Pipeline Configuration** セクションにある Notebook #2 になります。

-- COMMAND ----------

-- DBTITLE 0,--i18n-5c0ccf2a-333a-4d7c-bfef-b4b25e56b3ca
-- MAGIC %md
-- MAGIC ## サマリ(Summary)
-- MAGIC
-- MAGIC 以下を学習しました:
-- MAGIC * **`APPLY CHANGES INTO`** でCDCデータを処理する。
-- MAGIC * Live Viewの宣言
-- MAGIC * Live Tableのジョイン
-- MAGIC * DLTライブラリノートブックがパイプラインでどのように連携するか
-- MAGIC * DLTパイプラインで複数のノートブックをスケジューリング
-- MAGIC
-- MAGIC 次のノートブックでは、パイプラインのアウトプットを探ります。そして、DLTコードの反復開発とトラブルシューティングの方法について見ていきます。

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
