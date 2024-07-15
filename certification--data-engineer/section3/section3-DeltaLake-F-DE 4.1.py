# Databricks notebook source
# MAGIC %md
# MAGIC ## ● DLTパイプラインとは何か
# MAGIC
# MAGIC ### パイプライン
# MAGIC
# MAGIC Delta Live Tables (DLT)におけるパイプラインは、データソースとターゲットデータセットを接続する有向非循環グラフ (DAG) である。パイプラインはSQLクエリ、Spark SQL、またはKoalasデータフレームを返すPython関数を用いて定義される。
# MAGIC
# MAGIC #### Databricks特有の用語の説明
# MAGIC
# MAGIC 1. **パイプライン**:
# MAGIC    - データ処理のフローを示す主要な構造。データの入力から出力までの全てのステップを包含する。
# MAGIC
# MAGIC 2. **有向非循環グラフ (DAG)**:
# MAGIC    - データフローを表現するためのグラフ構造。循環がなく、データ処理が一方向に進む。
# MAGIC
# MAGIC 3. **SQLクエリ**:
# MAGIC    - データベースからデータを取得、操作するための標準的な問い合わせ言語。
# MAGIC
# MAGIC 4. **Spark SQL**:
# MAGIC    - Apache Spark上で動作するSQLクエリエンジン。大規模データの分散処理に最適。
# MAGIC
# MAGIC 5. **Koalasデータフレーム**:
# MAGIC    - Pandasライクなインターフェースを持つSparkデータフレーム。データ操作を容易にする。
# MAGIC
# MAGIC 6. **Databricksノートブック**:
# MAGIC    - Databricksプラットフォーム上でコードを記述・実行するためのインターフェース。PythonやSQLで記述可能。
# MAGIC
# MAGIC 7. **Databricks Repos**:
# MAGIC    - ノートブックやコードをバージョン管理するためのGitベースのリポジトリシステム。
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 新しい DLT パイプラインを作成するために必要なコンポーネントは何か
# MAGIC
# MAGIC Delta Live Tables (DLT) パイプラインの作成には、以下のコンポーネントが必要である。
# MAGIC
# MAGIC - **データソース**: パイプラインが処理する元のデータ。これは、ストレージシステム内のファイルや、既存のデータベース、ストリームなど様々な形態を取り得る。
# MAGIC - **変換ロジック**: データソースからターゲットデータセットへの変換を定義するロジック。SQLクエリ、Spark SQL、またはPython関数を用いて定義される。
# MAGIC - **ターゲットデータセット**: 変換ロジックによって生成されるデータセット。これは、分析やレポート作成のために使用される。
# MAGIC - **監視と管理ツール**: パイプラインの実行状態を監視し、エラー発生時に通知を行うツール。Databricksでは、DLTの管理画面がこれに該当する。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Databricks上で新しいDLTパイプラインを作成するために必要な画面や機能
# MAGIC Delta Live Tables (DLT) パイプラインの作成と管理には、Databricksプラットフォーム上の複数の画面や機能が関与する。以下に主要なものを列挙する。
# MAGIC
# MAGIC - **ワークフロー**:
# MAGIC ワークフロー画面は、DLTパイプラインを含むデータ処理タスクのスケジュール設定、実行、監視を行うための中心的なインターフェースである。ユーザーはこの画面を通じてパイプラインの実行状態を確認し、実行履歴を閲覧することができる。
# MAGIC
# MAGIC - **SQLエディタ**:
# MAGIC SQLエディタは、DLTパイプライン内で使用されるSQLクエリの作成とテストに利用される。このエディタを使用して、データ変換のロジックを定義し、即時にクエリの結果を確認することが可能である。
# MAGIC
# MAGIC - **クラスタ**:
# MAGIC DLTパイプラインの実行には、計算リソースが必要であり、これはクラスタを通じて提供される。クラスタ画面では、パイプライン実行用のクラスタの作成、設定、管理を行う。パイプラインの実行に適したクラスタサイズや構成を選択することが重要である。
# MAGIC
# MAGIC - **Databricksノートブック**:
# MAGIC Databricksノートブックは、DLTパイプラインの変換ロジックを開発するための主要なツールである。Python、SQL、Scalaなど複数の言語をサポートしており、インタラクティブな開発環境を提供する。ノートブックを使用して、データ変換のロジックを試行し、テストすることができる。
# MAGIC
# MAGIC - **Databricks Repos**:
# MAGIC Databricks Reposは、ノートブックやDLTパイプラインのコードをバージョン管理するための機能である。Gitとの統合により、コードの変更履歴を追跡し、チーム間でのコラボレーションを容易にする。
# MAGIC
# MAGIC これらの画面や機能を適切に活用することで、DLTパイプラインの作成、テスト、デプロイ、監視を効率的に行うことができる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● パイプライン作成におけるターゲットとノートブックライブラリの目的は何か
# MAGIC
# MAGIC ### ターゲット
# MAGIC
# MAGIC パイプライン作成におけるターゲットは、変換ロジックによって生成される最終的なデータセットである。ターゲットは、分析やレポート作成、データ共有のために使用される。DLTでは、ターゲットデータセットはDeltaテーブルとして保存され、データの整合性や品質を保証するための機能が提供される。
# MAGIC
# MAGIC ### 目的
# MAGIC
# MAGIC パイプライン作成におけるノートブックライブラリの目的は、変換ロジックの開発とテストを容易にすることである。ノートブックライブラリを使用することで、データエンジニアはインタラクティブな環境で変換ロジックを記述し、即座にテストを行うことができる。また、ノートブックはDatabricks Reposを通じてバージョン管理され、チーム間でのコラボレーションを促進する。ノートブックライブラリは、DLTパイプラインの開発プロセスを加速し、データ処理の品質を向上させるための重要なツールである。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● コストとレイテンシーの観点から、トリガー式のパイプラインと継続的なパイプラインを比較対照する

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - **パイプラインモード** - パイプラインの実行方法を指定する。レイテンシとコストの要件に基づいてモードを選択する。
# MAGIC   - `トリガー (Triggered)` パイプラインは一度実行され、次の手動またはスケジュールされた更新までシャットダウンされる。
# MAGIC   - `連続 (Continuous)` パイプラインは継続的に実行され、新しいデータが到着すると取り込む。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● パイプライン作成手順
# MAGIC
# MAGIC Databricksで Delta Live Tables (DLT) パイプラインを作成する基本的な手順は以下の通り:
# MAGIC
# MAGIC 1. ソースコードの準備:
# MAGIC    DLTパイプライン用のSQLまたはPythonコードを含むノートブックを作成する[1][5]。このコードでは、データの取り込み、変換、出力テーブルの定義などを行う。
# MAGIC
# MAGIC 2. パイプラインの構成:
# MAGIC    ~~Databricksワークスペースのワークフローセクションから「パイプライン」を選択し、新しいパイプラインを作成する[3]。~~  
# MAGIC    サイドバーのワークフローボタンをクリックし、Delta Live Tablesタブを選択して パイプラインを作成をクリック
# MAGIC
# MAGIC 3. パイプライン設定:
# MAGIC
# MAGIC | 設定項目 | 設定方法 |
# MAGIC |--|--|
# MAGIC | パイプライン名 | 例: `isomura-tjie-da-dewd-pipeline-demo-pipeline_demo: Example Pipeline` |
# MAGIC | 製品エディション | **Advanced** を選択|
# MAGIC | パイプラインモード | **トリガー** を選択|
# MAGIC | クラスターポリシー | 例: `DBAcademy DLT` |
# MAGIC | ノートブックライブラリ | 例: `/Repos/isomura@msi.co.jp/databricks-learning/data-engineering-with-databricks/DE 4 - Delta Live Tables/DE 4.1A - SQL Pipelines/DE 4.1.1 - Orders Pipeline` |
# MAGIC | ストレージの場所 | 例: `dbfs:/mnt/dbacademy-users/isomura@msi.co.jp/data-engineering-with-databricks/pipeline_demo/storage_location` このオプションフィールドにより、ユーザーはログ、テーブル、およびパイプラインの実行に関連するその他の情報を保存する場所を指定できる。指定しない場合、DLTは自動的にディレクトリを生成する。 |
# MAGIC | ターゲットスキーマ | `isomura_tjie_da_dewd_pipeline_demo`。このオプションフィールドが指定されていない場合、テーブルはメタストアに登録されないが、DBFSで引き続き使用可能である。このオプションの詳細については、[ドキュメント](https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables)を参照すること。 |
# MAGIC | クラスターモード | **固定サイズ**を選択し、オートスケーリングを無効にする。これらのフィールドは、パイプラインを処理する基盤となるクラスターのワーカー構成を制御する。ワーカー数を0に設定すると、これは、上で定義した **spark.master** パラメータと連携して、クラスターを単一ノードクラスターとして構成する |
# MAGIC | ワーカー | **`0`** （0個）に設定してシングルノードクラスターを使用する |
# MAGIC | Photonアクセラレータを使用 | チェックを有効にする |
# MAGIC | 設定 | **Advanced**をクリックして他の設定を追加する。<br>**設定を追加(Add Configuration)**をクリックし、下記#1の**キー**と**値**を入力する。<br>再度 **設定を追加(Add Configuration)** をクリックし、下記#2の**キー**と**値**を入力する。 |
# MAGIC | チャンネル | **現在**を選択して現在のランタイムバージョンを使用する |
# MAGIC
# MAGIC
# MAGIC | 設定 | キー                 | 値                                      |
# MAGIC | ------------- | ------------------- | ------------------------------------------ |
# MAGIC | #1            | **`spark.master`**  | **`local[*]`**                             |
# MAGIC | #2            | **`source`** | 例: `dbfs:/mnt/dbacademy-users/isomura@msi.co.jp/data-engineering-with-databricks/pipeline_demo/stream-source`  |
# MAGIC
# MAGIC 4. パイプラインの実行:
# MAGIC    設定が完了したら、「作成」ボタンをクリックしてパイプラインを作成し、その後「開始」ボタンをクリックして実行を開始する[5]。
# MAGIC
# MAGIC 5. モニタリングと管理:
# MAGIC    パイプラインの実行状況や結果は、Databricksのユーザーインターフェースで確認できる。必要に応じて、スケジュールの設定や更新のトリガーを行うことができる[1][5]。
# MAGIC
# MAGIC DLTパイプラインは、SQLまたはPythonを使用して実装できる。SQLは多くの新しいキーワードや構造を追加して拡張されており、データセット間の依存関係を宣言し、本番運用グレードのインフラストラクチャをデプロイすることができる[1]。より複雑な操作や広範なテストが必要な場合は、Pythonインターフェースの使用が推奨される[1]。
# MAGIC
# MAGIC パイプラインの作成と実行は、DatabricksのUI、API、CLI、またはDatabricksワークフロー内のタスクとして行うことができる[1][5]。初めての場合は、UIを使用してパイプラインを作成・実行することが推奨されている[1]。
# MAGIC
# MAGIC Citations:
# MAGIC - [1] https://docs.databricks.com/ja/delta-live-tables/tutorial-pipelines.html
# MAGIC - [2] https://docs.databricks.com/ja/delta-live-tables/settings.html
# MAGIC - [3] https://qiita.com/taka_yayoi/items/6e41f96b29e32f879650
# MAGIC - [4] https://www.databricks.com/jp/product/delta-live-tables
# MAGIC - [5] https://learn.microsoft.com/ja-jp/azure/databricks/delta-live-tables/tutorial-pipelines

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● パイプラインを実際に作ってみる
# MAGIC
# MAGIC やることリスト
# MAGIC - データを準備する
# MAGIC - パイプライン上で実行するソースを作る
# MAGIC   - ブロンズ処理
# MAGIC   - シルバー処理
# MAGIC   - ゴールド処理
# MAGIC - パイプラインを定義・設定する
# MAGIC - 実行・デバッグする
# MAGIC - ゴールド処理によって作られたデータを参照して正常に期待通りの値が見れるか確認する

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ データを準備する
# MAGIC
# MAGIC - `certification--data-engineer/section3/data/dirty_data.csv` に準備した。
# MAGIC - これは、`certification--data-engineer/section3/data/create-dirty-data.py` を実行して作成した。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ ブロンズ処理を作る
# MAGIC
# MAGIC - `certification--data-engineer/section3/src/pipeline-sql-1.sql` に作った。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ シルバー処理を作る
# MAGIC
# MAGIC - 同じく`certification--data-engineer/section3/src/pipeline-sql-1.sql` に作った。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ ゴールド処理を作る
# MAGIC
# MAGIC - 同じく`certification--data-engineer/section3/src/pipeline-sql-1.sql` に作った。

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ パイプラインを定義・設定する

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ・ Databricks Academy のライブラリを使ってパイプラインを作成する
# MAGIC
# MAGIC Hiveメタストアでは GUI で作れないらしく（Databricks Assistant 曰く）、仕方なく、セミナーで利用したライブラリを利用する。  
# MAGIC ※ただし、パイプラインを自分で作ってみる目的達成のため（一部上記のとおり未達成だが）、データやソースは自前で作ったものを利用する。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.1

# COMMAND ----------

pipeline_language = "SQL"
#pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### ・ GUI操作
# MAGIC - 左側のサイドバーから「Workflows」を選択。
# MAGIC - 「Delta Live Tables」タブをクリック。
# MAGIC - 「パイプラインを作成」ボタンをクリック。
# MAGIC - パイプライン作成画面で次のように入力
# MAGIC   - 一般
# MAGIC     - パイプライン名
# MAGIC       - training
# MAGIC     - 製品エディション
# MAGIC       - （デフォルト）Advanced
# MAGIC     - パイプラインモード
# MAGIC       - トリガー
# MAGIC   - ソースコード
# MAGIC     - パス
# MAGIC       - `/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/src/pipeline-sql-1.sql`
# MAGIC         - フォーム横のボタンをクリックし、エクスプローラみたいな UI でパスを選択できる。
# MAGIC   - 配信先
# MAGIC     - ストレージオプション
# MAGIC       - （デフォルト）Hiveメタストア
# MAGIC     - ストレージの場所
# MAGIC       - `pipeline-sql-1.sql`の内容に従い、`/mnt/my-pypeline/training/bronze` を入力。
# MAGIC       - ※セミナー時の値: `dbfs:/mnt/dbacademy-users/isomura@msi.co.jp/data-engineering-with-databricks/pipeline_demo/storage_location`
# MAGIC     - ターゲットスキーマ
# MAGIC       - `DA.print_pipeline_config(pipeline_language)` の出力項目「Target」の値を入力する。
# MAGIC         - 例: `isomura_tjie_da_dewd_pipeline_demo`
# MAGIC       - ※セミナー時の値: `isomura_tjie_da_dewd_pipeline_demo`
# MAGIC     - クラスター
# MAGIC       - クラスターポリシー
# MAGIC         - なし
# MAGIC           - なしの場合は、管理者によってクラスター作成の権限が与えられている必要がある。
# MAGIC           - エラーや警告が出ている場合は管理者に聞く。
# MAGIC       - クラスターモード
# MAGIC         - 固定サイズ
# MAGIC       - ワーカー
# MAGIC         - 0
# MAGIC           - 計算ノードを増やさないという設定になる。
# MAGIC       - Photonアクセラレータを使用
# MAGIC         - チェックを入れる
# MAGIC           - セミナー時がそうだったから。
# MAGIC       - クラスタータグ
# MAGIC         - 追加しない
# MAGIC     - 通知
# MAGIC       - 設定しない
# MAGIC     - Advanced
# MAGIC       - 設定
# MAGIC         - 設定しない
# MAGIC       - チャンネル
# MAGIC         - 現在
# MAGIC           - セミナー時がそうだったから。
# MAGIC       - インスタンスタイプ
# MAGIC         - ワーカータイプもdriverタイプも設定しない
# MAGIC           - セミナー時がそうだったから。
# MAGIC - 下部「保存」ボタンをクリックする。
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 実行・デバッグする
# MAGIC
# MAGIC - 「開始」ボタンをクリックする。
# MAGIC - エラーに対処する。
# MAGIC   - 実際に発生したエラー
# MAGIC     - 1. テーブルをセットアップ中に `org.apache.spark.sql.AnalysisException: [PATH_NOT_FOUND] Path does not exist: file:/Workspace/dirty_data.csv.` エラーが発生した。
# MAGIC       - CSV パスを変数を利用して構成していたが、変数は使えないようであったので、固定値を記載した。※下記は修正後。
# MAGIC         ```sql
# MAGIC         CREATE LIVE TABLE bronze_table
# MAGIC         USING DELTA
# MAGIC         -- LOCATION `${spark.sql.deltaDirFullPath}`
# MAGIC         LOCATION "/mnt/my-pypeline/training/bronze"
# MAGIC         AS
# MAGIC         SELECT *
# MAGIC         -- FROM csv.`file:/Workspace/${spark.sql.dataDirFullPath}/dirty_data.csv`
# MAGIC         FROM csv.`file:/Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/dirty_data.csv`
# MAGIC         ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ ゴールド処理によって作られたデータを参照して正常に期待通りの値が見れるか確認する
# MAGIC
# MAGIC ---> 下記が証跡
# MAGIC
# MAGIC - certification--data-engineer/section3/data/my-first-pipeline.PNG
# MAGIC - certification--data-engineer/section3/data/my-first-pipeline-gold-table.PNG

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 振り返り
# MAGIC
# MAGIC Databricks Academy のライブラリの成果物として利用したのは、「Target」の値、つまりスキーマだけ。  
# MAGIC なのでスキーマだけ定義できれば DA ライブラリは無用。  
# MAGIC ※先に進めたい・面倒なのでやり直さないけど。
