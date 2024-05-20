# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-7b85ffd8-b52c-4ea8-84db-b3668e13b402
# MAGIC %md
# MAGIC # ラボ：Databricks使用したジョブのオーケストレーション（Lab: Orchestrating Jobs with Databricks）
# MAGIC
# MAGIC このラボでは次のものからなるマルチタスクジョブを構成します：
# MAGIC * ストレージディレクトリに新しいデータバッチを配置するノートブック
# MAGIC * 複数のテーブルを通してこのデータを処理するDelta Live Tablesのパイプライン
# MAGIC * このパイプラインによって作成されたゴールドテーブルおよびDLTによるさまざまメトリックの出力を照会するノートブック
# MAGIC
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このラボでは、以下のことが学べます。
# MAGIC * ノートブックをDatabricksジョブとしてスケジュールする
# MAGIC * DLTパイプラインをDatabricksジョブとしてスケジュールする
# MAGIC * Databricks Jobs UIを使用してタスク間の線形依存関係を構成する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.2.1L

# COMMAND ----------

# DBTITLE 0,--i18n-003b65fd-018a-43e5-8d1d-1ce2bee52fe3
# MAGIC %md
# MAGIC ## 初期データの配置（Land Initial Data）
# MAGIC 先に進む前に、データを用いてランディングゾーンをシードします。 後でこのコマンドを再実行して追加データを配置します。

# COMMAND ----------

DA.data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-cc5b4584-59c4-49c9-a6d2-efcdadb98dbe
# MAGIC %md
# MAGIC ## ジョブ設定の生成(Generate Job Configuration)
# MAGIC
# MAGIC このジョブを設定するために、ユーザー毎にユニークなパラメタが必要です。
# MAGIC 次のセルを実行し、この後のステップでパイプラインを設定するために使う値を表示してください。

# COMMAND ----------

DA.print_job_config()

# COMMAND ----------

# DBTITLE 0,--i18n-542fd1c6-0322-4c8f-8719-fe980f2a8013
# MAGIC %md
# MAGIC ## 単一ノートブックのタスクによるジョブの設定(Configure Job with a Single Notebook Task)
# MAGIC
# MAGIC はじめのノートブックをスケジューリングすることから始めましょう。
# MAGIC
# MAGIC ステップ:
# MAGIC 1. サイドバーの**ワークフロー** を選択し、**Jobs** タブをクリックします。続いて**Create Job**ボタンをクリックします。
# MAGIC 2. 次のようにジョブとタスクを設定します。上のセルで生成した値を使います。
# MAGIC
# MAGIC | 設定項目 | 手順 |
# MAGIC |--|--|
# MAGIC | タスク名 | **Batch-Job**を入力 |
# MAGIC | 種類 | **Notebook**を選択 |
# MAGIC | ソース | **Workspace**を選択 |
# MAGIC | パス | 上のセルで生成された **Batchノートブックのパス** を選択 |
# MAGIC | クラスター | ドロップダウンメニューの **既存の多目的クラスター(Existing All Purpose Clusters)** 下にある、利用者のクラスタを選択 |
# MAGIC | ジョブの名前 | 	上のセルで出力された**ジョブの名前**を、画面の左上で入力。(タスク名ではありません) |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. **作成** ボタンを押下.
# MAGIC 4. 画面右上にある**今すぐ実行** ボタンを押下し、ジョブを開始
# MAGIC
# MAGIC 3. Click the **Create** button.
# MAGIC 4. Click the blue **Run now** button in the top right to start the job.
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **注意**: 
# MAGIC 多目的クラスター(All Purpose Cluster)を選択する際、多目的クラスターとして課金することについての警告が表示されます。本番ジョブは、ワークロードに適したサイズの新しいジョブクラスターを使って常にスケジューリングする必要があり、ジョブクラスターはずっと低いレートになるためです。

# COMMAND ----------

# DBTITLE 0,--i18n-4678fc9d-ab2f-4f8c-b4be-a67774b2afd4
# MAGIC %md
# MAGIC ## パイプラインを生成(Generate Pipeline)
# MAGIC
# MAGIC このステップでは、レッスンの最初に構成したタスクが正常に終了した後に実行するDLTパイプラインを追加します。
# MAGIC
# MAGIC パイプラインではなくジョブにフォーカスするため、シンプルなパイプラインを作る次のコマンドを利用します。

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-8ab4bc2a-e08e-47ca-8507-9565342acfb6
# MAGIC %md
# MAGIC ## パイプラインタスクの追加(Add a Pipeline Task)
# MAGIC
# MAGIC ステップ:
# MAGIC 1. ジョブ詳細のページで**タスク**タブを選択します。
# MAGIC 1. 画面の中央下にある **+** が付いている大きな青色の円形をクリックして新規タスクを追加します。
# MAGIC 1. 次のようにタスクを設定します:
# MAGIC
# MAGIC | 設定項目 | 手順 |
# MAGIC |--|--|
# MAGIC | タスク名 | **DLT** を選択 |
# MAGIC | タイプ | **Delta Live Tables パイプライン** を選択|
# MAGIC | パイプイライン | 上のセルで追加したDLTパイプラインを選択 |
# MAGIC | 依存先 | **Batch-Job**を選択 |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. 青い **タスクを作成** ボタンを押下します。
# MAGIC     - 2つのタスクの箱が線でつながれていることを確認できるはずです。
# MAGIC     - **`Batch-Job`** タスクに続いて、**`DLT`** タスクが配置されます。 
# MAGIC     - これはタスク間の依存関係を表しています。

# COMMAND ----------

# DBTITLE 0,--i18n-1cdf590a-6da0-409d-b4e1-bd5a829f3a66
# MAGIC %md
# MAGIC ## もうひとつのノートブックを追加(Add Another Notebook Task)
# MAGIC
# MAGIC DLTパイプラインで定義されたDLTメトリックとゴールドテーブルの一部を照会する追加のノートブックが用意されています。
# MAGIC
# MAGIC これを最終タスクとしてジョブに追加します。
# MAGIC
# MAGIC ステップ:
# MAGIC 1. 画面の左上で**タスク**タブをクリック（すでに選択中じゃない場合）し、画面の中央下にある**+**が付いている大きな青色の円形をクリックして新規タスクを追加します。
# MAGIC 1. 次のようにタスクを設定します:
# MAGIC
# MAGIC
# MAGIC
# MAGIC | 設定項目 | 手順 |
# MAGIC |--|--|
# MAGIC | タスク名 | **Query-Results**を入力 |
# MAGIC | タイプ | **Notebook**を選択 |
# MAGIC | ソース | **Workspace**を選択 |
# MAGIC | パス | 上のセルで生成された **Queryノートブックのパス** を選択 |
# MAGIC | クラスター | ドロップダウンメニューの **既存の多目的クラスター(Existing All Purpose Clusters)** 下にある、利用者のクラスタを選択 |
# MAGIC | 依存先 | **DLT**を選択 |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. 青色の**タスクを作成**ボタンをクリックします。
# MAGIC 5. 画面の右上にある青い**今すぐ実行**ボタンをクリックしてこのジョブを実行します。
# MAGIC     - **ジョブの実行**タブから、**アクティブな実行**セクションにあるこの実行の開始時刻をクリックして、タスクの進行状況を目で確認できます。
# MAGIC     - すべてのタスクが正常に終了したら、各タスクのコンテンツを確認して期待通りの動作であるかどうかを確認します。

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job()

# COMMAND ----------

DA.validate_job_config()

# COMMAND ----------

# ANSWER

# This function is provided to start the job and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
