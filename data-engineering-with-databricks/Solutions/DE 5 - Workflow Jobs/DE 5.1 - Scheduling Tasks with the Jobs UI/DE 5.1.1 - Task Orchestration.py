# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-4603b7f5-e86f-44b2-a449-05d556e4769c
# MAGIC %md
# MAGIC # Databricks使用したジョブのオーケストレーション（Orchestrating Jobs with Databricks）
# MAGIC
# MAGIC Databricks Jobs UIの新しい更新により、ジョブの一部として複数のタスクをスケジュールする機能が追加され、Databricks Jobsがほとんどの本番ワークロードのオーケストレーションを完全に処理できるようになりました。
# MAGIC
# MAGIC ここでは、ノートブックをトリガーされたスタンドアロンジョブとしてスケジュールする手順を確認してから、DLTパイプラインを使用して依存ジョブを追加します。
# MAGIC
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * ノートブックをDatabricksジョブとしてスケジュールする
# MAGIC * ジョブスケジューリングオプションとクラスタタイプの違いを説明する
# MAGIC * ジョブの実行を確認して進捗状況を追跡し、結果を確認する
# MAGIC * DLTパイプラインをDatabricksジョブとしてスケジュールする
# MAGIC * DatabricksジョブUIを使用してタスク間の線形依存関係を構成する

# COMMAND ----------

# MAGIC %run ../Includes/Classroom-Setup-05.1.1

# COMMAND ----------

# DBTITLE 0,--i18n-fb1b72e2-458f-4930-b070-7d60a4d3b34f
# MAGIC %md
# MAGIC ## ジョブ設定情報の生成(Generate Job Configuration)
# MAGIC
# MAGIC このジョブを設定するには、各ユーザーに固有のパラメータが必要です。
# MAGIC
# MAGIC 以下のセルを実行すると、以降のステップでパイプラインを構成するために使用する値が出力されます。

# COMMAND ----------

DA.print_job_config_v1()

# COMMAND ----------

# DBTITLE 0,--i18n-b3634ee0-e06e-42ca-9e70-a9a02410f705
# MAGIC %md
# MAGIC ## 単一ノートブックのタスクを使ったジョブの設定(Configure Job with a Single Notebook Task)
# MAGIC
# MAGIC ジョブUIを使用して複数のタスクを持つワークロードを編成する場合、はじめに単一のタスクを持つジョブを作成します。
# MAGIC
# MAGIC ステップ:
# MAGIC
# MAGIC 1. サイドバーの**ワークフロー**ボタンをクリックし、**ジョブ**タブをクリックし、**Create Job** ボタンをクリックします。
# MAGIC 2. ジョブとタスクを以下のように設定します。このステップでは、上のセルで出力した値が必要です。
# MAGIC
# MAGIC | 設定項目 | 手順 |
# MAGIC |--|--|
# MAGIC | タスク名 | **Reset** を入力 |
# MAGIC | 種類 | **ノートブック** を選択 |
# MAGIC | ソース | **ワークスペース** を選択 |
# MAGIC | パス | 上で提供された **Reset ノートブックのパス** を指定 |
# MAGIC | クラスター | ドロップダウンメニューの **既存の多目的クラスター(Existing All Purpose Clusters)** 下にある、利用者のクラスタを選択 |
# MAGIC | ジョブの名前 | 上のセルで出力された**ジョブの名前**を、画面の左上で入力。(タスク名ではありません) |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. **作成** ボタンを押下.
# MAGIC 4. 画面右上にある**今すぐ実行** ボタンを押下し、ジョブを開始
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **注意**: 
# MAGIC 多目的クラスター(All Purpose Cluster)を選択する際、多目的クラスターとして課金することについての警告が表示されます。本番ジョブは、ワークロードに適したサイズの新しいジョブクラスターを使って常にスケジューリングする必要があり、ジョブクラスターはずっと低いレートになるためです。

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v1()

# COMMAND ----------

DA.validate_job_v1_config()

# COMMAND ----------

# DBTITLE 0,--i18n-eb8d7811-b356-41d2-ae82-e3762add19f7
# MAGIC %md
# MAGIC ## スケジューリングのオプションを確認(Explore Scheduling Options)
# MAGIC
# MAGIC ステップ:
# MAGIC
# MAGIC 1. ジョブUI画面の右側に、**ジョブの詳細** セクションがあります。
# MAGIC 1. **スケジュール** セクションで、**スケジュールを追加**ボタンを押下します。
# MAGIC 1. **トリガータイプ** を**なし(手動)** から **スケジュール済み** に変更します。これによりcronでスケジュール設定するUIが表示されます。
# MAGIC    - このUIは、ジョブの時系列的なスケジューリングを設定するオプションを提供します。UIで設定した内容はcron構文で出力することも可能で、UIで利用できないカスタム設定が必要な場合は編集することができます。
# MAGIC 1. 現時点では、設定を**なし(手動)**のままにすることにします。**キャンセル** を押下し、**ジョブの詳細** セクションに戻ります。

# COMMAND ----------

# DBTITLE 0,--i18n-eb585218-f5df-43f8-806f-c80d6783df16
# MAGIC %md
# MAGIC ## 実行を確認する（Review Run）
# MAGIC
# MAGIC 実行の実行を確認する手順:
# MAGIC 1. ジョブの詳細ページの、画面左上にある**ジョブの実行** タブを押下します(押下前は、**タスク** タブにいるはずです)
# MAGIC 1. ジョブを見つけます:
# MAGIC     - もし **ジョブが実行中であれば**、**アクティブな実行** セクション下に表示されます。
# MAGIC     - もし **ジョブが終了していれば**、**完了済みの実行アイテム** セクション下に表示されます。
# MAGIC 1. **開始時刻**カラムの時刻をクリックし、出力の詳細を開きます。
# MAGIC     - もし **ジョブが実行中であれば**、右のパネルでジョブの**ステータス** が **保留** か **実行中** になります。
# MAGIC     - もし **ジョブが終了していれば**、右のパネルでジョブの**ステータス** が **成功** か **失敗** になります。
# MAGIC
# MAGIC ノートブックは、MAGICコマンド  **`%run`** を使用して、相対パスを使用して追加のノートブックを呼び出します。 このコースでは取り上げていませんが、<a href="https://docs.databricks.com/repos.html#work-with-non-notebook-files-in-a-databricks-repo" target="_blank">Databricks Reposに追加された新機能により、相対パスを使用してPythonモジュールをロードできるようになりました</a>。
# MAGIC
# MAGIC スケジュールされたノートブックの実際の結果は、新しいジョブとパイプラインの環境をリセットすることです。

# COMMAND ----------

# DBTITLE 0,--i18n-bc61c131-7d68-4633-afd7-609983e43e17
# MAGIC %md
# MAGIC ## パイプラインの作成(Generate Pipeline)
# MAGIC
# MAGIC このステップでは、レッスンの最初に構成したタスクが正常に終了した後に実行するDLTパイプラインを追加します。
# MAGIC
# MAGIC パイプラインではなくジョブにフォーカスするため、シンプルなパイプラインを作る次のコマンドを利用します。

# COMMAND ----------

DA.create_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-19e4daea-c893-4871-8937-837970dc7c9b
# MAGIC %md
# MAGIC ## DLTパイプラインの設定(Configure a DLT Pipeline Task)
# MAGIC
# MAGIC 作成したパイプラインを実行するタスクを追加します
# MAGIC
# MAGIC ステップ:
# MAGIC 1. ジョブ詳細のページで**タスク** タブを選択します。
# MAGIC 1. 画面の中央下にある + が付いている大きな青色の円形をクリックして新規タスクを追加します。
# MAGIC 1. 次のようにタスクを設定します:
# MAGIC
# MAGIC | 設定項目 | 手順 |
# MAGIC |--|--|
# MAGIC | タスク名 | **DLT** と入力|
# MAGIC | タイプ | **Delta Live Tables パイプライン** を選択|
# MAGIC | パイプライン | 上のセルで追加したDLTパイプラインを選択 |
# MAGIC | 依存先 | **Reset** を選択 |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 4. 青い **タスクを作成** ボタンを押下します。
# MAGIC     - 2つのタスクの箱が線でつながれていることを確認できるはずです。
# MAGIC     - **`Reset`** タスクに続いて、**`DLT`** タスクが配置されます。 
# MAGIC     - これはタスク間の依存関係を表しています。
# MAGIC 5. ジョブを実行して、結果を確認します。
# MAGIC     - エラーが起きた場合、次を繰り返し、エラーを取り除いて下さい:
# MAGIC       - エラーを修正
# MAGIC       - **タスクを作成** ボタンを押下
# MAGIC       - 設定を確認

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the job.
DA.create_job_v2()

# COMMAND ----------

DA.validate_job_v2_config()

# COMMAND ----------

# DBTITLE 0,--i18n-1c949168-e917-455d-8a54-2768592a16f1
# MAGIC %md
# MAGIC ## ジョブを実行する(Run the job)
# MAGIC
# MAGIC ジョブを正しく設定できたら、画面右上にある青い**今すぐ実行**ボタンを押下し、ジョブを開始します。
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **注意**: 多目的クラスター(All Purpose Cluster)を選択する際、多目的クラスターとして課金することについての警告が表示されます。本番ジョブは、ワークロードに適したサイズの新しいジョブクラスターを使って常にスケジューリングする必要があり、ジョブクラスターはずっと低いレートになるためです。
# MAGIC
# MAGIC **注意**: ジョブ実行環境が作られ、パイプラインがデプロイされるまで数分待つ必要があることがあります。

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_job()

# COMMAND ----------

# DBTITLE 0,--i18n-666a45d2-1a19-45ba-b771-b47456e6f7e4
# MAGIC %md
# MAGIC ## マルチタスク実行結果を確認する（Review Multi-Task Run Results）
# MAGIC
# MAGIC 実行結果の確認手段:
# MAGIC
# MAGIC 1. **ジョブの実行**タブをもう一度選択し、ジョブが完了したかどうかに応じて、**アクティブな実行**または**完了済みの実行アイテム**で最新の実行を選択します。
# MAGIC     - タスクのビジュアライゼーションは、アクティブに実行されているタスクを反映するためにリアルタイムで更新され、タスクに失敗すると色が変わります。
# MAGIC 1. タスクボックスをクリックすると、スケジュールされたノートブックがUIに表示されます。
# MAGIC     - これは、以前のDatabricksジョブUIの上のオーケストレーションの追加レイヤーだと考えれば分かりやすいかと思います。
# MAGIC     - CLIまたはREST APIを使用してジョブをスケジュールするワークロードがある場合、<a href="https://docs.databricks.com/dev-tools/api/latest/jobs.html" target="_blank">ジョブの構成と結果の取得に使用するJSON構造は、UIと同様の更新が行われることに注意してください</a>。
# MAGIC
# MAGIC **注**：現時点では、タスクとしてスケジュールされたDLTパイプラインは、Runs GUIで結果を直接レンダリングしません。代わりにDLTパイプラインGUIに移動します。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
