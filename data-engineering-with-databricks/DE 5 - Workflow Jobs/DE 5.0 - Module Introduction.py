# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-8d3d2aed-1539-4db1-8e52-0aa71a4ecc9d
# MAGIC %md
# MAGIC ## Databricksワークフロージョブを使ったオーケストレーション (Orchestration with Databricks Workflow Jobs)
# MAGIC
# MAGIC このモジュールはDatabricks Aacademyが提供するData Engineeringパスの一部です。
# MAGIC
# MAGIC #### レッスン(Lessons)
# MAGIC
# MAGIC レクチャー: ワークフローの紹介<br>
# MAGIC レクチャー: ワークフロージョブの構築と監視<br>
# MAGIC デモ: ワークフロージョブの構築と監視<br>
# MAGIC
# MAGIC DE 5.1 - ジョブUIを使ったタスクのスケジューリング<br>
# MAGIC
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.1 - Task Orchestration]($./DE 5.1 - Scheduling Tasks with the Jobs UI/DE 5.1.1 - Task Orchestration) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.2 - Reset]($./DE 5.1 - Scheduling Tasks with the Jobs UI/DE 5.1.2 - Reset) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.1.3 - DLT Job]($./DE 5.1 - Scheduling Tasks with the Jobs UI/DE 5.1.3 - DLT Job) <br>
# MAGIC DE 5.2L - Jobs Lab <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.1L - Lab Instructions]($./DE 5.2L - Jobs Lab/DE 5.2.1L - Lab Instructions) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.2L - Batch Job]($./DE 5.2L - Jobs Lab/DE 5.2.2L - Batch Job) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.3L - DLT Job]($./DE 5.2L - Jobs Lab/DE 5.2.3L - DLT Job) <br>
# MAGIC &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;[DE 5.2.4L - Query Results Job]($./DE 5.2L - Jobs Lab/DE 5.2.4L - Query Results Job) <br>
# MAGIC
# MAGIC
# MAGIC #### 学習の前提 (Prerequisites)
# MAGIC * Databricks Data Engineering & Data Science workspaceを使って、次のような基本的な開発ができること : クラスタの作成、ノートブックを使ったコードの実行、ノートブックの基本的な操作、gitからのリポジトリのインポートなど。
# MAGIC * Delta Live TablesのUIを使って、データパイプラインを設定できること
# MAGIC * PySparkを使って基礎的なDelta Live Tablesのパイプラインを定義できること:
# MAGIC   * AutoLoaderを使ってデータを取り込み、処理できる
# MAGIC   * APPLY CHANGES INTO 構文を使ってChange Data Capture Feedを処理できる
# MAGIC * パイプラインのイベントログおよび結果から、DLT構文のトラブルシューティングができること
# MAGIC * データウェアハウスとデータレイクについて本番環境での経験があること
# MAGIC
# MAGIC
# MAGIC #### 技術的な考慮事項 (Technical Considerations)
# MAGIC * このコースはDBR11.3で動作します
# MAGIC * このコースはDatabricks Community Editionには提供できません

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
