# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-05f37e48-e8d1-4b0c-87e8-38cd4c42edc6
# MAGIC %md
# MAGIC ## Delta Live Tablesによるデータパイプラインの構築(Building Data Pipelines with Delta Live Tables)
# MAGIC このモジュールはDatabricks Aacademyが提供するData Engineeringパスの一部です。
# MAGIC
# MAGIC #### Delta Live TablesのUI(DLT UI)
# MAGIC
# MAGIC スライド: Delta Live Tablesの紹介 <br>
# MAGIC [DE 4.1 - Using the DLT UI]($./DE 4.1 - DLT UI Walkthrough) <br>
# MAGIC
# MAGIC #### Delta Live Tablesの構文(DLT Syntax)
# MAGIC DE 4.1.1 - Orders Pipeline: [SQL]($./DE 4.1A - SQL Pipelines/DE 4.1.1 - Orders Pipeline) or [Python]($./DE 4.1B - Python Pipelines/DE 4.1.1 - Orders Pipeline)<br>
# MAGIC DE 4.1.2 - Customers Pipeline: [SQL]($./DE 4.1A - SQL Pipelines/DE 4.1.2 - Customers Pipeline) or [Python]($./DE 4.1B - Python Pipelines/DE 4.1.2 - Customers Pipeline) <br>
# MAGIC [DE 4.2 - Python vs SQL]($./DE 4.2 - Python vs SQL) <br>
# MAGIC
# MAGIC #### パイプラインの結果、モニタリング、トラブルシューティング(Pipeline Results, Monitoring, and Troubleshooting)
# MAGIC [DE 4.3 - Pipeline Results]($./DE 4.3 - Pipeline Results) <br>
# MAGIC [DE 4.4 - Pipeline Event Logs]($./DE 4.4 - Pipeline Event Logs) <br>
# MAGIC DE 4.1.3 - Status Pipeline: [SQL]($./DE 4.1A - SQL Pipelines/DE 4.1.3 - Status Pipeline) or [Python]($./DE 4.1B - Python Pipelines/DE 4.1.3 - Status Pipeline) <br>
# MAGIC [DE 4.99 - Land New Data]($./DE 4.99 - Land New Data) <br>
# MAGIC
# MAGIC
# MAGIC #### 学習の前提 (Prerequisites)
# MAGIC * クラウドコンピューティングの概念として仮想マシン、オブジェクトストレージなどについて基本的な理解があること。
# MAGIC * Databricks Data Engineering & Data Science workspaceを使って、次のような基本的な開発ができること : クラスタの作成、ノートブックを使ったコードの実行、ノートブックの基本的な操作、gitからのリポジトリのインポートなど。
# MAGIC * Delta Lakeの基本的なプログラミング経験があること
# MAGIC * Delta LakeのDDLを使って次ができること：テーブルの作成、ファイルの圧縮(コンパクト)、以前のバージョンへのレストア、バキュームによる古いファイルの削除
# MAGIC   * CTAS文を使ってクエリー結果をDeltaテーブルに保存できること
# MAGIC   * SQLを使って既存のテーブルの完全な / インクリメンタルな更新ができること
# MAGIC * Spark SQLを使った次のような基本的な開発経験があること : 
# MAGIC   * 様々なファイルフォーマットやデータソースからデータを抽出する
# MAGIC   * クリーンなデータに対し、一般的な変換処理を適用する
# MAGIC   * ビルトイン関数を使って、complex dataを整形したり処理する
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
