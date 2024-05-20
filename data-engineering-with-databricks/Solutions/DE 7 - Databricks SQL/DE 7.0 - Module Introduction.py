# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-e263b6d4-ac6b-42c3-9c79-086e0881358d
# MAGIC %md
# MAGIC ## Databricks SQL
# MAGIC
# MAGIC このモジュールは、Databricks Academyのデータ エンジニア ラーニング パスコースの一部です。
# MAGIC
# MAGIC #### レッスン
# MAGIC [DE 7.1 - Navigating Databricks SQL and Attaching to Warehouses]($./DE 7.1 - Navigating Databricks SQL and Attaching to Warehouses) <br>
# MAGIC [DE 7.2 - Last Mile ETL with DBSQL]($./DE 7.2 - Last Mile ETL with DBSQL) <br>
# MAGIC
# MAGIC #### 前提条件
# MAGIC * Databricks Data Engineering & Data Science ワークスペースを使用して基本的なコード開発 (クラスターの作成、ノートブックでのコード実行、基本的なノートブック操作、gitからのリポジトリのインポートなど)ができること
# MAGIC * Delta Live Tables UI を使用してデータ パイプラインを構成および実行できること
# MAGIC * PySpark を使用してDelta Live Tables (DLT) パイプラインを定義する初心者レベルの経験
# MAGIC * Auto Loader と PySpark 構文を使用してデータの取込と処理ができること
# MAGIC * APPLY CHANGES INTO 構文を使用して変更データキャプチャ (CDC) フィードを処理できること
# MAGIC * パイプライン イベント ログと結果を確認して、DLT 構文のトラブルシューティングができること
# MAGIC * データ ウェアハウスとデータ レイクの本番運用経験
# MAGIC
# MAGIC #### 技術的な考慮事項
# MAGIC * このコースは DBR 11.3 で実施することをお勧めします。
# MAGIC * 本コースは Databricks Community Edition では配信できません。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
