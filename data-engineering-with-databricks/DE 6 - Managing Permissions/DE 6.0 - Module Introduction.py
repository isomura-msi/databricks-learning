# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>

# COMMAND ----------

# MAGIC %md
# MAGIC ## 分析のためのデータ アクセスを管理する
# MAGIC このモジュールは、Databricks Academy によるデータ エンジニア ラーニング パスの一部です。
# MAGIC
# MAGIC #### レッスン
# MAGIC 講義: Unity Catalog の概要<br>
# MAGIC 講義: Unity Catalog におけるデータ アクセス制御<br>
# MAGIC DE 6.1 - [UC によるデータの作成と管理]($./DE 6.1 - Create and Govern Data with UC)<br>
# MAGIC DE 6.2L - [Unity Catalog でのテーブルの作成と共有]($./DE 6.2L - Create and Share Tables in Unity Catalog)<br>
# MAGIC レクチャー: Unity Catalog のパターンとベスト プラクティス<br>
# MAGIC DE 6.3L - [ビューの作成とテーブル アクセスの制限]($./DE 6.3L - Create Views and Limit Table Access)<br>
# MAGIC
# MAGIC
# MAGIC #### 前提条件
# MAGIC * Databricks Lakehouse プラットフォームに関する初級レベルの知識 (Lakehouse プラットフォームの構造と利点についての高度な知識)
# MAGIC * SQL の初級レベルの知識 (基本的なクエリを理解し構築する能力)
# MAGIC
# MAGIC
# MAGIC #### 技術的な考慮事項
# MAGIC このコースは Databricks Community Edition では実施できず、Unity Catalog をサポートするクラウドでのみ実施できます。すべての演習を完全に実行するには、ワークスペース レベルとアカウント レベルの両方での管理アクセスが必要です。クラウド環境への低レベルのアクセスを追加で必要とする、いくつかのオプションのタスクが示されています。
# MAGIC

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
