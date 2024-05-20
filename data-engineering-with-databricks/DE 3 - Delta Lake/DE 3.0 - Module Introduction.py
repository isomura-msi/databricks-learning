# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-dccf35eb-f70e-4271-ad22-3a2f10837a13
# MAGIC %md
# MAGIC ## Delta Lakeによるデータの管理 (Manage Data with Delta Lake)
# MAGIC このモジュールはDatabricks Aacademyが提供するData Engineeringパスの一部です。
# MAGIC
# MAGIC #### レッスン (Lessons)
# MAGIC スライド: What is Delta Lake <br>
# MAGIC [DE 3.1 - Schemas and Tables]($./DE 3.1 - Schemas and Tables) <br>
# MAGIC [DE 3.2 - Version and Optimize Delta Tables]($./DE 3.2 - Version and Optimize Delta Tables) <br>
# MAGIC [DE 3.3 - Manipulate Delta Tables Lab]($./DE 3.3L - Manipulate Delta Tables Lab) <br>
# MAGIC [DE 3.4 - Set Up Delta Tables]($./DE 3.4 - Set Up Delta Tables) <br>
# MAGIC [DE 3.5 - Load Data into Delta Lake]($./DE 3.5 - Load Data into Delta Lake) <br>
# MAGIC [DE 3.6 - Load Data Lab]($./DE 3.6L - Load Data Lab) <br>
# MAGIC
# MAGIC #### 学習の前提 (Prerequisites)
# MAGIC * クラウドコンピューティングの概念として仮想マシン、オブジェクトストレージなどについて基本的な理解があること。
# MAGIC * Databricks Data Engineering & Data Science workspaceを使って、次のような基本的な開発ができること : クラスタの作成、ノートブックを使ったコードの実行、ノートブックの基本的な操作、gitからのリポジトリのインポートなど。
# MAGIC * Spark SQLを使った次のような基本的な開発経験があること : 
# MAGIC   * 様々なファイルフォーマットやデータソースからデータを抽出する
# MAGIC   * クリーンなデータに対し、一般的な変換処理を適用する
# MAGIC   * ビルトイン関数を使って、complex dataを整形したり処理する
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
