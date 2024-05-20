# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-2fbad123-4065-4749-a925-d24f111ab27c
# MAGIC %md
# MAGIC ## Spark SQLでのデータ変換（Transform Data with Spark SQL）
# MAGIC このモジュールはDatabricks AcademyのData Engineer Learning Pathの一部であり、SQLまたはPythonで受けることが可能です。
# MAGIC
# MAGIC #### データ抽出（Extracting Data）
# MAGIC これらのノートブックはSQLとPySparkユーザーのどちらにも関連したSpark SQLのコンセプトを示します。
# MAGIC
# MAGIC [DE 2.1 - データをファイルから直接抽出する方法]($./DE 2.1 - Querying Files Directly)  
# MAGIC [DE 2.2 - 外部ソース用にオプションを指定する]($./DE 2.2 - Providing Options for External Sources)  
# MAGIC [DE 2.3L - データ抽出ラボ]($./DE 2.3L - Extract Data Lab)
# MAGIC
# MAGIC #### データ変換（Transforming Data）
# MAGIC これらのノートブックはSparkのSQLクエリーとPySpark DataFrameのコードを併記して同じコンセプトを両言語で説明します。
# MAGIC
# MAGIC [DE 2.4 - データのクリーンアップ]($./DE 2.4 - Cleaning Data)  
# MAGIC [DE 2.5 - 複雑なトランスフォーメーション]($./DE 2.5 - Complex Transformations)  
# MAGIC [DE 2.6L - データ再形成のラボ]($./DE 2.6L - Reshape Data Lab)
# MAGIC
# MAGIC #### さらなる関数（Additional Functions）
# MAGIC
# MAGIC [DE 2.7A - SQLのUDF]($./DE 2.7A - SQL UDFs)  
# MAGIC [DE 2.7B - PythonのUDF]($./DE 2.7B - Python UDFs)  
# MAGIC [DE 2.99 - 高階関数（オプション）]($./DE 2.99 - OPTIONAL Higher Order Functions)  
# MAGIC
# MAGIC ### 前提条件（Prerequisites）
# MAGIC 本コースの両方のバージョンの前提条件(Spark SQL and PySpark):
# MAGIC * クラウドコンピューティングの概念(仮想マシン、オブジェクトストレージ等)についての初歩的な知識
# MAGIC * DatabricksのDatabricks Data Engineering & Data Scienceワークスペースを使った基本的なコード開発の実践力（クラスタ作成、ノートブックでのコード実行、ノートブックの基本操作、gitからのリポジトリインポート、等）
# MAGIC * 基本的なSQLのコンセプトの中程度の知識（select、抽出、group by、join、等）
# MAGIC
# MAGIC このコースのPySpark版についての追加の前提条件:
# MAGIC * Pythonの初歩的なプログラミング経験（構文、条件、ループ、関数）
# MAGIC * SparkデータフレームAPIの初歩的なプログラミング経験
# MAGIC * データの読み取りと書き込みのためのDataFrameReaderとDataFrameWriterの設定
# MAGIC * DataFrameのメソッドとカラムの表現を使ったクエリートランスフォーメーションの表現
# MAGIC * Sparkのドキュメントから各種のトランスフォーメーションとデータ型を見つける
# MAGIC
# MAGIC 受講生はDatabricks AcademyのIntroduction to PySpark Programmingコースを受講することで、前提となるSpark DataFrame APIのプログラミングスキルを習得することができます。<br>
# MAGIC
# MAGIC #### 技術的な考慮事項（Technical Considerations）
# MAGIC * このコースはDBR 11.3上で実行します
# MAGIC * このコースはDatabricks Community Edition上では実施できません

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
