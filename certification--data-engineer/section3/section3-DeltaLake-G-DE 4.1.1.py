# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC ## ● メダリオンアーキテクチャの概要
# MAGIC
# MAGIC メダリオンアーキテクチャは、データレイクハウス上でのデータ処理と分析を段階的に行うフレームワークである。データはブロンズ、シルバー、ゴールドの各テーブルを通じて変換およびエンリッチされる。このアーキテクチャにより、データの品質と信頼性が向上し、最終的にはビジネスインサイトのための高品質なデータセットが得られる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ブロンズテーブル
# MAGIC ブロンズテーブルは、生データをそのまま取り込む層である。データはオブジェクトストレージから直接読み込まれ、元の形式で保存される。ここではデータの完全性が重要であり、データがどのように取り込まれたかを示すメタデータも保持される。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC 以下のSQLコードは、ブロンズテーブルを作成し、JSONデータを読み込む例である。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの作成とデータの挿入
# MAGIC CREATE OR REPLACE TABLE bronze_table (
# MAGIC   id INT,
# MAGIC   name STRING,
# MAGIC   value DOUBLE,
# MAGIC   timestamp STRING
# MAGIC );
# MAGIC
# MAGIC -- JSONデータの読み込み
# MAGIC COPY INTO bronze_table
# MAGIC FROM 's3://your-bucket/data/*.json'
# MAGIC FILEFORMAT = JSON;
# MAGIC
# MAGIC -- データの表示
# MAGIC SELECT * FROM bronze_table LIMIT 10;
