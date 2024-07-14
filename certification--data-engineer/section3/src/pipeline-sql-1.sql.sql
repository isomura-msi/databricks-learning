-- Databricks notebook source
-- MAGIC %md
-- MAGIC # ブロンズ

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## データフォルダのパス抽出

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC import os
-- MAGIC # --------------------------
-- MAGIC # 現在のノートブックのフルパスを取得
-- MAGIC notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
-- MAGIC print(notebook_path)
-- MAGIC # CSVフォルダのパスを取得
-- MAGIC # csv_dir_full_path = os.path.join(os.path.dirname(notebook_path), "..", "data")
-- MAGIC # print(csv_dir_full_path)
-- MAGIC # データフォルダのパス
-- MAGIC path_components = notebook_path.split('/')
-- MAGIC data_dir_path = "/".join(path_components[:-2] + ["data"])
-- MAGIC print(data_dir_path)
-- MAGIC print(dbutils.fs.ls("file:/Workspace" + data_dir_path))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## テスト
-- MAGIC データフォルダから対象CSVのデータをdisplayできるか確認する。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # print("###############################  テスト  ######################################")
-- MAGIC # df = spark.read.format("csv") \
-- MAGIC #     .option("header", "true") \
-- MAGIC #     .option("inferSchema", "true") \
-- MAGIC #     .load(f"file:/Workspace/{data_dir_path}/dirty_data.csv")
-- MAGIC # display(df)
-- MAGIC
-- MAGIC # df.write.parquet("dbfs:/mnt/my-external-data/external_table/raw/dirty_data")
-- MAGIC
-- MAGIC # df_parquet = spark.read.parquet("dbfs:/mnt/my-external-data/external_table/raw/dirty_data")
-- MAGIC # display(df_parquet)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## 環境変数に設定
-- MAGIC SQLソースでデータフォルダパスやDeltaデータファイル配置フォルダパスなどを参照できるように環境変数に設定する。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC # データフォルダパス
-- MAGIC spark.conf.set("spark.sql.dataDirFullPath", data_dir_path)
-- MAGIC # Deltaデータファイル配置フォルダパス
-- MAGIC DELTA_DATA_PATH = "/mnt/my-external-data/external_table"
-- MAGIC spark.conf.set("spark.sql.deltaDirFullPath", DELTA_DATA_PATH)

-- COMMAND ----------

-- #########################################
-- 下記だと、bronze_table のカラム名が c0, c1, c2 ... などとなってしまうため使えない。
-- #########################################
-- CREATE LIVE TABLE bronze_table
-- USING DELTA
-- -- LOCATION `${spark.sql.deltaDirFullPath}`
-- LOCATION "/mnt/my-pypeline/training/bronze"
-- AS
-- SELECT *
-- -- FROM csv.`file:/Workspace/${spark.sql.dataDirFullPath}/dirty_data.csv`
-- FROM csv.`file:/Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/dirty_data.csv`


-- COMMAND ----------

-- #########################################
-- cloud_files を使う場合、自動的にストリーミングテーブルとして扱われてしまうため使えない（...ExtendedAnalysisException: 'bronze_table' was read as a stream ... , but 'bronze_table' is not a streaming table. Either add the STREAMING keyword to the CREATE clause or read the input as a table rather than a stream というエラーになる）。
-- #########################################
-- CREATE LIVE TABLE bronze_table
-- USING DELTA
-- LOCATION "/mnt/my-pypeline/training/bronze"
-- AS
-- SELECT *
-- FROM cloud_files("file:/Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/dirty_data.csv", "csv", map("header", "true"))

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE bronze_table
USING DELTA
LOCATION "/mnt/my-pypeline/training/bronze"
AS
SELECT *
FROM cloud_files("file:/Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/dirty_data.csv", "csv", map("header", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # シルバー

-- COMMAND ----------

CREATE LIVE TABLE silver_table
USING DELTA
AS
SELECT
  ID,
  Name,
  Age,
  Email,
  Salary,
  Country,
  RegistrationDate
FROM
  LIVE.bronze_table
WHERE
  Name IS NOT NULL AND Name != '' AND
  Email IS NOT NULL AND Email != '' AND Email != 'invalid_email' AND
  Age IS NOT NULL AND
  Salary IS NOT NULL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # ゴールド

-- COMMAND ----------

CREATE LIVE TABLE gold_table
USING DELTA
AS
SELECT
  Country,
  AVG(Salary) AS AverageSalary,
  COUNT(*) AS NumberOfEmployees
FROM
  LIVE.silver_table
GROUP BY
  Country
HAVING
  COUNT(*) > 5
