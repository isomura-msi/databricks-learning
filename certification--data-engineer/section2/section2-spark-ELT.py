# Databricks notebook source
# MAGIC %md
# MAGIC # ■ セクション 2: Apache Spark での ELT

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 単一のファイルからのデータ抽出と、複数のファイルを含むディレクトリからのデータ抽出を行う
# MAGIC
# MAGIC ### ○ 参考記事
# MAGIC - DE 2.1 - Querying Files Directly
# MAGIC - [API] dbutils
# MAGIC   - https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils
# MAGIC - [API] リファレンス
# MAGIC   - https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.1

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 色々出力してみる
# MAGIC - DA
# MAGIC - DA.paths

# COMMAND ----------

print(DA)

# COMMAND ----------

# MAGIC %md
# MAGIC この `DA` というのは、`Include` 内でグローバルに定義している。
# MAGIC
# MAGIC ```python
# MAGIC from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig
# MAGIC :
# MAGIC DA = DBAcademyHelper(course_config=course_config,
# MAGIC                      lesson_config=lesson_config)
# MAGIC DA.reset_lesson()
# MAGIC DA.init()
# MAGIC ```
# MAGIC
# MAGIC DBAcademyHelper の API リファレンス・・・は無かった。「Academy」用なので商用で使うものでもないようだし、深追いはやめる。

# COMMAND ----------

print(DA.paths:)

# COMMAND ----------

print(DA.paths.kafka_events)

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.kafka_events)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● dbutils 
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● データを表示してみる
# MAGIC
# MAGIC パスを一重引用符ではなくバックティックで囲っている。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 
# MAGIC
# MAGIC ### ○ 
# MAGIC
# MAGIC ### ○ セミナーでの説明（汚いメモ）を補足として追記
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### ○ 参考記事
# MAGIC - XXX
# MAGIC   - https://
# MAGIC

# COMMAND ----------


