# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-6ee8963d-1bec-43df-9729-b4da38e8ad0d
# MAGIC %md
# MAGIC # Delta Live Tables: Python vs SQL
# MAGIC
# MAGIC このレッスンでは、Delta Live TablesのPythonとSQLの実装の主な相違点を確認します。
# MAGIC
# MAGIC レッソンの目標:
# MAGIC * Delta Live TablesのPythonとSQLの実装の主な相違点を把握する。

# COMMAND ----------

# DBTITLE 0,--i18n-48342971-ca6a-4774-88e1-951b8189bec4
# MAGIC %md
# MAGIC # Python vs SQL
# MAGIC | Python | SQL | Notes |
# MAGIC |--------|--------|--------|
# MAGIC | Python API | プロプライエタリな SQL API |  |
# MAGIC | 構文チェックがない | 構文チェックを行う | Pythonでは、DLTノートブックのセルを単独で実行するとエラーが表示されますが、SQLではコマンドが構文的に有効かどうかをチェックして教えてくれます。どちらの場合も、ノートブックの個々のセルは、DLTパイプラインのために実行されることは想定されていません。 |
# MAGIC | importに注意が必要 | importは必要ない | dltモジュールはPythonのノートブックライブラリに明示的にインポートする必要があります。SQLでは、この限りではありません。 |
# MAGIC | Tables as DataFrames | Tables as query results | Python DataFrame APIでは、複数のAPIコールをつなぎ合わせることで、データセットに複数の変換を施すことができます。SQLでは、同じ変換を行う際には、変換毎に一時テーブルに保存する必要があります。 |
# MAGIC |@dlt.table()  | SELECT statement | SQLでは、データへの変換を含むクエリのコアロジックは、SELECT文に含まれます。Pythonでは、@dlt.table()のオプションを設定する際に、データ変換を指定します。  |
# MAGIC | @dlt.table(comment = "Python comment",table_properties = {"quality": "silver"}) | COMMENT "SQL comment"       TBLPROPERTIES ("quality" = "silver") | PythonとSQLで、コメントやテーブルのプロパティを追加する方法です。 |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
