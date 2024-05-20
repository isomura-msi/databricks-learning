# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-2d116d36-8ed8-44aa-8cfa-156e16f88492
# MAGIC %md
# MAGIC # DLTパイプライン結果の探索(Exploring the Results of a DLT Pipeline)
# MAGIC
# MAGIC DLTは、Databricks上で本番のETLを実行する際に発生する多くの複雑な問題を抽象化しますが、多くの人々は実際に何が起きているのか疑問に思うかもしれません。
# MAGIC
# MAGIC このノートでは、あまり深入りせず、DLTによってデータとメタデータがどのように永続化されるかを探っていきます。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.3

# COMMAND ----------

# DBTITLE 0,--i18n-ee147dd8-867c-44a7-a4d6-964a5178e8ff
# MAGIC %md
# MAGIC ## ターゲットDatabase中のテーブルへのクエリ(Querying Tables in the Target Database)
# MAGIC
# MAGIC DLT Pipelineの設定時にターゲットデータベースが指定されていれば、Databricks環境全体でユーザーがテーブルを利用できるようになるはずです。
# MAGIC
# MAGIC 下のセルを実行すると、このデモで使用しているデータベースに登録されているテーブルが表示されます。

# COMMAND ----------

# MAGIC %sql
# MAGIC USE ${DA.schema_name};
# MAGIC
# MAGIC SHOW TABLES;

# COMMAND ----------

# DBTITLE 0,--i18n-d5368f5c-7a7d-41e6-b6a2-7a6af2d95c15
# MAGIC %md
# MAGIC パイプラインで定義したVewは、テーブルのリストにないことに注意してください。
# MAGIC
# MAGIC **`orders_bronze`** テーブルへのクエリ結果

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM orders_bronze

# COMMAND ----------

# DBTITLE 0,--i18n-a6a09270-4f8e-4f17-95ab-5e82220a83ed
# MAGIC %md
# MAGIC **`orders_bronze`** はDLTのStreaming Liveテーブルとして定義されていたことを思い出してください。しかし、ここでの結果は静的です。
# MAGIC
# MAGIC DLTはすべてのテーブルを保存するためにDelta Lakeを使用しているため、クエリが実行されるたびに、常にテーブルの最新バージョンを返すことになります。しかし、DLT以外のクエリは、どのように定義されたかにかかわらず、DLTテーブルのスナップショット結果を返すことになります。

# COMMAND ----------

# DBTITLE 0,--i18n-9439da5b-7ab5-4b31-a66d-ea50040f2501
# MAGIC %md
# MAGIC ## `APPLY CHANGES INTO`の結果検証(Examine Results of `APPLY CHANGES INTO`)
# MAGIC
# MAGIC **customers_silver**テーブルは、Type 1 SCDを適用したテーブルで、CDCフィードからの変更で実装されていたことを思い出してください。
# MAGIC
# MAGIC 以下、この表にクエリーを出してみましょう。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-20b2f8b4-9a16-4a8a-b63f-c974e9ab167f
# MAGIC %md
# MAGIC **`customers_silver`** テーブルは、変更が適用された Type 1 テーブルの現在のアクティブな状態を正しく表していますが、DLT UI に表示されているスキーマにある追加フィールドは含まれていないことに注意してください：**__Timestamp**, **__DeleteVersion**, および **__UpsertVersion**です
# MAGIC
# MAGIC これは、**`customers_silver`**テーブルが、実際には **__apply_changes_storage_customers_silver** という名前の隠しテーブルに対するViewとして実装されているからです。
# MAGIC
# MAGIC **`DESCRIBE EXTENDED`**を実行するとわかります。

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-6c70c0ce-abdd-4dab-99fb-661324056120
# MAGIC %md
# MAGIC この隠されたテーブルに問い合わせると、これら3つのフィールドが表示されます。しかし、このテーブルは、DLTが更新を正しい順序で適用して結果を正しく表示するために活用するだけなので、ユーザーが直接操作する必要はないはずです。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM __apply_changes_storage_customers_silver

# COMMAND ----------

# DBTITLE 0,--i18n-6c64b04a-77be-4a2b-9d2b-dd7978b213f7
# MAGIC %md
# MAGIC ## データファイルの確認(Examining Data Files)
# MAGIC
# MAGIC 次のセルを実行し、**Storage location**に設定した場所にあるファイルを確認します。

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.storage_location)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-54bf6537-bf01-4963-9b18-f16c4b2f7692
# MAGIC %md
# MAGIC **autoloader**と**checkpoint**のディレクトリには、Structured Streamingによるインクリメンタルなデータ処理を管理するために使用するデータが含まれています。
# MAGIC
# MAGIC **system** ディレクトリは、パイプラインに関連するイベントをキャプチャします。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/system/events")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-a459d740-2091-40e0-8b47-d67ecdb2fd8e
# MAGIC %md
# MAGIC これらのイベントログは、Deltaテーブルとして保存されています。そのテーブルにクエリをかけてみましょう。

# COMMAND ----------

display(spark.sql(f"SELECT * FROM delta.`{DA.paths.storage_location}/system/events`"))

# COMMAND ----------

# DBTITLE 0,--i18n-61fd77b8-9bd6-4440-a37a-f45169fbf4c0
# MAGIC %md
# MAGIC この後のノートブックで、その指標をより深く掘り下げていきます。
# MAGIC
# MAGIC **tables** ディレクトリの中身を表示してみましょう。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.storage_location}/tables")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-a36ca049-9586-4551-8988-c1b8ec1da349
# MAGIC %md
# MAGIC これらのディレクトリには、DLTが管理しているDelta Lakeのテーブルが格納されています。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
