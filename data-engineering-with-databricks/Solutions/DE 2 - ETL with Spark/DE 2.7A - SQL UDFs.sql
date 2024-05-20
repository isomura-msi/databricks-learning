-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-5ec757b3-50cf-43ac-a74d-6902d3e18983
-- MAGIC %md
-- MAGIC # SQL UDF と制御フロー(SQL UDFs and Control Flow)
-- MAGIC
-- MAGIC
-- MAGIC ## 学習目標(Learning Objectives)
-- MAGIC このレッスンを終了すると、次のことができるようになります。
-- MAGIC * SQL UDF の定義と登録
-- MAGIC * SQL UDF の共有に使用されるセキュリティ モデルについて説明する
-- MAGIC * SQL コードで **`CASE`** / **`WHEN`** ステートメントを使用する
-- MAGIC * カスタム制御フローの SQL UDF で **`CASE`** / **`WHEN`** ステートメントを活用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd5d37b8-b720-4a88-a2cf-9b3c43f697eb
-- MAGIC %md
-- MAGIC ## セットアップを実行(Run Setup)
-- MAGIC
-- MAGIC 次のセルを実行して、環境をセットアップします。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.7A

-- COMMAND ----------

-- DBTITLE 0,--i18n-e8fa445b-db52-43c4-a649-9904526c6a04
-- MAGIC %md
-- MAGIC ## ユーザー定義関数(User-Defined Functions)
-- MAGIC
-- MAGIC Spark SQL のユーザー定義関数 (UDF) を使用すると、カスタム SQL ロジックを関数としてデータベースに登録できるため、Databricks で SQL を実行できる場所ならどこでもこれらのメソッドを再利用できます。 これらの関数は SQL にネイティブに登録され、大規模なデータセットにカスタム ロジックを適用するときに Spark のすべての最適化を維持します。
-- MAGIC
-- MAGIC SQL UDF を作成するには、少なくとも、関数名、オプションのパラメーター、返される型、およびいくつかのカスタム ロジックが必要です。
-- MAGIC
-- MAGIC 以下の **`sale_announcement`** という名前の単純な関数は、**`item_name`** と **`item_price`** をパラメーターとして受け取ります。 元の価格の 80% でのアイテムの販売を通知する文字列を返します。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION sale_announcement(item_name STRING, item_price INT)
RETURNS STRING
RETURN concat("The ", item_name, " is on sale for $", round(item_price * 0.8, 0));

SELECT *, sale_announcement(name, price) AS message FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-5a5dfa8f-f9e7-4b5f-b229-30bed4497009
-- MAGIC %md
-- MAGIC この関数は、Spark 処理エンジン内で並列に列のすべての値に適用されることに注意してください。 SQL UDF は、Databricks での実行用に最適化されたカスタム ロジックを定義する効率的な方法です。

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9735833-a4f3-4966-8739-eb351025dc28
-- MAGIC %md
-- MAGIC ##SQL UDF のスコープとパーミッション(Scoping and Permissions of SQL UDFs)
-- MAGIC
-- MAGIC SQL ユーザー定義関数:
-- MAGIC - 実行環境間で持続します (ノートブック、DBSQL クエリ、およびジョブを含めることができます)。
-- MAGIC - メタストアにオブジェクトとして存在し、データベース、テーブル、またはビューと同じテーブル ACL によって管理されます。
-- MAGIC - SQL UDF を **create**するには、カタログに **`USE CATALOG`**、スキーマに **`USE SCHEMA`** と **`CREATE FUNCTION`** が必要です。
-- MAGIC - SQL UDF を **use**するには、カタログに **`USE CATALOG`**、スキーマに **`USE SCHEMA`**、関数に **`EXECUTE`** が必要です。
-- MAGIC
-- MAGIC **`DESCRIBE FUNCTION`** を使用して、関数が登録された場所と、期待される入力と返されるものに関する基本情報を確認できます (**`DESCRIBE FUNCTION EXTENDED`** を使用するとさらに多くの情報が得られます)。

-- COMMAND ----------

DESCRIBE FUNCTION EXTENDED sale_announcement

-- COMMAND ----------

-- DBTITLE 0,--i18n-091c02b4-07b5-4b2c-8e1e-8cb561eed5a3
-- MAGIC %md
-- MAGIC 関数の説明の下部にある **`Body`** フィールドは、関数自体で使用される SQL ロジックを示していることに留意してください。

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf549dbc-edb7-465f-a310-0f5c04cfbe0a
-- MAGIC %md
-- MAGIC ## シンプルな制御フロー関数(Simple Control Flow Functions)
-- MAGIC
-- MAGIC **`CASE`** / **`WHEN`** 句の形式で SQL UDF を制御フローと組み合わせると、SQL ワークロード内の制御フローの実行が最適化されます。 標準の SQL 構文構文 **`CASE`** / **`WHEN`** を使用すると、テーブルの内容に基づいて代替結果を持つ複数の条件文を評価できます。
-- MAGIC
-- MAGIC ここでは、この制御フロー ロジックを、SQL を実行できる場所ならどこでも再利用できる関数にラップする方法を示します。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION item_preference(name STRING, price INT)
RETURNS STRING
RETURN CASE 
  WHEN name = "Standard Queen Mattress" THEN "This is my default mattress"
  WHEN name = "Premium Queen Mattress" THEN "This is my favorite mattress"
  WHEN price > 100 THEN concat("I'd wait until the ", name, " is on sale for $", round(price * 0.8, 0))
  ELSE concat("I don't need a ", name)
END;

SELECT *, item_preference(name, price) FROM item_lookup

-- COMMAND ----------

-- DBTITLE 0,--i18n-14f5f9df-d17e-4e6a-90b0-d22bbc4e1e10
-- MAGIC %md
-- MAGIC ここで提供される例は単純ですが、同じ基本原則を使用して、Spark SQL でネイティブに実行するためのカスタム計算とロジックを追加できます。
-- MAGIC
-- MAGIC 特に、定義済みの手順やカスタム定義の式が多数あるシステムからユーザーを移行する可能性がある企業の場合、SQL UDF を使用すると、少数のユーザーが、一般的なレポートや分析クエリに必要な複雑なロジックを定義できます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-451ef10d-9e38-4b71-ad69-9c2ed74601b5
-- MAGIC %md
-- MAGIC 次のセルを実行して、このレッスンに関連付けられているテーブルとファイルを削除します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
