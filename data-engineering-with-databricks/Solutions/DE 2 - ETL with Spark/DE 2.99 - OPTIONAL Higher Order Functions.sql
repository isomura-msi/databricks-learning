-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-a51f84ef-37b4-4341-a3cc-b85c491339a8
-- MAGIC %md
-- MAGIC # Spark SQL の高階関数（Higher Order Functions in Spark SQL）
-- MAGIC
-- MAGIC Spark SQL の高階関数を使用すると、元の構造を維持しながら、配列やマップ型オブジェクトなどの複雑なデータ型を変換できます。 例は次のとおりです。
-- MAGIC - **`FILTER()`** は、指定されたラムダ関数を使用して配列をフィルタリングします。
-- MAGIC - **`EXIST()`** は、配列内の 1 つ以上の要素に対してステートメントが真かどうかをテストします。
-- MAGIC - **`TRANSFORM()`** は、指定されたラムダ関数を使用して、配列内のすべての要素を変換します。
-- MAGIC - **`REDUCE()`** は 2 つのラムダ関数を使用して、要素をバッファーにマージすることで配列の要素を単一の値に減らし、最終バッファーに仕上げ関数を適用します。
-- MAGIC
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC
-- MAGIC このレッスンを終了すると、次のことができるようになります。
-- MAGIC * 高階関数を使用して配列を操作する

-- COMMAND ----------

-- DBTITLE 0,--i18n-b295e5de-82bb-41c0-a470-2d8c6bbacc09
-- MAGIC %md
-- MAGIC ## セットアップを実行(Run Setup)
-- MAGIC
-- MAGIC 次のセルを実行して、環境をセットアップします。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.99

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc1d8e11-d1ff-4aa0-b4e9-3c2703826cd1
-- MAGIC %md
-- MAGIC ## フィルター(Filter)
-- MAGIC
-- MAGIC **`FILTER`** 関数を使用して、指定された条件に基づいて各配列から値を除外する新しい列を作成できます。
-- MAGIC これを使用して、**`sales`** データセットのすべてのレコードから、キングサイズではない **`items`** 列の製品を削除しましょう。
-- MAGIC
-- MAGIC **`FILTER (items, i -> i.item_id LIKE "%K") AS king_items`**
-- MAGIC
-- MAGIC 上記のステートメントでは：
-- MAGIC - **`FILTER`** : 高階関数の名前 <br>
-- MAGIC - **`items`** : 入力配列の名前 <br>
-- MAGIC - **`i`** : イテレータ変数の名前。 この名前を選択して、ラムダ関数で使用します。 配列を反復処理し、各値を一度に 1 つずつ関数に循環させます。<br>
-- MAGIC - **`->`** : 関数の開始を示します <br>
-- MAGIC - **`i.item_id LIKE "%K"`** : これは関数です。 各値は、大文字の K で終わるかどうかがチェックされます。大文字の K で終わる場合は、新しい列 **`king_items`** にフィルター処理されます。
-- MAGIC
-- MAGIC **注:** 作成された列に多数の空の配列を生成するフィルターを作成する場合があります。 その場合、 **`WHERE`** 句を使用して、返された列に空でない配列値のみを表示すると便利です。

-- COMMAND ----------

SELECT * FROM (
  SELECT
    order_id,
    FILTER (items, i -> i.item_id LIKE "%K") AS king_items
  FROM sales)
WHERE size(king_items) > 0

-- COMMAND ----------

-- DBTITLE 0,--i18n-3e2f5be3-1f8b-4a54-9556-dd72c3699a21
-- MAGIC %md
-- MAGIC ##　トランスフォーム（Transform）
-- MAGIC
-- MAGIC **`TRANSFORM()`** 高階関数は、配列内の各要素に既存の関数を適用する場合に特に役立ちます。
-- MAGIC **`items`** 配列列に含まれる要素を変換して **`item_revenues`** という新しい配列列を作成するためにこれを適用しましょう。
-- MAGIC
-- MAGIC 以下のクエリでは、**`items`** は入力配列の名前、**`i`** はイテレータ変数の名前(その名前を適宜指定し、ラムダ関数でその名前を使用します。配列を反復処理し、各値を一度に 1 つずつ関数に循環させます)、**`->`** は関数の開始を示します。 .

-- COMMAND ----------

SELECT *,
  TRANSFORM (
    items, i -> CAST(i.item_revenue_in_usd * 100 AS INT)
  ) AS item_revenues
FROM sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-ccfac343-4884-497a-a759-fc14b1666d6b
-- MAGIC %md
-- MAGIC 上記で指定したラムダ関数は、各値の **`item_revenue_in_usd`** サブフィールドを取得し、それを 100 倍して整数にキャストし、結果を新しい配列列 **`item_revenues`** に含めます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-9a5d0a06-c033-4541-b06e-4661804bf3c5
-- MAGIC %md
-- MAGIC ## 存在関数のラボ(Exists Lab)
-- MAGIC ここでは、高階関数 **`EXISTS`** を **`sales`** テーブルのデータと共に使用して、ブール列 **`mattress`** と **`pillow`** を作成します。 購入した商品がマットレスまたは枕のどちらであったかを示します。
-- MAGIC
-- MAGIC たとえば、**`items`** 列の **`item_name`** が文字列 **`"Mattress"`** で終わる場合、**`mattress`** の列の値は **`true`** で、**`pillow`** の値は **`false`** でなります。 項目と結果の値の例をいくつか示します。
-- MAGIC
-- MAGIC |  items  | mattress | pillow |
-- MAGIC | ------- | -------- | ------ |
-- MAGIC | **`[{..., "item_id": "M_PREM_K", "item_name": "Premium King Mattress", ...}]`** | true | false |
-- MAGIC | **`[{..., "item_id": "P_FOAM_S", "item_name": "Standard Foam Pillow", ...}]`** | false | true |
-- MAGIC | **`[{..., "item_id": "M_STAN_F", "item_name": "Standard Full Mattress", ...}]`** | true | false |
-- MAGIC
-- MAGIC <a href="https://docs.databricks.com/sql/language-manual/functions/exists.html" target="_blank">exists</a> 関数のドキュメントを参照してください。
-- MAGIC
-- MAGIC このように条件式 **`item_name LIKE "%Mattress"`** を使用して、文字列 **`item_name`** が単語 "Mattress" で終わるかどうかを確認できます。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TABLE sales_product_flags AS
SELECT
  items,
  EXISTS (items, i -> i.item_name LIKE "%Mattress") AS mattress,
  EXISTS (items, i -> i.item_name LIKE "%Pillow") AS pillow
FROM sales

-- COMMAND ----------

-- DBTITLE 0,--i18n-3dbc22b0-1092-40c9-a6cb-76ed364a4aae
-- MAGIC %md
-- MAGIC 以下のヘルパー関数は、指示に従わない場合、何を変更する必要があるかについてのメッセージとともにエラーを返します。 出力がない場合は、問題なく成功したことを意味します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC def check_table_results(table_name, num_rows, column_names):
-- MAGIC     assert spark.table(table_name), f"Table named **`{table_name}`** does not exist"
-- MAGIC     assert set(spark.table(table_name).columns) == set(column_names), "Please name the columns as shown in the schema above"
-- MAGIC     assert spark.table(table_name).count() == num_rows, f"The table should have {num_rows} records"

-- COMMAND ----------

-- DBTITLE 0,--i18n-caed8962-3717-4931-8ed2-910caf97740a
-- MAGIC %md
-- MAGIC 以下のセルを実行して、テーブルが正しく作成されたことを確認します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC check_table_results("sales_product_flags", 10510, ['items', 'mattress', 'pillow'])
-- MAGIC product_counts = spark.sql("SELECT sum(CAST(mattress AS INT)) num_mattress, sum(CAST(pillow AS INT)) num_pillow FROM sales_product_flags").first().asDict()
-- MAGIC assert product_counts == {'num_mattress': 9986, 'num_pillow': 1384}, "There should be 9986 rows where mattress is true, and 1384 where pillow is true"

-- COMMAND ----------

-- DBTITLE 0,--i18n-ffcde68f-163a-4a25-85d1-c5027c664985
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
