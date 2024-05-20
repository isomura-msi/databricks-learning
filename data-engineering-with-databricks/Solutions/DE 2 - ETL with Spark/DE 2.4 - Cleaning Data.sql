-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad42144-605b-486f-ad65-ca24b47b1924
-- MAGIC %md
-- MAGIC # データのクリーンアップ（Cleaning Data）
-- MAGIC
-- MAGIC データを調べてクリーンにしていく過程で、データセットに適用するトランスフォーメーションを表現するために各種のカラム式とクエリーを作る必要があります。
-- MAGIC
-- MAGIC カラム式は、既存の列、演算子、および組み込み関数から構築されます。 列式は、 **`SELECT`** 文で使用して、データセットから新しい列を作成する変換を表現できます。
-- MAGIC
-- MAGIC Spark SQLでは、 **`WHERE`** 、 **`DISTINCT`** 、 **`ORDER BY`** 、 **`GROUP BY`** など、 **`SELECT`** の他にも、トランスフォーメーションを表現するための多くの追加クエリコマンドがあります。
-- MAGIC
-- MAGIC このノートブックでは、これまで使用してきた他のシステムとは異なるいくつかの概念を見たり、一般的な操作に役立ついくつかの関数を呼び出したりします。
-- MAGIC
-- MAGIC **`NULL`** 値のにおける動作、および文字列と日時フィールドの書式設定に特に注意を払います。
-- MAGIC
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC - データセットを要約し、nullの動作を説明する
-- MAGIC - 重複を取得して削除する
-- MAGIC - 予想されるカウント、欠落値、重複レコードについてデータセットを検証する
-- MAGIC - データをきれいにして変換するための一般的なトランスフォーメーションを適用する

-- COMMAND ----------

-- DBTITLE 0,--i18n-2a604768-1aac-40e2-8396-1e15de60cc96
-- MAGIC %md
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC
-- MAGIC セットアップスクリプトでは、このノートブックの実行に必要なデータを作成し値を宣言します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-02.4

-- COMMAND ----------

-- DBTITLE 0,--i18n-31202e20-c326-4fa0-8892-ab9308b4b6f0
-- MAGIC %md
-- MAGIC ## データの概要（Data Overview）
-- MAGIC
-- MAGIC <strong>`users_dirty`</strong>にある新しいユーザーのレコードに対して作業しますが、これは以下のスキーマになっています：
-- MAGIC
-- MAGIC | フィールド | 型 | 説明 |
-- MAGIC |---|---|---|
-- MAGIC | user_id | string | ユニークなID |
-- MAGIC | user_first_touch_timestamp | long | ユーザーのレコードが作られた時間をエポックからのマイクロ秒数で表したもの |
-- MAGIC | email | string | アクションを完了するためにユーザーから提供された最新のEメールアドレス |
-- MAGIC | updated | timestamp | このレコードが最後に更新された時間 |
-- MAGIC
-- MAGIC データのそれぞれのフィールドについて値をカウントしてみましょう。

-- COMMAND ----------

SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
FROM users_dirty

-- COMMAND ----------

-- DBTITLE 0,--i18n-c414c24e-3b72-474b-810d-c3df32032c26
-- MAGIC %md
-- MAGIC ## 欠損データを調べる（Inspect Missing Data）
-- MAGIC
-- MAGIC 上記のカウント結果によると、全てのフィールドに少なくともいくつかのnull値があるようです。
-- MAGIC
-- MAGIC **注:** <strong>`count()`</strong>を含め、いくつかの数学関数ではnull値は不正確に振る舞います。Null values behave incorrectly in some math functions, including **`count()`**.
-- MAGIC
-- MAGIC - **`count(col)`** 特定のカラムや式をカウントする時に **`NULL`** 値をスキップします。
-- MAGIC - **`count(*)`** null値だけの行も含め、行の総数をカウントする特別ケースです。
-- MAGIC
-- MAGIC 以下のいずれかを使って、フィールドがnullであるレコードを抽出することによりnull値をカウントすることができます：
-- MAGIC **`col IS NULL`** の条件で抽出して<strong>`count(*)`</strong>か、<strong>`count_if(col IS NULL)`</strong>。
-- MAGIC
-- MAGIC 以下の式は両方ともEメールが欠損しているレコードを正確にカウントします。

-- COMMAND ----------

SELECT count_if(email IS NULL) FROM users_dirty;
SELECT count(*) FROM users_dirty WHERE email IS NULL;

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC from pyspark.sql.functions import col
-- MAGIC usersDF = spark.read.table("users_dirty")
-- MAGIC
-- MAGIC usersDF.selectExpr("count_if(email IS NULL)")
-- MAGIC usersDF.where(col("email").isNull()).count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-ea1ca35c-6421-472b-b70b-4f36bdab6d79
-- MAGIC %md
-- MAGIC ## 行の重複排除（Deduplicate Rows）
-- MAGIC
-- MAGIC <strong>`DISTINCT *`</strong>を使って、行全体で同じ値を含んでいる重複レコードを排除できます。

-- COMMAND ----------

SELECT DISTINCT(*) FROM users_dirty

-- COMMAND ----------

-- MAGIC %python
-- MAGIC usersDF.distinct().display()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5da6599b-756c-4d22-85cd-114ff02fc19d
-- MAGIC %md
-- MAGIC ## 特定の列に基づいて行の重複を排除する（Deduplicate Rows Based on Specific Columns）
-- MAGIC
-- MAGIC 以下のコードは **`GROUP BY`** を使用して、 **`user_id`** および **`user_first_touch_timestamp`** 列の値に基づいて重複レコードを削除します。 (これらのフィールドは両方とも、特定のユーザーが最初に検出されたときに生成されるため、一意のタプルが形成されることを思い出してください。)
-- MAGIC
-- MAGIC ここでは、集計関数 **`max`** をハックとして使用しています。
-- MAGIC - **`email`** 列と **`updated`** 列の値を group by の結果に保持します
-- MAGIC - 複数のレコードが存在する場合、null 以外の電子メールをキャプチャします

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW deduped_users AS 
SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
FROM users_dirty
WHERE user_id IS NOT NULL
GROUP BY user_id, user_first_touch_timestamp;

SELECT count(*) FROM deduped_users

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import max
-- MAGIC dedupedDF = (usersDF
-- MAGIC     .where(col("user_id").isNotNull())
-- MAGIC     .groupBy("user_id", "user_first_touch_timestamp")
-- MAGIC     .agg(max("email").alias("email"), 
-- MAGIC          max("updated").alias("updated"))
-- MAGIC     )
-- MAGIC
-- MAGIC dedupedDF.count()

-- COMMAND ----------

-- DBTITLE 0,--i18n-5e2c98db-ea2d-44dc-b2ae-680dfd85c74b
-- MAGIC %md
-- MAGIC **`user_id`** と **`user_first_touch_timestamp`** の個別の値に基づいて、重複排除後に予想される残りのレコード数があることを確認しましょう。

-- COMMAND ----------

SELECT COUNT(DISTINCT(user_id, user_first_touch_timestamp))
FROM users_dirty
WHERE user_id IS NOT NULL

-- COMMAND ----------

-- MAGIC %python
-- MAGIC (usersDF
-- MAGIC     .dropDuplicates(["user_id", "user_first_touch_timestamp"])
-- MAGIC     .filter(col("user_id").isNotNull())
-- MAGIC     .count())

-- COMMAND ----------

-- DBTITLE 0,--i18n-776b4ee7-9f29-4a19-89da-1872a1f8cafa
-- MAGIC %md
-- MAGIC ## データセットの検証(Validate Datasets)
-- MAGIC
-- MAGIC 上記の手動レビューに基づいて、カウントが期待どおりであることを視覚的に確認しました.
-- MAGIC  
-- MAGIC 単純なフィルターと **`WHERE`** 句を使用して、プログラムで検証を実行することもできます。
-- MAGIC
-- MAGIC 各行の **`user_id`** が一意であることを検証します。

-- COMMAND ----------

SELECT max(row_count) <= 1 no_duplicate_ids FROM (
  SELECT user_id, count(*) AS row_count
  FROM deduped_users
  GROUP BY user_id)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import count
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .groupBy("user_id")
-- MAGIC     .agg(count("*").alias("row_count"))
-- MAGIC     .select((max("row_count") <= 1).alias("no_duplicate_ids")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-d405e7cd-9add-44e3-976a-e56b8cdf9d83
-- MAGIC %md
-- MAGIC 各メールが最大で 1 つの **`user_id`** に関連付けられていることを確認します。

-- COMMAND ----------

SELECT max(user_id_count) <= 1 at_most_one_id FROM (
  SELECT email, count(user_id) AS user_id_count
  FROM deduped_users
  WHERE email IS NOT NULL
  GROUP BY email)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .where(col("email").isNotNull())
-- MAGIC     .groupby("email")
-- MAGIC     .agg(count("user_id").alias("user_id_count"))
-- MAGIC     .select((max("user_id_count") <= 1).alias("at_most_one_id")))

-- COMMAND ----------

-- DBTITLE 0,--i18n-8630c04d-0752-404f-bfd1-bb96f7b06ffa
-- MAGIC %md
-- MAGIC ## 日付形式と正規表現(Date Format and Regex)
-- MAGIC null フィールドを削除して重複を排除したので、データからさらに価値を引き出したいと思うかもしれません。
-- MAGIC
-- MAGIC 以下のコード：
-- MAGIC - **`user_first_touch_timestamp`** を有効なタイムスタンプに正しくスケーリングおよびキャストします
-- MAGIC - このタイムスタンプのカレンダーの日付と時刻を人間が読める形式で抽出します
-- MAGIC - **`regexp_extract`** を使用して、正規表現を使用してメール列からドメインを抽出します

-- COMMAND ----------

SELECT *, 
  date_format(first_touch, "MMM d, yyyy") AS first_touch_date,
  date_format(first_touch, "HH:mm:ss") AS first_touch_time,
  regexp_extract(email, "(?<=@).+", 0) AS email_domain
FROM (
  SELECT *,
    CAST(user_first_touch_timestamp / 1e6 AS timestamp) AS first_touch 
  FROM deduped_users
)

-- COMMAND ----------

-- MAGIC %python
-- MAGIC from pyspark.sql.functions import date_format, regexp_extract
-- MAGIC
-- MAGIC display(dedupedDF
-- MAGIC     .withColumn("first_touch", (col("user_first_touch_timestamp") / 1e6).cast("timestamp"))
-- MAGIC     .withColumn("first_touch_date", date_format("first_touch", "MMM d, yyyy"))
-- MAGIC     .withColumn("first_touch_time", date_format("first_touch", "HH:mm:ss"))
-- MAGIC     .withColumn("email_domain", regexp_extract("email", "(?<=@).+", 0))
-- MAGIC )

-- COMMAND ----------

-- DBTITLE 0,--i18n-c9e02918-f105-4c12-b553-3897fa7387cc
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
