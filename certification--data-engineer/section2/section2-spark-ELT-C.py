# Databricks notebook source
# MAGIC %md
# MAGIC # ■ セクション 2: Apache Spark での ELT-C

# COMMAND ----------

# MAGIC %md
# MAGIC ### データ準備

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.4

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 既存の Delta Lake テーブルから行の重複を排除する方法

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Delta Lake における重複排除の基本原理
# MAGIC
# MAGIC Delta Lakeにおいて、既存のテーブルから行の重複を排除するためには、`DISTINCT`キーワードや、`ROW_NUMBER`ウィンドウ関数を使用するのが一般的である。これにより、特定の条件に基づいて重複を検出し、不要な行を削除することができる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### DISTINCT キーワードを使用した重複排除
# MAGIC
# MAGIC 最も簡単な方法は、`DISTINCT`キーワードを使って重複行を排除することである。以下の例では、`table_name`テーブルから重複行を削除し、結果を新しいテーブル`deduplicated_table_name`に保存する。
# MAGIC
# MAGIC ```sql
# MAGIC CREATE OR REPLACE TABLE deduplicated_table_name AS
# MAGIC SELECT DISTINCT *
# MAGIC FROM table_name;
# MAGIC ```
# MAGIC
# MAGIC このクエリは、`table_name`テーブル内の重複行を削除し、重複行のないデータを新しいテーブルに書き込む。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users_dirty LIMIT 2

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*), count(user_id), count(user_first_touch_timestamp), count(email), count(updated)
# MAGIC FROM users_dirty

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT DISTINCT(*) FROM users_dirty

# COMMAND ----------

usersDF = spark.read.table("users_dirty")
usersDF.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ROW_NUMBER ウィンドウ関数を使用した詳細な重複排除
# MAGIC
# MAGIC より柔軟かつ詳細な重複排除を行うためには、`ROW_NUMBER`ウィンドウ関数を使用する方法がある。特定の列を基準に行の重複を排除する場合、以下のような手順を踏む。
# MAGIC
# MAGIC 1. `ROW_NUMBER`関数を用いて、各行に一意の行番号を付与する。
# MAGIC 2. 行番号が1の行のみを抽出する。
# MAGIC
# MAGIC 以下のSQLクエリは、その例である。
# MAGIC
# MAGIC ```sql
# MAGIC WITH ranked_data AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY another_column) AS row_num
# MAGIC     FROM table_name
# MAGIC )
# MAGIC CREATE OR REPLACE TABLE deduplicated_table_name AS
# MAGIC SELECT *
# MAGIC FROM ranked_data
# MAGIC WHERE row_num = 1;
# MAGIC ```
# MAGIC
# MAGIC このクエリでは、`key_column`列を基準に重複を検出し、`another_column`に基づいてソートされた最初の行（`row_num = 1`）のみを抽出している。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 一時的なビューを作成
# MAGIC CREATE OR REPLACE TEMP VIEW ranked_users_dirty AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated) AS row_num
# MAGIC     FROM users_dirty
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC WHERE row_num = 1;
# MAGIC
# MAGIC -- 一時的なビューから新しいテーブルを作成
# MAGIC CREATE OR REPLACE TABLE deduplicated_users_dirty AS
# MAGIC SELECT *
# MAGIC FROM ranked_users_dirty;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from deduplicated_users_dirty limit 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from deduplicated_users_dirty limit 10

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python の例

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# Sparkセッションの作成
# spark = SparkSession.builder \
#     .appName("Remove Duplicates from Delta Table") \
#     .getOrCreate()

# Deltaテーブルの読み込み
# df = spark.read.format("delta").load("/path/to/table_name")
usersDF = spark.read.table("users_dirty")

# ウィンドウ定義
window_spec = Window.partitionBy("email").orderBy("updated")

# ROW_NUMBER関数の適用
df_with_row_num = usersDF.withColumn("row_num", row_number().over(window_spec))

# row_numが1の行のみを保持
deduplicated_df = df_with_row_num.filter(df_with_row_num.row_num == 1).drop("row_num")

# 重複を排除したテーブルを新しいDeltaテーブルとして保存
# deduplicated_df.write.format("delta").mode("overwrite").save("/path/to/deduplicated_table_name")

# 重複を排除したテーブルデータの表示
deduplicated_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Delta Lake テーブルのアップデート
# MAGIC
# MAGIC 既存のDelta Lakeテーブルを直接更新して重複を削除する方法もある。以下のような手順で、重複行を削除してテーブルを更新する。
# MAGIC
# MAGIC ```sql
# MAGIC WITH ranked_data AS (
# MAGIC     SELECT *, ROW_NUMBER() OVER (PARTITION BY key_column ORDER BY another_column) AS row_num
# MAGIC     FROM table_name
# MAGIC )
# MAGIC DELETE FROM table_name
# MAGIC WHERE key_column IN (
# MAGIC     SELECT key_column
# MAGIC     FROM ranked_data
# MAGIC     WHERE row_num > 1
# MAGIC );
# MAGIC ```
# MAGIC
# MAGIC このクエリは、一度重複行を特定し、重複している行（`row_num > 1`）を削除することでテーブルを更新する。
# MAGIC
# MAGIC 以上の方法により、Delta Lakeにおける既存のテーブルから行の重複を効果的に排除することができる。各方法は、目的やシナリオに応じて使い分けることが推奨される。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 既存のテーブルから重複する行を削除して新しいテーブルを作成する方法
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 重複する行の識別と削除
# MAGIC
# MAGIC 重複行を削除するためには、特定の列を基準にして各行を唯一のものとして識別する。これにより、任意の条件に基づいて重複行を削除し、新しいテーブルを作成することができる。主に`ROW_NUMBER`ウィンドウ関数を使用する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC
# MAGIC 次の例では、`email`列を基準に重複行を識別し、最新のレコード（`updated`列基準）を残して重複行を削除する方法を示す。
# MAGIC
# MAGIC ```sql
# MAGIC -- 一時ビューの作成
# MAGIC CREATE OR REPLACE TEMP VIEW ranked_users AS
# MAGIC WITH ranked AS (
# MAGIC     SELECT 
# MAGIC         *, 
# MAGIC         ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated DESC) AS row_num
# MAGIC     FROM users
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked
# MAGIC WHERE row_num = 1;
# MAGIC
# MAGIC -- 一時ビューから新しいテーブルを作成
# MAGIC CREATE OR REPLACE TABLE deduplicated_users AS
# MAGIC SELECT *
# MAGIC FROM ranked_users;
# MAGIC ```
# MAGIC
# MAGIC このクエリでは以下のステップを行っている：
# MAGIC
# MAGIC 1. `WITH`句を使用して共通テーブル式`ranked`を定義し、`ROW_NUMBER`関数を用いて各`email`ごとに最新の行を識別する。
# MAGIC 2. 重複行をフィルタリングし、一意の行のみを含む一時ビュー`ranked_users`を作成。
# MAGIC 3. 一時ビュー`ranked_users`から新しいテーブル`deduplicated_users`を作成。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python
# MAGIC
# MAGIC 次のコードは、PySparkを使用して同様の処理を行う方法を示す。
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.window import Window
# MAGIC from pyspark.sql.functions import row_number
# MAGIC
# MAGIC # Sparkセッションの作成
# MAGIC spark = SparkSession.builder \
# MAGIC     .appName("Remove Duplicates from Delta Table") \
# MAGIC     .getOrCreate()
# MAGIC
# MAGIC # Deltaテーブルの読み込み
# MAGIC df = spark.read.format("delta").load("/path/to/users")
# MAGIC
# MAGIC # ウィンドウ定義
# MAGIC window_spec = Window.partitionBy("email").orderBy(col("updated").desc())
# MAGIC
# MAGIC # ROW_NUMBER関数の適用
# MAGIC df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
# MAGIC
# MAGIC # row_numが1の行のみを保持
# MAGIC deduplicated_df = df_with_row_num.filter(df_with_row_num.row_num == 1).drop("row_num")
# MAGIC
# MAGIC # 新しいDeltaテーブルとして保存
# MAGIC deduplicated_df.write.format("delta").mode("overwrite").save("/path/to/deduplicated_users")
# MAGIC
# MAGIC # 結果の表示（任意）
# MAGIC deduplicated_df.show(truncate=False)
# MAGIC ```
# MAGIC
# MAGIC このコードでは以下の手順を踏んでいる：
# MAGIC
# MAGIC 1. **Sparkセッションの作成**: Sparkセッションを初期化。
# MAGIC 2. **Deltaテーブルの読み込み**: Delta形式で保存されたテーブルを読み込む。
# MAGIC 3. **ウィンドウ定義**: `Window.partitionBy("email").orderBy(col("updated").desc())`を定義し、`email`でパーティションを分け、`updated`でソート。
# MAGIC 4. **ROW_NUMBER関数の適用**: `row_number().over(window_spec)`を使用して、各パーティション内で最新の行を識別するための一意の番号を付与。
# MAGIC 5. **フィルタリングと保存**: `row_num`が1の行のみを保持し、新しいDeltaテーブルとして保存。
# MAGIC
# MAGIC 以上の方法により、SQLおよびPython（PySpark）を使用して既存のテーブルから重複する行を削除し、新しいテーブルを作成することができる。各手法はシナリオに応じて使い分けることが推奨される。

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 特定の列に基づいて行の重複を排除する。

# COMMAND ----------

# MAGIC %md
# MAGIC ### GROUP BY を使用した重複排除
# MAGIC
# MAGIC `GROUP BY`を使用して重複を排除する場合は、どの列を基準に重複を判定し、その際にどの値を選択するか（最大値、最小値、最新値など）を明確に定義する必要がある。以下に、`email`列を基準に重複行を排除し、`updated`列の最新値を選択する具体例を示す。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC
# MAGIC 以下のSQL例では、`email`列を基準に、`updated`列で最新の行を保持する。
# MAGIC
# MAGIC ```sql
# MAGIC -- 一時ビューの作成
# MAGIC CREATE OR REPLACE TEMP VIEW aggregated_users AS
# MAGIC SELECT 
# MAGIC     user_id,
# MAGIC     email,
# MAGIC     MAX(updated) AS latest_updated
# MAGIC FROM users
# MAGIC GROUP BY user_id, email;
# MAGIC
# MAGIC -- 新しいテーブルを作成
# MAGIC CREATE OR REPLACE TABLE deduplicated_users AS
# MAGIC SELECT 
# MAGIC     u.user_id,
# MAGIC     u.email,
# MAGIC     a.latest_updated
# MAGIC FROM users u
# MAGIC INNER JOIN aggregated_users a
# MAGIC ON u.user_id = a.user_id AND u.email = a.email AND u.updated = a.latest_updated;
# MAGIC ```
# MAGIC
# MAGIC このクエリでは以下のステップを行っている：
# MAGIC
# MAGIC 1. `GROUP BY`を使用して各`email`ごとに最新の`updated`日時を取得し、一時ビュー`aggregated_users`に保存。
# MAGIC 2. オリジナルの`users`テーブルと一時ビュー`aggregated_users`を結合し、最新の行のみを保持する新しいテーブル`deduplicated_users`を作成。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW deduped_users AS 
# MAGIC   SELECT user_id, user_first_touch_timestamp, max(email) AS email, max(updated) AS updated
# MAGIC   FROM users_dirty
# MAGIC   WHERE user_id IS NOT NULL
# MAGIC   GROUP BY user_id, user_first_touch_timestamp;
# MAGIC
# MAGIC SELECT count(*) FROM deduped_users
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC
# MAGIC 以下のPySpark例では、同様の処理をPythonコードを用いて実現する。
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC from pyspark.sql.functions import col, max as max_
# MAGIC
# MAGIC # Sparkセッションの作成
# MAGIC spark = SparkSession.builder \
# MAGIC     .appName("Remove Duplicates with Group By from Delta Table") \
# MAGIC     .getOrCreate()
# MAGIC
# MAGIC # Deltaテーブルの読み込み
# MAGIC df = spark.read.format("delta").load("/path/to/users")
# MAGIC
# MAGIC # GROUP BYを使用して重複排除
# MAGIC aggregated_df = df.groupBy("user_id", "email").agg(
# MAGIC     max_("updated").alias("latest_updated")
# MAGIC )
# MAGIC
# MAGIC # 元のデータフレームと結合
# MAGIC deduplicated_df = df.join(
# MAGIC     aggregated_df,
# MAGIC     (df.user_id == aggregated_df.user_id) & 
# MAGIC     (df.email == aggregated_df.email) & 
# MAGIC     (df.updated == aggregated_df.latest_updated)
# MAGIC ).select(df["*"])
# MAGIC
# MAGIC # 新しいDeltaテーブルとして保存
# MAGIC deduplicated_df.write.format("delta").mode("overwrite").save("/path/to/deduplicated_users")
# MAGIC
# MAGIC # 結果の表示（任意）
# MAGIC deduplicated_df.show()
# MAGIC ```
# MAGIC
# MAGIC このコードでは以下の手順を踏んでいる：
# MAGIC
# MAGIC 1. **Sparkセッションの作成**: Sparkセッションを初期化。
# MAGIC 2. **Deltaテーブルの読み込み**: Delta形式で保存されたテーブルを読み込む。
# MAGIC 3. **GROUP BYを使用した重複排除**: `groupBy`を使用して各`email`ごとに最新の`updated`日時を取得し、`aggregated_df`に保存。
# MAGIC 4. **結合とフィルタリング**: オリジナルのデータフレーム`df`と`aggregated_df`を結合し、最新の行のみを保持する新しいデータフレーム`deduplicated_df`を作成。
# MAGIC 5. **保存**: 重複を排除したデータを新しいDeltaテーブルとして保存。
# MAGIC 6. **表示**: データを表示（任意）。
# MAGIC
# MAGIC これらの手法を用いることで、SQLおよびPython（PySpark）を使用して、既存のテーブルから重複行を削除し、新しいテーブルを作成することが可能である。各手法は特定のユースケースに応じて適宜選択することが望ましい。

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as max_

# Sparkセッションの作成
# spark = SparkSession.builder \
#     .appName("Remove Duplicates with Group By from Delta Table") \
#     .getOrCreate()

# Deltaテーブルの読み込み
# df = spark.read.format("delta").load("/path/to/users")
usersDF = spark.read.table("users_dirty")

# GROUP BYを使用して重複排除
aggregated_df = usersDF.groupBy("user_id", "email").agg(
    max_("updated").alias("latest_updated")
)

# 元のデータフレームと結合
deduplicated_df = usersDF.join(
    aggregated_df,
    (usersDF.user_id == aggregated_df.user_id) & 
    (usersDF.email == aggregated_df.email) & 
    (usersDF.updated == aggregated_df.latest_updated)
).select(usersDF["*"])

# 新しいDeltaテーブルとして保存
# deduplicated_df.write.format("delta").mode("overwrite").save("/path/to/deduplicated_users")

# 結果の表示（任意）
deduplicated_df.show()
deduplicated_df.count()

# COMMAND ----------

# セミナー版

from pyspark.sql.functions import max
dedupedDF = (usersDF
    .where(col("user_id").isNotNull())
    .groupBy("user_id", "user_first_touch_timestamp")
    .agg(max("email").alias("email"), 
         max("updated").alias("updated"))
    )

dedupedDF.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● すべての行に対してプライマリキーが一意であることを確認する。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 方法1: GROUP BY と COUNT を使用
# MAGIC
# MAGIC 特定の列がプライマリキーとして一意であることを確認するには、まずGROUP BYを使用して行をグループ化し、その後に各グループの出現回数を数える。出現回数が1を超えるグループが存在しないかを確認する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC
# MAGIC ```sql
# MAGIC SELECT primary_key_column, COUNT(*)
# MAGIC FROM table_name
# MAGIC GROUP BY primary_key_column
# MAGIC HAVING COUNT(*) > 1;
# MAGIC ```
# MAGIC
# MAGIC このクエリは、一意でないプライマリキー値を持つすべての行を選択する。このSELECT文の結果が空ならば、プライマリキーは一意であると結論付けられる。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users_dirty

# COMMAND ----------

users_dirty_ddl = spark.sql("DESCRIBE users_dirty")
users_dirty_ddl

# COMMAND ----------

df_users_dirty = spark.table("users_dirty")
df_users_dirty.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, COUNT(*)
# MAGIC FROM users_dirty
# MAGIC GROUP BY user_id
# MAGIC HAVING COUNT(*) > 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM users_dirty
# MAGIC WHERE user_id = "UA000000107391209"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Python
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql.functions import col, count
# MAGIC
# MAGIC df = spark.table('table_name')
# MAGIC result = df.groupBy('primary_key_column').count().filter(col('count') > 1)
# MAGIC
# MAGIC if result.count() == 0:
# MAGIC     print("Primary key is unique.")
# MAGIC else:
# MAGIC     print("Primary key is not unique.")
# MAGIC ```
# MAGIC
# MAGIC このスクリプトは一意でないプライマリキー値を見つけ、その数に応じてメッセージを表示する。resultのカウントが0の場合、一意であることが保証される。
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, count

df = spark.table('users_dirty')
result = df.groupBy('user_id').count().filter(col('count') > 1)

if result.count() == 0:
    print("Primary key is unique.")
else:
    print("Primary key is not unique.")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 方法2: ウィンドウ関数を使用
# MAGIC
# MAGIC ウィンドウ関数を使用してプライマリキーの一意性を確認する方法もある。ここではRow Numberウィンドウ関数を使用する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQL
# MAGIC
# MAGIC ```sql
# MAGIC WITH ranked_rows AS (
# MAGIC     SELECT 
# MAGIC         primary_key_column, 
# MAGIC         ROW_NUMBER() OVER (PARTITION BY primary_key_column ORDER BY some_column) AS row_num
# MAGIC     FROM table_name
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked_rows
# MAGIC WHERE row_num > 1;
# MAGIC ```
# MAGIC
# MAGIC このクエリは同じプライマリキー値が複数回出現する行を特定する。結果が空の場合、プライマリキーは一意である。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH ranked_rows AS (
# MAGIC     SELECT 
# MAGIC         user_id, 
# MAGIC         ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated) AS row_num
# MAGIC     FROM users_dirty
# MAGIC )
# MAGIC SELECT *
# MAGIC FROM ranked_rows
# MAGIC WHERE row_num > 1;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import Window
# MAGIC from pyspark.sql.functions import row_number
# MAGIC
# MAGIC window_spec = Window.partitionBy('primary_key_column').orderBy('some_column')
# MAGIC df_with_row_num = df.withColumn('row_num', row_number().over(window_spec))
# MAGIC
# MAGIC result = df_with_row_num.filter(col('row_num') > 1)
# MAGIC
# MAGIC if result.count() == 0:
# MAGIC     print("Primary key is unique.")
# MAGIC else:
# MAGIC     print("Primary key is not unique.")
# MAGIC ```
# MAGIC
# MAGIC このスクリプトも同様に、重複しているプライマリキーを持つ行を見つけ、その数に応じてメッセージを表示する。結果が存在しない場合、プライマリキーは一意である。

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql.functions import row_number

df = spark.table('users_dirty')
window_spec = Window.partitionBy('user_id').orderBy('updated')
df_with_row_num = df.withColumn('row_num', row_number().over(window_spec))

result = df_with_row_num.filter(col('row_num') > 1)

if result.count() == 0:
    print("Primary key is unique.")
else:
    print("Primary key is not unique.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 方法3: DISTINCTとCOUNTを使用
# MAGIC
# MAGIC DISTINCTを利用してプライマリキーの一意性を確認する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(*) as total_rows, COUNT(DISTINCT primary_key_column) as distinct_rows
# MAGIC FROM table_name;
# MAGIC ```
# MAGIC
# MAGIC このクエリは全行数と一意なプライマリキーの行数を比較する。同じであれば、一意性が保たれていると判断できる。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as total_rows, COUNT(DISTINCT user_id) as distinct_rows
# MAGIC FROM users_dirty;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Python
# MAGIC
# MAGIC ```python
# MAGIC total_rows = df.count()
# MAGIC distinct_rows = df.select('primary_key_column').distinct().count()
# MAGIC
# MAGIC if total_rows == distinct_rows:
# MAGIC     print("Primary key is unique.")
# MAGIC else:
# MAGIC     print("Primary key is not unique.")
# MAGIC ```
# MAGIC
# MAGIC total_rowsとdistinct_rowsを比較することで、プライマリキーの一意性を確認する。等しい場合、一意である。

# COMMAND ----------

df = spark.table('users_dirty')
total_rows = df.count()
distinct_rows = df.select('user_id').distinct().count()

if total_rows == distinct_rows:
    print("Primary key is unique.")
else:
    print("Primary key is not unique.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● フィールドの一意性制約の確認

# COMMAND ----------

# MAGIC %md
# MAGIC ### 一意性制約の説明
# MAGIC あるフィールドの各値が別のフィールド内の一意の値に対して一つだけ関連付けられていることを確認する。この操作は、リレーショナルデータベースにおける一対一の関係を保証するために行われる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL
# MAGIC SQLでは、サブクエリを用いて、対象フィールドの一意性を確認することができる。以下は、`field1` が `field2` に対して一意であることを確認する例である。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT field1, COUNT(DISTINCT field2) as unique_count
# MAGIC FROM table_name
# MAGIC GROUP BY field1
# MAGIC HAVING COUNT(DISTINCT field2) > 1;
# MAGIC ```
# MAGIC このクエリは、`field1` に対して複数の異なる `field2` が存在するレコードを返す。もし結果セットが空であれば、一意性が保たれていることを意味する。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT user_id, COUNT(DISTINCT updated) as unique_count
# MAGIC FROM users_dirty
# MAGIC GROUP BY user_id
# MAGIC HAVING COUNT(DISTINCT updated) > 1;
