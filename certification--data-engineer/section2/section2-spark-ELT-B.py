# Databricks notebook source
# MAGIC %md
# MAGIC # ■ セクション 2: Apache Spark での ELT-B

# COMMAND ----------

# MAGIC %md
# MAGIC ### データ準備

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.2

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS sales_csv
  (order_id LONG, email STRING, transactions_timestamp LONG, total_item_quantity INTEGER, purchase_revenue_in_usd DOUBLE, unique_items INTEGER, items STRING)
USING CSV
OPTIONS (
  header = "true",
  delimiter = "|"
)
LOCATION "{DA.paths.sales_csv}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM sales_csv LIMIT 10

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS users_jdbc;
# MAGIC
# MAGIC CREATE TABLE users_jdbc
# MAGIC USING JDBC
# MAGIC OPTIONS (
# MAGIC   url = "jdbc:sqlite:${DA.paths.ecommerce_db}",
# MAGIC   dbtable = "users"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM users_jdbc LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### ● count_if 関数の使用方法
# MAGIC
# MAGIC Databricksにおいて`count_if`関数は、指定された条件を満たす行数をカウントするために使用される。具体的な構文は以下の通りである。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT count_if(condition) AS alias_name
# MAGIC FROM table_name;
# MAGIC ```
# MAGIC
# MAGIC `condition`には、行が条件を満たすかどうかを判定する論理式を指定する。たとえば、スコアが70以上である学生の数をカウントしたい場合、以下のクエリを使用する。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT count_if(score >= 70) AS passing_students
# MAGIC FROM students;
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC   count_if(email IS NULL) AS mail_null, 
# MAGIC   count_if(email IS NOT NULL) AS mail_not_null, 
# MAGIC   count_if(user_first_touch_timestamp < 0) as timestamp_minus,
# MAGIC   count_if(user_first_touch_timestamp >= 0) as timestamp_not_minus
# MAGIC FROM users_jdbc;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### ● x が null のカウントの使用方法
# MAGIC
# MAGIC 列`x`が`NULL`である行の数をカウントするためには、`COUNT`関数と`CASE`文を併用する。具体的な構文は以下の通りである。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(CASE WHEN x IS NULL THEN 1 END) AS null_count
# MAGIC FROM table_name;
# MAGIC ```
# MAGIC
# MAGIC このクエリでは、`CASE`文を利用して`x`列が`NULL`である場合に`1`を返し、それを`COUNT`関数でカウントする。例えば、学生リストにおいて`email`列が`NULL`である行数をカウントする場合、以下のクエリを使用する。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(CASE WHEN email IS NULL THEN 1 END) AS null_emails
# MAGIC FROM students;
# MAGIC ```
# MAGIC
# MAGIC 以上の方法を用いることで、Databricksにおいて特定の条件を満たす行数や`NULL`値のカウントが効率的に行える。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(CASE WHEN email IS NULL THEN 1 END) AS null_emails
# MAGIC FROM users_jdbc;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● count(row) で NULL の値をスキップする方法
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### count 関数の基本的な使用方法
# MAGIC
# MAGIC Databricksにおける`count`関数は、指定された列に対する非NULLの行数をカウントするために使用される。基本的な構文は以下の通りである。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(column_name) AS alias_name
# MAGIC FROM table_name;
# MAGIC ```
# MAGIC
# MAGIC このクエリでは、`column_name`が`NULL`でない行数がカウントされ、結果が`alias_name`として返される。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(user_id) AS id_count
# MAGIC FROM users_jdbc;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### NULL の値をスキップする方法
# MAGIC
# MAGIC `count`関数はデフォルトで`NULL`値をスキップする特性を持つため、特に追加の条件を指定する必要はない。具体例を以下に示す。
# MAGIC
# MAGIC 例えば、学生リストにおいて`email`列が`NULL`でない行数、すなわち有効なメールアドレスが存在する学生の数をカウントしたい場合、以下のクエリを用いる。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(email) AS valid_emails
# MAGIC FROM students;
# MAGIC ```
# MAGIC
# MAGIC このクエリの実行結果は、`email`列が`NULL`でない行数を返す。従って、`email`が`NULL`である行は自動的にスキップされる。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(user_id) AS id_count, COUNT(email) AS email_count
# MAGIC FROM users_jdbc;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### その他の例
# MAGIC
# MAGIC 別の例として、製品リストにおける`price`列が`NULL`でない行数、すなわち価格が設定されている製品の数をカウントするクエリを以下に示す。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT COUNT(price) AS valid_prices
# MAGIC FROM products;
# MAGIC ```
# MAGIC
# MAGIC このクエリでは、`price`列が`NULL`でない製品の数が結果として返される。`count`関数のこの特性により、データのクレンジングや分析が効率的に行える。
# MAGIC
# MAGIC 以上の方法を用いることで、Databricksにおいて`NULL`値をスキップして非NULLの値の数をカウントすることが可能である。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.4

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count_if(email IS NULL) FROM users_dirty;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*) FROM users_dirty WHERE email IS NULL;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python の例

# COMMAND ----------

from pyspark.sql.functions import col
usersDF = spark.read.table("users_dirty")

result_df = usersDF.selectExpr("count_if(email IS NULL)")
display(result_df)

# COMMAND ----------

from pyspark.sql.functions import col
usersDF = spark.read.table("users_dirty")

result_df2 = usersDF.where(col("email").isNull()).count()
display(result_df2)
