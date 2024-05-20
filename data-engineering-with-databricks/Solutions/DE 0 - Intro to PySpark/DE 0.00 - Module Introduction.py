# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-0a463481-dd94-4286-9f6e-6522a3a090ec
# MAGIC %md
# MAGIC ## PySparkプログラミング入門 (Introduction to PySpark Programming)
# MAGIC
# MAGIC このモジュールはDatabricks AcademyのData Engineer Learning Pathの一部です。
# MAGIC
# MAGIC #### DataFrames
# MAGIC | Type | Lesson |
# MAGIC | --- | --- |
# MAGIC | Demo | [DE 0.01 - Spark SQL]($./DE 0.01 - Spark SQL) |
# MAGIC | Lab | [DE 0.02L - Spark SQLラボ]($./DE 0.02L - Spark SQL Lab) |
# MAGIC | Demo | [DE 0.03 - DataFrameとカラム]($./DE 0.03 - DataFrame & Column) |
# MAGIC | Lab | [DE 0.04L - 購入売上ラボ]($./DE 0.04L - Purchase Revenues Lab) |
# MAGIC | Demo | [DE 0.05 - 集約]($./DE 0.05 - Aggregation) |
# MAGIC | Lab | [DE 0.06L - トラフィックごとの売り上げラボ]($./DE 0.06L - Revenue by Traffic Lab) |
# MAGIC
# MAGIC #### 関数 (Functions)
# MAGIC | Type | Lesson |
# MAGIC | --- | --- |
# MAGIC | Demo | [DE 0.07 - Reader & Writer]($./DE 0.07 - Reader & Writer) |
# MAGIC | Lab | [DE 0.08L - データ取り込みラボ]($./DE 0.08L - Ingesting Data Lab) |
# MAGIC | Demo | [DE 0.09 - 日付時刻]($./DE 0.09 - Datetimes) |
# MAGIC | Demo | [DE 0.10 - 複雑タイプ]($./DE 0.10 - Complex Types) |
# MAGIC | Demo | [DE 0.11 - 追加の関数]($./DE 0.11 - Additional Functions) |
# MAGIC | Lab | [DE 0.12L - カゴ落ちラボ]($./DE 0.12L - Abandoned Carts Lab) |
# MAGIC
# MAGIC #### クエリーの実行 (Query Execution)
# MAGIC | Type | Lesson |
# MAGIC | --- | --- |
# MAGIC | Demo | [DE 0.13 - クエリーの最適化]($./DE 0.13 - Query Optimization) |

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
