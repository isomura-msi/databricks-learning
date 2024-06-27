# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## ● 本ノートブックについて
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### 対象の試験範囲内の項目
# MAGIC
# MAGIC - データとメタデータを比較対照する。
# MAGIC - マネージドテーブルと外部テーブルを比較対照する。
# MAGIC - 外部テーブルを使用するシナリオを特定する。
# MAGIC - マネージドテーブルを作成する。
# MAGIC - テーブルの場所を特定する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ## ● データとメタデータの比較
# MAGIC
# MAGIC データはテーブル内の実際の値やレコードで、メタデータはテーブルのスキーマ、属性、作成日などの情報を含む。Delta Lakeプロジェクトにおいて、データはテーブルの中に格納され、メタデータはテーブルの構造や属性を定義する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### データ (Data)
# MAGIC データとは、テーブル内に格納される実際の値やレコードの集積を指す。例えば、顧客情報を格納するテーブルの場合、データは各顧客の名前、住所、連絡先等の具体的な値から構成される。
# MAGIC
# MAGIC 具体例：
# MAGIC - 顧客の名前「John Doe」
# MAGIC - 住所「123 Main Street」
# MAGIC - 連絡先「john.doe@example.com」
# MAGIC
# MAGIC 以上のようなデータがテーブルの各行として格納される。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### メタデータ (Metadata)
# MAGIC メタデータは、データそのものではなく、そのデータの「データ」を指す。つまり、データの構造、属性、作成日、更新日、保存場所、データ型等を含む情報の集合である。
# MAGIC
# MAGIC 具体例：
# MAGIC - テーブル名：customers
# MAGIC - カラム名とデータ型：name (String), address (String), contact (String)
# MAGIC - テーブルの作成日：2023-10-05
# MAGIC - データ保存場所：`dbfs:/user/hive/warehouse/customers.db/`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Databricksにおける具体的な役割
# MAGIC 1. **データ**は直接的なクエリや分析対象となり、ビジネスインテリジェンスやデータサイエンスの価値を引き出す源となる。
# MAGIC 2. **メタデータ**はデータ管理を効率化し、データの整合性、アクセス制御、クエリ最適化等をサポートする役割を果たす。メタデータの管理がしっかりしていることで、データの検索性や利用効率が大幅に向上する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Delta Lakeにおけるデータとメタデータ
# MAGIC Delta Lakeでは、データとメタデータの管理がさらに強化されている。Delta Lakeはスナップショットの概念を使用しており、変更履歴やバージョン管理が容易に行える。
# MAGIC
# MAGIC - **データ**: `parquet`ファイル形式で保存される。これらのファイルは実際のレコードを含む。
# MAGIC - **メタデータ**: JSON形式で保存され、各スナップショットの状態や変更履歴を管理する。これにより、「タイムトラベル」機能として知られる、特定の時点でのデータ状態を簡単に再現できる。
# MAGIC
# MAGIC 以下に具体的なSQLおよびPythonコードを示す。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- スキーマ作成
# MAGIC CREATE SCHEMA IF NOT EXISTS example_default_location;
# MAGIC
# MAGIC -- スキーマ選択
# MAGIC USE example_default_location;
# MAGIC
# MAGIC -- テーブル作成
# MAGIC CREATE OR REPLACE TABLE customers (name STRING, address STRING, contact STRING);
# MAGIC
# MAGIC -- データ挿入
# MAGIC INSERT INTO customers VALUES 
# MAGIC ('John Doe', '123 Main Street', 'john.doe@example.com'),
# MAGIC ('Jane Smith', '456 Oak Avenue', 'jane.smith@example.com');
# MAGIC
# MAGIC -- データ表示
# MAGIC SELECT * FROM customers;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- メタデータ表示
# MAGIC DESCRIBE DETAIL customers;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python

# COMMAND ----------


from pyspark.sql import SparkSession

# Sparkセッションの取得
spark = SparkSession.builder.appName("Databricks Example").getOrCreate()

# スキーマ作成
spark.sql("CREATE DATABASE IF NOT EXISTS example_default_location")

# スキーマ選択
spark.sql("USE example_default_location")

# テーブル作成
spark.sql("""
CREATE OR REPLACE TABLE customers (
    name STRING, 
    address STRING, 
    contact STRING
)
""")

# データ挿入
data = [
    ('John Doe', '123 Main Street', 'john.doe@example.com'),
    ('Jane Smith', '456 Oak Avenue', 'jane.smith@example.com')
]
columns = ["name", "address", "contact"]
df = spark.createDataFrame(data, columns)
df.write.insertInto("customers")

# データ表示
display(spark.sql("SELECT * FROM customers"))

# メタデータ表示
tbl_location = spark.sql("DESCRIBE DETAIL customers").first().location
print(f"Table Location: {tbl_location}")

# 物理ファイルの確認
files = dbutils.fs.ls(tbl_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC これらのコードを実行すると、データとメタデータがどのように格納され、管理されるかを確認することができる。特にメタデータは、テーブルの場所や構造を定義するために不可欠であり、データの有効利用において重要な役割を果たす。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● マネージドテーブルと外部テーブルの比較

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Managed Tables
# MAGIC
# MAGIC マネージドテーブルでは、Databricksがテーブルのデータとメタデータの管理を全て行います。デフォルトのストレージロケーションに保存され、テーブルが削除されるとそのデータも完全に削除される。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 定義と管理
# MAGIC マネージドテーブルは、特定のスキーマ内で作成され、Databricksがそのライフサイクルを一元管理する。ここで言う「ライフサイクル」とは、テーブルの作成、データの読み書き、スキーマの変更、作成位置の管理、そしてテーブルの削除など全てを包括する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### データ保存場所
# MAGIC デフォルトでは、マネージドテーブルのデータとメタデータはDatabricksのデフォルトストレージロケーションに保存される。このストレージロケーションは通常、`dbfs:/user/hive/warehouse/`ディレクトリであり、スキーマ名に基づいてテーブルデータが格納される。例えば、スキーマ名が `example_default_location` である場合、そのテーブルのデータとメタデータは `dbfs:/user/hive/warehouse/example_default_location.db/` に保存される。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### データとメタデータの完全管理
# MAGIC 主要なポイントとして、マネージドテーブルが削除されると、対応するデータとメタデータも全て削除されることが挙げられる。これは、ユーザーがデータ消去を意識せずともストレージの整合性が保たれることを意味する。
# MAGIC
# MAGIC 以下では、SQLおよびPythonを使用してマネージドテーブルを作成、操作する方法を示す。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQLでの操作

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- スキーマ作成
# MAGIC CREATE SCHEMA IF NOT EXISTS example_default_location;
# MAGIC
# MAGIC -- スキーマ選択
# MAGIC USE example_default_location;
# MAGIC
# MAGIC -- マネージドテーブルの作成
# MAGIC CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
# MAGIC
# MAGIC -- データ挿入
# MAGIC INSERT INTO managed_table VALUES (3, 2, 1), (4, 5, 6), (7, 8, 9);
# MAGIC
# MAGIC -- データ表示
# MAGIC SELECT * FROM managed_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- メタデータ表示
# MAGIC DESCRIBE DETAIL managed_table;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブル削除
# MAGIC DROP TABLE managed_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Pythonでの操作

# COMMAND ----------


from pyspark.sql import SparkSession

# Sparkセッションの取得
spark = SparkSession.builder.appName("Databricks Managed Table Example").getOrCreate()

# スキーマ作成
spark.sql("CREATE DATABASE IF NOT EXISTS example_default_location")

# スキーマ選択
spark.sql("USE example_default_location")

# マネージドテーブル作成
spark.sql("""
CREATE OR REPLACE TABLE managed_table (
    width INT,
    length INT,
    height INT
)
""")

# データ挿入
data = [
    (3, 2, 1),
    (4, 5, 6),
    (7, 8, 9)
]
columns = ["width", "length", "height"]
df = spark.createDataFrame(data, columns)
df.write.insertInto("managed_table")

# データ表示
df = spark.sql("SELECT * FROM managed_table")
display(df)

# メタデータ表示
tbl_location = spark.sql("DESCRIBE DETAIL managed_table").first().location
print(f"Table Location: {tbl_location}")

# 物理ファイルの確認
files = dbutils.fs.ls(tbl_location)
display(files)

# テーブル削除
spark.sql("DROP TABLE managed_table")

# テーブル削除後、ディレクトリが削除されていることを確認
schema_location = "dbfs:/user/hive/warehouse/example_default_location.db/"
remaining_files = dbutils.fs.ls(schema_location)
print(f"Remaining files in schema directory: {remaining_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### まとめ
# MAGIC マネージドテーブルは以下の利点がある:
# MAGIC - **自動管理**: Databricksがデータとメタデータの全てを管理するため、ユーザーはストレージの細部を気にせず、データ処理に集中できる。
# MAGIC - **完全な削除**: テーブル削除と同時にデータも削除され、ストレージの整合性が保たれる。
# MAGIC - **デフォルトストレージ位置**: テーブルは自動的に`dbfs:/user/hive/warehouse/`に保存されるため、明示的に保存場所を指定する必要がない。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### External Tables
# MAGIC
# MAGIC 外部テーブルでは、データの具体的な物理的ストレージパスがユーザーによって管理される。削除されてもデータは物理的に残り、他のシステムからも参照可能である。  
# MAGIC 外部テーブル（External Tables）は、データとメタデータが分離されて管理されるテーブル形式である。Databricks上で外部テーブルを作成することは可能であり、特に複数のシステム間でデータを共有したり、ユーザーがデータの物理的保存場所を指定したりするシナリオで役立つ。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 特徴と利点
# MAGIC - **データの物理的保存場所の指定**:
# MAGIC   外部テーブルでは、データの保存場所をユーザーが明示的に指定する。これは特定のファイルシステムやデータソースに対するアクセス制御を強めるために有効。
# MAGIC   
# MAGIC - **テーブル削除の際のデータ保持**:
# MAGIC   テーブル定義を削除しても、基盤となるデータは物理的に削除されない。これにより、データを他のシステムから再利用したり、別のテーブル定義で再利用することが可能。
# MAGIC
# MAGIC - **データ共有**:
# MAGIC   データが外部に保存されるため、他のシステムやユーザーと容易にデータを共有でき、データの再利用性が高まる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 特定の用途
# MAGIC - データのライフサイクル管理を明確に行いたい場合
# MAGIC - 特定のフォルダーやストレージシステムを使用したい場合
# MAGIC - 他のシステムとデータを共有する場合
# MAGIC
# MAGIC Databricks上で外部テーブルを作成する手順は、以下の通りである。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### SQLでの操作
# MAGIC 以下に外部テーブルの作成手順を示す。

# COMMAND ----------

# 現在のノートブックのパスを取得
import os

# 現在のノートブックのフルパスを取得
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
print(notebook_path)
# 親フォルダのパスを取得
csv_dir_full_path = os.path.join(os.path.dirname(notebook_path), "data")

print(csv_dir_full_path)
spark.conf.set("spark.sql.csvDirFullPath", csv_dir_full_path)
# /Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/departuredelays.csv
# /Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/departuredelays.csv
# /Workspace/Repos/isomura@msi.co.jp/databricks-learning/certification--data-engineer/section3/data/departuredelays.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- スキーマ作成
# MAGIC CREATE SCHEMA IF NOT EXISTS example_default_location;
# MAGIC
# MAGIC -- スキーマ選択
# MAGIC USE example_default_location;
# MAGIC
# MAGIC -- サンプルデータをCSV形式で保存
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_data USING CSV OPTIONS (
# MAGIC   -- NG
# MAGIC   -- path = 'certification--data-engineer/section3/data/departuredelays.csv',
# MAGIC   -- OK
# MAGIC   path = 'file:/Workspace/${spark.sql.csvDirFullPath}/departuredelays.csv',
# MAGIC   -- OK
# MAGIC   -- path = 'file:/Workspace/Users/isomura@msi.co.jp/departuredelays.csv',
# MAGIC   header = 'true',
# MAGIC   inferSchema = 'true'
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 外部テーブル作成
# MAGIC CREATE OR REPLACE TABLE external_table LOCATION 'dbfs:/mnt/my-external-data/external_table' AS
# MAGIC SELECT * FROM temp_data;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルデータ表示
# MAGIC SELECT * FROM external_table LIMIT 10;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの詳細表示
# MAGIC DESCRIBE DETAIL external_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DESCRIBE TABLE EXTENDED external_table;

# COMMAND ----------

# MAGIC %md
# MAGIC #### Pythonでの操作
# MAGIC 以下に外部テーブルの作成手順を示すDatabricksノートブック上での動作例を示す。

# COMMAND ----------


from pyspark.sql import SparkSession

# Sparkセッションの取得
spark = SparkSession.builder.appName("Databricks External Table Example").getOrCreate()

# スキーマ作成
spark.sql("CREATE DATABASE IF NOT EXISTS example_default_location")

# スキーマ選択
spark.sql("USE example_default_location")

# テンポラリビューの作成
spark.sql("""
  CREATE OR REPLACE TEMPORARY VIEW temp_data USING CSV OPTIONS (
      path 'file:/Workspace/${spark.sql.csvDirFullPath}/departuredelays.csv',
      header 'true',
      inferSchema 'true'
  )
""")

# 外部テーブル作成
spark.sql("""
  CREATE OR REPLACE TABLE external_table LOCATION 'dbfs:/mnt/my-external-data/external_table' AS
  SELECT * FROM temp_data
""")

# データ表示
df = spark.sql("SELECT * FROM external_table")
display(df)

# テーブルの詳細表示
detail_df = spark.sql("DESCRIBE DETAIL external_table")
display(detail_df)

# データ表示
df_info = spark.sql("DESCRIBE TABLE EXTENDED external_table")
display(df_info)

# 外部テーブルの削除
spark.sql("DROP TABLE external_table")

# データがまだ存在することを確認
remaining_files = dbutils.fs.ls("dbfs:/mnt/my-external-data/external_table")
print(f"Remaining files: {remaining_files}")



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### 重要ポイント
# MAGIC 1. `LOCATION` キーワードを使用して、テーブルのデータ保存場所を指定する。
# MAGIC 2. テーブル定義を削除しても、物理的なデータは残ることを確認できる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● 外部テーブルを使用するシナリオ
# MAGIC
# MAGIC 外部テーブルは以下のようなシナリオで利用される：
# MAGIC - データが他のシステムとも共有される場合
# MAGIC - データの物理的な場所をユーザーが指定したい場合
# MAGIC - データのライフサイクル管理をユーザーが行いたい場合
# MAGIC - **データの長期保存**:
# MAGIC   ローカルストレージやクラウドストレージにデータを保存し、テーブル定義を削除してもデータを保持する場合。
# MAGIC - **大容量データの分散処理**:
# MAGIC   複数のシステムでデータを分散して保管し、そのデータに対して一元化した分析を実行したい場合。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● Managed Tablesの作成

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC 以下のSQLコードでマネージドテーブルを作成する。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS example_default_location;
# MAGIC USE example_default_location;
# MAGIC
# MAGIC CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
# MAGIC INSERT INTO managed_table VALUES (3, 2, 1);
# MAGIC SELECT * FROM managed_table;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED managed_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC 以下のPythonコードでマネージドテーブルを作成する。

# COMMAND ----------


from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Databricks Example").getOrCreate()
spark.sql("CREATE DATABASE IF NOT EXISTS example_default_location")
spark.sql("USE example_default_location")

# テーブル作成
spark.sql("CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT)")
spark.sql("INSERT INTO managed_table VALUES (3, 2, 1)")

# データ表示
df = spark.sql("SELECT * FROM managed_table")
display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE EXTENDED managed_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● テーブルの場所を特定する

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC 以下のSQLコードでテーブルの場所を特定する。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC USE example_default_location;
# MAGIC DESCRIBE DETAIL managed_table;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC 以下のPythonコードでテーブルの場所を特定する。

# COMMAND ----------


tbl_location = spark.sql("DESCRIBE DETAIL managed_table").first().location
print(tbl_location)

files = dbutils.fs.ls(tbl_location)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● 総括
# MAGIC
# MAGIC スキーマ作成からテーブル作成、テーブルの詳細位置情報確認、テーブルの削除までの一連の流れは以下のように整理できる。
# MAGIC
# MAGIC ### SQL
# MAGIC ```sql
# MAGIC -- スキーマ作成
# MAGIC CREATE SCHEMA IF NOT EXISTS example_default_location;
# MAGIC
# MAGIC -- スキーマ確認
# MAGIC DESCRIBE SCHEMA EXTENDED example_default_location;
# MAGIC
# MAGIC -- テーブル作成
# MAGIC USE example_default_location;
# MAGIC CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT);
# MAGIC
# MAGIC -- データ挿入と表示
# MAGIC INSERT INTO managed_table VALUES (3, 2, 1);
# MAGIC SELECT * FROM managed_table LIMIT 10;
# MAGIC
# MAGIC -- テーブル詳細表示
# MAGIC DESCRIBE DETAIL managed_table;
# MAGIC
# MAGIC -- テーブル削除
# MAGIC DROP TABLE managed_table;
# MAGIC ```
# MAGIC
# MAGIC ### Python
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # スキーマ作成と確認
# MAGIC spark = SparkSession.builder.appName("Databricks Example").getOrCreate()
# MAGIC spark.sql("CREATE DATABASE IF NOT EXISTS example_default_location")
# MAGIC spark.sql("DESCRIBE SCHEMA EXTENDED example_default_location").show()
# MAGIC
# MAGIC # テーブル作成
# MAGIC spark.sql("USE example_default_location")
# MAGIC spark.sql("CREATE OR REPLACE TABLE managed_table (width INT, length INT, height INT)")
# MAGIC spark.sql("INSERT INTO managed_table VALUES (3, 2, 1)")
# MAGIC
# MAGIC # データ表示
# MAGIC df = spark.sql("SELECT * FROM managed_table")
# MAGIC display(df)
# MAGIC
# MAGIC # テーブル詳細表示
# MAGIC tbl_location = spark.sql("DESCRIBE DETAIL managed_table").first().location
# MAGIC print(tbl_location)
# MAGIC files = dbutils.fs.ls(tbl_location)
# MAGIC display(files)
# MAGIC
# MAGIC # テーブル削除
# MAGIC spark.sql("DROP TABLE managed_table")
# MAGIC ```
