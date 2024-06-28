# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## ● 前のバージョンのテーブルを記述した人物を特定する
# MAGIC Delta Lakeテーブルの変更はすべてトランザクションログに記録されるため、どの人物が特定のバージョンのテーブルを記述したかを突き止めることができる。**`DESCRIBE HISTORY`** コマンドを用いると、この情報が簡単に確認可能である。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL
# MAGIC テーブルの履歴を確認するSQLクエリ例。

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE students;
# MAGIC -- データを確認するためのテーブルの作成
# MAGIC CREATE TABLE students
# MAGIC   (id INT, name STRING, value DOUBLE);
# MAGIC
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC INSERT INTO students VALUES (4, "Ted", 4.7);
# MAGIC INSERT INTO students VALUES (5, "Tiffany", 5.5);
# MAGIC INSERT INTO students VALUES (6, "Vini", 6.3);
# MAGIC INSERT INTO students VALUES (7, "Nanana", 7.3), (8, "VinVvivi", 2.3), (9, "Numnum", 2.9);
# MAGIC
# MAGIC -- テーブル履歴を確認
# MAGIC DESCRIBE HISTORY students

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC テーブルの履歴を確認するPythonコード例。

# COMMAND ----------


from pyspark.sql import SparkSession

# SparkSessionの生成
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()
spark.sql("DROP TABLE students_py")

# データの準備
data = [
    (1, "Yve", 1.0),
    (2, "Omar", 2.5),
    (3, "Elia", 3.3),
    (4, "Ted", 4.7),
    (5, "Tiffany", 5.5),
    (6, "Vini", 6.3)
]

df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("overwrite").format("delta").saveAsTable("students_py")
table_df = spark.sql("SELECT * FROM students_py")
display(table_df)

data = [
    (7, "Yve", 1.0),
    (8, "Omar", 2.5),
    (9, "Elia", 3.3),
    (10, "Ted", 4.7),
    (11, "Tiffany", 5.5),
    (12, "Vini", 6.3)
]

df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("append").format("delta").saveAsTable("students_py")  # ★append
table_df = spark.sql("SELECT * FROM students_py")
display(table_df)

data = [
    (13, "Yve", 1.0),
    (14, "Omar", 2.5),
    (15, "Elia", 3.3),
    (16, "Ted", 4.7),
    (17, "Tiffany", 5.5),
    (18, "Vini", 6.3)
]

df = spark.createDataFrame(data, ["id", "name", "value"])
df.write.mode("append").format("delta").saveAsTable("students_py")  # ★append
table_df = spark.sql("SELECT * FROM students_py")
display(table_df)

# テーブル履歴を表示
history_df = spark.sql("DESCRIBE HISTORY students_py")
display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 特定のバージョンのテーブルをクエリーする
# MAGIC Delta Lakeのタイムトラベル機能を使って特定のバージョンのテーブルをクエリーすることが可能である。具体的には **`VERSION AS OF`** 句を使う。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL
# MAGIC 以下に特定のバージョンのテーブルをクエリーするSQLの例を示す。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 特定のバージョン3をクエリー
# MAGIC SELECT * 
# MAGIC FROM students VERSION AS OF 3;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC Pythonを使った特定のバージョンのテーブルをクエリーする例。

# COMMAND ----------


# 特定のバージョン2をクエリーする
version_df = spark.read.format("delta").option("versionAsOf", 2).table("students_py")
display(version_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● テーブルトランザクションの履歴を確認する
# MAGIC **`DESCRIBE HISTORY`**コマンドを使って、テーブルの全トランザクション履歴を確認できる。これには各バージョンの作成者やタイムスタンプも含まれる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC 以下にテーブルトランザクションの履歴を確認するSQLの例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルの履歴を確認
# MAGIC DESCRIBE HISTORY students;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC Pythonを使ってテーブルトランザクションの履歴を確認する例。

# COMMAND ----------


# テーブルの履歴を取得
history_df = spark.sql("DESCRIBE HISTORY students_py")
display(history_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● テーブルを前のバージョンにロールバックする
# MAGIC **`RESTORE TABLE`** 機能を活用することで、簡単に前のバージョンにロールバックすることができる。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC 以下に特定のバージョン8にロールバックするSQLの例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- テーブルをバージョン3にロールバック
# MAGIC RESTORE TABLE students TO VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM students;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC Pythonを使ってテーブルを前のバージョンにロールバックする例。
# MAGIC

# COMMAND ----------


# テーブルをバージョン1にロールバック
spark.sql("RESTORE TABLE students_py TO VERSION AS OF 1")
df = spark.sql("SELECT * FROM students_py")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## ● テーブルを前のバージョンにロールバックできることを特定する
# MAGIC テーブルの状態を前のバージョンに戻すことが可能かどうかは、**`RESTORE`** コマンドを使って確認する。

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC 以下にテーブルがバージョン4にロールバックできることを確認するSQLの例を示す。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- バージョンを確認する
# MAGIC DESCRIBE HISTORY students;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM students VERSION AS OF 4;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC Pythonを使ってテーブルが前のバージョンにロールバックできることを確認する例。

# COMMAND ----------


# 履歴を確認してバージョンが存在するか確かめる
history_df = spark.sql("DESCRIBE HISTORY students_py")
display(history_df)

# バージョン1を確認する
version_df = spark.read.format("delta").option("versionAsOf", 1).table("students_py")
display(version_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● OPTIMIZE によって圧縮されるファイルの種類を特定する
# MAGIC Delta Lakeにおける **`OPTIMIZE`** コマンドは、テーブル内の小さなParquetファイルを最適なサイズに圧縮・結合するために使用される。これにより、ファイルサイズが均一化され、クエリパフォーマンスが向上する。
# MAGIC
# MAGIC ### セミナーメモ
# MAGIC - 10トランザクションごとにチェックポイントが作られ（？）1つのparquetにまとめられる（最適化）。
# MAGIC - 小さい parquet を OPTIMIZE コマンドでまとめる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL
# MAGIC 以下は、テーブルの小さなファイルを圧縮するための **`OPTIMIZE`** コマンドのSQL例である。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE students;
# MAGIC -- 確認テーブル作成
# MAGIC CREATE TABLE students
# MAGIC   (id INT, name STRING, value DOUBLE);
# MAGIC
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC INSERT INTO students VALUES (4, "Ted", 4.7);
# MAGIC INSERT INTO students VALUES (5, "Tiffany", 5.5);
# MAGIC INSERT INTO students VALUES (6, "Vini", 6.3);
# MAGIC INSERT INTO students VALUES (7, "Nanana", 7.3), (8, "VinVvivi", 2.3), (9, "Numnum", 2.9);
# MAGIC
# MAGIC -- ファイルの圧縮
# MAGIC OPTIMIZE students;

# COMMAND ----------

# MAGIC %md
# MAGIC #### OPTIMIZE コマンドの出力結果の解説
# MAGIC
# MAGIC `OPTIMIZE` コマンドを実行した結果として提供されたメトリクスについて詳細に解説する。
# MAGIC
# MAGIC ##### numFilesAdded
# MAGIC 1つのファイルが新たに追加されたことを示している。このファイルは、既存の小さなファイルを圧縮して生成されたものである。
# MAGIC
# MAGIC ##### numFilesRemoved
# MAGIC 合計で7つのファイルが削除されたことを示している。これらは、圧縮されて効率化されたファイルに置き換えられた元のファイルである。
# MAGIC
# MAGIC ##### filesAdded
# MAGIC 新たに追加されたファイルについての詳細を示している。
# MAGIC - min: 最小ファイルサイズは1208バイト
# MAGIC - max: 最大ファイルサイズは1208バイト
# MAGIC - avg: 平均ファイルサイズは1208バイト
# MAGIC - totalFiles: 追加されたファイルの総数は1
# MAGIC - totalSize: 追加された全ファイルの総サイズは1208バイト
# MAGIC
# MAGIC ##### filesRemoved
# MAGIC 削除されたファイルについての詳細を示している。
# MAGIC - min: 最小ファイルサイズは1055バイト
# MAGIC - max: 最大ファイルサイズは1109バイト
# MAGIC - avg: 平均ファイルサイズは1070.29バイト
# MAGIC - totalFiles: 削除されたファイルの総数は7
# MAGIC - totalSize: 削除された全ファイルの総サイズは7492バイト
# MAGIC
# MAGIC ##### partitionsOptimized
# MAGIC 最適化されたパーティションの数であり、この実行では0である。全パーティションを1全く与えているため、この項目は観察されなかった。
# MAGIC
# MAGIC ##### zOrderStats
# MAGIC この最適化においては `ZORDER` が使用されなかったため、この項目はnullである。
# MAGIC
# MAGIC ##### numBatches
# MAGIC 最適化が1バッチで行われたことを示している。
# MAGIC
# MAGIC ##### totalConsideredFiles
# MAGIC 最適化のために考慮されたファイルの総数は7である。
# MAGIC
# MAGIC ##### totalFilesSkipped
# MAGIC 最適化の過程でスキップされたファイルの数は0である。
# MAGIC
# MAGIC ##### preserveInsertionOrder
# MAGIC 挿入順序が保持されていることを示している。
# MAGIC
# MAGIC ##### numFilesSkippedToReduceWriteAmplification
# MAGIC データの書き込み増幅を減少させるためにスキップされたファイル数は0である。
# MAGIC
# MAGIC ##### numBytesSkippedToReduceWriteAmplification
# MAGIC データの書き込み増幅を減少させるためにスキップされたバイト数は0である。
# MAGIC
# MAGIC ##### startTimeMs および endTimeMs
# MAGIC この `OPTIMIZE` コマンドの実行開始および終了時刻をミリ秒単位で示している。
# MAGIC
# MAGIC ##### totalClusterParallelism
# MAGIC クラスタ全体の並列処理の度合いは4である。
# MAGIC
# MAGIC ##### totalScheduledTasks
# MAGIC 全体で1つのタスクがスケジュールされたことを示している。
# MAGIC
# MAGIC ##### autoCompactParallelismStats
# MAGIC この最適化実行では自動コンパクトの並列性に関する情報が記録されていないため、この項目はnullである。
# MAGIC
# MAGIC ##### deletionVectorStats
# MAGIC 削除ベクトルに関する統計情報が記載されており、ここでは削除ベクトルが使用されていないため、 `numDeletionVectorsRemoved` および `numDeletionVectorRowsRemoved` は共に0である。
# MAGIC
# MAGIC ##### numTableColumns
# MAGIC テーブルには3つのカラムが存在することを示している。
# MAGIC
# MAGIC ##### numTableColumnsWithStats
# MAGIC 統計情報を持つカラムの数は3である。
# MAGIC
# MAGIC ##### totalTaskExecutionTimeMs
# MAGIC 全タスクの実行時間の合計は306ミリ秒である。
# MAGIC
# MAGIC これらのメトリクスは、 `OPTIMIZE` コマンドによるファイル圧縮および最適化のパフォーマンスと効果についての情報を提供する、非常に重要な指標である。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python
# MAGIC Pythonを使用して小さなファイルを圧縮する例を示す。
# MAGIC

# COMMAND ----------


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# SparkSessionの生成
spark = SparkSession.builder.appName("DeltaLakeExample").getOrCreate()

# Define the schema explicitly
schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("value", DoubleType(), True)
])

# データフレームの作成と保存
data = [
    (1, "Yve", 1.0),
    (2, "Omar", 2.5),
    (3, "Elia", 3.3),
    (4, "Ted", 4.7),
    (5, "Tiffany", 5.5),
    (6, "Vini", 6.3)
]
df = spark.createDataFrame(data, schema)
df.write.mode("overwrite").format("delta").saveAsTable("students")

df = spark.sql("SELECT * FROM students")
display(df)

# ファイルの圧縮
spark.sql("OPTIMIZE students")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Z-Ordering が Delta Lake テーブルで有益な理由を特定する
# MAGIC **`Z-Ordering`** は、Delta Lakeにおいてデータファイル内で似たような値を物理的に近い位置に配置することにより、特定のフィールドでのフィルタリング性能を高める。これにより、クエリパフォーマンスが向上し、データアクセスが効率化される。
# MAGIC
# MAGIC ### セミナーメモ
# MAGIC - liquid クラスタリング  ※15.2以降
# MAGIC   - 今後新しく作られるテーブルでは、ZORDERではなく liquid clustering を指定するのがよい。第一候補。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### 「物理的に近い位置」について
# MAGIC
# MAGIC ※Databricks 生成AI の解説:
# MAGIC
# MAGIC >「物理的に近い位置に配置する」という表現は、Delta LakeにおけるZ-Orderingのテクニックを指しています。このテクニックでは、データファイル内で関連する情報（例えば、特定のフィールドの値が似ているレコード）を物理的に近い場所に保存します。これは、データの物理的な配置を最適化し、ストレージ上でのデータの連続性を高めることを意味します。  
# MAGIC 具体的には、Z-Orderingは、複数のフィールドにわたるデータの局所性を改善するために、Z曲線（またはモートン順序）という数学的な手法を使用してデータを並べ替えます。これにより、特定のクエリ条件にマッチするデータが物理的に近い場所に集まるため、クエリ実行時に読み込む必要があるデータの量が減少します。結果として、フィルタリング操作やデータアクセスが効率化され、クエリのパフォーマンスが向上します。  
# MAGIC たとえば、日付とイベントタイプに基づいてデータをZ-Orderingすると、特定の日付範囲とイベントタイプに関連するデータが物理的に近い位置に配置されます。これにより、そのようなクエリ条件に基づく検索が高速になり、全体的なデータスキャン量が削減されます。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### ZORDER BY は INDEX の代わり？
# MAGIC
# MAGIC ※Databricks 生成AIの回答
# MAGIC
# MAGIC はい、ある意味で、Delta LakeのOPTIMIZEコマンドにZORDER BYを使用することは、他のデータベースシステムにおけるインデックスの作成に似ています。ただし、実装の仕組みと目的にはいくつか違いがあります。
# MAGIC
# MAGIC #### インデックスとは
# MAGIC 一般的なデータベースシステムにおいて、インデックスはデータの検索速度を向上させるために使用されます。インデックスは、特定の列（または列の組み合わせ）に対して作成され、データベースがクエリを実行する際に、対象のデータをより迅速に見つけることができるようにします。
# MAGIC
# MAGIC #### Z-Orderingとは
# MAGIC Delta LakeにおけるZORDER BYは、データファイル内で関連する情報を物理的に近い位置に配置することで、データの読み込み量を削減し、クエリのパフォーマンスを向上させます。これは、特定のクエリパターンに対してデータを最適化することにより、データスキッピング（不要なデータの読み込みを避ける）を効果的に行うためです。
# MAGIC
# MAGIC #### 違い
# MAGIC - 実装の仕組み: 
# MAGIC   - インデックスはデータベースが管理する追加のデータ構造を作成しますが、ZORDER BYは既存のデータファイルの内容を再配置することで最適化を行います。
# MAGIC - 目的: 
# MAGIC   - インデックスは検索速度の向上が主な目的ですが、ZORDER BYはデータの物理的な配置を最適化することにより、データスキッピングを効果的に行い、結果としてクエリパフォーマンスを向上させます。
# MAGIC
# MAGIC したがって、ZORDER BYはインデックスと同じ目的（クエリパフォーマンスの向上）を持ちますが、その実現方法は異なります。また、ZORDER BYは特に大規模なデータセットにおいて、特定のクエリパターンに対するパフォーマンス向上に効果的です。

# COMMAND ----------

# MAGIC %md
# MAGIC ### Z-Ordering の利点
# MAGIC **`Z-Ordering`** の主な利点は以下の通りである：
# MAGIC
# MAGIC 1. **フィルタリングの高速化**： クエリが特定の列に対してフィルタリングを実行する際、データが物理的に近くに格納されるため、ディスクI/Oが減少しクエリが高速化される。
# MAGIC 2. **データスキャンの最小化**： 不要なデータをスキャンする量が減少し、クエリのパフォーマンスが向上する。
# MAGIC 3. **効率的なデータ管理**： データが特定の順序で配置されるため、データ管理がより効率的になる。
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL
# MAGIC **`Z-Ordering`** の利点を示すSQL例。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP TABLE students;
# MAGIC -- 確認テーブル作成
# MAGIC CREATE TABLE students
# MAGIC   (id INT, name STRING, value DOUBLE);
# MAGIC
# MAGIC -- データの挿入
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC INSERT INTO students VALUES (4, "Ted", 4.7);
# MAGIC INSERT INTO students VALUES (5, "Tiffany", 5.5);
# MAGIC INSERT INTO students VALUES (6, "Vini", 6.3);
# MAGIC INSERT INTO students VALUES (7, "Nanana", 7.3);
# MAGIC INSERT INTO students VALUES (8, "VinVvivi", 2.3);
# MAGIC INSERT INTO students VALUES (9, "Numnum", 2.9);
# MAGIC
# MAGIC -- ファイルの圧縮とZ-Ordering
# MAGIC -- `OPTIMIZE`** コマンドに **`ZORDER BY`** 句を追加して、特定の列（この場合は **`id`** 列）を基にデータを再配置する。この再配置により、 **`id`** 列に基づくクエリのパフォーマンスが向上する。特に、大規模なデータセットにおいては、特定の列でのフィルタが効率化される。
# MAGIC OPTIMIZE students
# MAGIC ZORDER BY id;
# MAGIC
# MAGIC -- 結果を確認
# MAGIC SELECT * FROM students;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python
# MAGIC Pythonを利用してZ-Orderingの利点を示すコード例。

# COMMAND ----------

# ファイルの圧縮とZ-Ordering
spark.sql("OPTIMIZE students ZORDER BY id")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● Delta Lake ファイルのディレクトリ構造を調べる
# MAGIC Delta Lakeテーブルは、クラウドオブジェクトストレージに保存されたParquetファイルと **`_delta_log`** ディレクトリから構成される。 **`_delta_log`** フォルダにはトランザクションログが記録され、どのParquetファイルが有効かを管理する。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### Python
# MAGIC Databricksユーティリティを使ってDelta Lakeファイルのディレクトリ構造を調べる例。

# COMMAND ----------


# Delta Lakeファイルのディレクトリ構造を調べる
display(dbutils.fs.ls(f"/user/hive/warehouse/students"))
display(dbutils.fs.ls(f"/user/hive/warehouse/students/_delta_log"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Parquetファイルと_delta_logディレクトリの解説
# MAGIC #### Parquetファイル
# MAGIC Parquetファイルは、列指向フォーマットでデータを格納するためのオープンソースファイル形式である。この形式は、大量のデータを効率的に保存し、分析するために設計されている。列指向の特性により、特定の列に対するクエリや計算を行う際に、必要なデータのみを読み込むことができるため、I/Oの負荷を大幅に削減し、クエリのパフォーマンスを向上させることが可能である。
# MAGIC
# MAGIC ##### 圧縮とエンコーディング
# MAGIC Parquetフォーマットは、データの圧縮とエンコーディングをサポートしており、これによりストレージの使用量を最適化することができる。データは列ごとに圧縮され、同様のデータ型を持つ値が連続して格納されるため、高い圧縮率を実現する。
# MAGIC
# MAGIC ##### スキーマ進化
# MAGIC Parquetはスキーマ進化をサポートしている。これにより、時間の経過とともにデータのスキーマを変更することが可能であり、後方互換性を保ちながら新しい列を追加したり、既存の列のデータ型を変更したりすることができる。
# MAGIC
# MAGIC #### _delta_logディレクトリ
# MAGIC _delta_logディレクトリは、Delta Lakeテーブルのトランザクションログを格納するためのディレクトリである。Delta Lakeは、ACIDトランザクションをサポートするストレージレイヤーであり、_delta_logディレクトリ内のログファイルを通じて、テーブルの変更履歴を管理する。
# MAGIC
# MAGIC ##### トランザクションログ
# MAGIC トランザクションログは、テーブルに対するすべての変更（挿入、更新、削除）を記録するJSON形式のファイルで構成される。これにより、データの整合性を保ちながら、複数のユーザーによる同時書き込みや読み込みを安全に行うことができる。
# MAGIC
# MAGIC ##### タイムトラベル
# MAGIC _delta_logディレクトリ内のトランザクションログを利用することで、Delta Lakeはタイムトラベル（特定の時点のデータにアクセスする機能）をサポートする。これにより、過去のデータの状態にアクセスしたり、データの変更履歴を分析したりすることが可能になる。
# MAGIC
# MAGIC ##### CRCファイル内
# MAGIC
# MAGIC - 統計情報、checksum
# MAGIC - どの parquet が有効か？みたいな情報
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● VACUUM でどのようにして削除がコミットされるのかを特定する
# MAGIC **`VACUUM`** コマンドは、一定期間（デフォルトは7日）使用されていない古いデータファイルを物理的に削除する。これにより、ディスクスペースを解放し、ストレージコストを削減する。
# MAGIC
# MAGIC
# MAGIC デフォルトでは、 **`VACUUM`** は7日間未満のファイルを削除できないようにする。これは、長時間実行される操作が削除対象ファイルを参照しないようにするためである。 
# MAGIC Deltaテーブルに対して **`VACUUM`** を実行すると、指定したデータ保持期間以前のバージョン（RETAIN 5 HOURS と指定したとしたら、6時間前とか7時間前のデータ）にタイムトラベルで戻れなくなる。 
# MAGIC 下記では、データ保持期間を **`0 HOURS`** としているが、これはあくまで機能を示すためであり、通常、本番環境ではこのようなことはしない。
# MAGIC
# MAGIC 例では次を行う：
# MAGIC 1. データファイルの早すぎる削除を防ぐチェックをオフにする
# MAGIC 1.  **`VACUUM`** コマンドのロギングが有効になっていることを確認する
# MAGIC 1. VACUUMの **`DRY RUN`** バージョンを使って、削除対象の全レコードを表示する

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### セミナーメモ
# MAGIC ```
# MAGIC   VACUUM students RETAIN 0 HOURS
# MAGIC   ただし、RETAIN 0 の 0（最新のveresion） では少なくてエラーになる。本当に消していいのか？と。
# MAGIC   そのチェックはオフにすることができる。
# MAGIC     -- 保持閾値のチェックを無効にするため、データの永久削除を有効化
# MAGIC     SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC     -- 確実に VACUUM 操作がトランザクションログに保存されるようにする
# MAGIC     SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### SQL
# MAGIC **`VACUUM`** コマンドを使用して古いデータファイルを削除するSQL例。

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 快速に削除するために設定をオフにし、ログを有効にする
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC SET spark.databricks.delta.vacuum.logging.enabled = true;
# MAGIC
# MAGIC -- 削除対象を確認
# MAGIC VACUUM students RETAIN 0 HOURS DRY RUN;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- 古いデータファイルを削除
# MAGIC VACUUM students RETAIN 0 HOURS;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Python
# MAGIC Pythonを使用した **`VACUUM`** コマンドの実行例。
# MAGIC

# COMMAND ----------


# 設定の変更
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
spark.conf.set("spark.databricks.delta.vacuum.logging.enabled", "true")

# 削除対象を確認（DRY RUN）
dry_run = spark.sql("VACUUM students RETAIN 0 HOURS DRY RUN")
display(dry_run)

# 古いデータファイルを削除
spark.sql("VACUUM students RETAIN 0 HOURS")
