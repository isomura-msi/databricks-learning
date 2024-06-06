# Databricks notebook source
# MAGIC %md
# MAGIC # ■ セクション 2: Apache Spark での ELT

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● データ準備（トレーニング時のデータを利用）

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 単一のファイルからのデータ抽出と、複数のファイルを含むディレクトリからのデータ抽出を行う
# MAGIC
# MAGIC ### ○ 参考記事
# MAGIC - DE 2.1 - Querying Files Directly
# MAGIC - [API] dbutils
# MAGIC   - https://learn.microsoft.com/ja-jp/azure/databricks/dev-tools/databricks-utils
# MAGIC - [API] リファレンス
# MAGIC   - https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.1

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.2

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-02.4

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 色々出力してみる
# MAGIC - DA
# MAGIC - DA.paths

# COMMAND ----------

print(DA)

# COMMAND ----------

# MAGIC %md
# MAGIC この `DA` というのは、`Include` 内でグローバルに定義している。
# MAGIC
# MAGIC ```python
# MAGIC from dbacademy.dbhelper import DBAcademyHelper, Paths, CourseConfig, LessonConfig
# MAGIC :
# MAGIC DA = DBAcademyHelper(course_config=course_config,
# MAGIC                      lesson_config=lesson_config)
# MAGIC DA.reset_lesson()
# MAGIC DA.init()
# MAGIC ```
# MAGIC
# MAGIC DBAcademyHelper の API リファレンス・・・は無かった。「Academy」用なので商用で使うものでもないようだし、深追いはやめる。

# COMMAND ----------

print(DA.paths)

# COMMAND ----------

print(DA.paths.kafka_events)

# COMMAND ----------

files = dbutils.fs.ls(DA.paths.kafka_events)
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ dbutils 
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.help()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ データを表示してみる
# MAGIC
# MAGIC パスを一重引用符ではなくバックティックで囲っている。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ Python でデータ抽出
# MAGIC
# MAGIC DatabricksにおけるSpark SQLの```SELECT * FROM json.`${DA.paths.kafka_events}/001.json` ``` と同等の操作をPythonで行う場合、`spark.read.json`を使用します。この関数は、指定されたJSONファイルを読み込み、データフレームとして返します。
# MAGIC
# MAGIC 以下に、Pythonコードの具体例を示します。
# MAGIC
# MAGIC #### Pythonコード例
# MAGIC
# MAGIC ```python
# MAGIC # Databricksのノートブックで実行する場合のPythonコード
# MAGIC
# MAGIC # 必要なライブラリのインポート
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # SparkSessionの作成
# MAGIC spark = SparkSession.builder.appName("example").getOrCreate()
# MAGIC
# MAGIC # JSONファイルのパス
# MAGIC json_file_path = f"{DA.paths.kafka_events}/001.json"
# MAGIC
# MAGIC # JSONファイルを読み込みデータフレームとして保持
# MAGIC df = spark.read.json(json_file_path)
# MAGIC
# MAGIC # データフレームの内容を表示
# MAGIC df.show()
# MAGIC ```
# MAGIC
# MAGIC #### 解説
# MAGIC
# MAGIC 1. **ライブラリのインポート**：
# MAGIC    `pyspark.sql`から`SparkSession`をインポートします。
# MAGIC
# MAGIC 2. **SparkSessionの作成**：
# MAGIC    Sparkセッションを作成します。Databricksノートブックでは、既にSparkセッションが存在する場合が多いので、このステップは省略しても構いません。
# MAGIC
# MAGIC 3. **JSONファイルのパス設定**：
# MAGIC    `json_file_path`変数にJSONファイルのパスを設定します。ここで、`DA.paths.kafka_events`は事前に定義されているパスと仮定しています。
# MAGIC
# MAGIC 4. **JSONファイルの読み込み**：
# MAGIC    `spark.read.json`を使用して、指定されたJSONファイルを読み込み、データフレームとして保持します。
# MAGIC
# MAGIC 5. **データフレームの表示**：
# MAGIC    `df.show()`を使用して、データフレームの内容を表示します。
# MAGIC
# MAGIC このコードは、Spark SQLでJSONファイルをクエリする操作と同等の結果をPythonで得るためのものです。ファイルパスが正しく設定されていることを確認し、必要に応じてパスを修正してください。

# COMMAND ----------

# 必要なライブラリのインポート
from pyspark.sql import SparkSession

# SparkSessionの作成
# spark = SparkSession.builder.appName("example").getOrCreate()

# JSONファイルのパス
json_file_path = f"{DA.paths.kafka_events}/001.json"

# JSONファイルを読み込みデータフレームとして保持
df = spark.read.json(json_file_path)

# データフレームの内容を表示
df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 複数のファイルの抽出
# MAGIC
# MAGIC - 参考
# MAGIC   - DE 2.1 `ファイルのディレクトリを照会する（Query a Directory of Files）`
# MAGIC
# MAGIC Databricksにおいて、ディレクトリ内のファイルが全て同じ形式とスキーマを持っている場合、個別のファイルではなくディレクトリパスを指定することで全てのファイルを同時にクエリすることが可能である。この手法により、大量のファイルを効率的に処理することができる。
# MAGIC
# MAGIC #### SQLによる例
# MAGIC
# MAGIC SQLを用いてディレクトリ内の全てのファイルをクエリする場合、以下のように`SELECT`文を使用する。例えば、`/mnt/data`というディレクトリにCSVファイルが格納されているとする。
# MAGIC
# MAGIC ```sql
# MAGIC SELECT * 
# MAGIC FROM '/mnt/data'
# MAGIC ```
# MAGIC
# MAGIC このクエリは、`/mnt/data`ディレクトリ内の全てのCSVファイルを対象とし、一つのテーブルとしてデータを取得する。全てのファイルが同じ形式とスキーマを持っているため、この方法で効率的にデータを処理できる。
# MAGIC
# MAGIC #### Pythonによる例
# MAGIC
# MAGIC Pythonを用いて同様の操作を行う場合、PySparkを利用する。以下のコードは、`/mnt/data`ディレクトリ内の全てのParquetファイルを読み込み、データフレームとして扱う例である。
# MAGIC
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # ディレクトリ内のParquetファイルを読み込む
# MAGIC df = spark.read.parquet("/mnt/data")
# MAGIC
# MAGIC # データフレームの内容を表示
# MAGIC df.show()
# MAGIC ```
# MAGIC
# MAGIC json の例: 
# MAGIC ```python
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # ディレクトリ内のJSONファイルを読み込む
# MAGIC df = spark.read.json("/mnt/data")
# MAGIC
# MAGIC # データフレームの内容を表示
# MAGIC df.show()
# MAGIC ```
# MAGIC
# MAGIC このコードにより、`/mnt/data`ディレクトリ内の全てのParquetファイルを一つのデータフレームにまとめることができる。ディレクトリパスを指定することで、各ファイルを個別に読み込む必要がなくなるため、処理が簡潔かつ効率的になる。
# MAGIC
# MAGIC 以上のように、ディレクトリ内の全てのファイルを同時にクエリすることで、データ処理の効率が向上する。Databricksではこの手法を活用することで、大規模データの取り扱いが容易になる。
# MAGIC
# MAGIC #### 利点
# MAGIC
# MAGIC 1. **簡素化されたクエリ**：
# MAGIC    個々のファイルを指定する必要がなく、ディレクトリパスを指定するだけでよいため、コードが簡素化される。
# MAGIC
# MAGIC 2. **効率的なデータ読み込み**：
# MAGIC    ディレクトリ内のすべてのファイルを一度に読み込むことで、ファイルごとのI/O操作を減らし、パフォーマンスが向上する。
# MAGIC
# MAGIC 3. **スケーラビリティ**：
# MAGIC    大規模なデータセットを扱う際に、ファイルが増えてもディレクトリパスを指定するだけで処理が可能。
# MAGIC
# MAGIC 4. **一貫性**：
# MAGIC    同じスキーマと形式を持つファイルを一括で処理するため、データの一貫性が保たれる。
# MAGIC
# MAGIC #### 注意点
# MAGIC
# MAGIC - **形式とスキーマの一致**：
# MAGIC   すべてのファイルが同じ形式（例：CSV、Parquet）とスキーマを持っている必要がある。異なるスキーマを持つファイルが混在しているとエラーが発生する可能性がある。
# MAGIC
# MAGIC - **パスの指定**：
# MAGIC   ディレクトリパスを正しく指定することが重要です。相対パスや絶対パスを適切に使用すること。
# MAGIC
# MAGIC #### まとめ
# MAGIC
# MAGIC ディレクトリにあるファイルが同じ形式とスキーマを持っている場合、ディレクトリパスを指定することで、個々のファイルを指定する手間を省き、効率的にデータをクエリすることができる。これにより、データ処理がシンプルかつ高速になり、大規模なデータセットを扱う際にも非常に有効である。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM json.`${DA.paths.kafka_events}`

# COMMAND ----------

from pyspark.sql import SparkSession

# ディレクトリ内のJSONファイルを読み込む
df = spark.read.json(f"{DA.paths.kafka_events}")

# データフレームの内容を表示
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## ● FROM キーワードの後ろにデータタイプとして含まれている接頭辞を特定する。
# MAGIC
# MAGIC Databricksにおいて、SQLクエリの`FROM`キーワードの後ろに指定されるデータソースの接頭辞には、複数の形式がある。  
# MAGIC 代表的なものとして、`csv`、`json`、`parquet`が挙げられる。これらは、各データ形式に応じてファイルを読み込む際に使用される。
# MAGIC
# MAGIC ### データソースの例
# MAGIC 以下は、DatabricksのSQLクエリでこれらの接頭辞を使用する例。
# MAGIC
# MAGIC - **CSVファイルを読み込む例**:
# MAGIC   ```sql
# MAGIC   SELECT * FROM csv.`/path/to/your/csvfile`
# MAGIC   ```
# MAGIC
# MAGIC - **JSONファイルを読み込む例**:
# MAGIC   ```sql
# MAGIC   SELECT * FROM json.`/path/to/your/jsonfile`
# MAGIC   ```
# MAGIC
# MAGIC - **Parquetファイルを読み込む例**:
# MAGIC   ```sql
# MAGIC   SELECT * FROM parquet.`/path/to/your/parquetfile`
# MAGIC   ```
# MAGIC
# MAGIC ### その他のデータソース
# MAGIC 公式ドキュメントによると、Databricksはこれら以外にも多様なデータソースに対応している。例えば、以下：
# MAGIC
# MAGIC - **Delta Lake**: 
# MAGIC   ```sql
# MAGIC   SELECT * FROM delta.`/path/to/delta/table`
# MAGIC   ```
# MAGIC
# MAGIC - **SQLデータベース**: JDBCを使用してSQLデータベースに接続し、データを取得できる。
# MAGIC   ```sql
# MAGIC   SELECT * FROM jdbc(`url=jdbc:mysql://hostname:port/dbname`, `user`, `password`)
# MAGIC   ```
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● ビュー、一時ビュー、CTE をファイルの参照として作成する
# MAGIC
# MAGIC - 参考
# MAGIC   - DE 2.1 `ファイルへの参照の作成（Create References to Files）`
# MAGIC   - DE 2.1 `ファイルへの一時的な参照の作成（Create Temporary References to Files）`
# MAGIC   - DE 2.1 `クエリー内での参照としてCTEを適用（Apply CTEs for Reference within a Query）`

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ ビュー
# MAGIC
# MAGIC 一般的なViewは、データベースに定義される永続的なビューである。これにより、基礎となるテーブルに対して論理的なビューを提供し、アクセス制御やデータの抽象化を行うことができる。ビューはデータベース内に永続的に保存され、再利用可能である。
# MAGIC
# MAGIC ```sql
# MAGIC -- 永続的なViewの作成
# MAGIC CREATE OR REPLACE VIEW persistent_view AS
# MAGIC SELECT * FROM some_table
# MAGIC ```
# MAGIC
# MAGIC このビューは、データの抽象化や再利用性を高め、特定のユーザーやアプリケーションに対してデータの論理的な視点を提供する。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW event_view_001
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

print(_sqldf)
dbutils.data.summarize(_sqldf)

# これだと結果が _sqldf に設定されない。「java.lang.UnsupportedOperationException: empty.reduceLeft」エラーになる。

# COMMAND ----------

# MAGIC %md
# MAGIC https://docs.databricks.com/ja/notebooks/notebooks-code.html#explore-sql-cell-results-in-python-notebooks-using-python に、 `クエリーでキーワードCACHE TABLEまたはUNCACHE TABLEを使用する場合、結果はPython DataFrameとして表示されません。` と書いてあるので、多分、 `create view`文だからだと思われる。

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW event_view_001
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

df_event_001 = _sqldf
print(df_event_001)
dbutils.data.summarize(df_event_001)

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 一時ビュー
# MAGIC
# MAGIC
# MAGIC Temporary View（一時ビュー）は、セッションの間だけ存在する一時的なビューである。これにより、セッションが終了するとビューも消滅する。一時ビューは、特定のユーザーやセッションに対してデータの一時的な抽象化を提供する。
# MAGIC
# MAGIC ```sql
# MAGIC -- Temporary Viewの作成
# MAGIC CREATE OR REPLACE TEMP VIEW temp_view AS
# MAGIC SELECT * FROM some_table
# MAGIC ```
# MAGIC
# MAGIC このビューは、ユーザーが特定のセッション内でデータを簡単に操作するために利用される。通常、ユニティカタログ（UC）に定義するほどの重要度はない。
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW events_temp_view
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}/001.json`
# MAGIC
# MAGIC -- # テンポラリビュー（Temporary views）は、そのSparkSession内でだけ存在します。Databricksでは、そのノートブック、ジョブ、またはDBSQLクエリ内でだけ有効であるということです。

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from events_temp_view limit 5

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 共通テーブル式 (Common table expressions、CTEs)
# MAGIC - 参考
# MAGIC   - DE 2.1 `クエリー内での参照としてCTEを適用（Apply CTEs for Reference within a Query）` 
# MAGIC   - 公式
# MAGIC     - https://learn.microsoft.com/ja-jp/azure/databricks/sql/language-manual/sql-ref-syntax-qry-select-cte
# MAGIC
# MAGIC #### 使い道
# MAGIC - コードの可読性向上：
# MAGIC   - CTEを使うことで、複雑なクエリを複数の簡潔なステップに分解できます。これにより、クエリ全体の理解が容易になります。
# MAGIC
# MAGIC - 再利用性：
# MAGIC   - 同じサブクエリを複数回使用する必要がある場合、CTEを使えばサブクエリを一度定義して再利用できるため、コードが冗長になりません。
# MAGIC
# MAGIC - 階層構造の処理：
# MAGIC   - 階層的なデータ（例：従業員とマネージャーの関係）を扱う際に、再帰的CTEを使うことができます。
# MAGIC
# MAGIC - 一時テーブルの代替：
# MAGIC   - CTEは一時テーブルのように使用できますが、データベースに一時的なストレージを必要としないため、パフォーマンスが向上する場合があります。
# MAGIC
# MAGIC #### 例
# MAGIC
# MAGIC - 基本的なCTEの例
# MAGIC   - 次の例では、salesテーブルから特定の条件を満たすデータを一時的に保存し、それをメインクエリで使用しています。
# MAGIC     ```sql
# MAGIC     WITH filtered_sales AS (
# MAGIC         SELECT product_id, SUM(quantity) as total_quantity
# MAGIC         FROM sales
# MAGIC         WHERE sale_date >= '2024-01-01'
# MAGIC         GROUP BY product_id
# MAGIC     )
# MAGIC     SELECT product_id, total_quantity
# MAGIC     FROM filtered_sales
# MAGIC     WHERE total_quantity > 100
# MAGIC     ```
# MAGIC - 再帰的CTEの例
# MAGIC   - 階層構造を持つ従業員テーブルを使って、特定のマネージャーの全ての部下を再帰的に取得します。
# MAGIC     ```sql
# MAGIC     WITH RECURSIVE EmployeeHierarchy AS (
# MAGIC         SELECT employee_id, manager_id, employee_name
# MAGIC         FROM employees
# MAGIC         WHERE manager_id IS NULL
# MAGIC         UNION ALL
# MAGIC         SELECT e.employee_id, e.manager_id, e.employee_name
# MAGIC         FROM employees e
# MAGIC         INNER JOIN EmployeeHierarchy eh ON e.manager_id = eh.employee_id
# MAGIC     )
# MAGIC     SELECT * FROM EmployeeHierarchy
# MAGIC     ```

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 他の View
# MAGIC
# MAGIC Databricksでは、SQLクエリの結果を保存・再利用するためにビュー(View)が利用される。ビューは、基礎となるデータに対して仮想的なテーブルを作成し、データの抽象化やアクセス制御を容易にする。
# MAGIC
# MAGIC #### Global Temporary View
# MAGIC
# MAGIC Global Temporary View（グローバル一時ビュー）は、Databricksクラスタ全体で共有される一時ビューである。ビューは`global_temp`データベースに格納され、クラスタ内のすべてのセッションでアクセス可能である。ただし、クラスタのライフサイクルに依存するため、クラスタがシャットダウンされるとビューも消失する。
# MAGIC
# MAGIC ```sql
# MAGIC -- Global Temporary Viewの作成
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW global_temp_view AS
# MAGIC SELECT * FROM some_table
# MAGIC ```
# MAGIC
# MAGIC このビューは、複数のセッション間で一時的にデータを共有するために使用される。
# MAGIC
# MAGIC #### Materialized View
# MAGIC
# MAGIC Materialized View（マテリアライズドビュー）は、定期的に更新される結果をキャッシュするビューである。これにより、クエリのパフォーマンスが向上する。Materialized Viewは、基礎となるデータのスナップショットを保持し、アクセス時にクエリの実行を避けるため、特に大規模データセットに対して有効である。
# MAGIC
# MAGIC ```sql
# MAGIC -- Materialized Viewの作成
# MAGIC CREATE MATERIALIZED VIEW materialized_view AS
# MAGIC SELECT * FROM some_table
# MAGIC ```
# MAGIC
# MAGIC マテリアライズドビューは、データの更新頻度とクエリのパフォーマンス要件に応じて定期的にリフレッシュされる。
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 外部ソースからのテーブルが Delta Lake テーブルでないことを特定する。
# MAGIC
# MAGIC ### 概要
# MAGIC Databricksを使用してデータ管理を行う際、外部ソースからテーブルをインポートすることがある。これらのテーブルが必ずしもDelta Lake形式であるとは限らない。本稿では、外部ソースからのテーブルがDelta Lakeテーブルでないことに関する情報を整理する。
# MAGIC
# MAGIC ### 外部ソースからのテーブル
# MAGIC 外部ソースからインポートされるテーブルは、多岐にわたるデータフォーマットを持つ場合がある。以下はその主要な例である。
# MAGIC
# MAGIC - **CSV（Comma-Separated Values）**: 一般的なテキストベースのデータフォーマット。各レコードがコンマで区切られた値として表現される。
# MAGIC - **Parquet**: 高効率の列指向ストレージフォーマット。大規模なデータ分析処理に適している。
# MAGIC - **JSON**: データ交換フォーマットとして広く使われている。構造化データを容易に表現できる。
# MAGIC - **Avro**: Apacheプロジェクトの一環で、スキーマ記述言語に基づくデータシリアライズのためのフォーマット。
# MAGIC - **ORC（Optimized Row Columnar）**: Hadoopエコシステムで利用されるフォーマット。効率的な圧縮とクエリ性能を提供する。
# MAGIC
# MAGIC ### Delta Lakeテーブルとは
# MAGIC Delta Lakeは、Apache Sparkの拡張として開発されたストレージレイヤーであり、次の特徴を持つ。
# MAGIC
# MAGIC - **ACIDトランザクション**: データの一貫性と耐障害性を確保するためのトランザクション管理を提供。
# MAGIC - **スキーマエンフォースメント**: データのスキーマが変更されると、エラーを検出して防ぐ機能。
# MAGIC - **スキーマエボリューション**: 新しいデータのスキーマを柔軟に進化させる機能。
# MAGIC - **高パフォーマンス**: キャッシュやインデックスの利用によりクエリ性能を向上させる。
# MAGIC
# MAGIC Delta Lakeテーブルは、これらの特徴を持たない他の外部ソースフォーマットと対照的である。
# MAGIC
# MAGIC ### 外部ソースからDelta Lakeへ変換する必要性
# MAGIC 外部ソースからのテーブルがDelta Lakeでない場合、次の理由からDelta Lakeテーブルへの変換が推奨される。
# MAGIC
# MAGIC - **データの一貫性と信頼性向上**: ACIDトランザクションによる。
# MAGIC - **データの管理とクエリ性能向上**: 高効率なストレージとクエリ処理。
# MAGIC - **スキーマ管理の容易化**: スキーマエンフォースメントとスキーマエボリューション機能。
# MAGIC
# MAGIC ### 変換方法の例
# MAGIC 外部ソースからのテーブルをDelta Lakeフォーマットに変換する方法は、多様であるが、以下に典型的な手法を示す。
# MAGIC
# MAGIC - **CSVからの変換**
# MAGIC     ```python
# MAGIC     df = spark.read.csv("path/to/csv/file")
# MAGIC     df.write.format("delta").save("path/to/delta/table")
# MAGIC     ```
# MAGIC
# MAGIC - **Parquetからの変換**
# MAGIC     ```python
# MAGIC     df = spark.read.parquet("path/to/parquet/file")
# MAGIC     df.write.format("delta").save("path/to/delta/table")
# MAGIC     ```
# MAGIC
# MAGIC これにより、外部ソースから提供されたデータがDelta Lakeテーブルとして活用可能になる。
# MAGIC
# MAGIC ### まとめ
# MAGIC 外部ソースからのテーブルは必ずしもDelta Lakeテーブルではないことが多くのケースである。Delta Lakeの特長を活かすために、外部ソースからのデータをDelta Lake形式に変換することが推奨される。多種多様なデータフォーマットに対応した変換手法を活用することで、データの一貫性、クエリ性能、管理の容易さが向上する。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● JDBC 接続と外部 CSV ファイルからテーブルを作成する
# MAGIC
# MAGIC - 参考
# MAGIC   - https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html
# MAGIC
# MAGIC ### 概要
# MAGIC Databricksを用いる環境でデータレイクやデータウェアハウスの一部として動作させる際に、多様なデータソースからのデータインジェストが求められることが多い。この記事では、JDBC接続と外部CSVファイルを用いてテーブルを作成する方法について詳述する。
# MAGIC
# MAGIC ### JDBC 接続からテーブルを作成する
# MAGIC
# MAGIC #### JDBC接続とは
# MAGIC JDBC（Java Database Connectivity）は、Javaプログラムからデータベースに接続し、SQLクエリを実行するためのAPIである。Databricksでは多様なデータベース（MySQL、PostgreSQL、SQL Serverなど）に対してJDBCを介して接続することが可能である。
# MAGIC
# MAGIC #### JDBC接続の手順
# MAGIC 1. **JDBCドライバの設定**
# MAGIC     - 接続先のデータベースに対応するJDBCドライバを準備し、Databricksクラスタにアップロードしなければならない。
# MAGIC
# MAGIC 2. **接続プロパティの設定**
# MAGIC     - 接続するためのプロパティ（URL、ユーザー名、パスワードなど）を定義する。
# MAGIC       ```python
# MAGIC       jdbc_url = "jdbc:mysql://your-database-url:3306/your-database-name"
# MAGIC       connection_properties = {
# MAGIC           "user" : "your-username",
# MAGIC           "password" : "your-password",
# MAGIC           "driver" : "com.mysql.jdbc.Driver"
# MAGIC       }
# MAGIC       ```
# MAGIC
# MAGIC 3. **データの読み込みおよびテーブルの作成**
# MAGIC     - JDBC接続を通じてデータベースからデータを読み込み、それをDatabricksのテーブルとして保存する。
# MAGIC       ```python
# MAGIC       df = spark.read.jdbc(jdbc_url, "your-table-name", properties=connection_properties)
# MAGIC       df.createOrReplaceTempView("your_temp_table_name")
# MAGIC       ```
# MAGIC
# MAGIC ### 外部CSVファイルからテーブルを作成する
# MAGIC
# MAGIC #### CSVファイルとは
# MAGIC CSV（Comma-Separated Values）ファイルは、一般的なテキストデータフォーマットである。各レコードは行単位で記載され、各フィールドはコンマで区切られている。データエクスチェンジの際によく用いられる形式である。
# MAGIC
# MAGIC #### CSVファイルからテーブル作成の手順
# MAGIC 1. **CSVファイルの読込み**
# MAGIC     - 外部CSVファイルをDatabricksの環境に読み込み、DataFrameを作成する。
# MAGIC       ```python
# MAGIC       df = spark.read.csv("path/to/csv/file.csv", header=True, inferSchema=True)
# MAGIC       ```
# MAGIC
# MAGIC 2. **テーブルの作成**
# MAGIC     - 読み込んだDataFrameを利用して、Databricks内のテーブルを作成する。
# MAGIC       ```python
# MAGIC       df.createOrReplaceTempView("csv_temp_table")
# MAGIC       ```
# MAGIC
# MAGIC ## 結論
# MAGIC Databricksでは、JDBC接続や外部CSVファイルを利用して容易にデータをインジェストし、テーブルを作成することができる。これにより、多様なデータソースを集約し、一元的に管理するデータレイクやデータウェアハウスの構築が容易になる。各手法の手順に従って適切に設定することで、信頼性の高いデータ管理環境を実現できる。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● count_if 関数の使用方法と、x が null のカウントの使用方法を特定する
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### データ準備

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW event_view_001
# MAGIC AS SELECT * FROM json.`${DA.paths.kafka_events}/001.json`

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from event_view_001 limit 10;

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
# MAGIC ### count_if 関数の使用方法
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
# MAGIC   count_if(partition >= 1) AS partition_above1, 
# MAGIC   count_if(partition == 0) AS partition_0, 
# MAGIC   count_if(topic =="clickstream") as topic_clickstream,
# MAGIC   count_if(topic !="clickstream") as topic_not_clickstream
# MAGIC FROM event_view_001;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC ### x が null のカウントの使用方法
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
# MAGIC ## ● 未整理

# COMMAND ----------

# MAGIC %md
# MAGIC ### ○ 読み取りオプション未使用で上手く読み込めない例

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM csv.`${DA.paths.sales_csv}`

# COMMAND ----------

# MAGIC %md
# MAGIC 上記から次のことが分かります：
# MAGIC
# MAGIC 1. ヘッダの列がテーブルの列として抽出されています
# MAGIC 1. すべての列が１つの列として読み込まれています
# MAGIC 1. ファイルはパイプ（ | ）区切りを使用しています
# MAGIC 1. 最後の列には、切り捨てられるネスト化されたデータが含まれています。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ● 
# MAGIC
# MAGIC ### ○ 
# MAGIC
# MAGIC ### ○ セミナーでの説明（汚いメモ）を補足として追記
# MAGIC ```
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC ### ○ 参考記事
# MAGIC - XXX
# MAGIC   - https://
# MAGIC

# COMMAND ----------

