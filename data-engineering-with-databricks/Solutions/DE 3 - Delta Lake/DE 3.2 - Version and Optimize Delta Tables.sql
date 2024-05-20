-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-0d527322-1a21-4a91-bc34-e7957e052a75
-- MAGIC %md
-- MAGIC # Delta Lakeにおけるバージョニング、最適化、バキューム(Versioning, Optimization, Vacuuming in Delta Lake)
-- MAGIC
-- MAGIC Delta Lakeを使った基本的なデータタスクに慣れてきたので、Delta Lake独自のいくつかの機能について説明します。
-- MAGIC
-- MAGIC ここで使用するキーワードには標準的なANSI SQLの一部ではないものもありますが、SQLを使ってDatabricksでのDelta Lakeの全操作が可能です。
-- MAGIC
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このレッスンでは、以下のことが学べます。
-- MAGIC *  **`OPTIMIZE`** を使って小さなファイルを圧縮する
-- MAGIC *  **`ZORDER`** を使ってテーブルにインデックスを付ける
-- MAGIC * Delta Lakeファイルのディレクトリ構造を記述する
-- MAGIC * テーブルトランザクション履歴を確認する
-- MAGIC * 以前のテーブルバージョンを照会して、そのバージョンにロールバックする
-- MAGIC *  **`VACUUM`** で古いデータファイルをクリーンアップする
-- MAGIC
-- MAGIC **リソース**
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-optimize.html" target="_blank">Delta Optimize - Databricksドキュメント</a>
-- MAGIC * <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-vacuum.html" target="_blank">Delta Vacuum - Databricksドキュメント</a>

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef1115dd-7242-476a-a929-a16aa09ce9c1
-- MAGIC %md
-- MAGIC ## セットアップを実行する（Run Setup）
-- MAGIC まずはセットアップスクリプトを実行します。 セットアップスクリプトは、ユーザー名、ユーザーホーム、各ユーザーを対象とするスキーマを定義します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-b10dbe8f-e936-4ca3-9d1e-8b471c4bc162
-- MAGIC %md
-- MAGIC ## 履歴のあるDeltaテーブルを作成する（Creating a Delta Table with History）
-- MAGIC
-- MAGIC
-- MAGIC このクエリが実行されるのを待つ間に、実行されるトランザクションの総数を確認できるかどうか考えてください。

-- COMMAND ----------

CREATE TABLE students
  (id INT, name STRING, value DOUBLE);
  
INSERT INTO students VALUES (1, "Yve", 1.0);
INSERT INTO students VALUES (2, "Omar", 2.5);
INSERT INTO students VALUES (3, "Elia", 3.3);

INSERT INTO students
VALUES 
  (4, "Ted", 4.7),
  (5, "Tiffany", 5.5),
  (6, "Vini", 6.3);
  
UPDATE students 
SET value = value + 1
WHERE name LIKE "T%";

DELETE FROM students 
WHERE value > 6;

CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
  (2, "Omar", 15.2, "update"),
  (3, "", null, "delete"),
  (7, "Blue", 7.7, "insert"),
  (11, "Diya", 8.8, "update");
  
MERGE INTO students b
USING updates u
ON b.id=u.id
WHEN MATCHED AND u.type = "update"
  THEN UPDATE SET *
WHEN MATCHED AND u.type = "delete"
  THEN DELETE
WHEN NOT MATCHED AND u.type = "insert"
  THEN INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-e932d675-aa26-42a7-9b55-654ac9896dab
-- MAGIC %md
-- MAGIC ## テーブルの詳細を調べる（Examine Table Details）
-- MAGIC
-- MAGIC DatabricksはデフォルトではHiveメタストアを使用して、スキーマ、テーブル、ビューを登録します。
-- MAGIC
-- MAGIC  **`DESCRIBE EXTENDED`** を使うと、テーブルに関する重要なメタデータを確認できます。

-- COMMAND ----------

DESCRIBE EXTENDED students

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6be5873-30b3-4e7e-9333-2c1e6f1cbe25
-- MAGIC %md
-- MAGIC **`DESCRIBE DETAIL`** はテーブルのメタデータを確認できるもう一つのコマンドです。

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-fd7b24fa-7a2d-4f31-ab7c-fcc28d617d75
-- MAGIC %md
-- MAGIC **`Location`** 項目に注意してください。
-- MAGIC
-- MAGIC これまでは、テーブルをデータベース内の単なるリレーショナルエンティティとして考えてきましたが、Delta Lakeテーブルは実のところ、クラウドオブジェクトストレージに保存されたファイルのコレクションを元にしています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-0ff9d64a-f0c4-4ee6-a007-888d4d082abe
-- MAGIC %md
-- MAGIC ## Delta Lakeファイルを調べる（Explore Delta Lake Files）
-- MAGIC
-- MAGIC Databricksユーティリティ機能を使うと、Delta Lakeテーブルの元となるファイルを確認できます。
-- MAGIC
-- MAGIC **注**：今のところ、Delta Lakeで作業するにはこれらのファイルについてすべてを知ることは重要ではありませんが、技術がどのように実装されているかについて理解を深めるのに役立ちます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-1a84bb11-649d-463b-85ed-0125dc599524
-- MAGIC %md
-- MAGIC ディレクトリには、多数のParquetデータファイルと **`_delta_log`** というのディレクトリが含まれていることに注意してください。
-- MAGIC
-- MAGIC Delta Lakeテーブルのレコードは、Parquetファイルにデータとして保存されています。
-- MAGIC
-- MAGIC Delta Lakeテーブルへのトランザクションは **`_delta_log`** に記録されます。
-- MAGIC
-- MAGIC **`_delta_log`** の中をのぞいて、詳細を確認できます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students/_delta_log"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-dbcbd76a-c740-40be-8893-70e37bd5e0d2
-- MAGIC %md
-- MAGIC 各トランザクションごとに、新しいJSONファイルがDelta Lakeトランザクションログに書き込まれます。 
-- MAGIC ここでは、このテーブル（Delta Lakeのインデックスは0から始まります）に対して合計8つのトランザクションがあることがわかります。

-- COMMAND ----------

-- DBTITLE 0,--i18n-101dffc0-260a-4078-97db-cb1de8d705a8
-- MAGIC %md
-- MAGIC ## データファイルについて推論する（Reasoning about Data Files）
-- MAGIC
-- MAGIC 明らかにとても小さなテーブルについて、たくさんのデータファイルを確認したところです。
-- MAGIC
-- MAGIC **`DESCRIBE DETAIL`** で、ファイル数など、Deltaテーブルについてのいくつかの他の詳細を確認することができます。

-- COMMAND ----------

DESCRIBE DETAIL students

-- COMMAND ----------

-- DBTITLE 0,--i18n-cb630727-afad-4dde-9d71-bcda9e579de9
-- MAGIC %md
-- MAGIC ここでは、現バージョンのテーブルに、現在4つのデータファイルが含まれていることがわかります。
-- MAGIC では、なぜこれら他のParquetファイルがテーブルディレクトリに入っているのでしょうか？
-- MAGIC
-- MAGIC Delta Lakeは、変更データを含むファイルを上書きしたり、すぐに削除したりするのではなく、トランザクションログを使って、現バージョンのテーブルでファイルが有効であるかどうかを示します。
-- MAGIC
-- MAGIC ここでは、レコードが挿入され、更新され、削除された上の **`MERGE`** 文に対応するトランザクションログを見てみます。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(spark.sql(f"SELECT * FROM json.`{DA.paths.user_db}/students/_delta_log/00000000000000000007.json`"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3221b77b-6d57-4654-afc3-dcb9dfa62be8
-- MAGIC %md
-- MAGIC **`add`** 列には、テーブルに書き込まれる新しいすべてのファイルのリストが含まれています。 **`remove`** 列は、もうテーブルに含めるべきでないファイルを示しています。
-- MAGIC
-- MAGIC Delta Lakeテーブルを照会する場合、クエリエンジンはトランザクションログを使って、現バージョンで有効なファイルをすべて特定し、他のデータファイルはすべて無視します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc6dee2e-406c-48b2-9780-74408c93162d
-- MAGIC %md
-- MAGIC ## 小さなファイルの圧縮とインデックスの作成（Compacting Small Files and Indexing）
-- MAGIC
-- MAGIC ファイルが小さくなってしまうのにはさまざま理由があります。今回の例では数多くの操作を行い、1つまたは複数のレコードが挿入されました。
-- MAGIC
-- MAGIC  **`OPTIMIZE`** コマンドを使うと、ファイルは最適なサイズ（テーブルの大きさに基づいて調整）になるように結合されます。
-- MAGIC
-- MAGIC  **`OPTIMIZE`** は、レコードを結合させて結果を書き直すことで、既存のデータを置き換えます。
-- MAGIC
-- MAGIC  **`OPTIMIZE`** を実行する場合、ユーザーはオプションで **`ZORDER`** インデックスのために、1つか複数のフィールドを指定できます。 Zオーダーの具体的な計算方法は重要ではありませんが、データファイル内で似たような値のデータを同じ位置に配置することにより、指定したフィールドでフィルタリングする際のデータ検索を高速化します。

-- COMMAND ----------

OPTIMIZE students
ZORDER BY id

-- COMMAND ----------

-- DBTITLE 0,--i18n-5f412c12-88c7-4e43-bda2-60ec5c749b2a
-- MAGIC %md
-- MAGIC ここで扱うデータは非常に小さいので、 **`ZORDER`** のメリットは何もありませんが、この操作によって生じるメトリックスをすべて確認できます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-2ad93f7e-4bb1-4051-8b9c-b685164e3b45
-- MAGIC %md
-- MAGIC ## Delta Lakeトランザクションの確認（Reviewing Delta Lake Transactions）
-- MAGIC
-- MAGIC Delta Lakeテーブルの変更はすべてトランザクションログに保存されるので、<a href="https://docs.databricks.com/spark/2.x/spark-sql/language-manual/describe-history.html" target="_blank">テーブルの履歴</a>は簡単に確認できます。

-- COMMAND ----------

DESCRIBE HISTORY students

-- COMMAND ----------

-- DBTITLE 0,--i18n-ed297545-7997-4e75-8bf6-0c204a707956
-- MAGIC %md
-- MAGIC 期待通り、 **`OPTIMIZE`** はテーブルの別バージョンを作成したので、バージョン8が最新のバージョンになります。
-- MAGIC
-- MAGIC トランザクションログに削除済みと印を付けられた追加のデータファイルがあったのを覚えていますか？ これにより、テーブルの以前のバージョンを照会できます。
-- MAGIC
-- MAGIC こうしたタイムトラベルクエリは、整数のバージョンかタイムスタンプのいずれかを指定することで実行できます。
-- MAGIC
-- MAGIC **注**：ほとんどの場合、タイムスタンプを使って関心がある時点のデータを再作成します。 （いつこのデモを実行しているかは分からないので）デモではバージョンを使用します。

-- COMMAND ----------

SELECT * 
FROM students VERSION AS OF 3

-- COMMAND ----------

-- DBTITLE 0,--i18n-d1d03156-6d88-4d4c-ae8e-ddfe49d957d7
-- MAGIC %md
-- MAGIC タイムトラベルについて注意すべきなのは、現バージョンに対するトランザクションを取り消すことにより、以前の状態のテーブルを再作成しているわけではなく、指定されたバージョンの時点で有効と示されたすべてのデータファイルを照会しているだけだということです。

-- COMMAND ----------

-- DBTITLE 0,--i18n-78cf75b0-0403-4aa5-98c7-e3aabbef5d67
-- MAGIC %md
-- MAGIC ## Rollback Versions
-- MAGIC
-- MAGIC テーブルから手動でいくつかのレコードを削除するクエリを書いていて、うっかりこのクエリを次の状態で実行するとします。

-- COMMAND ----------

DELETE FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-7f7936c3-3aa2-4782-8425-78e6e7634d79
-- MAGIC %md
-- MAGIC 削除の影響を受けた列の数に **`-1`** が表示されている場合、データのディレクトリ全体が削除されたことに注意してください。
-- MAGIC
-- MAGIC 以下でこれを確認しましょう。

-- COMMAND ----------

SELECT * FROM students

-- COMMAND ----------

-- DBTITLE 0,--i18n-9d3908f4-e6bb-40a6-92dd-d7d12d28a032
-- MAGIC %md
-- MAGIC テーブルのすべてのレコードを削除することは、たぶん望んだ結果ではありません。 幸い、このコミットを簡単にロールバックすることができます。

-- COMMAND ----------

RESTORE TABLE students TO VERSION AS OF 8

-- COMMAND ----------

-- DBTITLE 0,--i18n-902966c3-830a-44db-9e59-dec82b98a9c2
-- MAGIC %md
-- MAGIC **`RESTORE`** <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">コマンド</a>がトランザクションとして記録されていることに注意してください。うっかり、テーブルのレコードをすべて削除してしまったという事実は完全には隠せませんが、この操作を取り消し、テーブルを望ましい状態に戻すことはできます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-847452e6-2668-463b-afdf-52c1f512b8d3
-- MAGIC %md
-- MAGIC ## 古いファイルのクリーンアップ（Cleaning Up Stale Files）
-- MAGIC
-- MAGIC Databricksは、Delta Lakeテーブルの古いログファイル(デフォルトで30日以上昔)を自動的にクリーンアップします。
-- MAGIC
-- MAGIC チェックポイントが書き込まれるたびに、Databricksはこの保持間隔よりも古いログエントリを自動的にクリーンアップします。
-- MAGIC
-- MAGIC Delta Lakeのバージョン管理とタイムトラベルは、少し前のバージョンを照会したり、クエリをロールバックするには素晴らしいのですが、大きなプロダクションテーブルの全バージョンのデータファイルを無期限に手元に置いておくのは非常に高くつきます（またPIIがある場合はコンプライアンスの問題につながる可能性があります）。
-- MAGIC
-- MAGIC 手動で古いデータファイルをパージしたい場合、これは **`VACUUM`** 操作で実行できます。
-- MAGIC
-- MAGIC 次のセルのコメントアウトを外し、 **`0 HOURS`** の保持で実行して、現バージョンだけを保持してください：

-- COMMAND ----------

-- VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-b3af389e-e93f-433c-8d47-b38f8ded5ecd
-- MAGIC %md
-- MAGIC デフォルトでは、 **`VACUUM`** は7日間未満のファイルを削除できないようにします。これは、長時間実行される操作が削除対象ファイルを参照しないようにするためです。 
-- MAGIC Deltaテーブルに対して **`VACUUM`** を実行すると、指定したデータ保持期間以前のバージョンにタイムトラベルで戻れなくなります。 
-- MAGIC デモでは、データ保持期間を**`0 HOURS`** としましたが、これはあくまで機能を示すためであり、通常、本番環境ではこのようなことはしません。
-- MAGIC
-- MAGIC
-- MAGIC 次のセルでは次を行います：
-- MAGIC 1. データファイルの早すぎる削除を防ぐチェックをオフにする
-- MAGIC 1.  **`VACUUM`** コマンドのロギングが有効になっていることを確認する
-- MAGIC 1. VACUUMの **`DRY RUN`** バージョンを使って、削除対象の全レコードを表示する

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

VACUUM students RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c825ee6-e584-48a1-8d75-f616d7ed53ac
-- MAGIC %md
-- MAGIC (DRY RUNの指定を外し) **`VACUUM`** を実行して上の10個のファイルを削除することで、これらのファイルの実体を必要とするバージョンのテーブルへのアクセスを永久に削除します。

-- COMMAND ----------

VACUUM students RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-6574e909-c7ee-4b0b-afb8-8bac83dacdd3
-- MAGIC %md
-- MAGIC テーブルディレクトリを確認して、ファイルが正常に削除されたことを示します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC display(dbutils.fs.ls(f"{DA.paths.user_db}/students"))

-- COMMAND ----------

-- DBTITLE 0,--i18n-3437d5f0-c0e2-4486-8142-413a1849bc40
-- MAGIC %md
-- MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
-- MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
