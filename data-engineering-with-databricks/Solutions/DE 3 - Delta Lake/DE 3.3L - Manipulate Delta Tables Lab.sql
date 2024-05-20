-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>
-- MAGIC

-- COMMAND ----------

-- DBTITLE 0,--i18n-65583202-79bf-45b7-8327-d4d5562c831d
-- MAGIC %md
-- MAGIC # Deltaテーブル操作のラボ(Manipulating Delta Tables Lab)
-- MAGIC
-- MAGIC このノートブックでは、Delta Lakeがデータレイクハウスにもたらす高度な機能のいくつかを実践的に説明します。
-- MAGIC
-- MAGIC
-- MAGIC ## 学習目標（Learning Objectives）
-- MAGIC このラボでは、以下のことが学べます。
-- MAGIC - テーブル履歴の確認
-- MAGIC - 以前のテーブルバージョンを照会して、テーブルを特定のバージョンにロールバックする
-- MAGIC - ファイル圧縮とZ-ORDERインデックスの実行
-- MAGIC - 永久削除の印が付いたファイルをプレビューし、これらの削除をコミットする

-- COMMAND ----------

-- DBTITLE 0,--i18n-065e2f94-2251-4701-b0b6-f4b86323dec8
-- MAGIC %md
-- MAGIC ## セットアップ（Setup）
-- MAGIC 次のスクリプトを実行して必要な変数をセットアップし、このノートブックにおける過去の実行を消去します。 このセルを再実行するとラボを再起動できる点に注意してください。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-03.3L

-- COMMAND ----------

-- DBTITLE 0,--i18n-56940be8-afa9-49d8-8949-b4bcdb343f9d
-- MAGIC %md
-- MAGIC ## Beanコレクションの履歴を再作成する（Recreate the History of your Bean Collection）
-- MAGIC
-- MAGIC 次のセルは様々なテーブル操作を含んでいますが、作成された **`beans`** テーブルのスキーマは次のようになります：
-- MAGIC
-- MAGIC | フィールド名    | フィールド型  |
-- MAGIC | --------- | ------- |
-- MAGIC | name      | STRING  |
-- MAGIC | color     | STRING  |
-- MAGIC | grams     | FLOAT   |
-- MAGIC | delicious | BOOLEAN |

-- COMMAND ----------

CREATE TABLE beans 
(name STRING, color STRING, grams FLOAT, delicious BOOLEAN);

INSERT INTO beans VALUES
("black", "black", 500, true),
("lentils", "brown", 1000, true),
("jelly", "rainbow", 42.5, false);

INSERT INTO beans VALUES
('pinto', 'brown', 1.5, true),
('green', 'green', 178.3, true),
('beanbag chair', 'white', 40000, false);

UPDATE beans
SET delicious = true
WHERE name = "jelly";

UPDATE beans
SET grams = 1500
WHERE name = 'pinto';

DELETE FROM beans
WHERE delicious = false;

CREATE OR REPLACE TEMP VIEW new_beans(name, color, grams, delicious) AS VALUES
('black', 'black', 60.5, true),
('lentils', 'green', 500, true),
('kidney', 'red', 387.2, true),
('castor', 'brown', 25, false);

MERGE INTO beans a
USING new_beans b
ON a.name=b.name AND a.color = b.color
WHEN MATCHED THEN
  UPDATE SET grams = a.grams + b.grams
WHEN NOT MATCHED AND b.delicious = true THEN
  INSERT *;

-- COMMAND ----------

-- DBTITLE 0,--i18n-bf6ff074-4166-4d51-92e5-67e7f2084c9b
-- MAGIC %md
-- MAGIC ## テーブル履歴を確認する（Review the Table History）
-- MAGIC
-- MAGIC Delta Lakeのトランザクションログは、テーブルの内容や設定を変更する各トランザクションについての情報を保存します。
-- MAGIC
-- MAGIC 以下の **`beans`** テーブルの履歴を確認してください。

-- COMMAND ----------

-- ANSWER
DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fb56d746-8889-41c1-ba73-576282582534
-- MAGIC %md
-- MAGIC 以前の全操作が説明通りに完了している場合、テーブルの7つのバージョンが確認できるはずです（**注**：Delta Lakeのバージョン管理は0から始まるので、バージョンの最大値は6です）。
-- MAGIC
-- MAGIC 操作は次のようになるはずです：
-- MAGIC
-- MAGIC | バージョン | 操作 |
-- MAGIC | --- | --- |
-- MAGIC | 0   | CREATE TABLE |
-- MAGIC | 1   | WRITE        |
-- MAGIC | 2   | WRITE        |
-- MAGIC | 3   | UPDATE       |
-- MAGIC | 4   | UPDATE       |
-- MAGIC | 5   | DELETE       |
-- MAGIC | 6   | MERGE        |
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC  **`operationsParameters`** 列で、更新、削除、マージに使用した述語を確認できます。
-- MAGIC  **`operationMetrics`** 列は、各操作で追加された行とファイルの数を示しています。
-- MAGIC
-- MAGIC 時間をとってDelta Lakeの履歴を確認し、どのテーブルバージョンがどのトランザクションと一致しているかを理解してください。
-- MAGIC
-- MAGIC **注**： **`version`** 列は、特定のトランザクションが完了した時点でのテーブルの状態を指定しています。  **`readVersion`** 列は、実行された操作の対象となったテーブルのバージョンを示しています。 この単純な（並列のトランザクションがない）デモでは、この関係は常に1ずつ増加するはずです。

-- COMMAND ----------

-- DBTITLE 0,--i18n-00d8e251-9c9e-4be3-b8e7-6e38b07fac55
-- MAGIC %md
-- MAGIC ## 特定のバージョンを照会する（Query a Specific Version）
-- MAGIC
-- MAGIC テーブル履歴を確認した後、一番最初のデータが挿入された後のテーブルの状態を見たいとします。
-- MAGIC
-- MAGIC 以下のクエリを実行して、その状態を確認しましょう。

-- COMMAND ----------

SELECT * FROM beans VERSION AS OF 1

-- COMMAND ----------

-- DBTITLE 0,--i18n-90e3c115-6bed-4b83-bb37-dd45fb92aec5
-- MAGIC %md
-- MAGIC そして今度は、データの現在の状態を確認します。

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-f073a6d9-3aca-41a0-9452-a278fb87fa8c
-- MAGIC %md
-- MAGIC レコードを削除する前に、beanの重量を確認したいとします。
-- MAGIC
-- MAGIC 下の文を書き込んで、データが削除される直前のバージョンのテンポラリビューを登録してから、次のセルを実行して、そのビューを照会してください。

-- COMMAND ----------

-- ANSWER
CREATE OR REPLACE TEMP VIEW pre_delete_vw AS
  SELECT * FROM beans VERSION AS OF 4;

-- COMMAND ----------

SELECT * FROM pre_delete_vw

-- COMMAND ----------

-- DBTITLE 0,--i18n-bad13c31-d91f-454e-a14e-888d255dc8a4
-- MAGIC %md
-- MAGIC 以下のセルを実行して、正しいバージョンを取り込んだことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC assert spark.table("pre_delete_vw"), "Make sure you have registered the temporary view with the provided name `pre_delete_vw`"
-- MAGIC assert spark.table("pre_delete_vw").count() == 6, "Make sure you're querying a version of the table with 6 records"
-- MAGIC assert spark.table("pre_delete_vw").selectExpr("int(sum(grams))").first()[0] == 43220, "Make sure you query the version of the table after updates were applied"

-- COMMAND ----------

-- DBTITLE 0,--i18n-8450d1ef-c49b-4c67-9390-3e0550c9efbc
-- MAGIC %md
-- MAGIC ## 以前のバージョンを復元する（Restore a Previous Version）
-- MAGIC
-- MAGIC どうやら誤解があったようです。友人がくれたと思い、コレクションにマージしたbeanは、くれるつもりだったものではありませんでした。
-- MAGIC
-- MAGIC テーブルを、この **`MERGE`** 文が完了する前のバージョンに戻します。

-- COMMAND ----------

-- ANSWER
RESTORE TABLE beans TO VERSION AS OF 5

-- COMMAND ----------

-- DBTITLE 0,--i18n-405edc91-49e8-412b-99e7-96cc60aab32d
-- MAGIC %md
-- MAGIC テーブルの履歴を確認します。 以前のバージョンにリストアすると、別のテーブルのバージョンが追加されることに注意してください。

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.conf.get("spark.databricks.delta.lastCommitVersionInSession")
-- MAGIC assert spark.sql(f"DESCRIBE HISTORY beans").select("operation").first()[0] == "RESTORE", "Make sure you reverted your table with the `RESTORE` keyword"
-- MAGIC assert spark.table("beans").count() == 5, "Make sure you reverted to the version after deleting records but before merging"

-- COMMAND ----------

-- DBTITLE 0,--i18n-d430fe1c-32f1-44c0-907a-62ef8a5ca07b
-- MAGIC %md
-- MAGIC ## ファイルの圧縮（File Compaction）
-- MAGIC 元に戻す間のトランザクションメトリクスを見て、こんなにも小さなデータコレクションにたくさんのファイルがあることに驚きます。
-- MAGIC
-- MAGIC このサイズのテーブルにインデックスをつけてもパフォーマンスが改善する可能性は低いのですが、時間が経つにつれbeanのコレクションが飛躍的に増えることを見込んで、 **`名前`** フィールドにZ-ORDERインデックスを追加することにします。
-- MAGIC
-- MAGIC 以下のセルを使って、ファイル圧縮とZ-ORDERインデックスを実行してください。

-- COMMAND ----------

-- ANSWER
OPTIMIZE beans
ZORDER BY name

-- COMMAND ----------

-- DBTITLE 0,--i18n-8ef4ffb6-c958-4798-b564-fd2e65d4fa0e
-- MAGIC %md
-- MAGIC データは1つのファイルに圧縮されたはずです。次のセルを実行することにより、これを手動で確認してください。

-- COMMAND ----------

DESCRIBE DETAIL beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-8a63081c-1423-43f2-9608-fe846a4a58bb
-- MAGIC %md
-- MAGIC 以下のセルを実行してテーブルを正常に最適化し、インデックスを付けたことを確認してください。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC last_tx = spark.sql("DESCRIBE HISTORY beans").first()
-- MAGIC assert last_tx["operation"] == "OPTIMIZE", "Make sure you used the `OPTIMIZE` command to perform file compaction"
-- MAGIC assert last_tx["operationParameters"]["zOrderBy"] == '["name"]', "Use `ZORDER BY name` with your optimize command to index your table"

-- COMMAND ----------

-- DBTITLE 0,--i18n-6432b28c-18c1-4402-864c-ea40abca50e1
-- MAGIC %md
-- MAGIC ## 古いデータファイルのクリーンアップ（Cleaning Up Stale Data Files）
-- MAGIC
-- MAGIC おわかりのように、今は全データが1つのデータファイルに保存されていますが、以前のバージョンのテーブルのデータファイルがまだ一緒に保存されています。 テーブルに **`VACUUM`** を実行することで、これらのファイルとテーブルの以前のバージョンへのアクセスを削除したいと思います。
-- MAGIC
-- MAGIC  **`VACUUM`** を実行することで、テーブルディレクトリのゴミ掃除を行います。 デフォルトでは、7日間の保持閾値になります。
-- MAGIC  
-- MAGIC 以下のセルはSpark設定を変更します。 最初のコマンドは保持閾値のチェックを無効にするので、データの永久削除を実演できます。
-- MAGIC
-- MAGIC **注**：保持期間の短いプロダクションテーブルをバキューム処理することは、データの破損および/または実行時間の長いクエリの失敗につながるおそれがあります。 これは単なるデモ用で、この設定を無効にする際細心の注意が必要です。
-- MAGIC
-- MAGIC
-- MAGIC 二番目のコマンドは **`spark.databricks.delta.vacuum.logging.enabled`** を **`true`** に設定し、確実に **`VACUUM`** 操作がトランザクションログに保存されるようにします。
-- MAGIC
-- MAGIC **注**：さまざまなクラウドのストレージプロトコルのわずかな違いにより、DBR 9.1の時点では、一部のクラウドについて **`VACUUM`** コマンドのロギングはデフォルトではオンになっていません。

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = false;
SET spark.databricks.delta.vacuum.logging.enabled = true;

-- COMMAND ----------

-- DBTITLE 0,--i18n-04f27ab4-7848-4418-ac79-c339f9843b23
-- MAGIC %md
-- MAGIC データファイルを完全に削除する前に、 **`DRY RUN`** オプションを使ってそれらを手動で確認してください。

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS DRY RUN

-- COMMAND ----------

-- DBTITLE 0,--i18n-bb9ce589-09ae-47b8-b6a6-4ab8e4dc70e7
-- MAGIC %md
-- MAGIC 最新バージョンのテーブルにないデータファイルはすべて、上のプレビューに表示されます。
-- MAGIC
-- MAGIC **`DRY RUN`** を使わずにコマンドを再び実行し、これらのファイルを永久削除してください。
-- MAGIC
-- MAGIC **注**：テーブルのすべての以前のバージョンにはもうアクセスできなくなります。

-- COMMAND ----------

VACUUM beans RETAIN 0 HOURS

-- COMMAND ----------

-- DBTITLE 0,--i18n-1630420a-94f5-43eb-b37c-ccbb46c9ba40
-- MAGIC %md
-- MAGIC **`VACUUM`** は重要なデータセットにとって非常に破壊的な行為となる可能性があるので、保持期間のチェックをオンに戻すのをおすすめします。 以下のセルを実行して、この設定を再び有効にしてください。

-- COMMAND ----------

SET spark.databricks.delta.retentionDurationCheck.enabled = true

-- COMMAND ----------

-- DBTITLE 0,--i18n-8d72840f-49f1-4983-92e4-73845aa98086
-- MAGIC %md
-- MAGIC テーブル履歴が、 **`VACUUM`** 操作を完了したユーザー、削除したファイルの数、この操作中に保持チェックが無効化されたことをログを示すことに注意してください。

-- COMMAND ----------

DESCRIBE HISTORY beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-875b39be-103c-4c70-8a2b-43eaa4a513ee
-- MAGIC %md
-- MAGIC 再度テーブルを照会して、まだ最新バージョンが利用可能なことを確認してください。

-- COMMAND ----------

SELECT * FROM beans

-- COMMAND ----------

-- DBTITLE 0,--i18n-fdb81194-2e3e-4a00-bfb6-97e822ae9ec3
-- MAGIC %md
-- MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" /> Deltaキャッシュは、現在のセッションでクエリされたファイルのコピーを現在アクティブなクラスタにデプロイされたストレージボリュームに保存するため、以前のテーブルバージョンに一時的にアクセスできる可能性があります（しかし、システムをこうした動作を期待するように設計**すべきではありません**）。
-- MAGIC
-- MAGIC クラスタを再起動することで、これらのキャッシュされたデータを確実に永久パージできます。
-- MAGIC
-- MAGIC 次のセルのコメントアウトを外して実行することで、この例を確認できます。セルの実行は、（キャッシュの状態により）失敗するかもしれませんし、失敗しないかもしれません。

-- COMMAND ----------

-- SELECT * FROM beans@v1

-- COMMAND ----------

-- DBTITLE 0,--i18n-a5cfd876-c53a-4d60-96e2-cdbc2b00c19f
-- MAGIC %md
-- MAGIC このラボでは次のことを学びました。
-- MAGIC * 標準的なDelta Lakeテーブルの作成およびデータ操作コマンドの完了
-- MAGIC * テーブル履歴などのテーブルのメタデータの確認
-- MAGIC * Delta Lakeのバージョン管理をスナップショットクエリとロールバックに活用する
-- MAGIC * 小さなファイル群をまとめ、テーブルにインデックスを付ける
-- MAGIC *  **`VACUUM`** を使って、削除の印を付けたファイルを確認し、これらの削除をコミットする

-- COMMAND ----------

-- DBTITLE 0,--i18n-b541b92b-03a9-4f3c-b41c-fdb0ce4f2271
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
