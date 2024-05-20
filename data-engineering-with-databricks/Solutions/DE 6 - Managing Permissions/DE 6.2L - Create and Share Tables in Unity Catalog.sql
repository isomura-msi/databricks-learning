-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks 学習" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-246366e0-139b-4ee6-8231-af9ffc8b9f20
-- MAGIC %md
-- MAGIC # Unity Catalog でテーブルを作成して共有する
-- MAGIC
-- MAGIC このノートブックでは、次の方法を学びます。
-- MAGIC * スキーマとテーブルの作成
-- MAGIC * スキーマとテーブルへのアクセスを制御
-- MAGIC * Unity Catalog 内のさまざまなオブジェクトの権限を調べる

-- COMMAND ----------

-- DBTITLE 0,--i18n-675fb98e-c798-4468-9231-710a39216650
-- MAGIC %md
-- MAGIC ## セットアップ
-- MAGIC
-- MAGIC 次のセルを実行してセットアップを実行します。
-- MAGIC
-- MAGIC 共有トレーニング環境での競合を避けるために、ユーザー専用の固有のカタログ名が生成されます。
-- MAGIC
-- MAGIC 自分の環境では、独自のカタログ名を自由に選択できますが、その環境内の他のユーザーやシステムに影響を与えることに注意してください。

-- COMMAND ----------

-- MAGIC %run ./includes/Classroom-Setup-06.2

-- COMMAND ----------

-- DBTITLE 0,--i18n-1f8f7a5b-7c09-4333-9057-f9e25f635f94
-- MAGIC %md
-- MAGIC ## Unity Catalog の 3 レベルの名前空間
-- MAGIC
-- MAGIC ほとんどの SQL 開発者は、次のように 2 レベルの名前空間を使用してスキーマ内のテーブルを明確にアドレス指定することに慣れています。
-- MAGIC
-- MAGIC     SELECT * FROM schema.table;
-- MAGIC
-- MAGIC Unity Catalog では、オブジェクト階層のスキーマの上に存在する *カタログ* の概念が導入されています。メタストアは任意の数のカタログをホストでき、カタログは任意の数のスキーマをホストできます。この追加レベルに対処するために、Unity Catalog 内の完全なテーブル参照では 3 レベルの名前空間が使用されます。次のステートメントはこれを例示しています。
-- MAGIC
-- MAGIC     SELECT * FROM catalog.schema.table;
-- MAGIC     
-- MAGIC SQL 開発者は、テーブルを参照するときに常にスキーマを指定する必要がないように、デフォルトのスキーマを選択するための **`USE`** ステートメントにも精通しているでしょう。Unity Catalog はこれを **`USE CATALOG`** ステートメントで拡張し、同様にデフォルトのカタログを選択します。
-- MAGIC
-- MAGIC 演習を簡略化するために、次のコマンドでわかるように、カタログが作成されていることを確認し、すでにそれをデフォルトとして設定しておきました。

-- COMMAND ----------

SELECT current_catalog(), current_database()

-- COMMAND ----------

-- DBTITLE 0,--i18n-7608a78c-6f96-4f0b-a9fb-b303c76ad899
-- MAGIC %md
-- MAGIC ## 新しいスキーマを作成して使用する
-- MAGIC
-- MAGIC この演習で使用するためだけに新しいスキーマを作成し、これをデフォルトとして設定して、テーブルを名前だけで参照できるようにします。

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS my_own_schema;
USE my_own_schema;

SELECT current_database()

-- COMMAND ----------

-- DBTITLE 0,--i18n-013687c8-6a3f-4c8b-81c1-0afb0429914f
-- MAGIC %md
-- MAGIC ## デルタ アーキテクチャを作成する
-- MAGIC
-- MAGIC デルタ アーキテクチャに従って、スキーマとテーブルの単純なコレクションを作成して設定してみましょう。
-- MAGIC * 医療機器から読み取られた患者の心拍数データを含むシルバー スキーマ
-- MAGIC * 毎日の患者ごとの心拍数データを平均するゴールド スキーマ テーブル
-- MAGIC
-- MAGIC 今のところ、この単純な例にはブロンズ テーブルはありません。
-- MAGIC
-- MAGIC 上記でデフォルトのカタログとスキーマを設定しているため、以下にテーブル名を指定するだけでよいことに注意してください。

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS patient_silver;

CREATE OR REPLACE TABLE patient_silver.heartrate (
  device_id  INT,
  mrn        STRING,
  name       STRING,
  time       TIMESTAMP,
  heartrate  DOUBLE
);

INSERT INTO patient_silver.heartrate VALUES
  (23,'40580129','Nicholas Spears','2020-02-01T00:01:58.000+0000',54.0122153343),
  (17,'52804177','Lynn Russell','2020-02-01T00:02:55.000+0000',92.5136468131),
  (37,'65300842','Samuel Hughes','2020-02-01T00:08:58.000+0000',52.1354807863),
  (23,'40580129','Nicholas Spears','2020-02-01T00:16:51.000+0000',54.6477014191),
  (17,'52804177','Lynn Russell','2020-02-01T00:18:08.000+0000',95.033344842),
  (37,'65300842','Samuel Hughes','2020-02-01T00:23:58.000+0000',57.3391541312),
  (23,'40580129','Nicholas Spears','2020-02-01T00:31:58.000+0000',56.6165053697),
  (17,'52804177','Lynn Russell','2020-02-01T00:32:56.000+0000',94.8134313932),
  (37,'65300842','Samuel Hughes','2020-02-01T00:38:54.000+0000',56.2469995332),
  (23,'40580129','Nicholas Spears','2020-02-01T00:46:57.000+0000',54.8372685558)

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS patient_gold;

CREATE OR REPLACE TABLE patient_gold.heartrate_stats AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM patient_silver.heartrate
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
  
SELECT * FROM patient_gold.heartrate_stats;  

-- COMMAND ----------

-- DBTITLE 0,--i18n-3961e85e-2bba-460a-9e00-12e37a07cb87
-- MAGIC %md
-- MAGIC ## ゴールド スキーマへのアクセスを許可します [オプション]
-- MAGIC
-- MAGIC 次に、 **account users** グループのユーザーに **gold** スキーマからの読み取りを許可しましょう。
-- MAGIC
-- MAGIC このセクションを実行するには、コード セルのコメントを解除し、順番に実行します。
-- MAGIC いくつかのクエリを実行するように求められます。
-- MAGIC
-- MAGIC これを行うには:
-- MAGIC 1. 別のブラウザー タブを開き、Databricks ワークスペースを読み込みます。
-- MAGIC 1. アプリ スイッチャーをクリックして SQL を選択し、Databricks SQL に切り替えます。
-- MAGIC 1. *Unity Catalog での SQL ウェアハウスの作成* の手順に従って SQL ウェアハウスを作成します。
-- MAGIC 1. その環境で、以下の指示に従ってクエリを入力する準備をします。

-- COMMAND ----------

-- DBTITLE 0,--i18n-e4ec17fc-7dfa-4d3f-a82f-02eca61e6e53
-- MAGIC %md
-- MAGIC **gold** テーブルに対する **SELECT** 権限を付与しましょう。

-- COMMAND ----------

-- GRANT SELECT ON TABLE patient_gold.heartrate_stats to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-d20d5b94-b672-48cf-865a-7c9aa0629955
-- MAGIC %md
-- MAGIC ### ユーザーとしてテーブルをクエリする
-- MAGIC
-- MAGIC **SELECT** 権限を設定して、Databricks SQL 環境でテーブルのクエリを試みます。
-- MAGIC
-- MAGIC 次のセルを実行して、 **gold** テーブルから読み取るクエリ ステートメントを出力します。出力をコピーして SQL 環境内の新しいクエリに貼り付け、クエリを実行します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.patient_gold.heartrate_stats")

-- COMMAND ----------

-- DBTITLE 0,--i18n-3309187d-5a52-4b51-9198-7fc54ea9ca84
-- MAGIC %md
-- MAGIC 私たちはビューの所有者であるため、このコマンドは機能します。ただし、テーブルに対する **SELECT** 権限だけでは不十分であるため、 **アカウント ユーザー** グループの他のメンバーに対してはまだ機能しません。 **USAGE** 権限は、含まれる要素に対しても必要です。次のコマンドを実行して、これを修正しましょう。

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.catalog_name} TO `account users`;
-- GRANT USAGE ON SCHEMA patient_gold TO `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-a6fcc71b-ceb3-4067-acb2-85504ad4d6ea
-- MAGIC %md
-- MAGIC
-- MAGIC Databricks SQL 環境でクエリを繰り返すと、これら 2 つの許可が設定されているため、操作は成功するはずです。

-- COMMAND ----------

-- DBTITLE 0,--i18n-5dd6b7c1-6fed-4d09-b808-0615a10b2502
-- MAGIC %md
-- MAGIC
-- MAGIC ## 権限を調べる
-- MAGIC
-- MAGIC **gold** テーブルから始めて、Unity Catalog 階層内のいくつかのオブジェクトに対する許可を調べてみましょう。

-- COMMAND ----------

-- SHOW GRANT ON TABLE ${DA.catalog_name}.patient_gold.heartrate_stats

-- COMMAND ----------

-- DBTITLE 0,--i18n-e7b6ddda-b6ef-4558-a5b6-c9f97bb7db80
-- MAGIC %md
-- MAGIC 現在、以前に設定した **SELECT** 許可のみがあります。次に、 **シルバー**の権限を確認してみましょう。

-- COMMAND ----------

SHOW TABLES IN ${DA.catalog_name}.patient_silver;

-- COMMAND ----------

-- SHOW GRANT ON TABLE ${DA.catalog_name}.patient_silver.heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-1e9d3c88-af92-4755-a01c-6e0aefdd8c9d
-- MAGIC %md
-- MAGIC 現在、このテーブルには許可がありません。所有者のみがこのテーブルにアクセスできます。
-- MAGIC
-- MAGIC 次に、このテーブルが含まれるスキーマを見てみましょう。

-- COMMAND ----------

-- SHOW GRANT ON SCHEMA ${DA.catalog_name}.patient_silver

-- COMMAND ----------

-- DBTITLE 0,--i18n-17125954-7b25-40dc-ab84-5a159198e9fc
-- MAGIC %md
-- MAGIC 現在、このスキーマには許可がありません。
-- MAGIC
-- MAGIC それではカタログを調べてみましょう。

-- COMMAND ----------

-- SHOW GRANT ON CATALOG `${DA.catalog_name}`

-- COMMAND ----------

-- DBTITLE 0,--i18n-7453efa0-62d0-4af2-901f-c222dd9b2d07
-- MAGIC %md
-- MAGIC 現在、以前に設定した **USAGE** 権限が表示されています。

-- COMMAND ----------

-- DBTITLE 0,--i18n-b4f88042-e7cb-4a1f-b853-cb328356778c
-- MAGIC %md
-- MAGIC ## クリーンアップ
-- MAGIC 次のセルを実行して、この例で作成したスキーマを削除します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. 無断複写・転載を禁じます。<br/>
-- MAGIC Apache、Apache Spark、Spark、および Spark ロゴは、<a href="https://www.apache.org/">Apache Software Foundation</a> の商標です。<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">プライバシー ポリシー</a> | <a href="https://databricks.com/terms-of-use">利用規約</a> | <a href="https://help.databricks.com/">サポート</a>
