-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks 学習" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-20e4c711-c890-4ef6-bc9b-328f8550ed08
-- MAGIC %md
-- MAGIC # ビューを作成し、テーブルへのアクセスを制限する
-- MAGIC
-- MAGIC このノートブックでは、次の方法を学びます。
-- MAGIC * ビューの作成
-- MAGIC * ビューへのアクセスを管理
-- MAGIC * ダイナミック ビュー機能を使用して、テーブル内の列と行へのアクセスを制限します

-- COMMAND ----------

-- DBTITLE 0,--i18n-ede11fd8-42c7-4b13-863e-30eb9eee7fc3
-- MAGIC %md
-- MAGIC ## セットアップ
-- MAGIC
-- MAGIC 次のセルを実行してセットアップを実行します。共有トレーニング環境での競合を避けるために、これにより、ユーザー専用の固有の名前のデータベースが作成されます。これにより、 **silver** というサンプル テーブルが Unity Catalog  メタストア内に作成されます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-e7942e1d-097f-43a5-a1d7-d54af274b859
-- MAGIC %md
-- MAGIC 注: このノートブックは、Unity Catalog  メタストアに *main* という名前のカタログがあることを前提としています。別のカタログをターゲットにする必要がある場合は、続行する前に次のノートブック **Classroom-Setup** を編集してください。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.3

-- COMMAND ----------

-- DBTITLE 0,--i18n-ef87e152-8e6a-4fd9-8cc7-579545aa01f9
-- MAGIC %md
-- MAGIC **シルバー** テーブルの内容を調べてみましょう。
-- MAGIC
-- MAGIC 注: セットアップの一部として、デフォルトのカタログとデータベースが選択されているため、追加レベルなしでテーブル名またはビュー名を指定するだけで済みます。

-- COMMAND ----------

SELECT * FROM silver.heartrate_device

-- COMMAND ----------

-- DBTITLE 0,--i18n-f9c4630d-c04d-4ec3-8c20-8afea4ec7e37
-- MAGIC %md
-- MAGIC ## ゴールド ビューを作成する
-- MAGIC
-- MAGIC シルバー テーブルを用意したら、シルバーからのデータを集約し、メダリオン アーキテクチャのゴールド レイヤーに適したデータを表示するビューを作成してみましょう。

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.heartrate_avgs AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM silver.heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time))

-- COMMAND ----------

-- DBTITLE 0,--i18n-2eb72987-961a-4020-b775-1e5e29c08a1a
-- MAGIC %md
-- MAGIC ゴールドのビューを調べてみましょう。

-- COMMAND ----------

SELECT * FROM gold.heartrate_avgs

-- COMMAND ----------

-- DBTITLE 0,--i18n-f5ccd808-ffbd-4e06-8ca2-c03dd80ad8e2
-- MAGIC %md
-- MAGIC ## 表示へのアクセスを許可します [オプション]
-- MAGIC
-- MAGIC 新しいビューを用意したら、 **account users** グループのユーザーがそのビューをクエリできるようにしましょう。
-- MAGIC
-- MAGIC このセクションを実行するには、コード セルのコメントを解除し、順番に実行します。また、Databricks SQL でいくつかのクエリを実行するように求められます。次の手順で実行します：
-- MAGIC
-- MAGIC 1. 新しいタブを開き、Databricks SQL に移動します。
-- MAGIC 1. *Unity Catalog での SQL ウェアハウスの作成* の手順に従って SQL ウェアハウスを作成します。
-- MAGIC 1. その環境で、以下の指示に従ってクエリを入力する準備をします。

-- COMMAND ----------

-- SHOW GRANT ON VIEW gold.heartrate_avgs

-- COMMAND ----------

-- SHOW GRANT ON TABLE silver.heartrate_device

-- COMMAND ----------

-- DBTITLE 0,--i18n-9bf38439-6ad4-4db5-bb4f-fe70b8e4cfec
-- MAGIC %md
-- MAGIC ### ビューに対する SELECT 権限を付与します。
-- MAGIC
-- MAGIC 最初の要件は、ビューに対する **SELECT** 権限を **アカウント ユーザー** グループに付与することです。

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold.heartrate_avgs to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-fbb44902-4662-4d9a-ab11-462a2b07a665
-- MAGIC %md
-- MAGIC ### カタログとデータベースに対する USAGE 権限を付与します。
-- MAGIC
-- MAGIC テーブルと同様に、ビューをクエリするには、カタログとデータベースに対する **USAGE** 権限も必要です。

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.catalog_name} TO `account users`;
-- GRANT USAGE ON DATABASE gold TO `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-cfe19914-4f92-47da-a250-2d350a39736f
-- MAGIC %md
-- MAGIC ### ユーザーとしてビューをクエリする
-- MAGIC
-- MAGIC 適切な権限を付与して、Databricks SQL 環境でビューのクエリを試みます。
-- MAGIC
-- MAGIC 次のセルを実行して、ビューから読み取るクエリ ステートメントを出力します。出力をコピーして SQL 環境内の新しいクエリに貼り付け、クエリを実行します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.gold.heartrate_avgs")

-- COMMAND ----------

-- DBTITLE 0,--i18n-330f0448-6bef-42bc-930d-1716886c97fb
-- MAGIC %md
-- MAGIC クエリは成功し、出力は予想どおり上記の出力と同じであることに注目してください。
-- MAGIC
-- MAGIC ここで、 **`gold.heartrate_avgs`** を **`silver.heartrate_device`** に置き換えて、クエリを再実行します。クエリが失敗することに注目してください。これは、ユーザーが **`silver.heartrate_device`** テーブルに対する **SELECT** 権限を持っていないためです。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC print(f"SELECT * FROM {DA.catalog_name}.silver.heartrate_device ")

-- COMMAND ----------

-- DBTITLE 0,--i18n-7c6916be-d36a-4655-b95c-94402222573f
-- MAGIC %md
-- MAGIC
-- MAGIC ただし、 **`heartrate_avgs`** は **`heartrate_device`** から選択するビューであることを思い出してください。では、 **`heartrate_avgs`** に対するクエリはどのようにして成功するのでしょうか? そのビューの *所有者* が **`silver.heartrate_device`** に対する **SELECT** 権限を持っているため、Unity Catalog はクエリの通過を許可します。これは、保護しようとしている基になるテーブルへの直接アクセスを許可せずに、テーブルの行や列をフィルターまたはマスクできるビューを実装できるため、重要な性質です。次に、このメカニズムが実際に動作する様子を見ていきます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-4ea26d47-eee0-43e9-867b-4b6913679e41
-- MAGIC %md
-- MAGIC ## ダイナミックビュー
-- MAGIC
-- MAGIC ダイナミック ビューを使用すると、次のようなきめ細かいアクセス制御を構成できます。
-- MAGIC * 列または行レベルのセキュリティ。
-- MAGIC * データマスキング。
-- MAGIC
-- MAGIC アクセス制御は、ビューの定義内の関数を使用することによって実現されます。これらの機能には次のものが含まれます。
-- MAGIC * **`current_user()`**: 現在のユーザーの電子メール アドレスを返します
-- MAGIC * **`is_account_group_member()`**: 現在のユーザーが指定されたグループのメンバーである場合に TRUE を返します。
-- MAGIC
-- MAGIC 注: 従来の互換性のために、現在のユーザーが指定されたワークスペース レベルのグループのメンバーである場合に TRUE を返す関数 **`is_member()`** も存在します。Unity Catalog で動的ビューを実装する場合は、この関数の使用を避けてください。

-- COMMAND ----------

-- DBTITLE 0,--i18n-b39146b7-362b-46db-9a59-a8c589e94392
-- MAGIC %md
-- MAGIC ### 列を制限する
-- MAGIC **`is_account_group_member()`** を適用して、 **`SELECT`** 内の **`CASE`** ステートメントを通じて **account users** グループのメンバーの PII を含む列をマスクアウトしてみましょう。
-- MAGIC
-- MAGIC 注: これは、このトレーニング環境のセットアップに合わせた簡単な例です。運用システムでは、特定のグループのメンバーではないユーザーの行を制限することをお勧めします。

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.heartrate_avgs AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE mrn
  END AS mrn,
  CASE WHEN
    is_account_group_member('account users') THEN 'REDACTED'
    ELSE name
  END AS name,
  MEAN(heartrate) avg_heartrate,
  DATE_TRUNC("DD", time) date
  FROM silver.heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

-- DBTITLE 0,--i18n-c5c4afac-2736-4a57-be57-55535654a227
-- MAGIC %md
-- MAGIC 次に、更新されたビューに対して許可を再発行しましょう。

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold.heartrate_avgs to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-271fb0d5-34f1-435f-967e-4cc650facd01
-- MAGIC %md
-- MAGIC ビューをクエリしてみましょう。フィルタされていない出力が得られます (現在のユーザーが **analysts** グループに追加されていないと仮定します)。

-- COMMAND ----------

SELECT * FROM gold.heartrate_avgs

-- COMMAND ----------

-- DBTITLE 0,--i18n-1754f589-d48f-499d-b352-3fa735305eb9
-- MAGIC %md
-- MAGIC ここで、Databricks SQL 環境で以前に実行したクエリを再実行します (**`silver`** を **`gold_dailyavg`** に戻します)。PII がフィルタリングされたことに注目してください。PII はビューによって保護されており、基になるテーブルに直接アクセスできないため、このグループのメンバーが PII にアクセスする方法はありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-f7759fba-c2f8-4471-96f7-aa1ea5a6a00b
-- MAGIC %md
-- MAGIC ### 行を制限する
-- MAGIC 次に **`is_account_group_member()`** を適用して行をフィルター処理してみましょう。ここでは、 **analysts** グループのメンバーの場合、デバイス ID が 30 未満の行に制限されたタイムスタンプと心拍数の値を返す新しいゴールド ビューを作成します。行のフィルタリングは、 **`SELECT`** の **`WHERE`** 句として条件を適用することで実行できます。

-- COMMAND ----------

-- Create the "gold_allhr" view in the "gold" schema
CREATE OR REPLACE VIEW gold.gold_allhr AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM silver.heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END;

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_allhr to `account users`

-- COMMAND ----------

SELECT * FROM gold.gold_allhr

-- COMMAND ----------

-- DBTITLE 0,--i18n-63255ec6-2317-455c-9665-e1c2069546f4
-- MAGIC %md
-- MAGIC 次に、Databricks SQL 環境で以前に実行したクエリを再実行します (**`gold_dailyavg`** を **`gold_allhr`** に変更します)。デバイス ID が 30 以上の行は出力から省略されることに注意してください。

-- COMMAND ----------

-- DBTITLE 0,--i18n-d11e2467-cd67-411e-bedd-4ebe55301985
-- MAGIC %md
-- MAGIC ### データマスキング
-- MAGIC 動的ビューの最後の使用例は、データをマスクすることです。つまり、データのサブセットの通過を許可しますが、マスクされたフィールド全体が推定できないような方法でデータを変換します。
-- MAGIC
-- MAGIC ここでは、行フィルタリングと列フィルタリングのアプローチを組み合わせて、行フィルタリング ビューをデータ マスキングで強化します。ただし、列全体を文字列 **REDACTED** に置き換えるのではなく、SQL 文字列操作関数を利用して **mrn** の最後の 2 桁を表示し、残りをマスクします。
-- MAGIC
-- MAGIC SQL は、ニーズに応じて、さまざまな方法でデータをマスクするために活用できる文字列操作関数の非常に包括的なライブラリを提供します。以下に示すアプローチは、この簡単な例を示しています。

-- COMMAND ----------

CREATE OR REPLACE VIEW gold_allhr AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN CONCAT("******", RIGHT(mrn, 2))
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM silver.heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- GRANT SELECT ON VIEW gold_allhr to `account users`

-- COMMAND ----------

SELECT * FROM gold_allhr

-- COMMAND ----------

-- DBTITLE 0,--i18n-c379505f-ebad-474d-9dad-92fa8a7a7ee9
-- MAGIC %md
-- MAGIC Databricks SQL 環境で **gold_allhr** に対してクエリを最後に再実行します。一部の行がフィルタリングされることに加えて、 **mrn** 列がマスクされ、最後の 2 桁のみが表示されることに注意してください。これにより、記録を既知の​​患者と関連付けるために十分な情報が提供されますが、それ自体では PII が漏洩することはありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-8828d381-084f-43fa-afc3-a5a2ba170aeb
-- MAGIC %md
-- MAGIC ## クリーンアップ
-- MAGIC 次のセルを実行して、この例で使用されたアセットを削除します。

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. 無断複写・転載を禁じます。<br/>
-- MAGIC Apache、Apache Spark、Spark、および Spark ロゴは、<a href="https://www.apache.org/">Apache Software Foundation</a> の商標です。<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">プライバシー ポリシー</a> | <a href="https://databricks.com/terms-of-use">利用規約</a> | <a href="https://help.databricks.com/">サポート</a>
