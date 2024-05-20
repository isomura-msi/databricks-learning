-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks 学習" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- DBTITLE 0,--i18n-b5d0b5d6-287e-4dcd-8a4b-a0fe2b12b615
-- MAGIC %md
-- MAGIC # Unity Catalog を使用してデータ オブジェクトを作成および管理する
-- MAGIC
-- MAGIC このノートブックでは、次の方法を学びます。
-- MAGIC * カタログ、スキーマ、テーブル、ビュー、ユーザー定義関数を作成します
-- MAGIC * これらのオブジェクトへのアクセスを制御します
-- MAGIC * 動的ビューを使用してテーブル内の列と行を保護します
-- MAGIC * Unity Catalog 内のさまざまなオブジェクトの許可を調べる

-- COMMAND ----------

-- DBTITLE 0,--i18n-63c92b27-28a0-41f8-8761-a9ec5bb574e0
-- MAGIC %md
-- MAGIC ## 前提条件
-- MAGIC
-- MAGIC このラボを進めるには、以下が必要です。
-- MAGIC * カタログを作成および管理するためのメタストア管理者権限がある
-- MAGIC * 上記のユーザーがアクセスできる SQL ウェアハウスを持っている
-- MAGIC * ノートブックを参照: Unity Catalog  アクセス用のコンピューティング リソースの作成

-- COMMAND ----------

-- DBTITLE 0,--i18n-67495bbd-0f5b-4f81-9974-d5c6360fb86c
-- MAGIC %md
-- MAGIC ## セットアップ
-- MAGIC
-- MAGIC 次のセルを実行してセットアップを実行します。共有トレーニング環境での競合を避けるために、これにより、ユーザー専用の固有のカタログ名が生成されます。その名前をラボでは利用します。

-- COMMAND ----------

-- MAGIC %run ./Includes/Classroom-Setup-06.1

-- COMMAND ----------

-- DBTITLE 0,--i18n-a0720484-97de-4742-9c38-6c5bd312f3d7
-- MAGIC %md
-- MAGIC ## Unity Catalog の 3 レベルの名前空間
-- MAGIC
-- MAGIC SQL の経験がある人は、次のクエリの例に示すように、スキーマ内のテーブルまたはビューをアドレス指定するための従来の 2 レベルの名前空間に精通しているでしょう。
-- MAGIC
-- MAGIC     SELECT * FROM myschema.mytable;
-- MAGIC
-- MAGIC Unity Catalog は、 **カタログ** の概念を階層に導入します。カタログはスキーマのコンテナとして、組織がデータを分離するための新しい方法を提供します。カタログは好きなだけ存在でき、そのカタログには好きなだけスキーマを含めることができます (**スキーマ**の概念は Unity Catalog では変更されていません。スキーマにはテーブル、ビュー、ユーザー定義関数などのデータオブジェクトが含まれます)。
-- MAGIC
-- MAGIC この追加レベルに対処するために、Unity Catalog 内の完全なテーブル/ビュー参照は 3 レベルの名前空間を使用します。次のクエリはこれを例示しています。
-- MAGIC
-- MAGIC     SELECT * FROM mycatalog.myschema.mytable;
-- MAGIC
-- MAGIC これは多くの使用例で便利です。例えば：
-- MAGIC
-- MAGIC * 組織内のビジネスユニット (販売、マーケティング、人事など) に関連するデータを分離します。
-- MAGIC * SDLC 要件を満たす (開発、ステージング、本番など)
-- MAGIC * 内部使用のための一時的なデータセットを含むサンドボックスの確立

-- COMMAND ----------

-- DBTITLE 0,--i18n-aa4c68dc-6dc3-4aca-a352-affc98ac8089
-- MAGIC %md
-- MAGIC ### 新しいカタログを作成する
-- MAGIC メタストアに新しいカタログを作成しましょう。変数 **`${DA.my_new_catalog}`** が上記の設定セルによって表示され、ユーザー名に基づいて生成された一意の文字列が含まれています。
-- MAGIC
-- MAGIC 以下の **`CREATE`** ステートメントを実行し、左側のサイドバーにある **データ** アイコンをクリックして、この新しいカタログが作成されたことを確認します。

-- COMMAND ----------

CREATE CATALOG IF NOT EXISTS ${DA.my_new_catalog}

-- COMMAND ----------

-- DBTITLE 0,--i18n-e1f478c8-bbf2-4368-9cdd-e130d2fb7410
-- MAGIC %md
-- MAGIC ### デフォルトのカタログを選択します
-- MAGIC
-- MAGIC SQL 開発者は、デフォルトのスキーマを選択するための **`USE`** ステートメントにも精通しているでしょう。これにより、常にスキーマを指定する必要がなくなり、クエリが短縮されます。名前空間の追加レベルを処理しながらこの利便性を拡張するために、Unity Catalog は、以下の例に示す 2 つの追加ステートメントで言語を拡張します。
-- MAGIC
-- MAGIC     USE CATALOG mycatalog;
-- MAGIC     USE SCHEMA myschema;  
-- MAGIC     
-- MAGIC 新しく作成したカタログをデフォルトとして選択しましょう。今後は、カタログ参照によって明示的にオーバーライドされない限り、スキーマ参照はこのカタログ内にあるものとみなされます。

-- COMMAND ----------

USE CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- DBTITLE 0,--i18n-bc4ad8ed-6550-4457-92f7-d88d22709b3c
-- MAGIC %md
-- MAGIC ### 新しいスキーマを作成して使用する
-- MAGIC 次に、この新しいカタログにスキーマを作成しましょう。メタストアの他の部分から分離された一意のカタログを使用しているため、このスキーマに別の一意の名前を生成する必要はありません。これをデフォルトのスキーマとしても設定しましょう。これで、2 レベルまたは 3 レベルの参照によって明示的にオーバーライドされない限り、データ参照は作成したカタログとスキーマ内にあるものとみなされます。
-- MAGIC
-- MAGIC 以下のコードを実行し、左側のサイドバーにある [**データ**] アイコンをクリックして、作成した新しいカタログにこのスキーマが作成されたことを確認します。

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS example;
USE SCHEMA example

-- COMMAND ----------

-- DBTITLE 0,--i18n-87b6328c-4641-4d40-b66c-f166a4166902
-- MAGIC %md
-- MAGIC ### テーブルとビューをセットアップする
-- MAGIC
-- MAGIC データ管理構造の準備が整ったら、テーブルとビューを設定しましょう。この例では、モック データを使用して、合成患者心拍数データを含む *シルバー* 管理テーブルと、毎日の患者ごとの心拍数データを平均する *ゴールド* ビューを作成し、設定します。
-- MAGIC
-- MAGIC 以下のセルを実行し、左側のサイドバーにある **データ** アイコンをクリックして、 *example* スキーマの内容を調べます。デフォルトのカタログとスキーマを選択したため、以下のテーブル名またはビュー名を指定するときに 3 つのレベルを指定する必要がないことに注意してください。

-- COMMAND ----------

-- Creating the silver table with patient heart rate data and PII

CREATE OR REPLACE TABLE heartrate_device (device_id INT, mrn STRING, name STRING, time TIMESTAMP, heartrate DOUBLE);

INSERT INTO heartrate_device VALUES
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:01:58.000+0000", 54.0122153343),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:02:55.000+0000", 92.5136468131),
  (37, "65300842", "Samuel Hughes", "2020-02-01T00:08:58.000+0000", 52.1354807863),
  (23, "40580129", "Nicholas Spears", "2020-02-01T00:16:51.000+0000", 54.6477014191),
  (17, "52804177", "Lynn Russell", "2020-02-01T00:18:08.000+0000", 95.033344842);
  
SELECT * FROM heartrate_device

-- COMMAND ----------

-- Creating a gold view to share with other users

CREATE OR REPLACE VIEW agg_heartrate AS (
  SELECT mrn, name, MEAN(heartrate) avg_heartrate, DATE_TRUNC("DD", time) date
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)
);
SELECT * FROM agg_heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-3ab35bc3-e89b-48d0-bcef-91a268688a19
-- MAGIC %md
-- MAGIC 私たちがデータ所有者であるため、上記のテーブルのクエリは期待どおりに機能します。クエリを実行しているデータ オブジェクトの所有権を持っているからです。私たちはビューとビューが参照しているテーブルの両方の所有者であるため、ビューのクエリも機能します。したがって、これらのリソースにアクセスするためにオブジェクト レベルの権限は必要ありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-9ce42a62-c95a-45fb-9d6b-a4d3725110e7
-- MAGIC %md
-- MAGIC ## _account users_ グループ
-- MAGIC
-- MAGIC Unity Catalog が有効になっているアカウントには、_account users_ グループがあります。このグループには、Databricks アカウントからワークスペースに割り当てられたすべてのユーザーが含まれます。このグループを使用して、グループごとにデータ オブジェクトへのアクセスがどのように異なるかを示します。

-- COMMAND ----------

-- DBTITLE 0,--i18n-29eeb0ee-94e0-4b95-95be-a8776d20dc6c
-- MAGIC %md
-- MAGIC
-- MAGIC ## データ オブジェクトへのアクセスを許可する
-- MAGIC
-- MAGIC Unity Catalog はデフォルトで明示的な権限モデルを採用します。権限は暗黙に含まれることも、含まれる要素から継承されることもありません。したがって、データ オブジェクトにアクセスするには、ユーザーは含まれるすべての要素に対する **USAGE** 権限が必要になります。これにはスキーマとカタログが含まれています。
-- MAGIC
-- MAGIC 次に、 *account users* グループのメンバーが *gold* ビューをクエリできるようにしましょう。これを行うには、次の権限を付与する必要があります。
-- MAGIC 1. カタログとスキーマの USAGE
-- MAGIC 1. データ オブジェクト (ビューなど) の SELECT

-- COMMAND ----------

-- GRANT USAGE ON CATALOG ${DA.my_new_catalog} TO `account users`;

-- COMMAND ----------

-- GRANT USAGE ON SCHEMA example TO `account users`;

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-dabadee1-5624-4330-855d-d0c0de1f76b4
-- MAGIC %md
-- MAGIC ### ビューをクエリする
-- MAGIC
-- MAGIC データ オブジェクト階層と適切な権限をすべて設定した状態で、 *gold* ビューに対してクエリを実行してみます。
-- MAGIC
-- MAGIC 私たち全員が **アカウント ユーザー** グループのメンバーであるため、このグループを使用して構成を確認し、変更を加えたときの影響を観察できます。
-- MAGIC
-- MAGIC 1. 左上隅にあるアプリ スイッチャーをクリックして開きます。
-- MAGIC 1. **SQL** を右クリックし、 **リンクを新しいタブで開く** を選択します。
-- MAGIC 1. **クエリ** ページに移動し、 **クエリの作成** をクリックします。
-- MAGIC 1. *Unity Catalog  アクセス用のコンピューティング リソースの作成* デモに従って作成された共有 SQL ウェアハウスを選択します。
-- MAGIC 1. このノートブックに戻って、続きを続けます。プロンプトが表示されたら、Databricks SQL セッションに切り替えてクエリを実行します。
-- MAGIC
-- MAGIC 次のセルは、変数やデフォルトのカタログとスキーマが設定されていない環境でこれを実行するため、ビューの 3 つのレベルすべてを指定する完全修飾クエリ ステートメントを生成します。以下で生成されたクエリを Databricks SQL セッションで実行します。アカウント ユーザーがビューにアクセスするための適切な権限がすべて設定されているため、出力は、前に *gold* ビューをクエリしたときに表示されたものと似ているはずです。

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- DBTITLE 0,--i18n-7fb51344-7eb4-49a2-916c-6cdee06e534a
-- MAGIC %md
-- MAGIC ### シルバー テーブルのクエリ
-- MAGIC Databricks SQL セッションの同じクエリに戻り、 *gold* を *silver* に置き換えてクエリを実行してみましょう。データ所有者としてこれを実行すると、すべてのテーブルの結果が期待どおりに返されるはずです。ただし、これを別のユーザーとして実行すると、 *silver* テーブルにアクセス許可を設定していないため、これは失敗します。
-- MAGIC
-- MAGIC *gold* ビューによって表されるクエリは基本的にビューの所有者として実行されるため、_account users_ グループのすべてのメンバーに対して機能します。この重要なプロパティにより、いくつかの興味深いセキュリティの使用例が可能になります。このようにして、ビューは基になるデータ自体へのアクセスを提供せずに、機密データの制限されたビューをユーザーに提供できます。これについては、後ほど詳しく説明します。
-- MAGIC
-- MAGIC Databricks SQL セッションの *silver* クエリは閉じてしまいましょう。もう使用しません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-bea5cb66-3642-45b1-8906-906588b99b06
-- MAGIC %md
-- MAGIC ### ユーザー定義関数を作成してアクセスを許可する
-- MAGIC
-- MAGIC Unity Catalog は、スキーマ内のユーザー定義関数も管理できます。以下のコードは、文字列の最後の 2 文字を除くすべての文字をマスクする単純な関数を設定し、それを試します。繰り返しになりますが、私たちはデータ所有者であるため、 GRANT は必要ありません。

-- COMMAND ----------

CREATE OR REPLACE FUNCTION my_mask(x STRING)
  RETURNS STRING
  RETURN CONCAT(REPEAT("*", LENGTH(x) - 2), RIGHT(x, 2)
); 
SELECT my_mask('sensitive data') AS data

-- COMMAND ----------

-- DBTITLE 0,--i18n-d0945f12-4045-471b-a319-376f5f8f25dd
-- MAGIC %md
-- MAGIC
-- MAGIC *アカウント ユーザー* グループのメンバーが関数を実行できるようにするには、その関数に対する **EXECUTE** と、前に説明したスキーマとカタログに対する **USAGE** 権限が必要です。

-- COMMAND ----------

-- GRANT EXECUTE ON FUNCTION my_mask to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-e74e14a8-d372-44e0-a301-94cc046efd29
-- MAGIC %md
-- MAGIC ### 関数を実行する
-- MAGIC
-- MAGIC 次に、Databricks SQL で関数を試してみましょう。以下で生成された完全修飾クエリ ステートメントを新しいクエリに貼り付けて、Databricks SQL でこの関数を実行します。関数にアクセスするために適切な許可がすべて設定されているため、先ほどと同じ結果となります。

-- COMMAND ----------

SELECT "SELECT ${DA.my_new_catalog}.example.my_mask('sensitive data') AS data" AS Query

-- COMMAND ----------

-- DBTITLE 0,--i18n-8697f10a-6924-4bac-9c9a-7d91285eb9f5
-- MAGIC %md
-- MAGIC ## 動的ビューを使用してテーブルの列と行を保護する
-- MAGIC
-- MAGIC Unity Catalog のビューの処理により、ビューがテーブルへのアクセスを保護する機能が提供されることがわかりました。ユーザーには、ソース テーブルへの直接アクセスを提供することなく、ソース テーブルのデータを操作、変換、または隠蔽するビューへのアクセスを許可できます。
-- MAGIC
-- MAGIC 動的ビューは、クエリを実行するプリンシパルを条件として、テーブル内の列と行のきめ細かいアクセス制御を行う機能を提供します。動的ビューは、次のようなことを可能にする標準ビューの拡張機能です。
-- MAGIC * 列の値を部分的に隠すか、完全に編集します
-- MAGIC * 特定の基準に基づいて行を省略します
-- MAGIC
-- MAGIC 動的ビューでのアクセス制御は、ビューの定義内で関数を使用することによって実現されます。これらの機能には次のものが含まれます。
-- MAGIC * **`current_user()`**: ビューをクエリしているユーザーの電子メール アドレスを返します
-- MAGIC * **`is_account_group_member()`**: ビューをクエリしているユーザーが指定されたグループのメンバーである場合、TRUE を返します。
-- MAGIC
-- MAGIC 注: ワークスペース レベルのグループを参照するレガシー関数 **`is_member()`** の使用は控えてください。これは、Unity Catalog のコンテキストでは良い習慣ではありません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-8fc52e53-927e-4d6b-a340-7082c94d4e6e
-- MAGIC %md
-- MAGIC ### 列を編集する
-- MAGIC
-- MAGIC アカウント ユーザーが *ゴールド* ビューから集計されたデータの傾向を確認できるようにしたいが、患者の PII は開示したくないとします。 **`is_account_group_member()`** を使用して *mrn* 列と *name* 列を編集するようにビューを再定義しましょう。
-- MAGIC
-- MAGIC 注: これはトレーニング教材としての単純な例であり、一般的なベスト プラクティスと必ずしも一致するわけではありません。運用システムの場合、より安全なアプローチは、特定のグループのメンバーではないすべてのユーザーの列値を編集することです。

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
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
  FROM heartrate_device
  GROUP BY mrn, name, DATE_TRUNC("DD", time)

-- COMMAND ----------

-- DBTITLE 0,--i18n-01381247-0c36-455b-b64c-22df863d9926
-- MAGIC %md
-- MAGIC
-- MAGIC GRANT を再発行します。

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-0bf9bd34-2351-492c-b6ef-e48241339d0f
-- MAGIC %md
-- MAGIC
-- MAGIC 次に、Databricks SQL に戻り、 *gold* ビューでクエリを再実行します。このクエリを生成するには、以下のセルを実行します。
-- MAGIC
-- MAGIC *mrn* 列と *name* 列の値が編集されていることがわかります。

-- COMMAND ----------

SELECT "SELECT * FROM ${DA.my_new_catalog}.example.agg_heartrate" AS Query

-- COMMAND ----------

-- DBTITLE 0,--i18n-0bb3c639-daf8-4b46-9c28-cafd32a12917
-- MAGIC %md
-- MAGIC ### 行を制限する
-- MAGIC
-- MAGIC つづいて、列を集約して編集するのではなく、単にソースから行をフィルターで除外するビューが必要だと仮定します。同じ **`is_account_group_member()`** 関数を適用して、 *device_id* が 30 未満の行のみを通過するビューを作成しましょう。行のフィルタリングは、 **`WHERE`** 句として条件を適用することで行います。

-- COMMAND ----------

CREATE OR REPLACE VIEW agg_heartrate AS
SELECT
  mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- DBTITLE 0,--i18n-69bc283c-f426-4ba2-b296-346c69de1c20
-- MAGIC %md
-- MAGIC
-- MAGIC GRANT を再発行します。

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-c4f3366a-4992-4d5a-b597-e140091a8d00
-- MAGIC %md
-- MAGIC
-- MAGIC グループに属していないユーザーの場合、上記のビューをクエリすると、5 つのレコードすべてが表示されます。次に、Databricks SQL に戻り、 *gold* ビューでクエリを再実行します。レコードの 1 つが欠落していることがわかります。欠落しているレコードには、フィルターの条件に一致する *device_id* の値が含まれていました。

-- COMMAND ----------

-- DBTITLE 0,--i18n-dffccf13-5205-44d5-beab-4d08b085f54a
-- MAGIC %md
-- MAGIC ### データマスキング
-- MAGIC 動的ビューの最後の使用例は、データのマスキング、つまりデータの一部を隠すことです。最初の例では、列全体を編集しました。マスキングは原理的には同様ですが、データを完全に置き換えるのではなく、一部を表示する点が異なります。この簡単な例では、前に作成した *my_mask()* ユーザー定義関数を利用して *mrn* 列をマスクします。ただし、SQL には、利用できる組み込みデータ操作関数のかなり包括的なライブラリが用意されています。さまざまな方法でデータをマスクします。可能な場合はそれらを活用することをお勧めします。

-- COMMAND ----------

DROP VIEW IF EXISTS agg_heartrate;

CREATE VIEW agg_heartrate AS
SELECT
  CASE WHEN
    is_account_group_member('account users') THEN my_mask(mrn)
    ELSE mrn
  END AS mrn,
  time,
  device_id,
  heartrate
FROM heartrate_device
WHERE
  CASE WHEN
    is_account_group_member('account users') THEN device_id < 30
    ELSE TRUE
  END

-- COMMAND ----------

-- DBTITLE 0,--i18n-735fa71c-0d31-4484-9736-30dc098dee8d
-- MAGIC %md
-- MAGIC
-- MAGIC GRANT を再発行します。

-- COMMAND ----------

-- GRANT SELECT ON VIEW agg_heartrate to `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-9c3d9b8f-17ce-498f-b6dd-dfb470855086
-- MAGIC %md
-- MAGIC
-- MAGIC グループのメンバーではないユーザーに対しては、影響を受けていないレコードが表示されます。Databricks SQL に戻り、 *gold* ビューでクエリを再実行します。 *mrn* 列の値はすべてマスクされます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-3e809063-ad1b-4a2e-8cc3-b87492c8ffc3
-- MAGIC %md
-- MAGIC
-- MAGIC ## オブジェクトを探索する
-- MAGIC
-- MAGIC データ オブジェクトと権限を調べるために、いくつかの SQL ステートメントを調べてみましょう。まずは *examples* スキーマにあるオブジェクトを調べてみましょう。

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW VIEWS

-- COMMAND ----------

-- DBTITLE 0,--i18n-40599e0f-47f9-46da-be2b-7b856da0cba1
-- MAGIC %md
-- MAGIC 上記 2 つのステートメントでは、選択したデフォルトに依存しているため、スキーマを指定しませんでした。あるいは、 **`SHOW TABLES IN example`** のようなステートメントを使用して、より明示的にすることもできます。
-- MAGIC
-- MAGIC 次に、階層内のレベルを 1 つ上げて、カタログ内のスキーマの一覧表を作成しましょう。もう一度、デフォルトのカタログが選択されているという事実を利用します。もっと明示的にしたい場合は、 **`SHOW SCHEMAS IN ${DA.my_new_catalog}`** のように指定できます。

-- COMMAND ----------

SHOW SCHEMAS

-- COMMAND ----------

-- DBTITLE 0,--i18n-943178c2-9ab3-4941-ac1a-70b63103ecb7
-- MAGIC %md
-- MAGIC *example* スキーマは、もちろん、以前に作成したものです。 *default* スキーマは、新しいカタログの作成時に SQL 規則に従ってデフォルトで作成されます。
-- MAGIC
-- MAGIC 最後に、メタストア内のカタログをリストしてみましょう。

-- COMMAND ----------

SHOW CATALOGS

-- COMMAND ----------

-- DBTITLE 0,--i18n-e8579147-7d9a-4b27-b0e9-3ab6c4ec9a0c
-- MAGIC %md
-- MAGIC 予想よりも多くのエントリーがあるかもしれません。少なくとも次の内容が表示されます。
-- MAGIC * プレフィックス *dbacademy_* で始まるカタログ。これは以前に作成したものです。
-- MAGIC * *hive_metastore*。これはメタストア内の実際のカタログではなく、ワークスペースのローカル Hive メタストアの仮想表現です。これを使用して、ワークスペースローカルのテーブルとビューにアクセスします。
-- MAGIC * *main*、新しいメタストアごとにデフォルトで作成されるカタログ。
-- MAGIC * *samples*、Databricks が提供するサンプル データセットを示す別の仮想カタログ
-- MAGIC
-- MAGIC メタストア内の履歴アクティビティに応じて、さらに多くのカタログが存在する可能性があります。

-- COMMAND ----------

-- DBTITLE 0,--i18n-9477b0a2-3099-4fca-bb7d-f6e298ce254b
-- MAGIC %md
-- MAGIC ### 権限を調べる
-- MAGIC
-- MAGIC 次に、 **`SHOW GRANTS`** を使用して、 *gold* ビューから始めて上位に向かってパーミッションを調べてみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON VIEW agg_heartrate

-- COMMAND ----------

-- DBTITLE 0,--i18n-98d1534e-a51c-45f6-83d0-b99549ccc279
-- MAGIC %md
-- MAGIC 現在、設定したばかりの **SELECT** 許可のみがあります。次に、 *silver* の補助金を確認してみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON TABLE heartrate_device

-- COMMAND ----------

-- DBTITLE 0,--i18n-591b3fbb-7ed3-4e88-b435-3750b212521d
-- MAGIC %md
-- MAGIC 現在、このテーブルには許可がありません。このテーブルに直接アクセスできるのは、データ所有者である私たちだけです。私たちがデータ所有者でもある *ゴールド* ビューにアクセスする権限を持つ人は誰でも、このテーブルに間接的にアクセスできます。
-- MAGIC
-- MAGIC 次に、含まれるスキーマを見てみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON SCHEMA example

-- COMMAND ----------

-- DBTITLE 0,--i18n-ab78d60b-a596-4a19-80ae-a5d742169b6c
-- MAGIC %md
-- MAGIC 現在、以前に設定した **USAGE** 許可が表示されています。
-- MAGIC
-- MAGIC それではカタログを調べてみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON CATALOG ${DA.my_new_catalog}

-- COMMAND ----------

-- DBTITLE 0,--i18n-62f7e069-7260-4a48-9676-16088958cffc
-- MAGIC %md
-- MAGIC 同様に、先ほど許可した **USAGE** が表示されます。

-- COMMAND ----------

-- DBTITLE 0,--i18n-200fe251-2176-46ca-8ecc-e725d8c9da01
-- MAGIC %md
-- MAGIC ## アクセスを取り消します
-- MAGIC
-- MAGIC 以前に発行された GRANT を取り消す機能がなければ、データ ガバナンス プラットフォームは完成しません。まずは *my_mask()* 関数へのアクセスを調べてみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON FUNCTION my_mask

-- COMMAND ----------

-- DBTITLE 0,--i18n-9495b822-96bd-4fe5-aed7-9796ffd722d0
-- MAGIC %md
-- MAGIC さて、この GRANT を取り消しましょう。

-- COMMAND ----------

-- REVOKE EXECUTE ON FUNCTION my_mask FROM `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-c599d523-08cc-4d39-994d-ce919799c276
-- MAGIC %md
-- MAGIC さて、空になったアクセスを再検査してみましょう。

-- COMMAND ----------

-- SHOW GRANTS ON FUNCTION my_mask

-- COMMAND ----------

-- DBTITLE 0,--i18n-a47d66a4-aa65-470b-b8ea-d8e7c29fce95
-- MAGIC %md
-- MAGIC
-- MAGIC Databricks SQL セッションに再度アクセスし、 *gold* ビューに対してクエリを再実行します。これは以前と同様に機能することに注意してください。驚きましたか? なぜ、あるいはなぜそうではないのでしょうか？
-- MAGIC
-- MAGIC ビューは実質的にその所有者として実行されており、その所有者が関数とソース テーブルの所有者でもあることに注意してください。ビューの例では、ビューの所有者がテーブルの所有権を持っているため、クエリ対象のテーブルに直接アクセスする必要がなかったのと同様に、関数は同じ理由で動作し続けます。
-- MAGIC
-- MAGIC では、別のことを試してみましょう。カタログの **USAGE** を取り消して、権限チェーンを断ち切りましょう。

-- COMMAND ----------

-- REVOKE USAGE ON CATALOG ${DA.my_new_catalog} FROM `account users`

-- COMMAND ----------

-- DBTITLE 0,--i18n-b1483973-1ef7-4b6d-9a04-931c53947148
-- MAGIC %md
-- MAGIC
-- MAGIC Databricks SQL に戻り、 *gold* クエリを再実行すると、ビューとスキーマに対する適切な権限があるにもかかわらず、階層の上位にある権限が不足しているため、このリソースへのアクセスが中断されることがわかります。これは、Unity Catalog の明示的な権限モデルの動作を示しています。権限は暗黙的に適用または継承されません。

-- COMMAND ----------

-- DBTITLE 0,--i18n-1ffb00ac-7663-4206-84b6-448b50c0efe2
-- MAGIC %md
-- MAGIC ## クリーンアップ
-- MAGIC 次のセルを実行して、前に作成したカタログを削除してみましょう。 **`CASCADE`** 修飾子は、含まれている要素とともにカタログを削除します。

-- COMMAND ----------

USE CATALOG hive_metastore;
DROP CATALOG IF EXISTS ${DA.my_new_catalog} CASCADE;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DA.cleanup()

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC &copy; 2023 Databricks, Inc. 無断複写・転載を禁じます。<br/>
-- MAGIC Apache、Apache Spark、Spark、および Spark ロゴは、<a href="https://www.apache.org/">Apache Software Foundation</a> の商標です。<br/>
-- MAGIC <br/>
-- MAGIC <a href="https://databricks.com/privacy-policy">プライバシー ポリシー</a> | <a href="https://databricks.com/terms-of-use">利用規約</a> | <a href="https://help.databricks.com/">サポート</a>
