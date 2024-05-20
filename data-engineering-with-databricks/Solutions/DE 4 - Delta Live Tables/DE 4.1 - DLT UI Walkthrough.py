# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-da41af42-59a3-42d8-af6d-4ab96146397c
# MAGIC %md
# MAGIC # Delta Live TablesUIの使用（Using the Delta Live Tables UI）
# MAGIC
# MAGIC このデモではDLT UIについて見ていきます。
# MAGIC
# MAGIC このレッスンでは、以下を学びます。
# MAGIC * DLTパイプラインをデプロイする
# MAGIC * 成果物のDAGを調べる
# MAGIC * パイプラインの更新を実行する

# COMMAND ----------

# DBTITLE 0,--i18n-d84e8f59-6cda-4c81-8547-132eb20b48b2
# MAGIC %md
# MAGIC ## セットアップ(Classroom Setup)
# MAGIC
# MAGIC 次のセルを実行して、このコース向けの作業環境を設定します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-04.1

# COMMAND ----------

# DBTITLE 0,--i18n-ba2a4dfe-ca17-4070-b35a-37068ff9c51d
# MAGIC %md
# MAGIC ## パイプライン設定を作成 (Generate Pipeline Configuration)
# MAGIC
# MAGIC このパイプラインを設定するには、ユーザーに固有のパラメータが必要です。
# MAGIC
# MAGIC 以下のセルのコードで、適切な行のコメントを外し、使用する言語を指定します。
# MAGIC
# MAGIC そして、このセルを実行すると、後続のステップでパイプラインを設定するために使用する値が出力されます。

# COMMAND ----------

pipeline_language = "SQL"
#pipeline_language = "Python"

DA.print_pipeline_config(pipeline_language)

# COMMAND ----------

# DBTITLE 0,--i18n-bc4e7bc9-67e1-4393-a3c5-79a1f585cdc8
# MAGIC %md
# MAGIC このレッスンでは、上記のセル出力でノートブック#1として指定された1つのノートブックを持つパイプラインをデプロイします。
# MAGIC
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> **ヒント:**  この後のレッスンでノートブック#2、#3をパイプラインに追加する際には、上記のパスを参照します。

# COMMAND ----------

# DBTITLE 0,--i18n-9b609cc5-91c8-4213-b6f7-1c737a6e44a3
# MAGIC %md
# MAGIC ## パイプラインの作成と設定(Create and Configure a Pipeline)
# MAGIC
# MAGIC 1つのノートブック(ノートブック#1)を使ってパイプラインを作成します。
# MAGIC
# MAGIC 流れ：
# MAGIC 1. サイドバーの**ワークフロー**ボタンをクリックし、**Delta Live Tables**タブを選択して **パイプラインを作成**をクリックします。
# MAGIC 2. 以下のパイプライン設定を行います。上記セルのアウトプットを使って設定する必要があります。
# MAGIC
# MAGIC
# MAGIC | 設定項目 | 設定方法 |
# MAGIC |--|--|
# MAGIC | パイプライン名 | 上記のセルに記載されている **Pipeline Name** を入力|
# MAGIC | 製品エディション | **Advanced** を選択|
# MAGIC | パイプラインモード | **トリガー** を選択|
# MAGIC | クラスターポリシー |上記のセルに記載されている **Policy** を選択 |
# MAGIC | ノートブックライブラリ | 上記のセルに記載されている **Notebook #1 Path** を入力 |
# MAGIC | ストレージの場所 | 上記のセルに記載されている **Storage Location** を入力|
# MAGIC | ターゲットスキーマ | 上記のセルに記載されている **Target** を入力 |
# MAGIC | クラスターモード | **固定サイズ**を選択し、オートスケーリングを無効にする |
# MAGIC | ワーカー | **`0`** （0個）に設定してシングルノードクラスターを使用する |
# MAGIC | Photonアクセラレータを使用 | チェックを有効にする |
# MAGIC | 設定 | **Advanced**をクリックして他の設定を追加する。<br>**設定を追加(Add Configuration)**をクリックし、下記#1の**キー**と**値**を入力する。<br>再度 **設定を追加(Add Configuration)** をクリックし、下記#2の**キー**と**値**を入力する。 |
# MAGIC | チャンネル | **現在**を選択して現在のランタイムバージョンを使用する |
# MAGIC
# MAGIC
# MAGIC | 設定 | キー                 | 値                                      |
# MAGIC | ------------- | ------------------- | ------------------------------------------ |
# MAGIC | #1            | **`spark.master`**  | **`local[*]`**                             |
# MAGIC | #2            | **`source`** | 上記のセルに記載されている **Source**  |
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC 3. **作成(Create)** をクリックします。
# MAGIC 4. パイプラインモードが **Development** であることを確認します。

# COMMAND ----------

# ANSWER

# This function is provided for students who do not 
# want to work through the exercise of creating the pipeline.
DA.create_pipeline(pipeline_language)

# COMMAND ----------

DA.validate_pipeline_config(pipeline_language)

# COMMAND ----------

# DBTITLE 0,--i18n-d8e19679-0c2f-48cc-bc80-5f1243ff94c8
# MAGIC %md
# MAGIC #### パイプライン設定の追加的な注意事項(Additional Notes on Pipeline Configuration)
# MAGIC
# MAGIC 上記パイプライン設定の注意事項:
# MAGIC
# MAGIC
# MAGIC - **パイプライン モード** - パイプラインの実行方法を指定します。レイテンシとコストの要件に基づいてモードを選択します。
# MAGIC   - `トリガー(Triggered)` パイプラインは 1 回実行され、次の手動またはスケジュールされた更新までシャットダウンされます。
# MAGIC   - `連続(Continuous)` パイプラインは継続的に実行され、新しいデータが到着すると取り込みます。
# MAGIC - **ノートブック ライブラリ** - このドキュメントは標準の Databricks Notebook ですが、SQL 構文は DLT テーブル宣言に特化しています。次の演習では、構文を学びます。
# MAGIC - **ストレージの場所** - このオプションのフィールドにより、ユーザーはログ、テーブル、およびパイプラインの実行に関連するその他の情報を保存する場所を指定できます。指定しない場合、DLT は自動的にディレクトリを生成します。
# MAGIC - **ターゲット** - このオプションのフィールドが指定されていない場合、テーブルはメタストアに登録されませんが、DBFS で引き続き使用できます。 このオプションの詳細については、<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#publish-tables" target="_blank">ドキュメント</a> を参照してください。
# MAGIC - **クラスター モード**、**最小ワーカー**、**最大ワーカー** - これらのフィールドは、パイプラインを処理する基盤となるクラスターのワーカー構成を制御します。ここでは、ワーカー数を 0 に設定します。これは、上で定義した **spark.master** パラメータと連携して、クラスターを単一ノード クラスターとして構成します。
# MAGIC - **source** - これらのキーは大文字小文字を区別します。source "の文字がすべて小文字であることを確認してください！

# COMMAND ----------

# DBTITLE 0,--i18n-6f8d9d42-99e2-40a5-b80e-a6e6fedd7279
# MAGIC %md
# MAGIC ## パイプラインを実行する（Run a Pipeline）
# MAGIC
# MAGIC パイプラインを構築したら、そのパイプラインを実行します。
# MAGIC
# MAGIC 1. **開発**を選択し、開発モードでパイプラインを実行します。開発モードでは、（実行の度に新しいクラスタを作成するのではなく）クラスタを再利用し、リトライを無効にしてエラーの発見と修正をし易くすることで、より迅速なインタラクティブ開発を可能にします。この機能に関して詳しく知りたい場合は、こちらの<a href="https://docs.databricks.com/data-engineering/delta-live-tables/delta-live-tables-user-guide.html#optimize-execution" target="_blank">ドキュメント</a>を参考にしてください。
# MAGIC 2. **開始**をクリックします。
# MAGIC
# MAGIC 最初の実行ではクラスタを初期化するため数分程度の時間が掛かります。
# MAGIC その後の実行ではずっとクイックになります。

# COMMAND ----------

# ANSWER

# This function is provided to start the pipeline and  
# block until it has completed, canceled or failed
DA.start_pipeline()

# COMMAND ----------

# DBTITLE 0,--i18n-75d0f6d5-17c6-419e-aacf-be7560f394b6
# MAGIC %md
# MAGIC ## DAGを調べる（Exploring the DAG）
# MAGIC
# MAGIC パイプラインが完了すると、実行フローがグラフ化されます。
# MAGIC
# MAGIC テーブルを選択すると詳細を確認できます。
# MAGIC
# MAGIC
# MAGIC **orders_silver**を選択します。 **データ品質**セクションで報告されている結果に注目してください。
# MAGIC
# MAGIC トリガーされた更新ごとに、新しく到着したすべてのデータがパイプラインで処理されます。現在のランについてメトリクスは常に報告されます。

# COMMAND ----------

# DBTITLE 0,--i18n-4cef0694-c05f-44ba-84bf-cd14a63eda17
# MAGIC %md
# MAGIC ## 追加データの読み込み(Land another batch of data)
# MAGIC
# MAGIC 以下のセルを実行してソースディレクトリに追加のデータを読み込み、手動でパイプラインの更新をトリガーします。

# COMMAND ----------

DA.dlt_data_factory.load()

# COMMAND ----------

# DBTITLE 0,--i18n-58129206-f245-419e-b51e-b126376a9a45
# MAGIC %md
# MAGIC コースが進んでからも、このノートブックに戻り、上記の方法を使用して、新しいデータを読み込むことができます。
# MAGIC
# MAGIC このノートブック全体を再度実行すると、ソースデータとDLTパイプラインの両方の基礎データファイルが削除されます。
# MAGIC
# MAGIC クラスタと切断した場合などで、データを削除することなく追加のデータを読み込ませたい場合、<a href="$./DE 4.99 - Land New Data" target="_blank">DE 4.99 - Land New Data</a>のノートブックを参照ください。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
