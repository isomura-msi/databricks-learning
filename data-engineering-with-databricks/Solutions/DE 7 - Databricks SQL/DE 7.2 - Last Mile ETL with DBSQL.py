# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-9390aad6-4d8b-4e7f-8851-18ca0b6bf7c6
# MAGIC %md
# MAGIC # Databricks SQLを使用したラストワンマイルETL（Last Mile ETL with Databricks SQL）
# MAGIC
# MAGIC 続ける前に、これまでに学習した内容を少しおさらいしておきましょう。
# MAGIC 1. Databricksワークスペースには、データエンジニアリングの開発ライフサイクルを簡素化するために役立つツール群が含まれています。
# MAGIC 1. Databricksノートブックにより、ユーザーはSQLと他のプログラミング言語を組み合わせてETLワークロードを定義することができます。
# MAGIC 1. Delta LakeはACIDに準拠したトランザクションを提供し、レイクハウスで簡単に増分データの処理を行うことができます。
# MAGIC 1. Delta Live TablesはSQL構文を拡張し、レイクハウスの数多くのデザインパターンをサポートしたり、インフラの展開を簡素化したりします。
# MAGIC 1. マルチタスクジョブによって完全なタスクオーケストレーションが可能となり、ノートブックとDLTパイプラインを組み合わせてスケジューリングを行いながら依存関係を追加することができます。
# MAGIC 1. Databricks SQLでは、SQLクエリの編集と実行、ビジュアライゼーションの作成、ダッシュボードの定義などが可能です。
# MAGIC 1. Data ExplorerはテーブルACLの管理を簡素化し、レイクハウスのデータをSQLアナリストが利用できるようにします。
# MAGIC
# MAGIC このセクションでは、本番環境のワークロードをサポートするためにより多くのDBSQLの機能を説明することに焦点を当てます。
# MAGIC
# MAGIC まず、Databricks SQLを活用した分析のためのラストワンマイルETLをサポートするクエリの構成に焦点を当てます。 このデモではDatabricks SQL UIを使用しますが、SQLウェアハウスは<a href="https://docs.databricks.com/integrations/partners.html" target="_blank">他の多くのツールと統合して外部クエリの実行を可能にし</a>、<a href="https://docs.databricks.com/sql/api/index.html" target="_blank">プログラムを使用して任意のクエリを実行するフルAPIサポート</a>を備えていることにご注意ください。
# MAGIC
# MAGIC これらのクエリ結果から一連のビジュアライゼーションを生成し、ダッシュボードにまとめていきます。
# MAGIC
# MAGIC 最後に、クエリやダッシュボードの更新をスケジュールし、アラートを設定することで本番用データセットの状態の時系列に沿った監視をサポートする方法について説明します。
# MAGIC
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことが学べます。
# MAGIC * 分析ワークロードを支える本番環境のETLタスクをサポートするツールとしてDatabricks SQLを使用する
# MAGIC * Databricks SQLエディタを使用してSQLクエリおよびビジュアライゼーションを構成する
# MAGIC * Databricks SQLでダッシュボードを作成する
# MAGIC * クエリやダッシュボードの更新をスケジュールする
# MAGIC * SQLクエリのアラートを設定する

# COMMAND ----------

# DBTITLE 0,--i18n-9ff94613-d58b-48fe-b32d-3718cbcb2f30
# MAGIC %md
# MAGIC ## セットアップスクリプトの実行（Run Setup Script）
# MAGIC 次のセルでは、SQLクエリを生成するために使用するクラスを定義したノートブックを実行します。

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-07.2

# COMMAND ----------

# DBTITLE 0,--i18n-c384931f-c934-440f-9f0c-a7b04c1c28b0
# MAGIC %md
# MAGIC ## デモデータベースの作成（Create a Demo Database）
# MAGIC 次のセルを実行し、その結果をDatabricks SQL Editorへとコピーします。
# MAGIC
# MAGIC これらのクエリでは、以下の操作が実行されます。
# MAGIC * 新規データベースの作成
# MAGIC * 2つのテーブルの宣言（これらはデータの読み込みに使用します）
# MAGIC * 2つの関数の宣言（これらはデータの生成に使用します）
# MAGIC
# MAGIC コピーしたら、**実行**ボタンでクエリを実行します。

# COMMAND ----------

DA.generate_config()

# COMMAND ----------

# DBTITLE 0,--i18n-9cd7f410-eead-4759-bd99-83eafb03d0df
# MAGIC %md
# MAGIC **注**：上記のクエリは、環境を再構成するために、デモを完全にリセットした後に一度だけ実行するものです。 ユーザーは、これらのクエリを実行するためにカタログ上で **`CREATE`** および **`USAGE`** 権限を持っている必要があります。

# COMMAND ----------

# DBTITLE 0,--i18n-3652d1e3-fc73-44f9-9b07-6b1b8b5343d6
# MAGIC %md
# MAGIC <img src="https://files.training.databricks.com/images/icon_warn_32.png" /> **警告：**  **`USE`** 文はクエリを実行するデータベースをまだ変更しないため、<br/>先に進む前にデータベースを必ず選択しておいてください。

# COMMAND ----------

# DBTITLE 0,--i18n-a0b2608d-d22e-4085-b02f-be43b744ecf2
# MAGIC %md
# MAGIC ## クエリを作成してデータを読み込む（Create a Query to Load Data）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 次のセルを実行すると、前の手順で作成した **`user_ping`** テーブルにデータを読み込むためにフォーマットされたSQLクエリが出力されます。
# MAGIC 1. このクエリを**Load Ping Data**という名前で保存します。
# MAGIC 1. このクエリを実行して、データのバッチを読み込みます。

# COMMAND ----------

DA.generate_load()

# COMMAND ----------

# DBTITLE 0,--i18n-48fc3085-b405-450f-9463-ee1501cd56aa
# MAGIC %md
# MAGIC クエリを実行すると、いくつかのデータが読み込まれ、テーブル内にあるデータのプレビューが返されるはずです。
# MAGIC
# MAGIC **注**：データの定義と読み込みには乱数が使用されているため、ユーザーごとに若干異なる値が割り振られます。

# COMMAND ----------

# DBTITLE 0,--i18n-7dc1c1c9-96c5-49f7-a1ab-deb44594cddf
# MAGIC %md
# MAGIC ## クエリの更新スケジュールを設定する（Set a Query Refresh Schedule）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. SQLクエリエディターボックスの右上にある**スケジュール**をクリックします
# MAGIC 1. ドロップダウンを使用し、更新頻度を**1 week**、時刻を**12:00**へ変更します。
# MAGIC 1. 明日の曜日を選択します
# MAGIC 1. **OK**をクリックします
# MAGIC
# MAGIC **注:** クラスの目的で1週間の更新スケジュールを使用していますが、1分ごとに更新するスケジュールなど、本番環境ではより短いトリガー間隔が設定する場合があります。

# COMMAND ----------

# DBTITLE 0,--i18n-58aa7a7b-4b43-429d-8603-0cc1001a5ae6
# MAGIC %md
# MAGIC ## レコードの総数を追跡するクエリの作成（Create a Query to Track Total Records）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 以下のセルを実行します。
# MAGIC 1. このクエリを**User Counts**という名前で保存します。
# MAGIC 1. クエリを実行し、現在の結果を計算します。

# COMMAND ----------

DA.generate_user_counts()

# COMMAND ----------

# DBTITLE 0,--i18n-c43c8f47-c8c3-4232-9a76-f82bdd204317
# MAGIC %md
# MAGIC ## 棒グラフのビジュアライゼーションの作成（Create a Bar Graph Visualization）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. Resultsタブの右側の**＋**をクリックして **`可視化`** を選択します
# MAGIC 1. 名前（デフォルトの状態は **`Bar 1`** などになっています）をクリックし、名前を**Total User Records**へと変更します
# MAGIC 1. **X column**に **`user_id`** を設定します
# MAGIC 1. **Y column**に **`total_records`** を設定します
# MAGIC 1. **保存**をクリックします

# COMMAND ----------

# DBTITLE 0,--i18n-644501a5-c5fc-45e0-bd34-728413b5d267
# MAGIC %md
# MAGIC ## 新しいダッシュボードの作成（Create a New Dashboard）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. **Total User Records**の右側にある矢印をクリックし、**ダッシュボードに追加**を選択します
# MAGIC 1. **新規ダッシュボードを作成**オプションをクリックします
# MAGIC 1. ダッシュボードに<strong>User Ping Summary  **`<your_initials_here>`** </strong>という名前を付けます
# MAGIC 1. **保存**をクリックして新しいダッシュボードを作成します
# MAGIC 1. 新しく作成したダッシュボードが対象として選択されているはずですので、**OK**をクリックしてビジュアライゼーションを追加します

# COMMAND ----------

# DBTITLE 0,--i18n-400bfcee-6c58-4863-b876-609950543f6f
# MAGIC %md
# MAGIC ## 最近のPingの平均時間を計算するクエリの作成（Create a Query to Calculate the Recent Average Ping）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 次のセルを実行すると、フォーマットされたSQLクエリが出力されます。
# MAGIC 1. このクエリを**Avg Ping**という名前で保存します。
# MAGIC 1. クエリを実行し、現在の結果を計算します。

# COMMAND ----------

DA.generate_avg_ping()

# COMMAND ----------

# DBTITLE 0,--i18n-82ba8c89-8e4b-4ab5-817a-1161017a1168
# MAGIC %md
# MAGIC ## ダッシュボードへのラインプロットビジュアライゼーションの追加（Add a Line Plot Visualization to your Dashboard）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. **＋**ボタンをクリックします
# MAGIC 1. 名前（デフォルトの状態は **`Bar 1`** などになっています）をクリックし、名前を**Avg User Ping**へと変更します
# MAGIC 1. **Visualization Type**に **`Line`** を選択します。
# MAGIC 1. **X列**に **`end_time`** を設定します。
# MAGIC 1. **Y列**に **`avg_ping`** を設定します。
# MAGIC 1. **Group by**に **`user_id`** を設定します。
# MAGIC 1. **保存**をクリックします
# MAGIC 1. 結果の右側の矢印をクリックし、**ダッシュボードに追加**を選択します
# MAGIC 1. 先ほど作成したダッシュボードを選択します
# MAGIC 1. **OK**をクリックしてビジュアライゼーションを追加します

# COMMAND ----------

# DBTITLE 0,--i18n-9088bbce-cf24-4731-80c0-15972786eda1
# MAGIC %md
# MAGIC ## 統計情報の概要を報告するクエリの作成（Create a Query to Report Summary Statistics）
# MAGIC 手順は、次の通りです。
# MAGIC 1. 以下のセルを実行します。
# MAGIC 1. このクエリを**Ping Summary**という名前で保存します。
# MAGIC 1. クエリを実行し、現在の結果を計算します。

# COMMAND ----------

DA.generate_summary()

# COMMAND ----------

# DBTITLE 0,--i18n-e5832d13-b8f3-40db-95cb-8c24e895cda7
# MAGIC %md
# MAGIC ## ダッシュボードに概要テーブルを追加する（Add the Summary Table to your Dashboard）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. 結果の右側の矢印をクリックし、**ダッシュボードに追加**を選択します
# MAGIC 1. 先ほど作成したダッシュボードを選択します
# MAGIC 1. **OK**をクリックしてビジュアライゼーションを追加します

# COMMAND ----------

# DBTITLE 0,--i18n-626f8b1d-51cd-47b0-8828-b35180acb40c
# MAGIC %md
# MAGIC ## ダッシュボードを確認して更新する（Review and Refresh your Dashboard）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. 左側のサイドバーを使用して、**ダッシュボード**に移動します
# MAGIC 1. クエリを追加したダッシュボードを見つけます
# MAGIC 1. 青色の**更新**ボタンをクリックしてダッシュボードを更新します
# MAGIC 1. **スケジュール**ボタンをクリックしてダッシュボードのスケジュール設定オプションを確認します
# MAGIC   * ダッシュボードの更新をスケジュール設定すると、そのダッシュボードに関連付けられているすべてのクエリが実行されますのでご注意ください。
# MAGIC   * この時点ではダッシュボードのスケジュール設定を行わないでください

# COMMAND ----------

# DBTITLE 0,--i18n-4f69c53f-8bd9-48d5-9f6f-f97045581e49
# MAGIC %md
# MAGIC ## ダッシュボードの共有（Share your Dashboard）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. 青色の**Share**ボタンをクリックします
# MAGIC 1. 一番上のフィールドから**All Users**を選択します
# MAGIC 1. 右側のフィールドから**編集可能**を選択します
# MAGIC 1. **追加**をクリックします
# MAGIC 1. **資格情報**を**閲覧者として実行**に変更します
# MAGIC
# MAGIC **注**：テーブルACLを使用して元となっているデータベースおよびテーブルに権限が付与されていないため、現時点ではダッシュボードを実行するための権限を持っている他のユーザーはいないはずです。 他のユーザーがダッシュボードの更新をトリガーできるようにするには、**所有者として実行**の権限を対象のユーザーに付与するか、クエリで参照しているテーブルの権限を追加する必要があります。

# COMMAND ----------

# DBTITLE 0,--i18n-facded12-10b1-4c63-a075-b7790fa3cd17
# MAGIC %md
# MAGIC ## アラートを設定する（Set Up an Alert）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. 左側のサイドバーを使用して、**アラート**に移動します
# MAGIC 1. 右上にある**アラートを作成**をクリックします
# MAGIC 1. **User Counts**クエリを選択します
# MAGIC 1. 画面の左上にあるフィールドをクリックし、アラートに **`<your_initials>Count Check`** という名前を付けます
# MAGIC 1. **トリガー条件**オプションを、次のように構成します。
# MAGIC   * **値列**： **`total_records`** 
# MAGIC   * **条件**： **`>`** 
# MAGIC   * **しきい値**： **`15`** 
# MAGIC 1. **リフレッシュ**で、**なし**を選択します
# MAGIC 1. **Create Alert**をクリックします
# MAGIC 1. 次の画面で、右上にある青色の**更新**をクリックし、アラートを評価します

# COMMAND ----------

# DBTITLE 0,--i18n-f9f22ebd-4283-474a-ab45-0d7584a6b6ac
# MAGIC %md
# MAGIC ## アラートの送信先オプションを確認する（Review Alert Destination Options）
# MAGIC
# MAGIC
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC 1. アラートのプレビューから、画面の右側にある**送信先**の右にある青色の **追加**ボタンをクリックします
# MAGIC 1. 表示されたウィンドウの一番下にある**アラート送信先に新規送信先を作成する**というメッセージの中にある青いテキストを探してクリックします
# MAGIC 1. 利用可能なアラートオプションを確認します

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
