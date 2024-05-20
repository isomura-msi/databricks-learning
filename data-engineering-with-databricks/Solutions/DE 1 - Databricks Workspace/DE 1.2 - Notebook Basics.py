# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-2d4e57a0-a2d5-4fad-80eb-98ff30d09a37
# MAGIC %md
# MAGIC # ノートブックの基本（Notebook Basics）
# MAGIC
# MAGIC ノートブックは、Databricksでインタラクティブにコードを開発および実行するための主要な手段です。 このレッスンでは、Databricksノートブックの基本的な使い方を説明します。
# MAGIC
# MAGIC Databricksノートブックの実行とDatabricks Reposでのノートブック実行は基本的に同じ機能になります。次のレッスンでは、Databricks Reposのほうで追加されたノートブック機能の一部を説明します。
# MAGIC
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このレッスンでは、以下のことを学びます。
# MAGIC * ノートブックをクラスタにアタッチする
# MAGIC * ノートブックでセルを実行する
# MAGIC * ノートブックの言語を設定する
# MAGIC * MAGICコマンドを記述して使用する
# MAGIC * SQLセルを作成して実行する
# MAGIC * Pythonセルを作成して実行する
# MAGIC * Markdownセルを作成する
# MAGIC * Databricksノートブックをエクスポートする
# MAGIC * Databricksノートブックのコレクションをエクスポートする

# COMMAND ----------

# DBTITLE 0,--i18n-e7c4cc85-5ab7-46c1-9da3-f9181d77d118
# MAGIC %md
# MAGIC ## クラスタにアタッチする（Attach to a Cluster）
# MAGIC
# MAGIC 前のレッスンでは、クラスタをデプロイしたか、使用できるように管理者が設定したクラスタを確認したかと思います。
# MAGIC
# MAGIC 画面右上端にあるクラスタセレクタ(「接続」ボタン)をクリックして、ドロップダウンメニューからクラスタを選択してください。ノートブックがクラスタに接続されると、このボタンはクラスタ名を表示します。
# MAGIC
# MAGIC **注**：クラスタのデプロイには数分かかります。リソースがデプロイされると、緑色の円がクラスタ名の左側に表示されます。 クラスタの左側にグレーの実線の円が表示されている場合は、手順に従って<a href="https://docs.databricks.com/clusters/clusters-manage.html#start-a-cluster" target="_blank">クラスタを起動する</a>必要があります。

# COMMAND ----------

# DBTITLE 0,--i18n-23aed87f-d375-4b81-8c3c-d4375cda0384
# MAGIC %md
# MAGIC ## ノートブックの基本（Notebooks Basics）
# MAGIC
# MAGIC ノートブックでは、コードをセルごとに実行できます。 ノートブックには複数の言語を混在させることができます。ユーザーは、プロット、画像、Markdownテキストを追加して、コードを拡張できます。
# MAGIC
# MAGIC このコースを通して、ノートブックを学習の道具として作成しています。 ノートブックは本番コードとして簡単にデプロイできるだけでなく、データ探索、レポート作成、ダッシュボード用のツールセットも備わっています。
# MAGIC
# MAGIC ### セルの実行（Running a Cell）
# MAGIC * 次のいずれかのオプションを使って、以下のセルを実行します：
# MAGIC   * **CTRL+ENTER**または**CTRL+RETURN**
# MAGIC   * **SHIFT+ENTER**または**SHIFT+RETURN**でセルを実行し、次のセルに移動します
# MAGIC   * 画像のように**セルを実行**または**上記をすべてを実行**または**以下のすべてを実行**を使い分けます<br/><img style="box-shadow: 5px 5px 5px 0px rgba(0,0,0,0.25); border: 1px solid rgba(0,0,0,0.25);" src="https://files.training.databricks.com/images/notebook-cell-run-cmd.png" />

# COMMAND ----------

print("I'm running Python!")

# COMMAND ----------

# DBTITLE 0,--i18n-5b14b4c2-c009-4786-8058-a3ddb61fa41d
# MAGIC %md
# MAGIC **注**：セルごとのコードの実行では、セルを複数回実行したり、順序が狂ったりする可能性があります。 明確に指示されない限り、このコースのノートブックは、上から下に向かって一度に一つずつセルを実行すると思ってください。 エラーが発生した場合は、トラブルシューティングを試みる前に、セルの前後のテキストを読んで、エラーが意図的な学習の機会ではないことを確認してください。 ほとんどのエラーは、見落としていた以前のセルをノートブックで実行するか、ノートブック全体を上から再実行することで解決できます。

# COMMAND ----------

# DBTITLE 0,--i18n-9be4ac54-8411-45a0-ad77-7173ec7402f8
# MAGIC %md
# MAGIC ### ノートブックのデフォルト言語の設定（Setting the Default Notebook Language）
# MAGIC
# MAGIC ノートブックの現在のデフォルト言語がPythonに設定されているため、上のセルはPythonコマンドを実行します。
# MAGIC
# MAGIC Databricksノートブックは、Python、SQL、Scala、Rをサポートしています。ノートブックの作成時に言語を選択できますが、これはいつでも変更できます。
# MAGIC
# MAGIC デフォルトの言語は、ページ上部のノートブックタイトルのすぐ右側に表示されます。 このコースでは全体的に、SQLノートブックとPythonノートブックを組み合わせて使用します。
# MAGIC
# MAGIC このノートブックのデフォルト言語をSQLに変更します。
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC * 画面上部のノートブックタイトルの横にある**Python**をクリックします
# MAGIC * ポップアップしたUIで、ドロップダウンリストから**SQL**を選択します
# MAGIC
# MAGIC **注**：このセルの直前のセルに、 <strong><code>&#37;python</code></strong>が付いた新しい行が表示されるはずです。 これについては後ほど説明します。

# COMMAND ----------

# DBTITLE 0,--i18n-3185e9b5-fcba-40aa-916b-5f3daa555cf5
# MAGIC %md
# MAGIC ### SQLセルを作成して実行する（Create and Run a SQL Cell）
# MAGIC
# MAGIC * このセルをハイライトし、キーボードの**B**ボタンを押すと、下に新しいセルが作成されます
# MAGIC * 次のコードを下のセルにコピーして、セルを実行します
# MAGIC
# MAGIC **`%sql`**<br/> **`SELECT "I'm running SQL!"`**
# MAGIC
# MAGIC **注**：セルを追加、移動、および削除するには、GUIオプションやキーボードショートカットなど、さまざまな方法があります。 詳細については、<a href="https://docs.databricks.com/notebooks/notebooks-use.html#develop-notebooks" target="_blank">ドキュメント</a>を参照してください。

# COMMAND ----------

# DBTITLE 0,--i18n-5046f81c-cdbf-42c3-9b39-3be0721d837e
# MAGIC %md
# MAGIC ## MAGICコマンド
# MAGIC * MAGICコマンドは、Databricksノートブック固有のものです
# MAGIC * 同等のノートブックプロダクトにみられるMAGICコマンドと非常によく似ています
# MAGIC * これらはノートブックの言語に関係なく、同じ結果をもたらす組み込みコマンドです
# MAGIC * セルの先頭にある1つのパーセント（%）記号は、MAGICコマンドであることを示します
# MAGIC   * 1つのセルにつき1つのMAGICコマンドしか使えません
# MAGIC   * MAGICコマンドはセルの最初に置かなければなりません

# COMMAND ----------

# DBTITLE 0,--i18n-39d2c50e-4b92-46ef-968c-f358114685be
# MAGIC %md
# MAGIC ### 言語MAGIC（Language Magics）
# MAGIC 言語MAGICコマンドを使えば、ノートブックのデフォルト以外の言語のコードを実行できます。 このコースでは、次の言語MAGICが見られます：
# MAGIC * <strong><code>&#37;python</code></strong>
# MAGIC * <strong><code>&#37;sql</code></strong>
# MAGIC
# MAGIC 現在設定されているノートブックのタイプに言語MAGICを追加する必要はありません。
# MAGIC
# MAGIC 上記のノートブックの言語をPythonからSQLに変更したとき、Pythonで記述された既存のセルに<strong><code>&#37;python</code></strong>コマンドが追加されました。
# MAGIC
# MAGIC **注**：ノートブックのデフォルト言語を何度も変更するのではなく、デフォルトとして第一言語を使用し、別の言語でコードを実行する必要がある場合にのみ言語MAGICを使いましょう。

# COMMAND ----------

print("Hello Python!")

# COMMAND ----------

# MAGIC %sql
# MAGIC select "Hello SQL!"

# COMMAND ----------

# DBTITLE 0,--i18n-94da1696-d0cf-418f-ba5a-d105a5ecdaac
# MAGIC %md
# MAGIC ### Markdown
# MAGIC
# MAGIC MAGICコマンド **&percnt;md** を使うと、Markdownをセルにレンダリングできます：
# MAGIC * このセルをダブルクリックして、編集を開始します
# MAGIC * 次に **`Esc`** を押すと編集を停止します
# MAGIC
# MAGIC # タイトル1（Title One）
# MAGIC ## タイトル2（Title One）
# MAGIC ### タイトル3（Title Three）
# MAGIC
# MAGIC これは単なるテキストです。
# MAGIC
# MAGIC これは **太字の** 単語を含むテキストです。
# MAGIC
# MAGIC これは、*イタリック体* の単語を含むテキストです。
# MAGIC
# MAGIC これは順序付きリストです
# MAGIC 1. 一
# MAGIC 1. 二
# MAGIC 1. 三
# MAGIC
# MAGIC これは順不同のリストです
# MAGIC * リンゴ
# MAGIC * モモ
# MAGIC * バナナ
# MAGIC
# MAGIC リンク/埋め込みHTML：<a href="https://en.wikipedia.org/wiki/Markdown" target="_blank">Markdown - ウィキペディア</a>
# MAGIC
# MAGIC 画像： ![Sparkエンジン](https://files.training.databricks.com/images/Apache-Spark-Logo_TM_200px.png)
# MAGIC
# MAGIC テーブル：
# MAGIC
# MAGIC | 名前     | 値 |
# MAGIC | ------ | - |
# MAGIC | Yi     | 1 |
# MAGIC | Ali    | 2 |
# MAGIC | Selina | 3 |

# COMMAND ----------

# DBTITLE 0,--i18n-537e86bc-782f-4167-9899-edb3bd2b9e38
# MAGIC %md
# MAGIC ### %run
# MAGIC * MAGICコマンド **%run** を使うと、ノートブックを別のノートブックから実行できます
# MAGIC * 実行するノートブックは、相対パスで指定されます
# MAGIC * 参照されたノートブックは、現在のノートブックの一部であるかのように実行されるため、テンポラリビューやその他のローカル宣言は、呼び出し元のノートブックから利用できます。

# COMMAND ----------

# DBTITLE 0,--i18n-d5c27671-b3c8-4b8c-a559-40cf7988f92f
# MAGIC %md
# MAGIC 次のセルからコメントアウトを外して実行すると次のエラーが発生します：<br/> **`Error in SQL statement: AnalysisException: Table or view not found: demo_tmp_vw`**

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-d0df4a17-abb4-42d3-ba37-c9a78f4fc9c0
# MAGIC %md
# MAGIC しかし、このセルを実行することで、それと他のいくつかの変数と関数を宣言できます：

# COMMAND ----------

# MAGIC %run ./Includes/Classroom-Setup-01.2

# COMMAND ----------

# DBTITLE 0,--i18n-6a001755-5259-4fef-a5d3-2661d5301237
# MAGIC %md
# MAGIC 参照した **`../Includes/Classroom-Setup-01.2`** ノートブックには、データベースを作成し、 **`USE`** するためのロジックとテンポラリビュー **`demo_temp_vw`** の作成も含まれています。
# MAGIC
# MAGIC 次のクエリを使って、このテンポラリビューは、現在のノートブックセッションで利用できるようになっていることが確認できます。

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM demo_tmp_vw

# COMMAND ----------

# DBTITLE 0,--i18n-c28ecc03-8919-488f-bce7-e2fc0a451870
# MAGIC %md
# MAGIC このパターンの「セットアップ」ノートブックをコース全体で使用して、レッスンとラボの環境構成に役立てます。
# MAGIC
# MAGIC これらの「提供された」変数、関数、その他のオブジェクトは、 **`DBAcademyHelper`** のインスタンスである **`DA`** オブジェクトの一部であると簡単に識別できるかと思います。
# MAGIC
# MAGIC このことを念頭に置き、大部分のレッスンでは、ユーザー名から派生した変数を使用してファイルやデータベースを整理します。
# MAGIC
# MAGIC このパターンを使うことで、共有ワークスペースでの他のユーザーとの衝突を回避できます。
# MAGIC
# MAGIC 以下のセルは、Pythonを使用して、このノートブックのセットアップスクリプトで以前に定義された変数の一部を表示します：

# COMMAND ----------

print(f"DA:                   {DA}")
print(f"DA.username:          {DA.username}")
print(f"DA.paths.working_dir: {DA.paths.working_dir}")
print(f"DA.schema_name:       {DA.schema_name}")

# COMMAND ----------

# DBTITLE 0,--i18n-1145175f-c51e-4cf5-a4a5-b0e3290a73a2
# MAGIC %md
# MAGIC これに加えて、これらの同じ変数がSQLコンテキストに「注入される」ため、SQL文の中で使えます。
# MAGIC
# MAGIC これについては後ほど詳しく説明しますが、次のセルで簡単な例を見ることができます。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_32.png" /> この2つの例では、単語 **`da`** と **`DA`** の大文字と小文字という微妙ですが重要な違いに注意してください。

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT '${da.username}' AS current_username,
# MAGIC        '${da.paths.working_dir}' AS working_directory,
# MAGIC        '${da.schema_name}' as schema_name

# COMMAND ----------

# DBTITLE 0,--i18n-8330ad3c-8d48-42fa-9b6f-72e818426ed4
# MAGIC %md
# MAGIC ## Databricksユーティリティ （Databricks Utilities）
# MAGIC Databricksノートブックには、環境を設定したり操作したりするための多数のユーティリティコマンドを提供する　　**`dbutils`** が含まれています：<a href="https://docs.databricks.com/user-guide/dev-tools/dbutils.html" target="_blank">dbutils docs</a>
# MAGIC
# MAGIC このコースでは、時折 **`dbutils.fs.ls()`** を使ってPythonセルからディレクトリに存在するファイルを出力します。

# COMMAND ----------

path = f"{DA.paths.datasets}"
dbutils.fs.ls(path)

# COMMAND ----------

# DBTITLE 0,--i18n-25eb75f8-6e75-41a6-a304-d140844fa3e6
# MAGIC %md
# MAGIC ## display()
# MAGIC
# MAGIC セルからSQLクエリを実行した場合、結果は常にレンダリングされた表形式で表示されます。
# MAGIC
# MAGIC Pythonセルが返した表形式のデータがある場合、 **`display`** を呼び出して、同じタイプのプレビューを取得できます。
# MAGIC
# MAGIC ここでは、ファイルシステム上で先ほどのリストコマンドを **`display`** で囲います。

# COMMAND ----------

path = f"{DA.paths.datasets}"
files = dbutils.fs.ls(path)
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-42b624ff-8cc9-4318-bedc-e3b340cb1b81
# MAGIC %md
# MAGIC **`display()`** コマンドには次の機能と制限があります：
# MAGIC * 結果のプレビューの上限は1000レコードまで
# MAGIC * 結果データをCSV形式でダウンロードするボタンを提供する
# MAGIC * プロットのレンダリングが可能

# COMMAND ----------

# DBTITLE 0,--i18n-c7f02dde-9b21-4edb-bded-5ce0eed56d03
# MAGIC %md
# MAGIC ## ノートブックのダウンロード(Downloading Notebooks)
# MAGIC
# MAGIC 個々のノートブックやノートブックのコレクションはさまざまな方法でダウンロードできます。
# MAGIC
# MAGIC ここでは、このノートブックだけでなく、このコースのすべてのコレクションをダウンロードする手順を詳しく説明します。
# MAGIC
# MAGIC ### ノートブックをダウンロードする（Download a Notebook）
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC * ノートブック左上部の**ファイル**オプションをクリックします
# MAGIC * 表示されたメニューから、**エクスポート**にカーソルを重ね、**ソースファイル**を選択します
# MAGIC
# MAGIC ノートブックがノートパソコンにダウンロードされます。 ファイル名は現在のノートブック名で、デフォルト言語のファイル拡張子が付いています。 このノートブックは任意のファイルエディタで開いて、Databricksノートブックの未加工の内容を見ることができます。
# MAGIC
# MAGIC これらのソースファイルは、どんなDatabricksワークスペースにもアップロードできます。
# MAGIC
# MAGIC ### ノートブックのコレクションをダウンロードする（Download a Collection of Notebooks）
# MAGIC
# MAGIC **注**：次の説明は、**Repos**を使ってこれらのデータをインポートしていることを前提としています。
# MAGIC
# MAGIC 手順は、次の通りです。
# MAGIC * 左のサイドバーにある  ![](https://files.training.databricks.com/images/repos-icon.png) **リポジトリ** をクリックします
# MAGIC   * これにより、このノートブックの親ディレクトリのプレビューが表示されるはずです
# MAGIC * 画面の中央付近のディレクトリプレビューの左側に、左矢印があるはずです。 これをクリックして、ファイル階層の上へ移動してください。
# MAGIC * **Data Engineer Learning Path**というディレクトリが表示されるはずです。 下矢印/逆V字型をクリックすると、メニューが表示されます
# MAGIC * このメニューから、**エクスポート**にカーソルを重ね、**DBCアーカイブ**を選択します
# MAGIC
# MAGIC ダウンロードされたDBC（Databricksクラウド）ファイルには、このコースのディレクトリとノートブックの圧縮されたコレクションが含まれています。 ユーザーはこれらのDBCファイルをローカルで編集しないでください。ただし、任意のDatabricksワークスペースに安全にアップロードして、ノートブックのコンテンツを移動または共有することは可能です。
# MAGIC
# MAGIC **注**：DBCのコレクションをダウンロードすると、結果のプレビューとプロットもエクスポートされます。 ソースノートブックをダウンロードする場合、コードのみが保存されます。

# COMMAND ----------

# DBTITLE 0,--i18n-30e63e01-ca85-461a-b980-ea401904731f
# MAGIC %md
# MAGIC ## さらに学ぶ（Learning More）
# MAGIC
# MAGIC Databricksプラットフォームとノートブックのさまざまな機能について詳しく知るために、ドキュメントをよく読むことをお勧めします。
# MAGIC * <a href="https://docs.databricks.com/user-guide/index.html#user-guide" target="_blank">ユーザーガイド</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/getting-started.html" target="_blank">Databricks入門</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/notebooks/index.html" target="_blank">ユーザーガイド / ノートブック</a>
# MAGIC * <a href="https://docs.databricks.com/notebooks/notebooks-manage.html#notebook-external-formats" target="_blank">ノートブックのインポート - サポートされている形式</a>
# MAGIC * <a href="https://docs.databricks.com/repos/index.html" target="_blank">Repos</a>
# MAGIC * <a href="https://docs.databricks.com/administration-guide/index.html#administration-guide" target="_blank">管理ガイド</a>
# MAGIC * <a href="https://docs.databricks.com/user-guide/clusters/index.html" target="_blank">クラスタの設定</a>
# MAGIC * <a href="https://docs.databricks.com/api/latest/index.html#rest-api-2-0" target="_blank">REST API</a>
# MAGIC * <a href="https://docs.databricks.com/release-notes/index.html#release-notes" target="_blank">リリースノート</a>

# COMMAND ----------

# DBTITLE 0,--i18n-9987fd58-1023-4dbd-8319-40332f909181
# MAGIC %md
# MAGIC ## もうひとつだけ！（One more note!）
# MAGIC
# MAGIC 各レッスンの最後に、次のコマンド、 **`DA.cleanup()`** が表示されます。
# MAGIC
# MAGIC この方法では、ワークスペースをクリーンに保ち、各レッスンの不変性を維持するために、レッスン固有のデータベースと作業ディレクトリを削除します。
# MAGIC
# MAGIC 次のセルを実行して、このレッスンに関連するテーブルとファイルを削除してください。

# COMMAND ----------

DA.cleanup()

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
