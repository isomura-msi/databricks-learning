# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-4212c527-b1c1-4cce-b629-b5ffb5c57d68
# MAGIC %md
# MAGIC # Databricksプラットフォーム入門（Getting Started with the Databricks Platform）
# MAGIC
# MAGIC このノートブックでは、Databricksデータサイエンスおよびエンジニアリングのワークスペースの基本機能の一部を実践的に説明します。
# MAGIC
# MAGIC ## 学習目標（Learning Objectives）
# MAGIC このラボでは、以下のことが学べます。
# MAGIC - ノートブックの名前を変更してデフォルト言語を変更する
# MAGIC - クラスタをアタッチする
# MAGIC - MAGICコマンド  **`%run`**  を使う
# MAGIC - PythonセルとSQLセルを実行する
# MAGIC - Markdownセルを作成する

# COMMAND ----------

# DBTITLE 0,--i18n-eb166a05-9a22-4a74-b54e-3f9e5779f342
# MAGIC %md
# MAGIC # ノートブックの名称変更（Renaming a Notebook）
# MAGIC
# MAGIC ノートブックの名前の変更は簡単です。 このページの上部にある名前をクリックしてから、名前を変更します。 後で必要になったときにこのノートブックに簡単に戻れるように、既存の名前の末尾に短いテスト文字列を追加します。

# COMMAND ----------

# DBTITLE 0,--i18n-a975b60c-9871-4736-9b4f-194577d730f0
# MAGIC %md
# MAGIC # クラスタのアタッチ（Attaching a cluster）
# MAGIC
# MAGIC ノートブックでセルを実行するには、クラスタによって提供されるコンピュートリソースが必要です。 ノートブックでセルを初めて実行するときに、まだクラスタがアタッチされていない場合、クラスタにアタッチするように指示されます。
# MAGIC
# MAGIC このページの右上隅付近にあるドロップダウンをクリックして、このノートブックにクラスタをアタッチします。 以前に作成したクラスタを選択します。 これにより、ノートブックの実行状態がクリアされ、選択したクラスタにノートブックが接続されます。
# MAGIC
# MAGIC ドロップダウンメニューには、必要に応じてクラスタを起動または再起動するオプションがあることにご注意ください。 また、1回の動作でクラスタをデタッチして再アタッチすることもできます。 これは、必要なときに実行状態をクリアする場合に便利です。

# COMMAND ----------

# DBTITLE 0,--i18n-4cd4b089-e782-4e81-9b88-5c0abd02d03f
# MAGIC %md
# MAGIC # %runを使う（Using %run）
# MAGIC
# MAGIC どのような種類の複雑なプロジェクトでも、よりシンプルで再利用可能なコンポーネントに分解する機能があれば便利です。
# MAGIC
# MAGIC Databricksノートブックのコンテキストでは、この機能は  **`%run`**  MAGICコマンドによって提供されます。
# MAGIC
# MAGIC このように使用すると、変数、関数、コードブロックが現在のプログラミングコンテキストの一部になります。
# MAGIC
# MAGIC 次の例を考えてみましょう：
# MAGIC
# MAGIC  **`Notebook_A`** には4つのコマンドがあります：
# MAGIC   1.   **`name = "John"`**  
# MAGIC   2.   **`print(f"Hello {name}")`**   
# MAGIC   3.   **`%run ./Notebook_B`**  
# MAGIC   4.   **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC  **`Notebook_B`**  にはコマンドが1つしかありません：
# MAGIC   1.   **`full_name = f"{name} Doe"`**  
# MAGIC
# MAGIC  **`Notebook_B`**  を実行すると、変数  **`name`**  が  **`Notebook_B`**  では定義されていないため、実行に失敗します
# MAGIC
# MAGIC 同様に、  **`Notebook_A`**  は、  **`Notebook_A`**  で同じく定義されていない変数  **`full_name`**  を使用しているため、失敗すると思うかもしれませんが、そうではありません！
# MAGIC
# MAGIC 実際に起きるのは、2つのノートブックが以下のようにマージされ、 **それから**実行されるのです：
# MAGIC 1.  **`name = "John"`** 
# MAGIC 2.  **`print(f"Hello {name}")`** 
# MAGIC 3.  **`full_name = f"{name} Doe"`** 
# MAGIC 4.  **`print(f"Welcome back {full_name}")`**
# MAGIC
# MAGIC そしてこの結果、期待通りに動作します：
# MAGIC *  **`Hello John`** 
# MAGIC *  **`Welcome back John Doe`**

# COMMAND ----------

# DBTITLE 0,--i18n-40ca42ab-4275-4d92-b151-995429e54486
# MAGIC %md
# MAGIC このノートブックを含むフォルダには、 **`ExampleSetupFolder`** というのサブフォルダが含まれています。このサブフォルダには同様に、 **`example-setup`** というのノートブックが含まれています。
# MAGIC
# MAGIC この単純なノートブックは変数 **`my_name`** を宣言し、それを **`None`** に設定してから、 **`example_df`** というのデータフレームを作成します。
# MAGIC
# MAGIC example-setupノートブックを開き、名前が **`None`** ではなく、自分の名前 （または誰かの名前）を引用符で囲むように変更します。次の2つのセルは、 **`AssertionError`** を出さずに実行されます。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_note_24.png"> **`_utility-methods`**  と **`DBAcademyHelper`** もコースの設定に含まれているがこの練習には不要なので無視してください。

# COMMAND ----------

# MAGIC %run ./ExampleSetupFolder/example-setup

# COMMAND ----------

assert my_name is not None, "Name is still None"
print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-e5ef8dff-bfa6-4f9e-8ad3-d5ef322b978d
# MAGIC %md
# MAGIC ## Pythonセルを実行する（Run a Python cell）
# MAGIC
# MAGIC 次のセルを実行して、 **`example_df`** データフレームを表示することにより、  **`example-setup`** ノートブックが実行されたことを確認してください。 このテーブルは、値が増加する16行で構成されています。

# COMMAND ----------

display(example_df)

# COMMAND ----------

# DBTITLE 0,--i18n-6cb46bcc-9797-4782-931c-a7b8350146b2
# MAGIC %md
# MAGIC # 言語の変更（Change Language）
# MAGIC
# MAGIC このノートブックのデフォルト言語が、Pythonに設定されていることに注意してください。 これを変更するには、ノートブック名の右にある**Python**ボタンをクリックします。 デフォルト言語をSQLに変更します。
# MAGIC
# MAGIC セルの有効性を維持するために、Pythonセルには<strong><code>&#37;python</code></strong>MAGICコマンドが先頭に自動的に追加されていることに注意してください。 この操作によって実行状態もクリアされることに注意してください。

# COMMAND ----------

# DBTITLE 0,--i18n-478faa69-6814-4725-803b-3414a1a803ae
# MAGIC %md
# MAGIC # Markdownセルを作成する（Create a Markdown Cell）
# MAGIC
# MAGIC このセルの下に新しいセルを追加します。 少なくとも次の要素を含むMarkdownをいくつか追加します：
# MAGIC * ヘッダ
# MAGIC * 箇条書き
# MAGIC * リンク（HTMLまたはMarkdown記法で好みのものを使用）

# COMMAND ----------

# DBTITLE 0,--i18n-55b2a6c6-2fc6-4c57-8d6d-94bba244d86e
# MAGIC %md
# MAGIC ## SQLセルを実行する（Run a SQL cell）
# MAGIC
# MAGIC SQLを使用してDeltaテーブルを照会するには、次のセルを実行します。 これは、すべてのDBFSインストールに含まれる、DataBricks提供のサンプルデータセットに基づいているテーブルに対し、単純なクエリを実行します。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`${DA.paths.datasets}/nyctaxi-with-zipcodes/data`

# COMMAND ----------

# DBTITLE 0,--i18n-9993ed50-d8cf-4f37-bc76-6b18789447d6
# MAGIC %md
# MAGIC 次のセルを実行して、このテーブルが基づいている基本ファイルを表示します。

# COMMAND ----------

files = dbutils.fs.ls(f"{DA.paths.datasets}/nyctaxi-with-zipcodes/data")
display(files)

# COMMAND ----------

# DBTITLE 0,--i18n-c31a3318-a114-46e8-a744-18e8f8aa071e
# MAGIC %md
# MAGIC # ノートブックの状態をクリアする (Clearing notebook state)
# MAGIC 時には、ノートブックで定義されているすべての変数をクリアして、最初からやり直すのが便利な場合があります。例えば、セルを単独でテストしたり、実行状態をリセットしたりする場合に便利です。
# MAGIC
# MAGIC **実行** メニューにアクセスし、**状態と出力をクリア** を選択します。
# MAGIC
# MAGIC 下のセルを実行してみてください。上のセルを再実行するまで、以前に定義した変数が定義されていないことに注目してください。

# COMMAND ----------

print(my_name)

# COMMAND ----------

# DBTITLE 0,--i18n-1e11bea0-7be9-4df7-be4e-b525c625dfee
# MAGIC %md
# MAGIC # 変更を確認する（Review Changes）
# MAGIC
# MAGIC Databricks Repoを使用してこのデータをワークスペースにインポートした場合、このページの左上隅にある **`published`** ブランチボタンをクリックして、Repoダイアログを開いてください。 変更が3つあります：
# MAGIC 1. **削除**項目で以前のノートブック名前
# MAGIC 1. **追加**項目で新しいノートブックの名前
# MAGIC 1. **変更**項目で上記のMarkdownセルの作成
# MAGIC
# MAGIC ダイアログを使用して変更を元に戻し、このノートブックを元の状態に復元します。

# COMMAND ----------

# DBTITLE 0,--i18n-9947d429-2c10-4047-811f-3f5128527c6d
# MAGIC %md
# MAGIC ## まとめ（Wrapping Up）
# MAGIC
# MAGIC このラボでは、ノートブックの操作、新しいセルの作成、ノートブック内でのノートブックの実行を学びました。

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
