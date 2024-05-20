# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-2396d183-ba9d-4477-a92a-690506110da6
# MAGIC %md
# MAGIC # Databricks SQL のナビゲートと SQL ウェアハウスへのアタッチ (Navigating Databricks SQL and Attaching to SQL Warehouses)
# MAGIC
# MAGIC * Databricks SQLに切り替えます  
# MAGIC   * サイドバー (Databricks ロゴのすぐ下) のワークスペース オプションから SQL が選択されていることを確認します
# MAGIC * SQL ウェアハウスがオンになっており、アクセス可能であることを確認します
# MAGIC   * サイドバーの SQL ウェアハウスに移動します
# MAGIC   * 状態が **`Running`** になっているSQL ウェアハウスが存在するの場合、このSQLウェアハウスを使用します
# MAGIC   * SQL ウェアハウスが存在するが **`停止`** されている場合、 **`開始`** ボタン(ボタンがある場合)をクリックして起動します ( **注** :利用可能な最小の SQL ウェアハウスを起動します) 
# MAGIC   * SQL ウェアハウスが存在せず、 **`SQL ウェアハウスを作成`** オプションがある場合はそれをクリックします。SQLウェアハウスにわかりやすい名前を付け、クラスター サイズを 2X-Small に設定します。他のすべてのオプションはデフォルトのままにします
# MAGIC   * SQL ウェアハウスを作成またはアタッチする権限がない場合は、ワークスペース管理者に連絡して、Databricks SQL のコンピューティング リソースへのアクセスを要求して続行する必要があります。
# MAGIC * Databricks SQL のホームページに移動します
# MAGIC   * サイド バーの上部にある Databricks のロゴをクリックします。
# MAGIC * **サンプルダッシュボード**を見つけて、 **`ギャラリーに移動`** をクリックします
# MAGIC * **Retail Revenue & Supply Chain**オプションの横にある **`インポート`** をクリックします
# MAGIC   * 利用可能なSQL ウェアハウスがある前提で、ダッシュボードが読み込まれ、すぐに結果が表示されます
# MAGIC   * 右上の **更新** をクリックします (今回は基になるデータは変更されていませんが、これは変更を取得するために使用されるボタンです)
# MAGIC
# MAGIC # DBSQL ダッシュボードの更新 (Updating a DBSQL Dashboard)
# MAGIC
# MAGIC * サイドバー ナビゲーターから **ダッシュボード**をクリックします
# MAGIC   * ロードしたばかりのサンプル ダッシュボードがリストにあります。 **Retail Revenue & Supply Chain**という名前で、 **`作成者`** フィールドの下にユーザー名が表示されています。 **注** : 右側の**マイ ダッシュボード**オプションは、ワークスペース内の他のダッシュボードを除外するためのショートカットとして機能します
# MAGIC   * ダッシュボード名をクリックすると表示されます
# MAGIC *  **Shifts in Pricing Priorities** プロットの裏にあるクエリを表示します
# MAGIC   * プロットの上にカーソルを置きます。 3 つの縦のドットが表示されます。クリックしてください
# MAGIC   * 表示されるメニューから **クエリーを表示** を選択します
# MAGIC * このプロットの作成のためのSQL コードを確認します
# MAGIC   * ソース テーブルを識別するために 3 層の名前空間が使用されることに注意してください。これは、Unity カタログでサポートされる新機能です
# MAGIC   * 画面の右上にある **`実行`** をクリックして、クエリの結果をプレビューします
# MAGIC * ビジュアライゼーションを確認します
# MAGIC   * クエリの下で、 **Results**という名前のタブを選択します。 **Price by Priority over Time**をクリックして、プロットのプレビューに切り替えます
# MAGIC   * **Price by Priority over Time**の右側にある矢印から **編集** をクリックして設定を確認します
# MAGIC   * 設定の変更がビジュアライゼーションにどのように影響するかを確認します
# MAGIC   * 変更を適用する場合は、 **保存**をクリックします。それ以外の場合は、 **キャンセル**をクリックします
# MAGIC * クエリ エディターに戻り、ビジュアライゼーション名の右側にある **＋** ボタンをクリックします
# MAGIC   * 棒グラフを作成する
# MAGIC   * **X columns**を **`Date`** として設定します
# MAGIC   * **Y columns**を **`Total Price`** として設定します
# MAGIC   * **Group by** を **`Priority`** として設定します
# MAGIC   * **Stacking**を **`Stack`** に設定します
# MAGIC   * その他の設定はすべてデフォルトのままにします
# MAGIC   * **保存** をクリックします 
# MAGIC * クエリ エディターに戻り、このビジュアライゼーションのデフォルト名をクリックして編集します。ビジュアライゼーション名を **`Stacked Price`** に変更します
# MAGIC * **`Stacked Price`** の右側の矢印をクリックします
# MAGIC   * メニューから **ダッシュボードに追加** を選択します
# MAGIC   * **`Retail Revenue & Supply Chain`** ダッシュボードを選択します
# MAGIC * ダッシュボードに戻ってこの変更を確認します
# MAGIC
# MAGIC # 新しいクエリを作成する
# MAGIC
# MAGIC * サイドバーを使用して **クエリー**に移動します
# MAGIC * 右上の **`クエリーの作成`** ボタンをクリックします
# MAGIC * SQL ウェアハウスに接続していることを確認します。**スキーマブラウザ**で、現在のメタストアをクリックし、 **`samples`** を選択します
# MAGIC   * **`tpch`** データベースを選択
# MAGIC   * **`partsupp`** テーブルをクリックして、スキーマのプレビューを確認します
# MAGIC   * **`partsupp`** テーブル名にカーソルを合わせて **>>** ボタンをクリックし、クエリーにテーブル名を挿入します
# MAGIC * 最初のクエリーを書きます
# MAGIC   * **`SELECT * FROM`** インポートされた **`partsupp`** テーブル。 [**実行**]をクリックして結果をプレビューします
# MAGIC   * このクエリーを **`GROUP BY ps_partkey`** に変更し、 **`ps_partkey`** と **`sum(ps_availqty)`** を返します。 [**実行**]をクリックして結果をプレビューします
# MAGIC   * クエリーを更新して、2 番目の列に **`total_availqty`** という名前のエイリアスを付け、クエリを再実行します
# MAGIC * クエリを保存
# MAGIC   * 画面の右上近くにある**保存**ボタンをクリックします
# MAGIC   * 覚えやすい名前をクエリーに付けます
# MAGIC * クエリをダッシュボードに追加します
# MAGIC   * 結果の右側にある矢印をクリックします
# MAGIC   * **ダッシュボードに追加** をクリックします 
# MAGIC   * **`Retail Revenue & Supply Chain`** ダッシュボードを選択します
# MAGIC * ダッシュボードに戻ってこの変更を確認します
# MAGIC   * ビジュアライゼーションの構成を変更する場合は、画面の右上にある 3 つの縦のボタンをクリックします。表示されるメニューで **編集** をクリックすると、ビジュアライゼーションをドラッグしてサイズを変更できます

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
