# Databricks notebook source
# MAGIC %md-sandbox
# MAGIC
# MAGIC <div style="text-align: center; line-height: 0; padding-top: 9px;">
# MAGIC   <img src="https://databricks.com/wp-content/uploads/2018/03/db-academy-rgb-1200px.png" alt="Databricks Learning" style="width: 600px">
# MAGIC </div>
# MAGIC

# COMMAND ----------

# DBTITLE 0,--i18n-17f3cfcf-2ad7-48f2-bf4e-c2adcb372926
# MAGIC %md
# MAGIC # PythonによるDLTのトラブルシューティング (Troubleshooting DLT Python Syntax)
# MAGIC
# MAGIC 2つのノートブックを使ってパイプラインの構成と実行を行ったので、3つ目のノートブックを開発し、追加するシミュレーションを行うことにします。
# MAGIC
# MAGIC **DON'T PANIC!** 物事が壊れるのはこれからです。
# MAGIC
# MAGIC 以下に提供するコードには、意図的に小さな構文エラーが含まれています。
# MAGIC これらのエラーをトラブルシューティングすることで、DLTコードを繰り返し開発し、構文のエラーを特定する方法を学ぶことができます。
# MAGIC
# MAGIC このレッスンは、コード開発やテストのための堅牢なソリューションを提供するものではなく、むしろ、DLTを始めようとするユーザーが、馴染みのない構文に対処するのを助けることを目的としています。
# MAGIC
# MAGIC
# MAGIC **学習の目標**
# MAGIC 次を学習します:
# MAGIC * DLTシンタックスの把握とトラブルシューティング
# MAGIC * ノートブックによるDLTパイプラインの反復開発

# COMMAND ----------

# DBTITLE 0,--i18n-b6eb7861-af09-4009-a272-1c5c91f87a8b
# MAGIC %md
# MAGIC ## このノートブックををDLTパイプラインに追加する(Add this Notebook to a DLT Pipeline)
# MAGIC
# MAGIC この時点で、DLT Pipelineに2つのノートブックライブラリを追加しているはずです。
# MAGIC
# MAGIC このパイプラインでレコードのバッチをいくつか処理しているはずです。また、パイプラインの新しい実行をトリガーする方法と、ライブラリを追加する方法を理解しているはずです。
# MAGIC
# MAGIC このレッスンを始めるため、DLT UIを使用してこのノートブックをパイプラインに追加し、更新をトリガーしてください。
# MAGIC
# MAGIC <img src="https://files.training.databricks.com/images/icon_hint_24.png"> このノートブックは、[DE 4.1 - DLT UI Walkthrough]($../DE 4.1 - DLT UI Walkthrough)の **Generate Pipeline Configuration** セクションにある **Notebook #3** になります。

# COMMAND ----------

# DBTITLE 0,--i18n-fb7a717a-7921-45c3-bb1d-4c2e1bff55ab
# MAGIC %md
# MAGIC ## エラーのトラブルシューティング(Troubleshooting Errors)
# MAGIC
# MAGIC 以下の3つの関数にはそれぞれ構文エラーが含まれていますが、DLTによるこれらのエラーの検出と報告は若干異なります。
# MAGIC
# MAGIC **初期化中** の段階では、DLTがコマンドを正しく解析できないため、いくつかの構文エラーが検出されます。
# MAGIC
# MAGIC その他の構文エラーは、 **テーブルをセットアップ中** の段階で検出されます。
# MAGIC
# MAGIC なお、DLTはパイプラインのテーブルの順番を段階的に解決していくため、後工程のエラーが先に投げられることがあります。
# MAGIC
# MAGIC 初期のデータセットから最終的なデータセットに向けて、一度に1つのテーブルを修正するアプローチも効果的です。コメントされたコードは自動的に無視されるので、コードを完全に削除することなく、安全に開発ランから削除することができます。
# MAGIC
# MAGIC 以下のコードですぐにエラーを発見できたとしても、UIからのエラーメッセージを参考にして、これらのエラーを特定するようにしてください。解答コードは以下のセルに続きます。

# COMMAND ----------

# TODO
import pyspark.sql.functions as F
 
source = spark.conf.get("source")
 
def status_bronze():
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .load(f"{source}/status")
            .select(
                F.current_timestamp().alias("processing_time"), 
                F.input_file_name().alias("source_file"), 
                "*"
            )
    )
 
 
@dlt.table(
    table_name = "status_silver"
    )
@dlt.expect_or_drop("valid_timestamp", "status_timestamp > 1640995200")
def status_silver():
    return (
        dlt.read_stream("status_bronze")
            .drop("source_file", "_rescued_data")
    )
 
 
@dlt.table
def email_updates():
    return (
        spark.read("status_silver").alias("a")
            .join(
                dlt.read("subscribed_order_emails_v").alias("b"), 
                on="order_id"
            ).select(
                "a.*", 
                "b.email"
            )
    )

# COMMAND ----------

# DBTITLE 0,--i18n-5bb5f3ef-7a9e-4ea5-a6c3-f85cac306e04
# MAGIC %md
# MAGIC ## ソリューション(Solutions)
# MAGIC
# MAGIC 上記の各機能の正しい文法は、Solutionsフォルダ内の同名のノートブックに記載されています。
# MAGIC
# MAGIC これらのエラーに対処するためには、いくつかの選択肢があります：
# MAGIC * 各問題を解決し、自分で上の問題を解決する
# MAGIC * 同じ名前のSolutionフォルダのノートブックから、 **`# ANSWER`** セルの内容をコピー＆ペーストします。
# MAGIC * パイプラインを更新して、同名のSolutionsノートブックを直接参照する。
# MAGIC
# MAGIC **注意**: セルに**`import dlt`**文を追加するまでは、他のエラーは表示されません。
# MAGIC
# MAGIC
# MAGIC 各クエリにおける課題：
# MAGIC * **`@dlt.table`** デコレータが関数定義にありません。
# MAGIC * カスタムテーブル名を設定するキーワード引数は **`name`** であり、 **`table_name`** ではありません。
# MAGIC * DLTパイプラインのテーブルを読むためには **`dlt.read`** を設定する。 **`spark.read`** ではない。

# COMMAND ----------

# DBTITLE 0,--i18n-f8fb12d5-c515-43fc-ab55-0d1f97baf05c
# MAGIC %md
# MAGIC ## サマリ(Summary)
# MAGIC
# MAGIC 次を学習しました:
# MAGIC * DLTシンタックスの把握とトラブルシューティング
# MAGIC * ノートブックによるDLTパイプラインの反復開発

# COMMAND ----------

# MAGIC %md-sandbox
# MAGIC &copy; 2023 Databricks, Inc. All rights reserved.<br/>
# MAGIC Apache, Apache Spark, Spark and the Spark logo are trademarks of the <a href="https://www.apache.org/">Apache Software Foundation</a>.<br/>
# MAGIC <br/>
# MAGIC <a href="https://databricks.com/privacy-policy">Privacy Policy</a> | <a href="https://databricks.com/terms-of-use">Terms of Use</a> | <a href="https://help.databricks.com/">Support</a>
