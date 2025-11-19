# %%
import dotenv
import os
import nekt
import datetime

dotenv.load_dotenv()


def read_query(path):
    with open(path) as open_file:
        return open_file.read()


def date_range(start, stop, monthly=False):
    dates = []
    while start <= stop:
        dates.append(start)
        dt_start = datetime.datetime.strptime(start, '%Y-%m-%d') + datetime.timedelta(days=1)
        start = datetime.datetime.strftime(dt_start, '%Y-%m-%d')
        
    if monthly:
        return [i for i in dates if i.endswith("01")]
    return dates


def exec_iterable_query(layer, folder, table_name, query, dates):

    try:
        df_all = nekt.load_table(layer_name=layer, table_name=table_name)
        df_all.union(spark.sql(query.format(date=dates.pop(0))))
        
    except Exception:
        df_all = spark.sql(query.format(date=dates.pop(0)))

    for d in dates:
        df_all = df_all.union(spark.sql(query.format(date=d)))

    return df_all


nekt.data_access_token = os.getenv("NEKT_TOKEN")

(nekt.load_table(layer_name="Bronze", table_name="points_transacoes")
     .createOrReplaceTempView("points_transacoes"))

(nekt.load_table(layer_name="Bronze", table_name="points_transacao_produto")
     .createOrReplaceTempView("points_transacao_produto"))

(nekt.load_table(layer_name="Bronze", table_name="points_produtos")
     .createOrReplaceTempView("points_produtos"))


dates = date_range("2024-03-01", "2025-10-19", monthly=True)[:2]
spark = nekt.get_spark_session()


query_life_cycle = read_query("life_cycle.sql")
query_fs_transacional = read_query("fs_transacional.sql")


df_life_cycle = exec_iterable_query(layer="Silver",
                                    folder="tmw_ecosys",
                                    table_name="life_cycle",
                                    query=query_life_cycle,
                                    dates=dates)


df_fs_transacional = exec_iterable_query(layer="Silver",
                                    folder="tmw_ecosys",
                                    table_name="fs_transacional",
                                    query=query_fs_transacional,
                                    dates=dates)

# %%

(df_life_cycle.coalesce(1)
              .write
              .format("parquet")
              .mode("overwrite")
              .save("../../data/life_cycle")
              )

(df_fs_transacional.coalesce(1)
              .write
              .format("parquet")
              .mode("overwrite")
              .save("../../data/fs_transacional")
              )
