# %%
import dotenv
import os
import nekt

dotenv.load_dotenv()

nekt.data_access_token = os.getenv("NEKT_TOKEN")

spark = nekt.get_spark_session()

# %%
(nekt.load_table(layer_name="Silver", table_name="life_cycle")
     .createOrReplaceTempView("life_cycle"))

(nekt.load_table(layer_name="Silver", table_name="fs_transacional")
     .createOrReplaceTempView("fs_transacional"))

(nekt.load_table(layer_name="Silver", table_name="fs_education")
     .createOrReplaceTempView("fs_education"))

(nekt.load_table(layer_name="Silver", table_name="fs_life_cycle")
     .createOrReplaceTempView("fs_life_cycle"))

# %%

def read_query(path):
    with open(path) as open_file:
        return open_file.read()
    

# %%

query = read_query("abt_fiel.sql")
df = spark.sql(query)
pdf = df.toPandas()
pdf.to_csv("../../../data/abt_fiel.csv", index=False, sep=";")