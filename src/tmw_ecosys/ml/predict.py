# %%

import dotenv
import os
import nekt
import mlflow

dotenv.load_dotenv()
nekt.data_access_token = os.getenv("NEKT_TOKEN")
df = nekt.load_table(layer_name="Silver", table_name="fs_all").toPandas()

mlflow.set_tracking_uri(os.getenv("MLFLOW_URI"))
MODEL_NAME = 'tmw_fiel'

versions = mlflow.search_registered_models(filter_string="name='tmw_fiel'")[0]
last_version = max([int(i.version) for i in versions.latest_versions])

model = mlflow.sklearn.load_model(f"models:///{MODEL_NAME}/{last_version}")

df['proba_fiel'] = model.predict_proba(df[model.feature_names_in_])[:,1]
df[['idcliente', 'proba_fiel']]

print(df)