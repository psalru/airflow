import pandas as pd
from fuzzywuzzy import fuzz
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_region_id_by_title(title):
    df = df_region.copy()
    df['result'] = df['title'].apply(lambda x: fuzz.ratio(title, x))
    df = df.sort_values(by=['result'], ascending=False)

    return int(df.iloc[0]['id'])


pg_hook = PostgresHook(postgres_conn_id='application')
engine = pg_hook.get_sqlalchemy_engine()
df_region = pd.read_sql('select id, title from geo_federal_region where deleted_at is null', engine)
