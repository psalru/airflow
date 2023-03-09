import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

dag_id = 'hh_currency_exchange_rate'

with DAG(
    dag_id=dag_id,
    description='Обменный курс по ЦБ РФ',
    start_date=pendulum.datetime(2023, 3, 1, tz='Europe/Moscow'),
    schedule_interval='0 12 * * *',
    catchup=True,
    max_active_runs=1,
    tags=['hh']
) as DAG:
    s3 = S3Hook(aws_conn_id='ya.cloud')
    s3_bucket = Variable.get('s3_bucket', default_var='psal.private')
    s3_hostname = Variable.get('s3_hostname', default_var='local')
    s3_prefix = f'hosts/{s3_hostname}/{dag_id}'

    @task
    def extract(**kwargs):
        import requests

        date_str = kwargs['execution_date'].format('DD/MM/YYYY')
        url = f'https://cbr.ru/scripts/XML_daily.asp?date_req={date_str}'
        resp = requests.get(url)

        if resp.status_code == 200:
            dt_str = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M-%S')
            s3_key_output = f"{s3_prefix}/{dt_str}/extracted.xml"
            xml = resp.text

            s3.load_string(
                string_data=xml,
                key=s3_key_output,
                bucket_name=s3_bucket,
                replace=True,
                encoding='utf-8'
            )

            return s3_key_output
        else:
            raise AirflowFailException(f'{url} is return status code — {resp.status_code}')


    @task
    def transform(s3_key_input: str, **kwargs):
        import pandas as pd

        file_name = f'transformed.json'
        dt_str = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M-%S')
        s3_key_output = f"{s3_prefix}/{dt_str}/{file_name}"
        xml = s3.read_key(key=s3_key_input, bucket_name=s3_bucket)
        df = pd.read_xml(xml)
        df.columns = [x.lower() for x in df.columns]
        df = df.rename(columns={'id': 'currency_id', 'charcode': 'char_code', 'name': 'title', 'numcode': 'num_code'})
        df['date'] = kwargs['data_interval_end'].strftime('%Y-%m-%d')
        df['value'] = df.apply(lambda x: float(x['value'].replace(',', '.')) / x['nominal'], axis=1)
        df['nominal'] = 1

        result = df.to_json(orient="records", force_ascii=False, indent=4)

        s3.load_string(
            string_data=result,
            key=s3_key_output,
            bucket_name=s3_bucket,
            replace=True,
            encoding='utf-8'
        )

        return s3_key_output


    @task
    def load(s3_key_input: str):
        import json
        from jinja2 import Template
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        currency_exchange_rate = json.loads(s3.read_key(key=s3_key_input, bucket_name=s3_bucket))
        postgres_hook = PostgresHook(postgres_conn_id='application')

        with open(f'dags/sql/{dag_id}/insert_data.sql', 'r') as sql_tpl:
            sql = Template(sql_tpl.read()).render({'items': currency_exchange_rate})

        postgres_hook.run(sql)

        return len(currency_exchange_rate)

    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    extracted >> transformed >> loaded
