import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator

import warnings

dag_id = 'rosrid_actives'

with DAG(
    dag_id=dag_id,
    description='Забор НИОКТР, РИД и диссертаций из ЕГИСУ НИОКТР',
    start_date=pendulum.datetime(2020, 1, 1, tz='Europe/Moscow'),
    schedule_interval='0 7 15 * *',
    catchup=True,
    max_active_runs=1,
    tags=['rosrid']
) as DAG:
    s3 = S3Hook(aws_conn_id='ya.cloud')
    s3_bucket = Variable.get('s3_bucket', default_var='psal.private')
    s3_hostname = Variable.get('s3_hostname', default_var='local')
    s3_prefix = f'hosts/{s3_hostname}/{dag_id}'


    @task
    def extract_university_list():
        from psycopg2.extras import RealDictCursor
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        postgres_hook = PostgresHook(postgres_conn_id='application')
        conn = postgres_hook.get_conn()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute('''
                select id as rosrid_university_id, university_id, rosrid_id as rosrid_id
                from rosrid_rosrid_university
                where deleted_at is null order by university_id
            ''')
        university_list = cursor.fetchall()

        return university_list

    @task
    def extract_universities_actives(university_list, **kwargs):
        import json
        from helpers.rosrid.func import get_data_by_id

        data_interval_end = kwargs['data_interval_end']
        dt_str = data_interval_end.strftime('%Y-%m-%d-%H-%M-%S')
        dt_minus_interval = data_interval_end.subtract(months=2)
        start_date = dt_minus_interval.start_of('month').strftime('%Y-%m-%d')
        end_date = dt_minus_interval.end_of('month').strftime('%Y-%m-%d')
        s3_out_folder = f"{s3_prefix}/{dt_str}/actives"

        # todo надо не забыть убрать ограничение
        for u in university_list[:5]:
            rosrid_id = u['rosrid_id']
            university_id = u['university_id']
            rosrid_university_id = u['rosrid_university_id']
            data = get_data_by_id(rosrid_id, start_date, end_date)

            s3.load_string(
                string_data=json.dumps({
                    'rosrid_id': rosrid_id,
                    'university_id': university_id,
                    'rosrid_university_id': rosrid_university_id,
                    'start_date': start_date,
                    'end_date': end_date,
                    'actives_count': sum([len(data[key]) for key in data.keys()]),
                    'actives': data
                }, ensure_ascii=False, indent=4),
                key=f'{s3_out_folder}/{rosrid_id}.json',
                bucket_name=s3_bucket,
                replace=True,
                encoding='utf-8'
            )

        return s3_out_folder

    @task
    def transform_actives(s3_input_folder, **kwargs):
        import json
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        data_interval_end = kwargs['data_interval_end']
        dt_str = data_interval_end.strftime('%Y-%m-%d-%H-%M-%S')
        s3_key_output = f"{s3_prefix}/{dt_str}/transformed_actives.json"
        actives_list_keys = s3.list_keys(bucket_name=s3_bucket, prefix=s3_input_folder)

        pg_hook = PostgresHook(postgres_conn_id='application')
        engine = pg_hook.get_sqlalchemy_engine()
        oecd_dict = pd.read_sql('select id, code from dict_oecd order by id', engine, index_col='code')
        actives_type_dict = pd.read_sql('''
            select
                distinct on (title_short)
                id, title_short
            from rosrid_active_type
            where deleted_at is null
            order by title_short
        ''', engine, index_col='title_short')

        actives_type_map = {'nioktrs': 1, 'rids': 2}
        result = []

        for actives_key in actives_list_keys:
            university_actives = json.loads(s3.read_key(bucket_name=s3_bucket, key=actives_key))
            rosrid_university_id = university_actives['rosrid_university_id']

            for active_type, actives_by_type in university_actives['actives'].items():
                for active in actives_by_type:
                    object_id = active['_id']
                    active_title = active['_source']['name'].strip()

                    # Приводим типы активов к существующему справочнику
                    # НИОКТР применяется по умолчанию
                    active_type_id = actives_type_dict.loc['НИОКТР']['id']

                    if active_type == 'rids':
                        active_type_id = actives_type_dict.loc['РИД']['id']
                    elif active_type == 'dissertations':
                        dissertation_type = active['_source']['dissertation_type']['name']
                        active_type_id = actives_type_dict.loc['ДД']['id'] if dissertation_type == 'Докторская' else actives_type_dict.loc['КД']['id']

                    # Формируем список OECD
                    # хитро, т.к. в ЕГИСУ НИОКТР хранится OECD + WoS
                    # https://www.vyatsu.ru/uploads/file/1703/kody_oecd_mezhdunarodnye.pdf
                    oecd_list = set()

                    for oecd in active['_source']['oecds']:
                        code = oecd['code']
                        # Вот тут не очевидная магия, т.к. OECD + WoS
                        oecd_true_code = '.'.join(code.split('.')[:2])
                        oecd_list.add(oecd_dict.loc[oecd_true_code]['id'])

                    result.append({
                        'rosrid_university_id': rosrid_university_id,
                        'type_id': int(active_type_id),
                        'object_id': object_id,
                        'title': active_title,
                        'oecd': [int(x) for x in oecd_list],
                        's3_bucket': s3_bucket,
                        's3_key': actives_key,
                    })

        s3.load_string(
            string_data=json.dumps(result, ensure_ascii=False, indent=4),
            key=s3_key_output,
            bucket_name=s3_bucket,
            replace=True,
            encoding='utf-8'
        )

        return s3_key_output


    @task
    def soft_delete(s3_key_input):
        import json
        from jinja2 import Template
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        actives_for_load = json.loads(s3.read_key(key=s3_key_input, bucket_name=s3_bucket))
        actives_for_soft_delete = [f"'{x['object_id']}'" for x in actives_for_load]
        postgres_hook = PostgresHook(postgres_conn_id='application')

        with open(f'dags/sql/{dag_id}/soft_delete.sql', 'r') as sql_tpl:
            sql_tpl = sql_tpl.read()

        sql = Template(sql_tpl).render({'object_ids': actives_for_soft_delete})
        postgres_hook.run(sql)

        return len(actives_for_soft_delete)

    @task
    def load_actives(s3_key_input):
        import json
        from jinja2 import Template
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        actives_for_load = json.loads(s3.read_key(key=s3_key_input, bucket_name=s3_bucket))
        postgres_hook = PostgresHook(postgres_conn_id='application')

        with open(f'dags/sql/{dag_id}/insert_data.sql', 'r') as sql_tpl:
            sql_tpl = sql_tpl.read()

        for a in actives_for_load:
            sql = Template(sql_tpl).render(a)
            postgres_hook.run(sql)

        return len(actives_for_load)


    ex_ul = extract_university_list()
    ex_ua = extract_universities_actives(ex_ul)
    tr_ac = transform_actives(ex_ua)
    sd = soft_delete(tr_ac)
    l_ac = load_actives(tr_ac)

    ex_ul >> ex_ua >> tr_ac >> sd >> l_ac
