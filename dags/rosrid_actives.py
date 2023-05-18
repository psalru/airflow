import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

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
                select rru.id as rosrid_university_id, rru.university_id, rru.rosrid_id as rosrid_id, uu.ogrn
                from rosrid_rosrid_university rru
                join university_university uu on uu.id = rru.university_id
                where rru.deleted_at is null and uu.deleted_at is null
                order by rru.university_id
            ''')
        university_list = cursor.fetchall()

        return university_list

    @task
    def extract_universities_actives(university_list, **kwargs):
        import json
        from helpers.rosrid.func import get_data_by_id

        data_interval_end = kwargs['data_interval_end']
        dt_str = data_interval_end.strftime('%Y-%m-%d-%H-%M-%S')
        dt_minus_interval = data_interval_end.subtract(months=3)
        start_date = dt_minus_interval.start_of('month').strftime('%Y-%m-%d')
        end_date = dt_minus_interval.end_of('month').strftime('%Y-%m-%d')
        s3_out_folder = f"{s3_prefix}/{dt_str}/actives"

        for u in university_list:
            rosrid_id = u['rosrid_id']
            data = get_data_by_id(rosrid_id, start_date, end_date)

            s3.load_string(
                string_data=json.dumps({
                    'rosrid_id': rosrid_id,
                    'university_id': u['university_id'],
                    'university_ogrn': u['ogrn'],
                    'rosrid_university_id': u['rosrid_university_id'],
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
        result = []

        for actives_key in actives_list_keys:
            university_actives = json.loads(s3.read_key(bucket_name=s3_bucket, key=actives_key))
            universities_ogrn = university_actives['university_ogrn']
            rosrid_university_id = university_actives['rosrid_university_id']

            for active_type, actives_by_type in university_actives['actives'].items():
                for active in actives_by_type:
                    object_id = active['_id']
                    active_date = active['_source']['last_status']['created_date']
                    active_title = active['_source']['name'].strip()
                    active_url = f"https://rosrid.ru/{active_type[:-1]}/detail/{object_id}"

                    # Разбираемся, является ли актив университетским
                    # В НИОКТР и РИД executor-ом должен быть университет
                    # В Диссертациях author_organization должна быть университет
                    is_executor = False

                    if active_type == 'nioktrs':
                        is_executor = 'ogrn' in active['_source']['executor'].keys() and \
                                      active['_source']['executor']['ogrn'] == universities_ogrn
                    elif active_type == 'rids':
                        executors_orgn = [x['ogrn'] for x in active['_source']['executors']]
                        is_executor = universities_ogrn in executors_orgn
                    elif active_type == 'dissertations':
                        is_executor = 'ogrn' in active['_source']['author_organization'].keys() and \
                                      active['_source']['author_organization']['ogrn'] == universities_ogrn

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
                        'date': active_date,
                        'title': active_title,
                        'oecd': [int(x) for x in oecd_list],
                        'url': active_url,
                        'is_executor': is_executor,
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


    @task
    def month_dump(loaded_count):
        import io
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        path_to_dumps = {
            'xlsx': f'hosts/{s3_hostname}/dumps/{dag_id}_by_month.xlsx',
            'csv': f'hosts/{s3_hostname}/dumps/{dag_id}_by_month.csv'
        }

        if loaded_count > 0:
            postgres_hook = PostgresHook(postgres_conn_id='application')
            conn = postgres_hook.get_conn()

            df_by_month = pd.read_sql(open(f'dags/sql/{dag_id}/month_dump.sql', 'r').read(), conn)
            df_by_month = df_by_month[list(filter(lambda x: x not in ['created_at', 'updated_at', 'deleted_at'], df_by_month.columns))]
            s3.load_string(string_data=df_by_month.to_csv(sep='|'), key=path_to_dumps['csv'], bucket_name=s3_bucket, replace=True, encoding='utf-8')
            xlsx_output = io.BytesIO()
            xlsx_writer = pd.ExcelWriter(xlsx_output, engine='xlsxwriter')
            df_by_month.to_excel(xlsx_writer)
            xlsx_writer.save()
            xlsx_data = xlsx_output.getvalue()
            s3.load_bytes(bytes_data=xlsx_data, key=path_to_dumps['xlsx'], bucket_name=s3_bucket, replace=True)

        return path_to_dumps


    ex_ul = extract_university_list()
    ex_ua = extract_universities_actives(ex_ul)
    tr_ac = transform_actives(ex_ua)
    sd = soft_delete(tr_ac)
    l_ac = load_actives(tr_ac)
    d = month_dump(l_ac)

    ex_ul >> ex_ua >> tr_ac >> sd >> l_ac >> d
