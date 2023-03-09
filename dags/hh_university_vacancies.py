import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag_id = 'hh_university_vacancies'

with DAG(
    dag_id=dag_id,
    description='Забор вакансий с HH по организациям',
    start_date=pendulum.datetime(2023, 3, 1, tz='Europe/Moscow'),
    schedule_interval='0 15 * * 7',
    catchup=False,
    tags=['hh']
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
        cursor.execute('select id as hh_university_id, employer_id from hh_hh_university where deleted_at is null')
        university_list = cursor.fetchall()

        return university_list


    @task
    def extract_lists_of_vacancies_by_university(university_list, **kwargs):
        import json
        from helpers.hh_api import get_vacancies_list

        dt_str = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M-%S')
        s3_key_output = f"{s3_prefix}/{dt_str}/lists_of_vacancies_by_university.json"
        vacancies = []

        for u in university_list:
            hh_university_id = u['hh_university_id']
            employer_id = u['employer_id']

            vacancies.append({
                'hh_university_id': hh_university_id,
                'lists_of_vacancies': get_vacancies_list(employer_id),
            })

        s3.load_string(
            string_data=json.dumps(vacancies, ensure_ascii=False, indent=4),
            key=s3_key_output,
            bucket_name=s3_bucket,
            replace=True,
            encoding='utf-8'
        )

        return s3_key_output


    @task
    def extract_vacancies_data(s3_key_input, **kwargs):
        import json
        from helpers.hh_api import get_vacancy_data

        list_of_vacancies_by_university = json.loads(s3.read_key(key=s3_key_input, bucket_name=s3_bucket))
        dt_str = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M-%S')
        s3_out_folder = f"{s3_prefix}/{dt_str}/vacancies"

        for u in list_of_vacancies_by_university:
            hh_university_id = u['hh_university_id']

            for v in u['lists_of_vacancies']:
                if v['type']['id'] != 'closed':
                    vacancy = get_vacancy_data(v['id'])
                    vacancy_id = vacancy['id']
                    vacancy['hh_university_id'] = hh_university_id

                    s3.load_string(
                        string_data=json.dumps(vacancy, ensure_ascii=False, indent=4),
                        key=f'{s3_out_folder}/{vacancy_id}.json',
                        bucket_name=s3_bucket,
                        replace=True,
                        encoding='utf-8'
                    )

        return s3_out_folder


    @task
    def transform_vacancies(s3_input_folder, **kwargs):
        import json
        from helpers.hh_api import salary_in_rubles, get_region_id

        dt_str = kwargs['execution_date'].strftime('%Y-%m-%d-%H-%M-%S')
        s3_key_output = f"{s3_prefix}/{dt_str}/transformed_vacancies.json"
        vacancies_list_keys = s3.list_keys(bucket_name=s3_bucket, prefix=s3_input_folder)
        result = []

        for vacancy_key in vacancies_list_keys:
            vacancy = json.loads(s3.read_key(bucket_name=s3_bucket, key=vacancy_key))
            result.append({
                'hh_university_id': vacancy['hh_university_id'],
                'hh_id': vacancy['id'],
                'hh_created_at': vacancy['created_at'],
                'hh_initial_created_at': vacancy['initial_created_at'],
                'name': vacancy['name'].replace("'", ""),
                'description': vacancy['description'].replace("'", ""),
                'salary_from': salary_in_rubles(vacancy['salary']['from'], vacancy['salary']['currency']) if vacancy['salary'] else None,
                'salary_to': salary_in_rubles(vacancy['salary']['to'], vacancy['salary']['currency']) if vacancy['salary'] else None,
                'salary_currency': 'RUR' if vacancy['salary'] else None,
                'salary_gross': vacancy['salary']['gross'] if vacancy['salary'] else None,
                'experience': vacancy['experience']['name'],
                'schedule': vacancy['schedule']['name'],
                'employment': vacancy['employment']['name'],
                'federal_region_id': get_region_id(vacancy['area']['id']),
                'url': vacancy['alternate_url'],
                's3_bucket': s3_bucket,
                's3_key': vacancy_key,
                'professional_roles': [{
                    'hh_professional_role_dict_id': x['id'],
                    'name': x['name']
                } for x in vacancy['professional_roles']],
                'skills': [{'name': x['name'].replace("'", "")} for x in vacancy['key_skills']]
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
    def load_vacancies(s3_key_input):
        import json
        from jinja2 import Template
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        vacancies_for_load = json.loads(s3.read_key(key=s3_key_input, bucket_name=s3_bucket))
        postgres_hook = PostgresHook(postgres_conn_id='application')

        with open(f'dags/sql/{dag_id}/insert_data.sql', 'r') as sql_tpl:
            sql_tpl = sql_tpl.read()

        for v in vacancies_for_load:
            sql = Template(sql_tpl).render(v)
            postgres_hook.run(sql)

        return len(vacancies_for_load)


    ex_ul = extract_university_list()
    ex_lvu = extract_lists_of_vacancies_by_university(ex_ul)
    ex_vd = extract_vacancies_data(ex_lvu)
    tr_v = transform_vacancies(ex_vd)
    sd = PostgresOperator(task_id='soft_delete', postgres_conn_id='application', sql=f'sql/{dag_id}/soft_delete.sql')
    l_v = load_vacancies(tr_v)

    ex_ul >> ex_lvu >> ex_vd >> tr_v >> sd >> l_v
