import requests
import pandas as pd
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.providers.postgres.hooks.postgres import PostgresHook
from helpers.geo import get_region_id_by_title


def get_vacancies_list(employer_id, page=0):
    vacancies_api_url = 'https://api.hh.ru/vacancies?employer_id={0}&page={1}'.format(employer_id, page)
    resp = session.get(vacancies_api_url)
    vac_list = []

    if resp.status_code == 200:
        json = resp.json()
        vac_list.extend(json['items'])
        page_count = json['pages']

        if page < page_count - 1:
            vac_list.extend(get_vacancies_list(employer_id, page + 1))
    else:
        raise AirflowFailException(f'get_vac_list: url {vacancies_api_url} return status {resp.status_code}')

    return vac_list


def get_vacancy_data(vacancy_id):
    vac_api_url = 'https://api.hh.ru/vacancies/{0}?host=hh.ru'.format(vacancy_id)
    resp = session.get(vac_api_url)

    if resp.status_code == 200:
        return resp.json()
    else:
        raise AirflowFailException(f'get_vac_data: url {vac_api_url} return status {resp.status_code}')


def salary_in_rubles(value, currency):
    if value is None or currency == 'RUR':
        return value
    else:
        return value * df_currency.loc[currency, 'value']


def get_areas(area_list):
    result = pd.DataFrame(columns={
        'parent_id': str,
        'name': str
    })

    for area in area_list:
        area_id = str(area['id'])
        result.loc[area_id, 'parent_id'] = area['parent_id']
        result.loc[area_id, 'name'] = area['name']
        areas_children = area['areas']

        if len(areas_children) > 0:
            result = pd.concat([result, get_areas(areas_children)])

    return result


def get_area_path(area_id):
    area_item = areas_dict.loc[str(area_id)]

    result = [{
        'is': area_id,
        'name': area_item['name']
    }]

    if area_item['parent_id']:
        result = [*result, *get_area_path(area_item['parent_id'])]

    return result


def get_region_id(area_id):
    area_path = get_area_path(str(area_id))

    if area_path[-1:][0]['name'] != 'Россия' or len(area_path) == 1:
        return None

    return get_region_id_by_title(area_path[-2:-1][0]['name'])


hh_access_token = Variable.get('hh_access_token_app')
session = requests.session()
session.headers.update({'Authorization': f"Bearer {hh_access_token}"})

area_dict_url = 'https://api.hh.ru/areas'
resp_area_dict = session.get(area_dict_url)
areas_dict = get_areas(resp_area_dict.json()) if resp_area_dict.status_code == 200 else pd.DataFrame()

if len(areas_dict) == 0:
    raise AirflowFailException(f'Problems with HH area dict {area_dict_url}')

pg_hook = PostgresHook(postgres_conn_id='application')
engine = pg_hook.get_sqlalchemy_engine()
df_currency = pd.read_sql('select distinct on (char_code) id, char_code, value from hh_currency_exchange_rate order by char_code, date DESC', engine).set_index('char_code')
