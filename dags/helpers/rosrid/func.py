import os
import time
import json
import requests
import warnings


def get_search_results(data, timeout=1):
    items_in_page = 10
    search_results = []

    try:
        resp = session.request("POST", search_url, data=json.dumps(data, ensure_ascii=False).encode('utf-8'))

        if resp.status_code == 200:
            json_resp = resp.json()
            page = data['page']
            total = json_resp['hits']['total']['value']
            count_of_pages = (int(total / items_in_page) + 1) if total % items_in_page else total / items_in_page

            if page < count_of_pages:
                time.sleep(timeout)
                search_results += json_resp['hits']['hits'] + get_search_results({**data, 'page': page + 1}, timeout)
            else:
                search_results += json_resp['hits']['hits']
    except BaseException as e:
        warnings.warn(f"Retry connection: {e}")
        search_results = get_search_results({**data, 'page': 1}, timeout)

    return search_results


def get_data_by_id(rosrid_id, start_date, end_date):
    data = {}

    for object_type in ['nioktrs', 'rids', 'dissertations']:
        payload = {
            **payload_tmpl,
            'organization': [rosrid_id],
            f'{object_type}': True,
            'start_date': start_date,
            'end_date': end_date
        }
        data_by_object = get_search_results(payload)
        data[object_type] = data_by_object

    return data


session = requests.session()
dir_path = os.path.dirname(os.path.realpath(__file__))
session.headers.update(json.load(open(f"{dir_path}/headers.json", 'r')))
payload_tmpl = json.load(open(f"{dir_path}/payload.json", 'r'))
search_url = 'https://rosrid.ru/api/base/search'
