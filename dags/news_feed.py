import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

dag_id = 'news_feed'

with DAG(
    dag_id=dag_id,
    description='DAG работающий с Google Alers',
    start_date=pendulum.datetime(2022, 12, 23, tz='Europe/Moscow'),
    schedule_interval='0 14 * * *',
    catchup=False
) as DAG:
    s3 = S3Hook(aws_conn_id='ya.cloud')
    s3_bucket = Variable.get('s3_bucket', default_var='psal.private')
    s3_hostname = Variable.get('s3_hostname', default_var='local')
    s3_prefix = f'hosts/{s3_hostname}/{dag_id}'


    @task
    def extract(**kwargs):
        import requests

        google_alert = Variable.get('google_alert', deserialize_json=True)
        feeds = []

        for i, topic in enumerate(google_alert):
            for j, feed in enumerate(topic['feeds']):
                count = i + j
                feed_url = feed['url']
                resp = requests.get(feed_url)

                if resp.status_code == 200:
                    xml = resp.text
                    data_interval_end = kwargs['data_interval_end'].strftime('%Y-%m-%d-%H-%M')
                    s3_key_output = f"{s3_prefix}/{data_interval_end}/feed-{count}.xml"

                    s3.load_string(
                        string_data=xml,
                        key=s3_key_output,
                        bucket_name=s3_bucket,
                        replace=True,
                        encoding='utf-8'
                    )

                    feeds.append({
                        'topic': topic['topic'],
                        'keyword': feed['keyword'],
                        'feed': feed['url'],
                        's3_key': s3_key_output
                    })
                else:
                    raise AirflowFailException(f'{feed_url} return status code {resp.status_code}')

        return feeds


    @task
    def transform(feeds, **kwargs):
        import re
        import json
        import hashlib
        import requests
        import warnings
        from bs4 import BeautifulSoup

        data_interval_end = kwargs['data_interval_end'].strftime('%Y-%m-%d-%H-%M')
        s3_key_output = f"{s3_prefix}/{data_interval_end}/transformed-feeds.json"
        news = []

        for i, feed in enumerate(feeds):
            rss = s3.read_key(key=feed['s3_key'], bucket_name=s3_bucket)
            soup = BeautifulSoup(rss, features='lxml')
            entries = soup.find_all('entry')
            topic = feed['topic']
            keyword = feed['keyword']

            for j, entry in enumerate(entries):
                url = entry.link['href'].strip()
                s3_key = None

                if url[:23] == 'https://www.google.com/':
                    url = url[url.find('&url=') + 5:url.find('&ct=ga')]

                domain_match = re.match('https?://[^/]+', url)

                if domain_match:
                    domain = re.sub('https?://', '', domain_match[0])
                    domain = re.sub('^www\.', '', domain)
                else:
                    domain = None

                try:
                    source_resp = requests.get(url, headers={
                        'pragma': 'no-cache',
                        'cache-control': 'no-cache',
                        'accept': 'application/json, text/plain, */*',
                        'content-type': 'application/json;charset=UTF-8',
                        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.80 Safari/537.36',
                        'accept-language': 'ru,en-US;q=0.9,en;q=0.8,ru-RU;q=0.7',
                    })

                    if source_resp.status_code == 200:
                        html = source_resp.text
                        s3_key = f"{s3_prefix}/{data_interval_end}/html/feed-{i}-entry-{j}.html"

                        s3.load_string(
                            string_data=html,
                            key=s3_key,
                            bucket_name=s3_bucket,
                            replace=True,
                            encoding='utf-8'
                        )
                except BaseException:
                    warnings.warn(f'Не получилось загрузить: {url}')

                news.append({
                    'created_at': str(kwargs['data_interval_end']),
                    'hash': hashlib.md5((topic + keyword + url).encode('utf-8')).hexdigest(),
                    'topic': topic,
                    'keyword': keyword,
                    'date': pendulum.parse(entry.published.get_text()).to_date_string(),
                    'domain': domain,
                    'url': url,
                    'title': entry.title.get_text().strip(),
                    'content': entry.content.get_text(),
                    's3_bucket': s3_bucket,
                    's3_key': s3_key
                })

        s3.load_string(
            string_data=json.dumps(news, ensure_ascii=False, indent=4),
            key=s3_key_output,
            bucket_name=s3_bucket,
            replace=True,
            encoding='utf-8'
        )

        return s3_key_output


    @task
    def load(s3_key_input):
        import json
        from jinja2 import Template
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        items = json.loads(s3.read_key(key=s3_key_input, bucket_name=s3_bucket))
        items_count = len(items)

        if items_count > 0:
            postgres_hook = PostgresHook(postgres_conn_id='application')

            with open(f'dags/sql/{dag_id}/insert_data.sql', 'r') as sql_tpl:
                sql = Template(sql_tpl.read()).render({'items': items})

            postgres_hook.run(sql)

        return len(items)


    extracted = extract()
    transformed = transform(extracted)
    loaded = load(transformed)

    extracted >> transformed >> loaded
