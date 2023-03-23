select
    id,
    topic,
    keyword,
    date,
    domain,
    url,
    title,
    content,
    case
        when s3_key is null then ''
        else concat('https://storage.yandexcloud.net/', s3_bucket, '/', s3_key)
    end as s3
from google_alerts_newsfeed gan
where gan.date >= date(now() - interval '1 month') and deleted_at is null;