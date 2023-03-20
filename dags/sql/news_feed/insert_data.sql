INSERT INTO google_alerts_newsfeed (created_at, hash, topic, keyword, date, domain, url, title, content, s3_bucket, s3_key)
VALUES
    {% for item in items %}
        (
            '{{ item.created_at }}'::timestamp,
            '{{ item.hash }}',
            '{{ item.topic }}',
            '{{ item.keyword }}',
            '{{ item.date }}'::date,
            {% if item.domain %}'{{ item.domain }}'{% else %}null{% endif %},
            '{{ item.url }}',
            '{{ item.title }}',
            {% if item.content %}'{{ item.content }}'{% else %}null{% endif %},
            '{{ item.s3_bucket }}',
            {% if item.s3_key %}'{{ item.s3_key }}'{% else %}null{% endif %}
        ){% if loop.nextitem is defined %},{% endif %}
    {% endfor %}
ON CONFLICT (hash) DO NOTHING;
