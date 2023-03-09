-- insert in table
INSERT INTO hh_currency_exchange_rate (created_at, date, title, nominal, value, char_code, num_code, currency_id)
VALUES
    {% for item in items %}
        (
            current_timestamp,
            '{{ item.date }}',
            '{{ item.title }}',
            {{ item.nominal }},
            {{ item.value }},
            '{{ item.char_code }}',
            {{ item.num_code }},
            '{{ item.currency_id }}'
        ){% if loop.nextitem is defined %},{% endif %}
    {% endfor %}
;
