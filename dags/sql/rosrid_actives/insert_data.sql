with new_active as (
    insert into rosrid_active (created_at, rosrid_university_id, type_id, object_id, title, s3_bucket, s3_key)
    values (
                current_timestamp,
                {{ rosrid_university_id }},
                {{ type_id }},
                '{{ object_id }}',
                '{{ title }}',
                '{{ s3_bucket }}',
                '{{ s3_key }}'
            )
    returning id as active_id
)
insert into rosrid_active_oecd (active_id, oecd_id)
values
    {% for oecd_id in oecd %}
        (
            (select active_id from new_active),
            {{ oecd_id }}
        ){% if loop.nextitem is defined %},{% endif %}
    {% endfor %}
