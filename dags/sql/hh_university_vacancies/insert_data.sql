with new_vacancy as (
    insert into hh_vacancy (created_at, hh_university_id, hh_id, hh_created_at, hh_initial_created_at, title, description, salary_from, salary_to, salary_currency, salary_gross, experience, schedule, employment, federal_region_id, url, s3_bucket, s3_key)
    values (
                current_timestamp,
                {{ hh_university_id }},
                {{ hh_id }},
                {% if hh_created_at %}'{{ hh_created_at }}'{% else %}null{% endif %},
                {% if hh_initial_created_at %}'{{ hh_initial_created_at }}'{% else %}null{% endif %},
                '{{ name }}',
                '{{ description }}',
                {% if salary_from %}{{ salary_from }}{% else %}null{% endif %},
                {% if salary_to %}{{ salary_to }}{% else %}null{% endif %},
                {% if salary_currency %}'{{ salary_currency }}'{% else %}null{% endif %},
                {% if salary_gross %}'{{ salary_gross }}'{% else %}null{% endif %},
                {% if experience %}'{{ experience }}'{% else %}null{% endif %},
                {% if schedule %}'{{ schedule }}'{% else %}null{% endif %},
                {% if employment %}'{{ employment }}'{% else %}null{% endif %},
                {% if federal_region_id %}{{ federal_region_id }}{% else %}null{% endif %},
                '{{ url }}',
                '{{ s3_bucket }}',
                '{{ s3_key }}'
            )
    returning id as vacancy_id
)
{% if skills|length > 0 %}
, skills as (
    insert into hh_skill (created_at, vacancy_id, title)
    values
        {% for s in skills %}
            (
                current_timestamp,
                (select vacancy_id from new_vacancy),
                '{{ s.name }}'
            ){% if loop.nextitem is defined %},{% endif %}
        {% endfor %}
    returning id as skill_id
)
{% endif %}
insert into hh_professional_role (created_at, vacancy_id, hh_professional_role_dict_id, title)
    values
        {% for r in professional_roles %}
            (
                current_timestamp,
                (select vacancy_id from new_vacancy),
                {{ r.hh_professional_role_dict_id }},
                '{{ r.name }}'
            ){% if loop.nextitem is defined %},{% endif %}
        {% endfor %}