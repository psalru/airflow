with vacancies as (
    select distinct on (hh_id)
        v.id as id,
        uu.mon_id as university_one_monitoring_id,
        uu.title_short as university_title,
        uu.title_display as university_abbreviation,
        v.created_at,
        v.deleted_at,
        v.hh_id,
        v.url,
        date(v.hh_initial_created_at) as initial_created_at,
        v.title,
        v.description,
        v.salary_from,
        v.salary_to,
        v.salary_gross,
        v.experience,
        v.schedule,
        v.employment,
        gfr.title as region,
        case
            when v.s3_key is null then ''
            else concat('https://storage.yandexcloud.net/', v.s3_bucket, '/', v.s3_key)
        end as s3
    from hh_vacancy v
    join hh_hh_university hhu on hhu.id = v.hh_university_id
    join university_university uu on uu.id = hhu.university_id
    join geo_federal_region gfr on gfr.id = v.federal_region_id
    where v.hh_initial_created_at >= date(now() - interval '1 month')
    order by v.hh_id, v.created_at desc
), skills as (
    select v.id as vacancy_id, string_agg(hs.title, '; ') as skills from vacancies v
    join hh_skill hs on hs.vacancy_id = v.id
    group by v.id
), professional_roles as (
    select v.id as vacancy_id, string_agg(hpr.title, '; ') as professional_roles from vacancies v
    join hh_professional_role hpr on hpr.vacancy_id = v.id
    group by v.id
)
select
    id, university_one_monitoring_id, university_title, university_abbreviation, created_at, deleted_at, hh_id, url,
    initial_created_at, title, description, salary_from, salary_to, salary_gross, experience, schedule, employment,
    region, skills, professional_roles, s3 from vacancies v
join skills s on s.vacancy_id = v.id
join professional_roles pr on pr.vacancy_id = v.id;