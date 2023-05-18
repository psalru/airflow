select
    ra.id,
    uu.mon_id as university_one_monitoring_id,
    uu.title_short as university_title,
    uu.title_display as university_abbreviation,
    ra.created_at,
    ra.object_id,
    rat.title as type,
    ra.date,
    ra.title,
    o.oecd,
    ra.url,
    concat('https://storage.yandexcloud.net/', ra.s3_bucket, '/', ra.s3_key) as s3
from rosrid_active ra
join rosrid_active_type rat on rat.id = ra.type_id
join rosrid_rosrid_university rru on rru.id = ra.rosrid_university_id
join university_university uu on uu.id = rru.university_id
join (
    select rac.active_id, string_agg(concat(d.code, ' - ', d.title), '; ') as oecd from rosrid_active_oecd rac
    join dict_oecd d on rac.oecd_id = d.id
    group by active_id
) o on o.active_id = ra.id
where
    ra.deleted_at is null and
    ra.date >= (select max(date) from rosrid_active) and
    ra.date <= date_trunc('month', (select max(date) from rosrid_active)) + interval '1 month' - interval '1 day'
