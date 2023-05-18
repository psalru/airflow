UPDATE rosrid_active
SET
    updated_at = now(),
    deleted_at = now()
WHERE object_id in ({{ object_ids | join(', ') }});
