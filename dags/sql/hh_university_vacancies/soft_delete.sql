UPDATE hh_vacancy
SET
    updated_at = now(),
    deleted_at = now()
WHERE deleted_at is null;