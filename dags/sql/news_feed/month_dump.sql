select *
from google_alerts_newsfeed gan
where gan.date >= date(now() - interval '1 month') and deleted_at is null;