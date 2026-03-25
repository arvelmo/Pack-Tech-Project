CREATE OR REPLACE TABLE fct_sessions AS
WITH ordered AS (
    SELECT
        user_id,
        mentor_id,
        event_type,
        timestamp,
        LEAD(event_type) OVER (
            PARTITION BY user_id, mentor_id ORDER BY timestamp
        ) next_event,
        LEAD(timestamp) OVER (
            PARTITION BY user_id, mentor_id ORDER BY timestamp
        ) next_ts
    FROM stg_events
),
sessions AS (
    SELECT
        user_id,
        mentor_id,
        timestamp AS session_start,
        CASE
            WHEN next_event = 'session_ended' THEN next_ts
            ELSE timestamp + INTERVAL '30 minutes'
        END AS session_end
    FROM ordered
    WHERE event_type = 'session_started'
)
SELECT
    ROW_NUMBER() OVER () AS session_id,
    *,
    EXTRACT(EPOCH FROM (session_end - session_start)) / 60 AS duration_minutes
FROM sessions;