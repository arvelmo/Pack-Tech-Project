WITH ranked AS (
    SELECT
        s.*,
        m.tier,
        ROW_NUMBER() OVER (PARTITION BY s.user_id ORDER BY s.session_start) rn
    FROM fct_sessions s
    JOIN dim_mentors m 
        ON s.mentor_id = m.mentor_id
),
first_sessions AS (
    SELECT * FROM ranked WHERE rn = 1
),
second_sessions AS (
    SELECT * FROM ranked WHERE rn = 2
)
SELECT
    tier,
    COUNT(DISTINCT f.user_id) AS total_users,
    COUNT(DISTINCT s.user_id) AS rebooked_users,
    COUNT(DISTINCT s.user_id) * 1.0 
        / COUNT(DISTINCT f.user_id) AS rebooking_rate
FROM first_sessions f
LEFT JOIN second_sessions s
    ON f.user_id = s.user_id
    AND s.session_start <= f.session_start + INTERVAL '30 days'
GROUP BY tier
ORDER BY rebooking_rate DESC;