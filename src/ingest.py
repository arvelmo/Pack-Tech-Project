import duckdb
import logging
import os

DB_PATH = "dwh.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

#FILES EXPECTED
REQUIRED_FILES = [
    "data/users_db_export.csv",
    "data/mentor_tiers.csv",
    "data/booking_events.json"
]

#SCHEMAS EXPECTED FOR EACH ENTITY
EXPECTED_SCHEMAS = {
    "users": {"user_id", "company_id", "signup_date", "status"},
    "mentors": {"mentor_id", "tier", "hourly_rate"},
    "events": {"event_id", "user_id", "mentor_id", "timestamp", "event_type"}
}

###############################################
#FUNCTIONS
###############################################

# VALIDATE ALL FILES ARE PRESENT

def validate_files():
    for file in REQUIRED_FILES:
        if not os.path.exists(file):
            raise FileNotFoundError(f"Missing required file: {file}")

# VALIDATE SCHEMA OF A CSV FILE

def validate_csv_schema(con, file_path, expected_columns, name):
    df = con.execute(f"SELECT * FROM read_csv_auto('{file_path}') LIMIT 1").df()
    if not expected_columns.issubset(set(df.columns)):
        raise ValueError(f"{name} schema mismatch: {df.columns}")
    logging.info(f"{name} schema valid")

# VALIDATE SCHEMA OF A JSON FILE

def validate_json_schema(con, file_path, expected_columns):
    df = con.execute(f"SELECT * FROM read_json_auto('{file_path}') LIMIT 5").df()
    if not expected_columns.issubset(set(df.columns)):
        raise ValueError(f"JSON schema mismatch: {df.columns}")
    logging.info("Events schema valid")

# VALIDATE DATA TYPES

def validate_types(con):
    invalid_dates = con.execute("""
        SELECT COUNT(*)
        FROM read_csv_auto('data/users_db_export.csv')
        WHERE TRY_CAST(signup_date AS TIMESTAMP) IS NULL
    """).fetchone()[0]

    if invalid_dates > 0:
        raise ValueError(f"Invalid signup_date values: {invalid_dates}")

    invalid_ts = con.execute("""
        SELECT COUNT(*)
        FROM read_json_auto('data/booking_events.json')
        WHERE TRY_CAST(timestamp AS TIMESTAMP) IS NULL
    """).fetchone()[0]

    if invalid_ts > 0:
        raise ValueError(f"Invalid timestamps: {invalid_ts}")

    logging.info("Type validation OK")


# FILES ARE NOT EMPTY VALIDATION

def validate_non_empty(con):
    checks = [
        ("users", "read_csv_auto('data/users_db_export.csv')"),
        ("mentors", "read_csv_auto('data/mentor_tiers.csv')"),
        ("events", "read_json_auto('data/booking_events.json')")
    ]

    for name, query in checks:
        count = con.execute(f"SELECT COUNT(*) FROM {query}").fetchone()[0]
        if count == 0:
            raise ValueError(f"{name} file is empty")
        logging.info(f"{name}: {count} rows")


# EXECUTE QUERY

def run_query(con, query, name):
    try:
        con.execute(query)
        logging.info(f"✅ {name} created")
    except Exception as e:
        logging.error(f"❌ Failed {name}: {e}")
        raise


def main():
    try:
        logging.info("Start ingestion")

        validate_files()
        con = duckdb.connect(DB_PATH)

        # VALIDATIONS
        validate_csv_schema(con, "data/users_db_export.csv", EXPECTED_SCHEMAS["users"], "users")
        validate_csv_schema(con, "data/mentor_tiers.csv", EXPECTED_SCHEMAS["mentors"], "mentors")
        validate_json_schema(con, "data/booking_events.json", EXPECTED_SCHEMAS["events"])
        validate_types(con)
        validate_non_empty(con)

        # USERS
        run_query(con, """
        CREATE OR REPLACE TABLE stg_users AS
        WITH raw AS (
            SELECT
                CAST(user_id AS VARCHAR) AS user_id,
                company_id,
                CAST(signup_date AS TIMESTAMP) AS signup_date,
                status,
                NOW() AS ingested_at,
                md5(
                    COALESCE(user_id, '') ||
                    COALESCE(company_id, '') ||
                    COALESCE(status, '')
                ) AS row_hash
            FROM read_csv_auto('data/users_db_export.csv')
        ),
        dedup AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id
                    ORDER BY signup_date DESC
                ) rn
            FROM raw
        )
        SELECT * EXCLUDE rn FROM dedup WHERE rn = 1;
        """, "stg_users")

        # MENTORS
        run_query(con, """
        CREATE OR REPLACE TABLE stg_mentors AS
        SELECT
            UPPER(TRIM(CAST(mentor_id AS VARCHAR))) AS mentor_id,
            tier,
            hourly_rate,
            NOW() AS ingested_at,
            md5(
                COALESCE(mentor_id, '') ||
                COALESCE(tier, '')
            ) AS row_hash
        FROM read_csv_auto('data/mentor_tiers.csv');
        """, "stg_mentors")

        # EVENTS
        run_query(con, """
        CREATE OR REPLACE TABLE stg_events AS
        SELECT
            event_id,
            CAST(user_id AS VARCHAR) AS user_id,
            UPPER(TRIM(CAST(mentor_id AS VARCHAR))) AS mentor_id,
            CAST(timestamp AS TIMESTAMP) AS timestamp,
            event_type,
            NOW() AS ingested_at,
            md5(
                COALESCE(event_id, '') ||
                COALESCE(user_id, '') ||
                COALESCE(event_type, '')
            ) AS row_hash
        FROM read_json_auto('data/booking_events.json');
        """, "stg_events")

        logging.info("Ingestion completed")

    except Exception as e:
        logging.critical(f"Ingestion failed: {e}")
        raise


if __name__ == "__main__":
    main()