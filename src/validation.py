import duckdb
import logging

DB_PATH = "dwh.duckdb"

logging.basicConfig(level=logging.INFO)


def run_check(con, query, msg, condition):
    result = con.execute(query).fetchone()[0]
    logging.info(f"Check result: {result}")
    if condition(result):
        raise ValueError(msg)


def main():
    con = duckdb.connect(DB_PATH)

    run_check(
        con,
        "SELECT COUNT(*) FROM fct_sessions WHERE duration_minutes < 0 OR duration_minutes > 300",
        "Invalid durations",
        lambda x: x > 0
    )

    run_check(
        con,
        """
        SELECT COUNT(*) * 1.0 / (SELECT COUNT(*) FROM fct_sessions)
        FROM fct_sessions s
        LEFT JOIN dim_users u ON s.user_id = u.user_id
        WHERE u.user_id IS NULL
        """,
        "Too many orphan users",
        lambda x: x > 0.05
    )

    run_check(
        con,
        """
        SELECT COUNT(*)
        FROM fct_sessions s
        LEFT JOIN dim_mentors m ON s.mentor_id = m.mentor_id
        WHERE m.mentor_id IS NULL
        """,
        "Invalid mentors",
        lambda x: x > 0
    )

    logging.info("All validations passed")


if __name__ == "__main__":
    main()