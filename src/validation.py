import duckdb
import logging

DB_PATH = "dwh.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)


def run_check(con, query, msg, condition, severity="WARNING"):

    result = con.execute(query).fetchone()[0]

    

    if condition(result):

        if severity == "ERROR":

            raise ValueError(msg)
        
        elif severity == "WARNING":

            logging.warning(f"{msg} - threshold exceeded")

    else:
            
        logging.info(f"{msg}: {result:.2%}")


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
        SELECT COUNT(*) / (SELECT COUNT(*) FROM fct_sessions)
        FROM fct_sessions s
        LEFT JOIN dim_users u ON s.user_id = u.user_id
        WHERE u.user_id IS NULL
        """,
        "High orphan user ratio (Root cause: batch vs stream)",
        lambda x: x > 0.3
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


if __name__ == "__main__":
    main()