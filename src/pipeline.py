import subprocess
import logging
import duckdb

DB_PATH = "dwh.duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

SQL_MODELS = [
    "models/dim_users.sql",
    "models/dim_mentors.sql",
    "models/fct_sessions.sql"
]


def run_sql_models():
    
    con = duckdb.connect(DB_PATH)

    for file in SQL_MODELS:

        try:
            with open(file) as f:
                
                con.execute(f.read())

        except Exception as e:
            
            logging.error(f"{file} failed: {e}")
            raise


def run_step(cmd, name):
    
    try:

        subprocess.run(cmd, check=True)

    except Exception as e:

        logging.error(f"{name} failed: {e}")
        raise


def main():

    logging.info("Start pipeline")

    run_step(["python", "src/ingest.py"], "Ingestion")

    run_sql_models()
    
    run_step(["python", "src/validation.py"], "Validation")

    logging.info("Pipeline completed")


if __name__ == "__main__":
    main()