import os
import logging
import psycopg2
from psycopg2 import sql
from datetime import datetime

# Configure logging to Cloud Logging format
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_db_connection():
    return psycopg2.connect(
        dbname=os.environ.get("DB_NAME"),
        user=os.environ.get("DB_USER"),
        password=os.environ.get("DB_PASSWORD"),
        host=os.environ.get("DB_HOST"),
        port=os.environ.get("DB_PORT", 5432)
    )

def main():
    current_hour = datetime.utcnow().hour  # use UTC for consistency with Cloud Run Jobs
    logging.info(f"Starting materialized view refresh job. Current UTC hour: {current_hour}")

    conn = get_db_connection()
    conn.autocommit = True
    cur = conn.cursor()

    # 1. Get all schemas in the database
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
    """)
    
    logging.info(f"Getting list of schemas in the database.")
    schemas = [row[0] for row in cur.fetchall()]

    for schema in schemas:
        # 2. Check if schema has a table called materializedviews
        cur.execute(sql.SQL("""
            SELECT EXISTS (
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema = %s
                AND table_name = 'materializedviews'
            )
        """), [schema])

        logging.info(f"Checking for materializedviews table in schema: {schema}")
        fetch_result = cur.fetchone()
        exists = fetch_result[0] if fetch_result is not None else False

        if not exists:
            logging.info(f"No materializedviews table found in schema: {schema}")
            continue

        logging.info(f"Found materializedviews table in schema: {schema}")

        # 3. Query the table for rows
        logging.info(f"Querying materializedviews table in schema: {schema}")
        cur.execute(sql.SQL("""
            SELECT name, hours FROM {}.materializedviews
        """).format(sql.Identifier(schema)))

        rows = cur.fetchall()
        logging.info(f"Fetched {len(rows)} rows from {schema}.materializedviews")

        for name, hours in rows:
            logging.info(f"Checking UTC hours listed in materialized view '{name}' in schema '{schema}'")

            if not hours:
                logging.warning(f"No hours specified for {schema}.materializedviews for {name}")
                continue

            try:
                hour_list = [int(h.strip()) for h in hours.split(",") if h.strip().isdigit()]
            except Exception:
                logging.warning(f"Invalid hours format in {schema}.materializedviews for {name}: {hours}")
                continue

            if current_hour in hour_list:
                logging.info(f"Executing REFRESH MATERIALIZED VIEW {schema}.{name}")
                try:
                    cur.execute(sql.SQL("REFRESH MATERIALIZED VIEW {}.{}")
                                .format(sql.Identifier(schema), sql.Identifier(name)))
                    logging.info(f"Successfully refreshed {schema}.{name}")
                except Exception as e:
                    logging.error(f"Failed to refresh {schema}.{name}: {e}")
            else:
                logging.info(f"Skipping {schema}.{name}; current UTC hour {current_hour} not in {hour_list}")

    cur.close()
    conn.close()
    logging.info("Finished materialized view refresh job.")

if __name__ == "__main__":
    main()
