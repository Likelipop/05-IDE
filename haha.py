import psycopg2
from psycopg2 import OperationalError, extensions
from typing import Optional, Dict

def connect_to_db(source_config: Dict[str, str]) -> Optional[extensions.connection]:
    """
    Establish a connection to a PostgreSQL database using parameters from source_config.

    Args:
        source_config (Dict[str, str]): A dictionary containing 'host', 'database', 'user', and optionally 'password' and 'port'.

    Returns:
        Optional[psycopg2.extensions.connection]: A connection object if successful, or None if failed.
    """
    try:
        conn = psycopg2.connect(
            host=source_config.get("host", "localhost"),
            database=source_config.get("database", "postgres"),
            user=source_config.get("user", "postgres"),
            password=source_config.get("password", ""),
            port=source_config.get("port", "5432")
        )
        print("Connection successful.")
        return conn

    except OperationalError as e:
        print(f"Error connecting to the database: {e}")
        return None


def create_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS students (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                age INTEGER,
                grade VARCHAR(10)
            );
        """)
        conn.commit()
        print("Table 'students' created.")


def insert_sample_data(conn):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO students (name, age, grade)
            VALUES 
                ('Alice', 20, 'A'),
                ('Bob', 22, 'B'),
                ('Charlie', 19, 'A')
            ON CONFLICT DO NOTHING;
        """)
        conn.commit()
        print("Sample data inserted.")


def query_data(conn):
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM students;")
        rows = cur.fetchall()
        print("Data from students table:")
        for row in rows:
            print(row)


config = {
    "host": "localhost",
    "database": "mydatabase",
    "user": "myuser",
    "password": "mypassword",
    "port": "15432"
}

conn = connect_to_db(config)

if conn:
    create_table(conn)
    insert_sample_data(conn)
    query_data(conn)
    conn.close()
