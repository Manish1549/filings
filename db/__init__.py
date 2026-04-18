import os
import psycopg2
import psycopg2.extras

DATABASE_URL = os.environ.get("DATABASE_URL", "")


def get_conn() -> psycopg2.extensions.connection:
    url = DATABASE_URL
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    if "sslmode" not in url:
        url += ("&" if "?" in url else "?") + "sslmode=require"
    conn = psycopg2.connect(url)
    conn.autocommit = False
    return conn
