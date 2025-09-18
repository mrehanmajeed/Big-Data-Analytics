import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
import psycopg2

load_dotenv()

PG_USER = os.getenv("PG_USER")
PG_PASS = os.getenv("PG_PASS")
PG_PORT = os.getenv("PG_PORT")
PG_DB   = os.getenv("PG_DB")

try:
    raw_conn = psycopg2.connect(
        dbname=PG_DB,
        user=PG_USER,
        password=PG_PASS,
        host="localhost",
        port=PG_PORT
    )
    print("✅ Raw psycopg2 connection successful")

    engine = create_engine("postgresql+psycopg2://", creator=lambda: raw_conn)
    with engine.connect() as conn:
        print("✅ SQLAlchemy connection successful")
except Exception as e:
    print("❌ Connection failed:", e)
