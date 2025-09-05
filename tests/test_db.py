import psycopg2
from dotenv import load_dotenv
import os

def test_connection():
    try:
        load_dotenv()
        # Neon provides a single DATABASE_URL
        conn = psycopg2.connect(
            os.getenv("DATABASE_URL"),
            sslmode="require"   # Required for Neon
        )
        cur = conn.cursor()
        cur.execute("SELECT 'Neon connection successful!'")
        result = cur.fetchone()
        print(result[0])
        conn.close()
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    test_connection()
