import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from pathlib import Path
import os
import sys

# Load environment variables
env_path = Path('config')/'.env'
load_dotenv(dotenv_path=env_path)  # This reads .env and loads values into os.environ

# Environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DEFAULT_DB = os.getenv("DB_NAME")  
TARGET_DB = "datawarehouse"        # The DB we want to drop & recreate

def create_connection(db_name):
    """Create connection to a specific database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=db_name,
            user=DB_USER,
            password=DB_PASSWORD
        )
        conn.autocommit = True
        return conn
    except Exception as e:
        print(f"❌ Connection to '{db_name}' failed: {e}")
        return None

def check_database_exists(conn, db_name):
    """Check if a database exists"""
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
        return cur.fetchone() is not None
    except Exception as e:
        print(f"❌ Error checking for database existence: {e}")
        return False

def recreate_database():
    """Drop and recreate the database"""
    conn = create_connection(DEFAULT_DB)
    if conn:
        try:
            cur = conn.cursor()
            # Terminate active connections
            cur.execute(sql.SQL("""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE datname = %s AND pid <> pg_backend_pid();
            """), [TARGET_DB])
            # Drop and recreate
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(TARGET_DB)))
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(TARGET_DB)))
            print(f"\n✅ Database '{TARGET_DB}' dropped and recreated.")
        except Exception as e:
            print(f"❌ Error recreating database: {e}")
        finally:
            conn.close()

def create_schemas():
    """Create schemas in the new database"""
    conn = create_connection(TARGET_DB)
    if conn:
        try:
            cur = conn.cursor()
            schemas = ['bronze', 'silver', 'gold']
            for schema in schemas:
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
            print("\n✅ Schemas created: bronze 🥉, silver 🥈, gold 🥇")
        except Exception as e:
            print(f"❌ Error creating schemas: {e}")
        finally:
            conn.close()

# === Main Logic ===
if __name__ == "__main__":
    conn = create_connection(DEFAULT_DB)
    if not conn:
        sys.exit(1)

    if check_database_exists(conn, TARGET_DB):
        print("\n⚠ Database 'datawarehouse' already exists, make sure to have proper backups before proceding this step. ⚠")
        choice = input(f"\n Database '{TARGET_DB}' already exists. Do you want to drop and recreate it? (yes/no): ").strip().lower()
        if choice not in ['yes', 'y']:
            print("❌ Operation cancelled by user.")
            conn.close()
            sys.exit(0)
    
    recreate_database()
    create_schemas()
