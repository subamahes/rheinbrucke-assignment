
import psycopg2


DB_HOST="localhost"
DB_PORT = 5432
DB_DATABASE = "rheinbrucke"
DB_USER="postgres"
DB_PASS="pgadmin"

def db_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_DATABASE,
        user=DB_USER,
        password=DB_PASS
    )