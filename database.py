"""
Database module — Lavoro Giupponi6 (Supabase / Postgres version)
"""
import psycopg2
import psycopg2.extras
import os
import hashlib
from dotenv import load_dotenv

load_dotenv()

# PROVA 2: Utilizzo del POOLER di Supabase invece della connessione diretta
# DATABASE_URL=postgresql://postgres.[REF]:[PASSWORD]@aws-0-eu-west-1.pooler.supabase.com:6543/postgres?sslmode=require
DB_URL = os.getenv("DATABASE_URL_POOLER") or os.getenv("DATABASE_URL")

class PostgresCursorWrapper:
    def __init__(self, cursor):
        self.cursor = cursor
    def execute(self, query, vars=None):
        return self.cursor.execute(query.replace("?", "%s"), vars)
    def __getattr__(self, name):
        return getattr(self.cursor, name)
    def __iter__(self):
        return iter(self.cursor)

class PostgresWrapper:
    def __init__(self, conn):
        self.conn = conn
    def cursor(self, **kwargs):
        return PostgresCursorWrapper(self.conn.cursor(cursor_factory=psycopg2.extras.DictCursor))
    def execute(self, query, params=None):
        cur = self.cursor()
        cur.execute(query, params)
        return cur
    def commit(self): self.conn.commit()
    def rollback(self): self.conn.rollback()
    def close(self): self.conn.close()

def get_connection():
    if not DB_URL:
        raise ValueError("DATABASE_URL non configurata nel file .env")
    conn = psycopg2.connect(DB_URL)
    return PostgresWrapper(conn)

def init_db():
    conn = get_connection()
    defaults = {
        'tolleranza_contanti_arrotondamento': '2.00',
        'tolleranza_carte_fisiologica':       '1.00',
        'tolleranza_satispay':                '0.01',
        'tolleranza_buoni':                   '0.01',
        'tolleranza_petrolifere':             '0.01',
        'scarto_giorni_buoni':                '1',
        'scarto_giorni_contanti_inf':         '3',
        'scarto_giorni_contanti_sup':         '7',
        'openrouter_api_key':                 '',
    }
    for k, v in defaults.items():
        conn.execute("INSERT INTO config (chiave, valore) VALUES (?, ?) ON CONFLICT (chiave) DO NOTHING", (k, v))
    pw_hash = hashlib.sha256("calor2024".encode()).hexdigest()
    conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?) ON CONFLICT (username) DO NOTHING", ("admin", pw_hash))
    conn.commit()
    conn.close()
    print("[DB] Supabase Inizializzato.")

def get_config(conn=None) -> dict:
    close = False
    if conn is None:
        conn = get_connection()
        close = True
    rows = conn.execute("SELECT chiave, valore FROM config").fetchall()
    cfg = {r['chiave']: r['valore'] for r in rows}
    if close: conn.close()
    return cfg

if __name__ == "__main__":
    init_db()
