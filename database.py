"""
Database module — Lavoro Giupponi6 (Supabase HTTP API Version)
Risolve i problemi di connessione IPv6 su Render usando supabase-py.
"""
import os
import hashlib
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

URL: str = os.getenv("SUPABASE_URL", "")
KEY: str = os.getenv("SUPABASE_KEY", "")

def get_client() -> Client:
    if not URL or not KEY:
        raise ValueError("SUPABASE_URL o SUPABASE_KEY mancanti in .env")
    return create_client(URL, KEY)

class SupabaseCursor:
    def __init__(self, data):
        self.data = data or []
        self.index = 0

    def fetchone(self):
        if self.index < len(self.data):
            row = self.data[self.index]
            self.index += 1
            return row
        return None

    def fetchall(self):
        return self.data

    def __iter__(self):
        return iter(self.data)

class SupabaseWrapper:
    def __init__(self):
        self.client = get_client()

    def execute(self, query, params=None):
        # Converte i ? in input per l'RPC se necessario o pulisce la query
        # Usiamo l'RPC 'exec_sql' che abbiamo creato su Supabase per compatibilità
        clean_query = query.replace("?", "%s")
        if params:
            # Semplice escape/formattazione per i parametri (molto basica per questo uso interno)
            from psycopg2.extensions import adapt
            formatted_params = tuple(adapt(p).getquoted().decode('utf-8') for p in params)
            try:
                clean_query = clean_query % formatted_params
            except:
                pass

        try:
            # Eseguiamo tramite RPC per mantenere il supporto SQL completo
            res = self.client.rpc("exec_sql", {"query_text": clean_query}).execute()
            return SupabaseCursor(res.data)
        except Exception as e:
            print(f"[DB Error] Query fallita: {query[:100]}... Errore: {e}")
            return SupabaseCursor([])

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass

def get_connection():
    return SupabaseWrapper()

def init_db():
    """Inizializza default via API."""
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
    conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?) ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash", ("admin", pw_hash))
    print("[DB] Supabase HTTP API inizializzata.")

def get_config(conn=None) -> dict:
    if conn is None: conn = get_connection()
    rows = conn.execute("SELECT chiave, valore FROM config").fetchall()
    return {r['chiave']: r['valore'] for r in rows}

if __name__ == "__main__":
    init_db()
