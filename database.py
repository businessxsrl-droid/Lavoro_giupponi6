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
        """Esegue query via RPC. Gestisce i parametri ? convertendoli in stringhe SQL safe."""
        clean_query = query
        if params:
            formatted_params = []
            for p in params:
                if p is None:
                    formatted_params.append("NULL")
                elif isinstance(p, (int, float)):
                    formatted_params.append(str(p))
                elif isinstance(p, bool):
                    formatted_params.append("TRUE" if p else "FALSE")
                else:
                    # Stringa o altro: escape degli apici e racchiudi in '
                    val = str(p).replace("'", "''")
                    formatted_params.append(f"'{val}'")
            
            # Sostituiamo i ? uno alla volta
            for p_str in formatted_params:
                clean_query = clean_query.replace("?", p_str, 1)

        try:
            res = self.client.rpc("exec_sql", {"query_text": clean_query}).execute()
            return SupabaseCursor(res.data)
        except Exception as e:
            msg = str(e)
            print(f"[DB Error] Query fallita: {query[:100]}... Errore: {msg}")
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
