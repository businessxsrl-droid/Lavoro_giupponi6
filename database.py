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


class DualAccessRow(dict):
    """
    Un dict che supporta anche l'accesso per indice numerico (row[0], row[1], ...),
    mantenendo la compatibilità con il codice che si aspetta dict (row['chiave'])
    e il codice che si aspetta tuple (row[0]).
    """
    def __init__(self, data):
        if isinstance(data, dict):
            super().__init__(data)
            self._values = list(data.values())
        else:
            super().__init__()
            self._values = []

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key]
        return super().__getitem__(key)


class SupabaseCursor:
    def __init__(self, data):
        # Converte ogni riga in DualAccessRow per accesso ibrido dict/tuple
        if isinstance(data, list):
            self.data = [DualAccessRow(r) if isinstance(r, dict) else r for r in data]
        elif isinstance(data, dict):
            # Il risultato RPC per INSERT/UPDATE/DELETE è un dict singolo
            self.data = [DualAccessRow(data)]
        else:
            self.data = []
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
        """Esegue query via RPC exec_sql. Gestisce i parametri ? convertendoli in stringhe SQL safe."""
        clean_query = query
        if params:
            formatted_params = []
            for p in params:
                if p is None:
                    formatted_params.append("NULL")
                elif isinstance(p, bool):
                    formatted_params.append("TRUE" if p else "FALSE")
                elif isinstance(p, (int, float)):
                    formatted_params.append(str(p))
                else:
                    # Stringa: escape apici singoli e racchiudi tra apici
                    val = str(p).replace("'", "''")
                    formatted_params.append(f"'{val}'")

            # Sostituiamo i ? uno alla volta (da sinistra a destra)
            for p_str in formatted_params:
                clean_query = clean_query.replace("?", p_str, 1)

        try:
            res = self.client.rpc("exec_sql", {"query_text": clean_query}).execute()
            return SupabaseCursor(res.data)
        except Exception as e:
            msg = str(e)
            print(f"[DB Error] Query: {clean_query[:120]}... | Errore: {msg}")
            return SupabaseCursor([])

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass


def get_connection():
    return SupabaseWrapper()


def init_db():
    """Inizializza configurazioni default e utente admin."""
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
        conn.execute(
            "INSERT INTO config (chiave, valore) VALUES (?, ?) ON CONFLICT (chiave) DO NOTHING",
            (k, v)
        )

    pw_hash = hashlib.sha256("calor2024".encode()).hexdigest()
    conn.execute(
        "INSERT INTO users (username, password_hash) VALUES (?, ?) "
        "ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash",
        ("admin", pw_hash)
    )
    print("[DB] Supabase HTTP API inizializzata con successo.")


def get_config(conn=None) -> dict:
    if conn is None:
        conn = get_connection()
    rows = conn.execute("SELECT chiave, valore FROM config").fetchall()
    return {r['chiave']: r['valore'] for r in rows}


if __name__ == "__main__":
    init_db()
