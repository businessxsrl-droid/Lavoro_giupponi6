"""
Database module — Lavoro Giupponi6
Utilizza l'API HTTP di Supabase (via RPC exec_sql) per massima compatibilità con Render (No IPv6 issues).
Ottimizzato con batching per evitare timeout.
"""
import os
import hashlib
import json
import time
from datetime import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from supabase import create_client, Client

from dotenv import load_dotenv

load_dotenv()

# ── Configurazione ─────────────────────────────────────────────────────────────
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
_supabase_client = None

def get_supabase_client():
    global _supabase_client
    if _supabase_client is None:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise ValueError("SUPABASE_URL e SUPABASE_KEY devono essere configurate nel .env se non è presente DATABASE_URL")
        from supabase import create_client
        _supabase_client = create_client(SUPABASE_URL, SUPABASE_KEY)
    return _supabase_client


class DualAccessRow(dict):
    """Supporta row['col'] e row[0]."""
    def __init__(self, data):
        if isinstance(data, dict):
            super().__init__(data)
            self._values = list(data.values())
        else:
            super().__init__()
            self._values = []

    def __getitem__(self, key):
        if isinstance(key, int):
            return self._values[key] if key < len(self._values) else None
        return super().__getitem__(key)

class SupabaseCursor:
    def __init__(self, results):
        self.results = results or []
        self.index = 0

    def fetchone(self):
        if self.index < len(self.results):
            row = self.results[self.index]
            self.index += 1
            return DualAccessRow(row)
        return None

    def fetchall(self):
        return [DualAccessRow(r) for r in self.results]

    def __iter__(self):
        return (DualAccessRow(r) for r in self.results)

class SupabaseConnection:
    def execute(self, query, params=None):
        sql = self._format_sql(query, params)
        try:
            client = get_supabase_client()
            # Chiamata RPC alla funzione 'exec_sql' definita su Supabase
            res = client.rpc("exec_sql", {"query": sql}).execute()

            # Se la query è un SELECT, res.data conterrà le righe.
            # Se è INSERT/UPDATE, res.data potrebbe essere vuoto o un conteggio.
            data = res.data if res.data else []
            if isinstance(data, (int, float, str)): # Fallback se restituisce un numero
                data = [{"result": data}]
            return SupabaseCursor(data)
        except Exception as e:
            print(f"[Supabase HTTP Error] {e} | Query: {sql[:200]}")
            raise

    def executemany(self, query, params_list):
        """Esegue insert multipli in un'unica chiamata per performance."""
        if not params_list: return
        
        q_upper = query.strip().upper()
        # Se è un INSERT con VALUES, usiamo multi-row insert (molto più veloce)
        if q_upper.startswith("INSERT INTO") and "VALUES" in q_upper:
            try:
                parts = query.split("VALUES")
                base = parts[0] + " VALUES "
                
                # Estraiamo i placeholder (es: "(?, ?, ?)" o "(%s, %s)") e l'eventuale ON CONFLICT
                values_part = parts[1]
                on_conflict_idx = values_part.upper().find("ON CONFLICT")
                
                if on_conflict_idx != -1:
                    placeholders = values_part[:on_conflict_idx].strip()
                    tail = " " + values_part[on_conflict_idx:]
                else:
                    placeholders = values_part.strip()
                    tail = ""
                
                batch_size = 100 # Bilanciamento tra velocità e dimensione payload
                for i in range(0, len(params_list), batch_size):
                    chunk = params_list[i : i + batch_size]
                    rows_sql = [self._format_sql(placeholders, p) for p in chunk]
                    combined_sql = base + ", ".join(rows_sql) + tail
                    
                    try:
                        client = get_supabase_client()
                        client.rpc("exec_sql", {"query": combined_sql}).execute()

                    except Exception as e:
                        print(f"[DB Batch Error] Chiamata fallita, provo record singolarmente: {e}")
                        for p in chunk:
                            try: self.execute(query, p)
                            except Exception as ex: print(f"[DB Batch Fallback Error] {ex}")
                return
            except Exception as e:
                print(f"[DB Optimization Error] Fallback al loop standard: {e}")

        # Fallback universale (loop per query non-insert o se l'ottimizzazione fallisce)
        for p in params_list:
            try: self.execute(query, p)
            except Exception as ex: print(f"[DB Loop Error] {ex}")

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass

    def _format_sql(self, query, params):
        if not params: return query
        
        # Converte parametri in formato SQL string-safe
        formatted_params = []
        for p in params:
            if p is None:
                formatted_params.append("NULL")
            elif isinstance(p, (int, float)):
                formatted_params.append(str(p))
            elif isinstance(p, bool):
                formatted_params.append("TRUE" if p else "FALSE")
            else:
                # Stringhe e date: escape dei singoli apici
                s = str(p).replace("'", "''")
                formatted_params.append(f"'{s}'")
        
        # Sostituisce i placeholder (? o %s) in modo posizionale sicuro
        parts = query.split('?') if '?' in query else query.split('%s')
        
        res = ""
        # Uniamo le parti alternando con i parametri formattati
        for i in range(min(len(parts) - 1, len(formatted_params))):
            res += parts[i] + formatted_params[i]
        
        # Aggiungiamo l'ultima parte di query o quelle rimanenti
        if len(parts) > len(formatted_params):
            res += "".join(parts[len(formatted_params):])
        
        return res

class PostgresCursor:
    def __init__(self, cursor):
        self._cursor = cursor

    def execute(self, query, params=None):
        """Permette l'uso di c = conn.cursor(); c.execute(...).fetchone()"""
        q = query.replace('?', '%s')
        try:
            self._cursor.execute(q, params)
        except Exception as e:
            print(f"[PostgresCursor Error] {e} | Query: {q[:200]}")
            raise
        return self

    def fetchone(self):
        row = self._cursor.fetchone()
        return DualAccessRow(dict(row)) if row else None

    def fetchall(self):
        return [DualAccessRow(dict(r)) for r in self._cursor.fetchall()]

    def __iter__(self):
        for r in self._cursor:
            yield DualAccessRow(dict(r))

    @property
    def rowcount(self):
        return self._cursor.rowcount

class PostgresConnection:
    def __init__(self, url):
        self._url = url
        self._conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
        self._conn.autocommit = True

    def cursor(self):
        return PostgresCursor(self._conn.cursor())

    def execute(self, query, params=None):
        # Converte ? in %s per compatibilità con psycopg2
        q = query.replace('?', '%s')
        cur = self._conn.cursor()
        try:
            cur.execute(q, params)
            return PostgresCursor(cur)
        except Exception as e:
            print(f"[Postgres Error] {e} | Query: {q[:200]}")
            raise


    def executemany(self, query, params_list):
        if not params_list: return
        q = query.replace('?', '%s')
        cur = self._conn.cursor()
        cur.executemany(q, params_list)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

def get_connection():
    return SupabaseConnection()

def init_db():
    """Inizializza le tabelle via RPC."""
    conn = get_connection()
    # SQL di creazione tabelle (stesso di prima)
    tables_sql = """
    CREATE TABLE IF NOT EXISTS impianti (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL UNIQUE, nome TEXT, comune TEXT, indirizzo TEXT, alias_terminale TEXT, tipo_gestione TEXT DEFAULT 'PRESIDIATO', senza_servizio_riconciliazione BOOLEAN DEFAULT FALSE
    );
    CREATE TABLE IF NOT EXISTS transazioni_fortech (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv), data TEXT NOT NULL, totale_contante NUMERIC DEFAULT 0, totale_pos NUMERIC DEFAULT 0, totale_buoni NUMERIC DEFAULT 0, totale_satispay NUMERIC DEFAULT 0, totale_petrolifere NUMERIC DEFAULT 0, prove_erogazione NUMERIC DEFAULT 0, clienti_fine_mese NUMERIC DEFAULT 0, diversi NUMERIC DEFAULT 0, UNIQUE(codice_pv, data)
    );
    CREATE TABLE IF NOT EXISTS transazioni_contanti (
        id SERIAL PRIMARY KEY, data TEXT NOT NULL, codice_pv INTEGER REFERENCES impianti(codice_pv), importo NUMERIC, note_raw TEXT
    );
    CREATE TABLE IF NOT EXISTS transazioni_pos (
        id SERIAL PRIMARY KEY, data TEXT NOT NULL, alias_terminale TEXT, importo NUMERIC, circuito TEXT
    );
    CREATE TABLE IF NOT EXISTS transazioni_satispay (
        id SERIAL PRIMARY KEY, data TEXT NOT NULL, codice_pv INTEGER REFERENCES impianti(codice_pv), importo NUMERIC
    );
    CREATE TABLE IF NOT EXISTS transazioni_buoni (
        id SERIAL PRIMARY KEY, data TEXT NOT NULL, codice_pv INTEGER REFERENCES impianti(codice_pv), importo NUMERIC, esercente TEXT
    );
    CREATE TABLE IF NOT EXISTS transazioni_petrolifere (
        id SERIAL PRIMARY KEY, data TEXT NOT NULL, codice_pv INTEGER REFERENCES impianti(codice_pv), importo NUMERIC
    );
    CREATE TABLE IF NOT EXISTS riconciliazione_risultati (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv), data TEXT NOT NULL, categoria TEXT NOT NULL, valore_teorico NUMERIC DEFAULT 0, valore_reale   NUMERIC DEFAULT 0, differenza     NUMERIC DEFAULT 0, stato          TEXT DEFAULT 'IN_ATTESA', tipo_match     TEXT DEFAULT 'nessuno', note           TEXT, UNIQUE(codice_pv, data, categoria)
    );

    CREATE TABLE IF NOT EXISTS contanti_matching (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv), data TEXT NOT NULL, contanti_teorico NUMERIC DEFAULT 0, contanti_versato NUMERIC DEFAULT 0, differenza       NUMERIC DEFAULT 0, stato            TEXT DEFAULT 'IN_ATTESA', tipo_match       TEXT DEFAULT 'nessuno', risolto          BOOLEAN DEFAULT FALSE, verificato_da    TEXT, data_verifica    TEXT, note             TEXT, UNIQUE(codice_pv, data)
    );
    CREATE TABLE IF NOT EXISTS config (chiave TEXT PRIMARY KEY, valore TEXT);
    CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL);
    """
    conn.execute(tables_sql)

    # Migration: aggiungi colonne nuove se non esistono (per DB già esistenti)
    migrations = [
        "ALTER TABLE impianti ADD COLUMN IF NOT EXISTS senza_servizio_riconciliazione BOOLEAN DEFAULT FALSE",
        "ALTER TABLE transazioni_fortech ADD COLUMN IF NOT EXISTS prove_erogazione NUMERIC DEFAULT 0",
        "ALTER TABLE transazioni_fortech ADD COLUMN IF NOT EXISTS clienti_fine_mese NUMERIC DEFAULT 0",
        "ALTER TABLE transazioni_fortech ADD COLUMN IF NOT EXISTS diversi NUMERIC DEFAULT 0",
    ]
    for sql in migrations:
        try:
            conn.execute(sql)
        except Exception:
            pass  # Colonna già presente

    # Dati di default
    defaults = {
        'tolleranza_contanti_arrotondamento':  '2.00',
        'tolleranza_carte_fisiologica':        '1.00',
        'tolleranza_satispay':                 '0.01',
        'tolleranza_buoni':                    '0.01',
        'tolleranza_petrolifere':              '0.01',
        'scarto_giorni_buoni':                 '1',
        'scarto_giorni_contanti_inf':          '3',
        'scarto_giorni_contanti_sup':          '7',
        'riconciliazione_contanti_abilitata':  'false',
    }
    for k, v in defaults.items():
        conn.execute("INSERT INTO config (chiave, valore) VALUES (?, ?) ON CONFLICT (chiave) DO NOTHING", (k, v))

    pw_hash = hashlib.sha256("calor2024".encode()).hexdigest()
    conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?) ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash", ("admin", pw_hash))

    # Marca impianti senza servizio di riconciliazione
    _SENZA_SERVIZIO_PVS = [47831, 45874, 47832, 41118, 42840, 45818, 49788]
    for pv in _SENZA_SERVIZIO_PVS:
        try:
            conn.execute(
                "UPDATE impianti SET senza_servizio_riconciliazione = TRUE WHERE codice_pv = ?", (pv,)
            )
        except Exception:
            pass

    # Inserisci Famagosta se non presente
    try:
        conn.execute('''
            INSERT INTO impianti (codice_pv, nome, comune, indirizzo, tipo_gestione, senza_servizio_riconciliazione)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(codice_pv) DO UPDATE SET senza_servizio_riconciliazione = TRUE
        ''', (45818, 'Famagosta', 'Milano', 'Viale Famagosta 15', 'PRESIDIATO', True))
    except Exception:
        pass

    print("[DB] DB inizializzato.")

def get_config(conn=None) -> dict:
    if conn is None: conn = get_connection()
    rows = conn.execute("SELECT chiave, valore FROM config").fetchall()
    return {r['chiave']: r['valore'] for r in rows}

if __name__ == "__main__":
    init_db()
