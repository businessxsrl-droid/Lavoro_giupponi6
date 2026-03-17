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
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ── Configurazione ─────────────────────────────────────────────────────────────
URL = os.getenv("SUPABASE_URL")
KEY = os.getenv("SUPABASE_KEY")

if not URL or not KEY:
    raise ValueError("SUPABASE_URL e SUPABASE_KEY devono essere configurate nel .env")

supabase: Client = create_client(URL, KEY)

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
            # Chiamata RPC alla funzione 'exec_sql' definita su Supabase
            res = supabase.rpc("exec_sql", {"query": sql}).execute()
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
        if not params_list:
            return
        
        # Aumentiamo la dimensione del batch
        batch_size = 200
        
        for i in range(0, len(params_list), batch_size):
            chunk = params_list[i : i + batch_size]
            
            # Ottimizzazione: se è un INSERT semplice, possiamo trasformarlo in un multi-value INSERT
            # Es: INSERT INTO t (a,b) VALUES (?,?) -> INSERT INTO t (a,b) VALUES (1,2), (3,4)
            upper_query = query.strip().upper()
            if upper_query.startswith("INSERT INTO") and "VALUES (" in upper_query:
                try:
                    base_sql = query.split("VALUES")[0] + " VALUES "
                    values_parts = []
                    for p in chunk:
                        # Estraiamo la riga formattata (senza l'intestazione INSERT INTO)
                        # Usiamo un placeholder temporaneo per formattare solo i valori
                        formatted_row = self._format_sql("VALUES (?)", [p])
                        # Prendi solo quello che c'è tra le parentesi tonde estreme
                        row_vals = formatted_row[formatted_row.find("("):]
                        values_parts.append(row_vals)
                    
                    # Gestione ON CONFLICT (se presente)
                    tail = ""
                    conflict_idx = query.upper().find("ON CONFLICT")
                    if conflict_idx != -1:
                        tail = " " + query[conflict_idx:]
                    
                    combined_sql = base_sql + ", ".join(values_parts) + tail
                    supabase.rpc("exec_sql", {"query": combined_sql}).execute()
                    continue # Successo con multi-value insert
                except Exception as e:
                    print(f"[Supabase Multi-Value Error] {e} - Falling back to block")

            # Fallback: Blocco BEGIN...COMMIT (per query complesse o se fallisce sopra)
            combined_sql = "BEGIN;\n"
            for p in chunk:
                # Aggiungiamo WHERE TRUE alle DELETE se necessario (ma executemany di solito è per INSERT)
                stmt = self._format_sql(query, p).strip().rstrip(';')
                combined_sql += stmt + ";\n"
            combined_sql += "COMMIT;"
            
            try:
                supabase.rpc("exec_sql", {"query": combined_sql}).execute()
            except Exception as e:
                print(f"[Supabase Batch Error] {e}")
                # Fallback lento riga per riga se tutto il blocco fallisce
                for p in chunk:
                    try:
                        self.execute(query, p)
                    except:
                        pass

    def commit(self): pass
    def rollback(self): pass
    def close(self): pass

    def _format_sql(self, query, params):
        if not params:
            return query
        
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
                # Stringhe e date: escape dei singoli apici e encoding UTF-8
                s = str(p).replace("'", "''")
                formatted_params.append(f"'{s}'")
        
        # Sostituisce i placeholder ? con i valori
        parts = query.split('?')
        if len(parts) - 1 != len(formatted_params):
            # Se il numero di ? non coincide, proviamo con %s (stile psycopg2)
            parts = query.split('%s')
            
        res = ""
        for i in range(len(formatted_params)):
            res += parts[i] + formatted_params[i]
        res += parts[-1]
        return res

def get_connection():
    return SupabaseConnection()

def init_db():
    """Inizializza le tabelle via RPC."""
    conn = get_connection()
    # SQL di creazione tabelle (stesso di prima)
    tables_sql = """
    CREATE TABLE IF NOT EXISTS impianti (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL UNIQUE, nome TEXT, comune TEXT, indirizzo TEXT, alias_terminale TEXT, tipo_gestione TEXT DEFAULT 'PRESIDIATO'
    );
    CREATE TABLE IF NOT EXISTS transazioni_fortech (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv), data TEXT NOT NULL, totale_contante NUMERIC DEFAULT 0, totale_pos NUMERIC DEFAULT 0, totale_buoni NUMERIC DEFAULT 0, totale_satispay NUMERIC DEFAULT 0, totale_petrolifere NUMERIC DEFAULT 0, UNIQUE(codice_pv, data)
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
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv), data TEXT NOT NULL, categoria TEXT NOT NULL, valore_teorico NUMERIC DEFAULT 0, valore_reale   NUMERIC DEFAULT 0, differenza     NUMERIC DEFAULT 0, stato          TEXT DEFAULT 'IN_ATTESA', note           TEXT, UNIQUE(codice_pv, data, categoria)
    );
    CREATE TABLE IF NOT EXISTS contanti_matching (
        id SERIAL PRIMARY KEY, codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv), data TEXT NOT NULL, contanti_teorico NUMERIC DEFAULT 0, contanti_versato NUMERIC DEFAULT 0, differenza       NUMERIC DEFAULT 0, stato            TEXT DEFAULT 'IN_ATTESA', tipo_match       TEXT DEFAULT 'nessuno', risolto          BOOLEAN DEFAULT FALSE, verificato_da    TEXT, data_verifica    TEXT, note             TEXT, UNIQUE(codice_pv, data)
    );
    CREATE TABLE IF NOT EXISTS config (chiave TEXT PRIMARY KEY, valore TEXT);
    CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL);
    """
    conn.execute(tables_sql)

    # Dati di default
    defaults = {
        'tolleranza_contanti_arrotondamento': '2.00',
        'tolleranza_carte_fisiologica':       '1.00',
        'tolleranza_satispay':                '0.01',
        'tolleranza_buoni':                   '0.01',
        'tolleranza_petrolifere':             '0.01',
        'scarto_giorni_buoni':                '1',
        'scarto_giorni_contanti_inf':         '3',
        'scarto_giorni_contanti_sup':         '7',
    }
    for k, v in defaults.items():
        conn.execute("INSERT INTO config (chiave, valore) VALUES (?, ?) ON CONFLICT (chiave) DO NOTHING", (k, v))
    
    pw_hash = hashlib.sha256("calor2024".encode()).hexdigest()
    conn.execute("INSERT INTO users (username, password_hash) VALUES (?, ?) ON CONFLICT (username) DO UPDATE SET password_hash = EXCLUDED.password_hash", ("admin", pw_hash))
    
    print("[DB] HTTP Supabase inizializzato.")

def get_config(conn=None) -> dict:
    if conn is None: conn = get_connection()
    rows = conn.execute("SELECT chiave, valore FROM config").fetchall()
    return {r['chiave']: r['valore'] for r in rows}

if __name__ == "__main__":
    init_db()
