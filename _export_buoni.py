"""Script one-shot: riconcilia i buoni e salva in Excel."""
import os, sqlite3, datetime
import pandas as pd

os.chdir(os.path.dirname(os.path.abspath(__file__)))
os.environ.pop('DATABASE_URL', None)

import database
from ingestion import ingest_fortech, ingest_buoni
from reconciler import reconcile

EXCEL_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "excel")
DB_PATH   = "reconciliator_buoni_export.db"

# ── Setup DB temporaneo ───────────────────────────────────────────────────────
if os.path.exists(DB_PATH):
    os.remove(DB_PATH)

SCHEMA = """
CREATE TABLE IF NOT EXISTS impianti (id INTEGER PRIMARY KEY AUTOINCREMENT, codice_pv INTEGER NOT NULL UNIQUE, nome TEXT, comune TEXT, indirizzo TEXT, alias_terminale TEXT, tipo_gestione TEXT, identificativo_contanti TEXT);
CREATE TABLE IF NOT EXISTS transazioni_fortech (id INTEGER PRIMARY KEY AUTOINCREMENT, codice_pv INTEGER NOT NULL, data TEXT NOT NULL, totale_contante NUMERIC DEFAULT 0, totale_pos NUMERIC DEFAULT 0, totale_buoni NUMERIC DEFAULT 0, totale_satispay NUMERIC DEFAULT 0, totale_petrolifere NUMERIC DEFAULT 0, UNIQUE(codice_pv, data));
CREATE TABLE IF NOT EXISTS transazioni_contanti (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, codice_pv INTEGER, importo NUMERIC, note_raw TEXT);
CREATE TABLE IF NOT EXISTS transazioni_pos (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, alias_terminale TEXT, importo NUMERIC, circuito TEXT);
CREATE TABLE IF NOT EXISTS transazioni_satispay (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, codice_pv INTEGER, importo NUMERIC);
CREATE TABLE IF NOT EXISTS transazioni_buoni (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, codice_pv INTEGER, importo NUMERIC, esercente TEXT);
CREATE TABLE IF NOT EXISTS transazioni_petrolifere (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT, codice_pv INTEGER, importo NUMERIC);
CREATE TABLE IF NOT EXISTS contanti_matching (id INTEGER PRIMARY KEY AUTOINCREMENT, codice_pv INTEGER, data TEXT, contanti_teorico NUMERIC, contanti_versato NUMERIC, differenza NUMERIC, stato TEXT, tipo_match TEXT, risolto BOOLEAN, verificato_da TEXT, data_verifica TEXT, note TEXT);
CREATE TABLE IF NOT EXISTS riconciliazione_risultati (id INTEGER PRIMARY KEY AUTOINCREMENT, codice_pv INTEGER, data TEXT, categoria TEXT, valore_teorico NUMERIC DEFAULT 0, valore_reale NUMERIC DEFAULT 0, differenza NUMERIC DEFAULT 0, stato TEXT, note TEXT, tipo_match TEXT, UNIQUE(codice_pv, data, categoria));
CREATE TABLE IF NOT EXISTS pos_alias (alias TEXT PRIMARY KEY, pv_code INTEGER);
CREATE TABLE IF NOT EXISTS config (chiave TEXT PRIMARY KEY, valore TEXT);
"""

raw_conn = sqlite3.connect(DB_PATH)
raw_conn.row_factory = sqlite3.Row
raw_conn.executescript(SCHEMA)
raw_conn.commit()

class MockConn:
    def __init__(self, c): self.conn = c
    def execute(self, *a, **k): return self.conn.execute(*a, **k)
    def executemany(self, q, p):
        if "ON CONFLICT" in q:
            q = q.split("ON CONFLICT")[0].replace("INSERT INTO", "INSERT OR REPLACE INTO")
        self.conn.executemany(q, p)
    def commit(self): self.conn.commit()
    def close(self): self.conn.close()
    def cursor(self): return self.conn.cursor()

conn = MockConn(raw_conn)

def my_get_connection():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return MockConn(c)

database.get_connection = my_get_connection

# ── Ingestione ────────────────────────────────────────────────────────────────
fortech_files = [f for f in os.listdir(EXCEL_DIR)
                 if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")]

from classifier import identify_file_type
from ingestion  import ingest_impianti

ingest_impianti(conn)

for fn in fortech_files:
    path = os.path.join(EXCEL_DIR, fn)
    try:
        tipo = identify_file_type(path).get("categoria", "")
    except Exception:
        tipo = ""
    if tipo == "FORTECH":
        print(f"  Fortech: {fn}")
        ingest_fortech(path, conn)
    elif tipo == "BUONI":
        print(f"  Buoni:   {fn}")
        ingest_buoni(path, conn)

# ── Riconciliazione ───────────────────────────────────────────────────────────
print("Riconciliazione in corso...")
reconcile(conn)

# ── Export Excel ──────────────────────────────────────────────────────────────

# Foglio 1: Riconciliazione buoni
df_ric = pd.read_sql_query('''
    SELECT r.data              AS "Data",
           r.codice_pv        AS "Cod. PV",
           COALESCE(i.nome, 'N/D') AS "Impianto",
           r.valore_teorico   AS "Fortech (€)",
           r.valore_reale     AS "Reale (€)",
           r.differenza       AS "Diff (€)",
           r.stato            AS "Stato",
           r.note             AS "Note"
    FROM riconciliazione_risultati r
    LEFT JOIN impianti i ON r.codice_pv = i.codice_pv
    WHERE r.categoria = 'buoni'
    ORDER BY r.data, i.nome
''', raw_conn)

# Foglio 2: Transazioni buoni grezze
df_raw = pd.read_sql_query('''
    SELECT b.data             AS "Data",
           b.codice_pv        AS "Cod. PV",
           COALESCE(i.nome, 'N/D') AS "Impianto",
           b.importo          AS "Importo (€)",
           b.esercente        AS "Esercente (raw)"
    FROM transazioni_buoni b
    LEFT JOIN impianti i ON b.codice_pv = i.codice_pv
    ORDER BY b.data, b.codice_pv
''', raw_conn)

out = os.path.join(EXCEL_DIR, f"Riconciliazione_Buoni_{datetime.date.today().isoformat()}_v2.xlsx")

with pd.ExcelWriter(out, engine="openpyxl") as writer:
    df_ric.to_excel(writer, sheet_name="Riconciliazione", index=False)
    df_raw.to_excel(writer, sheet_name="Transazioni_raw",  index=False)

    # Autofit colonne
    for sheet_name, df in [("Riconciliazione", df_ric), ("Transazioni_raw", df_raw)]:
        ws = writer.sheets[sheet_name]
        for col_cells in ws.columns:
            max_len = max(len(str(c.value or "")) for c in col_cells)
            ws.column_dimensions[col_cells[0].column_letter].width = min(max_len + 2, 40)

print(f"\nFile salvato: {out}")
print(f"  Righe riconciliazione: {len(df_ric)}")
print(f"  Righe transazioni raw: {len(df_raw)}")

raw_conn.close()
os.remove(DB_PATH)
