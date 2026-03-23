"""
migrate_to_postgres.py
----------------------
Copia tutti i dati dal database sorgente (Supabase) a un PostgreSQL di destinazione
(Render, locale Docker, o qualsiasi altro).

Uso:
    python migrate_to_postgres.py --target "postgresql://user:pass@host:5432/dbname"

Se --target non viene specificato, usa DATABASE_URL dal .env locale.
"""

import os
import sys
import argparse
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ── DDL tabelle ────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS impianti (
    id SERIAL PRIMARY KEY,
    codice_pv INTEGER NOT NULL UNIQUE,
    nome TEXT,
    comune TEXT,
    indirizzo TEXT,
    alias_terminale TEXT,
    tipo_gestione TEXT DEFAULT 'PRESIDIATO'
);
CREATE TABLE IF NOT EXISTS transazioni_fortech (
    id SERIAL PRIMARY KEY,
    codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv),
    data TEXT NOT NULL,
    totale_contante NUMERIC DEFAULT 0,
    totale_pos NUMERIC DEFAULT 0,
    totale_buoni NUMERIC DEFAULT 0,
    totale_satispay NUMERIC DEFAULT 0,
    totale_petrolifere NUMERIC DEFAULT 0,
    UNIQUE(codice_pv, data)
);
CREATE TABLE IF NOT EXISTS transazioni_contanti (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    codice_pv INTEGER REFERENCES impianti(codice_pv),
    importo NUMERIC,
    note_raw TEXT
);
CREATE TABLE IF NOT EXISTS transazioni_pos (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    alias_terminale TEXT,
    importo NUMERIC,
    circuito TEXT
);
CREATE TABLE IF NOT EXISTS transazioni_satispay (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    codice_pv INTEGER REFERENCES impianti(codice_pv),
    importo NUMERIC
);
CREATE TABLE IF NOT EXISTS transazioni_buoni (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    codice_pv INTEGER REFERENCES impianti(codice_pv),
    importo NUMERIC,
    esercente TEXT
);
CREATE TABLE IF NOT EXISTS transazioni_petrolifere (
    id SERIAL PRIMARY KEY,
    data TEXT NOT NULL,
    codice_pv INTEGER REFERENCES impianti(codice_pv),
    importo NUMERIC
);
CREATE TABLE IF NOT EXISTS riconciliazione_risultati (
    id SERIAL PRIMARY KEY,
    codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv),
    data TEXT NOT NULL,
    categoria TEXT NOT NULL,
    valore_teorico NUMERIC DEFAULT 0,
    valore_reale NUMERIC DEFAULT 0,
    differenza NUMERIC DEFAULT 0,
    stato TEXT DEFAULT 'IN_ATTESA',
    tipo_match TEXT DEFAULT 'nessuno',
    note TEXT,
    UNIQUE(codice_pv, data, categoria)
);
CREATE TABLE IF NOT EXISTS contanti_matching (
    id SERIAL PRIMARY KEY,
    codice_pv INTEGER NOT NULL REFERENCES impianti(codice_pv),
    data TEXT NOT NULL,
    contanti_teorico NUMERIC DEFAULT 0,
    contanti_versato NUMERIC DEFAULT 0,
    differenza NUMERIC DEFAULT 0,
    stato TEXT DEFAULT 'IN_ATTESA',
    tipo_match TEXT DEFAULT 'nessuno',
    risolto BOOLEAN DEFAULT FALSE,
    verificato_da TEXT,
    data_verifica TEXT,
    note TEXT,
    UNIQUE(codice_pv, data)
);
CREATE TABLE IF NOT EXISTS config (chiave TEXT PRIMARY KEY, valore TEXT);
CREATE TABLE IF NOT EXISTS users (username TEXT PRIMARY KEY, password_hash TEXT NOT NULL);
"""

# Tabelle in ordine di dipendenza (impianti prima, poi le tabelle che la referenziano)
TABLES = [
    ("impianti",                   "codice_pv"),
    ("transazioni_fortech",        "codice_pv, data"),
    ("transazioni_contanti",       None),
    ("transazioni_pos",            None),
    ("transazioni_satispay",       None),
    ("transazioni_buoni",          None),
    ("transazioni_petrolifere",    None),
    ("riconciliazione_risultati",  "codice_pv, data, categoria"),
    ("contanti_matching",          "codice_pv, data"),
    ("config",                     "chiave"),
    ("users",                      "username"),
]


def connect(url):
    conn = psycopg2.connect(url, cursor_factory=RealDictCursor)
    conn.autocommit = True
    return conn


def migrate(src_url: str, dst_url: str):
    print(f"\n{'='*60}")
    print(f"SORGENTE : {src_url[:60]}...")
    print(f"DESTINAZIONE: {dst_url[:60]}...")
    print('='*60)

    src = connect(src_url)
    dst = connect(dst_url)

    # Crea schema sulla destinazione
    print("\n[1/3] Creazione schema sul database di destinazione...")
    with dst.cursor() as cur:
        cur.execute(DDL)
    print("      Schema creato.")

    # Copia dati tabella per tabella
    print("\n[2/3] Copia dati...")
    for table, _ in TABLES:
        with src.cursor() as cur:
            cur.execute(f"SELECT * FROM {table}")
            rows = cur.fetchall()

        if not rows:
            print(f"      {table:<35} — vuota, skip")
            continue

        cols = list(rows[0].keys())
        # Esclude 'id' dalle colonne di insert (auto-generated da SERIAL)
        insert_cols = [c for c in cols if c != "id"]
        placeholders = ", ".join(["%s"] * len(insert_cols))
        col_list = ", ".join(insert_cols)

        insert_sql = (
            f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
            f"ON CONFLICT DO NOTHING"
        )

        values = [[row[c] for c in insert_cols] for row in rows]
        with dst.cursor() as cur:
            cur.executemany(insert_sql, values)

        print(f"      {table:<35} — {len(rows)} righe copiate")

    # Ripristina le sequence SERIAL
    print("\n[3/3] Aggiornamento sequence SERIAL...")
    serial_tables = ["impianti", "transazioni_fortech", "transazioni_contanti",
                     "transazioni_pos", "transazioni_satispay", "transazioni_buoni",
                     "transazioni_petrolifere", "riconciliazione_risultati",
                     "contanti_matching"]
    with dst.cursor() as cur:
        for t in serial_tables:
            cur.execute(f"SELECT setval(pg_get_serial_sequence('{t}', 'id'), COALESCE(MAX(id), 1)) FROM {t}")
    print("      Sequence aggiornate.")

    src.close()
    dst.close()
    print("\n✓ Migrazione completata con successo!\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Migra dati da Supabase a PostgreSQL")
    parser.add_argument("--source", help="URL sorgente (default: DATABASE_URL da .env)")
    parser.add_argument("--target", required=True, help="URL destinazione PostgreSQL")
    args = parser.parse_args()

    src_url = args.source or os.getenv("DATABASE_URL")
    if not src_url:
        print("ERRORE: specifica --source oppure definisci DATABASE_URL nel .env")
        sys.exit(1)

    migrate(src_url, args.target)
