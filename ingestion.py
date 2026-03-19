"""
Ingestion module — Lavoro Giupponi6
Legge i file Excel di ogni tipo e li carica nel database SQLite.
Ogni funzione ingest_* accetta il path del file e una connessione DB aperta.
"""
import os
import json
import pandas as pd
from datetime import datetime

from database import get_connection, init_db
from classifier import identify_file_type, get_fortech_records, _carica_excel

def _leggi_excel_multi_engine(file_path: str, **kwargs):
    """Prova più engine pandas per leggere file .xls/.xlsx con formato ambiguo, incluso HTML fallback."""
    try:
        with open(file_path, 'rb') as f:
            header_bytes = f.read(200).decode('utf-8', errors='ignore').lstrip()
        is_html = header_bytes.lower().startswith('<html') or header_bytes.lower().startswith('<!doctype')
    except Exception:
        is_html = False

    if is_html:
        html_kwargs = {k: v for k, v in kwargs.items() if k in ('header', 'skiprows', 'encoding')}
        for parser in ['lxml', 'html.parser', 'html5lib']:
            try:
                dfs = pd.read_html(file_path, flavor=parser, **html_kwargs)
                if dfs:
                    df = max(dfs, key=len)
                    return df.dropna(how='all').reset_index(drop=True)
            except Exception:
                continue
        return None

    ext = os.path.splitext(file_path)[1].lower()
    engines = ["xlrd", "openpyxl"] if ext == ".xls" else ["openpyxl", "xlrd"]
    for engine in [None] + engines:
        try:
            read_kwargs = dict(kwargs)
            if engine:
                read_kwargs["engine"] = engine
            return pd.read_excel(file_path, **read_kwargs)
        except Exception:
            continue
            
    # Ultimo fallback
    html_kwargs = {k: v for k, v in kwargs.items() if k in ('header', 'skiprows', 'encoding')}
    for parser in ['lxml', 'html.parser', 'html5lib']:
        try:
            dfs = pd.read_html(file_path, flavor=parser, **html_kwargs)
            if dfs:
                df = max(dfs, key=len)
                return df.dropna(how='all').reset_index(drop=True)
        except Exception:
            continue
            
    return None


# ── Percorsi di riferimento ────────────────────────────────────────────────────
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))
# Su Render i dati ora stanno in una sottocartella locale per essere pushabili su Git
ELENCO_IMPIANTI_PATH = os.path.join(ROOT_DIR, "data", "db_schema", "INPUT", "Elenco impianti.xlsx")
ALIAS_MAPPING_PATH   = os.path.join(ROOT_DIR, "alias_mapping.json")


# ═══════════════════════════════════════════════════════════════════════════════
#  ANAGRAFICA IMPIANTI
# ═══════════════════════════════════════════════════════════════════════════════

def _load_impianti_from_xlsx(path: str) -> list[dict]:
    """Legge Elenco impianti.xlsx e ritorna lista di dict con i campi necessari."""
    if not os.path.exists(path):
        print(f"  [!] Elenco impianti non trovato: {path}")
        return []
    try:
        df = pd.read_excel(path)
        records = []
        for _, row in df.iterrows():
            pv = str(row.get("COD. PV", "")).strip()
            if not pv or pv == "nan":
                continue
            records.append({
                "codice_pv":       int(float(pv)),
                "nome":            f"{row.get('COMUNE', '')} – {row.get('INDIRIZZO', '')}".strip(" –"),
                "comune":          str(row.get("COMUNE", "")).strip(),
                "indirizzo":       str(row.get("INDIRIZZO", "")).strip(),
                "ident_contanti":  str(row.get("IDENTIFICATIVO MOVIMENTO DI ACCREDITO", "")).strip(),
                "tipo_gestione":   str(row.get("TIPO GESTIONE", "PRESIDIATO")).strip(),
            })
        return records
    except Exception as e:
        print(f"  [ERR] Lettura Elenco impianti: {e}")
        return []


def _load_alias_mapping(path: str) -> list[dict]:
    """Legge alias_mapping.json se presente."""
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("impianti", [])
    except Exception:
        return []


def ingest_impianti(conn=None) -> int:
    """
    Popola la tabella impianti da Elenco impianti.xlsx + alias_mapping.json.
    Ritorna il numero di impianti inseriti/aggiornati.
    """
    close = conn is None
    if conn is None:
        conn = get_connection()

    impianti = _load_impianti_from_xlsx(ELENCO_IMPIANTI_PATH)
    if not impianti:
        # Fallback: usa alias_mapping.json (ha solo comune+indirizzo)
        raw = _load_alias_mapping(ALIAS_MAPPING_PATH)
        for r in raw:
            pv = r.get("COD. PV")
            if not pv:
                continue
            impianti.append({
                "codice_pv":      int(pv),
                "nome":           f"{r.get('COMUNE', '')} – {r.get('INDIRIZZO', '')}".strip(" –"),
                "comune":         str(r.get("COMUNE", "")).strip(),
                "indirizzo":      str(r.get("INDIRIZZO", "")).strip(),
                "ident_contanti": "",
                "tipo_gestione":  "PRESIDIATO",
            })

    count = 0
    for imp in impianti:
        nome = imp.get("nome") or f"{imp['comune']} {imp['indirizzo']}".strip()
        conn.execute('''
            INSERT INTO impianti (codice_pv, nome, comune, indirizzo, tipo_gestione)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(codice_pv) DO UPDATE SET
                nome          = excluded.nome,
                comune        = excluded.comune,
                indirizzo     = excluded.indirizzo,
                tipo_gestione = excluded.tipo_gestione
        ''', (imp["codice_pv"], nome, imp["comune"], imp["indirizzo"], imp.get("tipo_gestione", "PRESIDIATO")))
        count += 1

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] Impianti caricati: {count}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  HELPER – Mappa identificativi/alias → codice_pv
# ═══════════════════════════════════════════════════════════════════════════════

def _build_ident_map(conn) -> dict[str, int]:
    """Costruisce una mappa {identificativo_contanti → codice_pv} da Elenco impianti."""
    impianti = _load_impianti_from_xlsx(ELENCO_IMPIANTI_PATH)
    mappa = {}
    for imp in impianti:
        ident = imp.get("ident_contanti", "").strip()
        if ident and ident != "nan":
            mappa[ident] = imp["codice_pv"]
    return mappa


def _build_alias_to_pv(conn) -> list[dict]:
    """Legge impianti dal DB e ritorna lista per alias matching (carte bancarie)."""
    rows = conn.execute("SELECT codice_pv, comune, indirizzo FROM impianti").fetchall()
    return [{"pv": r["codice_pv"], "comune": (r["comune"] or "").upper(),
             "indirizzo": (r["indirizzo"] or "").upper()} for r in rows]


def _trova_pv_da_alias(alias: str, impianti: list[dict]) -> int | None:
    """Ricerca fuzzy alias terminale POS → codice_pv (porta logica di calcolo_carte_bancarie.py)."""
    if not alias or str(alias) == "nan":
        return None
    kw = str(alias).upper().replace(" SELF", "").replace(" CORDLESS", "").strip()

    for imp in impianti:
        if kw == imp["comune"]:
            return imp["pv"]
    for imp in impianti:
        if kw in imp["comune"] or imp["comune"] in kw or kw in imp["indirizzo"]:
            return imp["pv"]

    # Hardcode di emergenza per alias noti
    HARDCODES = {
        "SEGGIANO": 43699, "BELFIORE": 47831, "GIUSEPPINA": 43958,
        "BEATRICE": 48979, "REPUBBLICA": 43809, "MANTEGNA": 45531,
        "MALEO": 46273, "CREMONA": 48765, "MONTODINE": 43695,
        "MARMIROLO": 47832, "ROMANO": 43596, "SELVINO": 40297,
        "ROVETTA": 42840, "BERGAMO": 45874, "TALEGGIO": 41010,
        "PIOLTELLO": 43699,  # nota: ambiguo; prevale seggiano
    }
    for key, pv in HARDCODES.items():
        if key in kw:
            return pv
    return None


# ═══════════════════════════════════════════════════════════════════════════════
#  FORTECH
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_fortech(file_path: str, conn=None) -> int:
    """Legge un file Fortech e aggiorna transazioni_fortech."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    records = get_fortech_records(file_path)
    if not records:
        print(f"  [!] Nessun record Fortech in {os.path.basename(file_path)}")
        if close:
            conn.close()
        return 0

    count = 0
    params = [(r["codice_pv"], r["data"], r["contanti"], r["pos"],
               r["buoni"], r["satispay"], r["petrolifere"]) for r in records]
    
    conn.executemany('''
        INSERT INTO transazioni_fortech
            (codice_pv, data, totale_contante, totale_pos, totale_buoni,
             totale_satispay, totale_petrolifere)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(codice_pv, data) DO UPDATE SET
            totale_contante    = transazioni_fortech.totale_contante    + EXCLUDED.totale_contante,
            totale_pos         = transazioni_fortech.totale_pos         + EXCLUDED.totale_pos,
            totale_buoni       = transazioni_fortech.totale_buoni       + EXCLUDED.totale_buoni,
            totale_satispay    = transazioni_fortech.totale_satispay    + EXCLUDED.totale_satispay,
            totale_petrolifere = transazioni_fortech.totale_petrolifere + EXCLUDED.totale_petrolifere
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] Fortech: {count} record da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  CONTANTI (AS400 / Doc Finance)
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_contanti(file_path: str, conn=None) -> int:
    """Legge un file Contanti (DocFinance/AS400) e popola transazioni_contanti."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    df = _carica_excel(file_path)
    if df is None or df.empty:
        if close:
            conn.close()
        return 0

    # Normalizza nomi colonne
    col_data = col_importo = None
    for col in df.columns:
        cl = str(col).strip().lower()
        if cl == "dt operaz.":
            col_data = col
        elif cl == "importo":
            col_importo = col

    if not col_data or not col_importo:
        print(f"  [!] Colonne mancanti in {os.path.basename(file_path)}: Dt Operaz. o Importo")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_importo"] = pd.to_numeric(df[col_importo], errors="coerce").fillna(0.0)
    df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

    ident_map = _build_ident_map(conn)

    # Catturiamo tutte le colonne (escluse data/importo) per la ricerca keywords.
    # Questo è più robusto rispetto al controllo del dtype 'object'.
    exclude_cols = {col_data, col_importo, "_data", "_importo"}
    text_cols = [c for c in df.columns if c not in exclude_cols]

    count = 0
    params = []
    for _, row in df.iterrows():
        # Concatena tutti i valori delle colonne di testo/metadati
        all_text_parts = []
        for c in text_cols:
            val = str(row[c]).strip()
            if val and val.lower() != "nan":
                all_text_parts.append(val)
        
        all_text = " ".join(all_text_parts)
        all_text_upper = all_text.upper()

        codice_pv = None
        for ident, pv in ident_map.items():
            if ident and ident.upper() in all_text_upper:
                codice_pv = pv
                break
        
        # Fallback: ricerca hardcoded se il nome comune è presente nel testo
        if codice_pv is None:
            # REPUBBLICA (43809), MALEO (46273)
            if "MALEO" in all_text_upper: codice_pv = 46273
            elif "REPUBBLICA" in all_text_upper or "43809" in all_text_upper: codice_pv = 43809

        params.append((row["_data"], codice_pv, row["_importo"], all_text[:1000]))

    conn.executemany('''
        INSERT INTO transazioni_contanti (data, codice_pv, importo, note_raw)
        VALUES (?, ?, ?, ?)
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    trovati = sum(1 for _, r in df.iterrows() if r.get("_data"))
    print(f"  [OK] Contanti: {count} righe da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  CARTE BANCARIE (POS / Numia)
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_pos(file_path: str, conn=None) -> int:
    """Legge un file Carte Bancarie (Numia) e popola transazioni_pos."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    # Trova la riga header cercando "Importo" e "Data e ora"
    try:
        df_raw = _leggi_excel_multi_engine(file_path, header=None, nrows=15)
    except Exception as e:
        print(f"  [!] Impossibile leggere {os.path.basename(file_path)}: {e}")
        if close:
            conn.close()
        return 0
    if df_raw is None:
        if close:
            conn.close()
        return 0
    header_row = None
    for i, row in df_raw.iterrows():
        vals = {str(v).strip().lower() for v in row.values if pd.notna(v)}
        if "importo" in vals and "data e ora" in vals:
            header_row = i
            break

    if header_row is not None:
        try:
            df = _leggi_excel_multi_engine(file_path, header=header_row)
        except Exception as e:
            print(f"  [!] Lettura con header fallita: {e}")
            df = None
    else:
        df = _carica_excel(file_path)
    if df is None:
        if close:
            conn.close()
        return 0



    # Identifica colonne
    col_importo = col_data = col_alias = col_circuito = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == "importo":
            col_importo = c
        elif cl == "data e ora":
            col_data = c
        elif cl == "alias terminale":
            col_alias = c
        elif cl == "circuito":
            col_circuito = c

    if not col_importo or not col_data:
        print(f"  [!] Colonne mancanti in {os.path.basename(file_path)}")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_importo"] = pd.to_numeric(df[col_importo], errors="coerce").fillna(0.0)
    df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

    params = []
    for _, row in df.iterrows():
        alias    = str(row[col_alias]).strip() if col_alias else ""
        circuito = str(row[col_circuito]).strip() if col_circuito else ""
        params.append((row["_data"], alias, row["_importo"], circuito))

    conn.executemany('''
        INSERT INTO transazioni_pos (data, alias_terminale, importo, circuito)
        VALUES (?, ?, ?, ?)
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] POS/Carte: {count} righe da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  SATISPAY
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_satispay(file_path: str, conn=None) -> int:
    """Legge un file Satispay e popola transazioni_satispay."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    df = _carica_excel(file_path)
    if df is None or df.empty:
        if close:
            conn.close()
        return 0

    col_importo = col_data = col_negozio = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == "importo totale":
            col_importo = c
        elif cl == "data transazione":
            col_data = c
        elif cl == "codice negozio":
            col_negozio = c

    if not col_importo or not col_data:
        print(f"  [!] Colonne mancanti Satispay in {os.path.basename(file_path)}")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_importo"] = pd.to_numeric(df[col_importo], errors="coerce").fillna(0.0)
    df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

    # Lista codici PV dal DB
    pv_list = [str(r["codice_pv"]) for r in conn.execute("SELECT codice_pv FROM impianti").fetchall()]

    params = []
    for _, row in df.iterrows():
        codice_pv = None
        if col_negozio:
            negozio_str = str(row[col_negozio]).strip()
            for pv in pv_list:
                if pv in negozio_str:
                    codice_pv = int(pv)
                    break
        params.append((row["_data"], codice_pv, row["_importo"]))

    conn.executemany('''
        INSERT INTO transazioni_satispay (data, codice_pv, importo)
        VALUES (?, ?, ?)
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] Satispay: {count} righe da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  BUONI / VOUCHER (iP Portal)
# ═══════════════════════════════════════════════════════════════════════════════

def _pulisci_df_buoni(df: pd.DataFrame) -> pd.DataFrame:
    """Rimuove righe-titolo e imposta gli header corretti per i file Buoni HTML."""
    if df is None or df.empty:
        return df
    if all(isinstance(c, int) for c in df.columns):
        for i in range(min(5, len(df))):
            vals = [str(v).strip() for v in df.iloc[i].values if pd.notna(v) and str(v).strip()]
            joined = " ".join(vals).lower()
            if any(kw in joined for kw in ["importo", "prodotto", "codice cliente"]):
                df.columns = [str(v).strip() if pd.notna(v) else f"Col_{j}"
                              for j, v in enumerate(df.iloc[i].values)]
                return df.iloc[i + 1:].reset_index(drop=True)
    return df


def ingest_buoni(file_path: str, conn=None) -> int:
    """Legge un file Buoni/Voucher (iP Portal) e popola transazioni_buoni."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    df = _carica_excel(file_path)
    if df is None or df.empty:
        if close:
            conn.close()
        return 0
    df = _pulisci_df_buoni(df)

    col_importo = col_data = col_esercente = None
    for c in df.columns:
        cl = str(c).replace(" ", "").lower()
        if cl == "importo" and not col_importo:
            col_importo = c
        elif "data" in cl and "operazione" in cl:
            col_data = c
        elif "data" in cl and "documento" in cl and not col_data:
            col_data = c
        elif cl == "esercente":
            col_esercente = c

    if not col_importo or not col_data:
        print(f"  [!] Colonne mancanti Buoni in {os.path.basename(file_path)}")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_importo"] = pd.to_numeric(df[col_importo], errors="coerce").fillna(0.0)
    df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

    pv_list = [str(r["codice_pv"]) for r in conn.execute("SELECT codice_pv FROM impianti").fetchall()]

    params = []
    for _, row in df.iterrows():
        codice_pv = None
        esercente = str(row[col_esercente]).strip() if col_esercente else ""
        for pv in pv_list:
            if pv in esercente:
                codice_pv = int(pv)
                break
        # Fallback: stripping zeri dall'esercente
        if codice_pv is None and esercente and esercente.lstrip("0").isdigit():
            stripped = esercente.lstrip("0")
            for pv in pv_list:
                if stripped == pv:
                    codice_pv = int(pv)
                    break

        params.append((row["_data"], codice_pv, row["_importo"], esercente))

    conn.executemany('''
        INSERT INTO transazioni_buoni (data, codice_pv, importo, esercente)
        VALUES (?, ?, ?, ?)
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] Buoni: {count} righe da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  CARTE PETROLIFERE (DKV / UTA / Maxima)
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_petrolifere(file_path: str, conn=None) -> int:
    """Legge un file Carte Petrolifere e popola transazioni_petrolifere."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    # Trova la riga header cercando 'importo' tra le prime 8 righe
    try:
        df_raw = _leggi_excel_multi_engine(file_path, header=None, nrows=8)
    except Exception as e:
        print(f"  [!] Impossibile leggere {os.path.basename(file_path)}: {e}")
        if close:
            conn.close()
        return 0
    if df_raw is None:
        if close:
            conn.close()
        return 0
    header_row = 0
    for i, row in df_raw.iterrows():
        vals = {str(v).strip().replace("\n", "").lower() for v in row.values if pd.notna(v)}
        if "importo" in vals:
            header_row = i
            break

    try:
        df = pd.read_excel(file_path, header=header_row)
    except Exception:
        df = None

    if df is None or df.empty:
        if close:
            conn.close()
        return 0

    # Normalizza i nomi delle colonne (rimuovi newline, lowercase per confronto)
    clean_cols = {c: str(c).strip().replace("\n", "").replace(" ", "").lower() for c in df.columns}

    col_importo = col_data = col_pv = col_segno = None
    for c, cl in clean_cols.items():
        if cl == "importo":
            col_importo = c
        elif cl in ("dataoperazione", "data"):
            col_data = c
        elif cl == "pv":
            col_pv = c
        elif cl == "segno":
            col_segno = c

    if not col_importo or not col_data or not col_pv:
        print(f"  [!] Colonne mancanti Petrolifere in {os.path.basename(file_path)}: trovate={list(clean_cols.values())}")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], errors="coerce").dt.strftime("%Y-%m-%d")
    df["_importo"] = pd.to_numeric(df[col_importo], errors="coerce").fillna(0.0)
    df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

    # Carica i PV validi per evitare errori di foreign key
    valid_pvs = {r[0] for r in conn.execute("SELECT codice_pv FROM impianti").fetchall()}

    params = []
    for _, row in df.iterrows():
        importo = row["_importo"]
        if col_segno:
            segno = str(row[col_segno]).strip().upper()
            if segno in ("-", "S", "STORNO") and importo > 0:
                importo = -importo

        pv_raw = str(row[col_pv]).strip().lstrip("0")
        try:
            codice_pv = int(pv_raw) if pv_raw.isdigit() else None
        except Exception:
            codice_pv = None

        if codice_pv not in valid_pvs:
            codice_pv = None

        params.append((row["_data"], codice_pv, importo))

    conn.executemany('''
        INSERT INTO transazioni_petrolifere (data, codice_pv, importo)
        VALUES (?, ?, ?)
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] Petrolifere: {count} righe da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  ORCHESTRATORE — Elabora una cartella di file
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_folder(folder: str) -> dict:
    """
    Scansiona la cartella, classifica ogni file ed esegue l'ingestion corretta.
    Ritorna un riepilogo con il numero di righe inserite per tipo.
    """
    files = [
        os.path.join(folder, f)
        for f in os.listdir(folder)
        if f.lower().endswith((".xlsx", ".xls")) and not f.startswith("~$")
    ]

    if not files:
        return {"files_found": 0}

    conn = get_connection()

    # Svuota le tabelle reali (ma non fortech) per reimportare da zero
    for tbl in ("transazioni_contanti", "transazioni_pos", "transazioni_satispay",
                "transazioni_buoni", "transazioni_petrolifere", "transazioni_fortech"):
        conn.execute(f"DELETE FROM {tbl} WHERE TRUE")
    conn.commit()

    # Carica anagrafica impianti all'inizio
    ingest_impianti(conn)

    summary = {"files_found": len(files), "FORTECH": 0, "CONTANTI": 0,
               "CARTE_BANCARIE": 0, "SATISPAY": 0, "BUONI": 0,
               "carte_petrolifere": 0, "ANAGRAFICA": 0, "SCONOSCIUTO": 0}

    handler = {
        "FORTECH":          ingest_fortech,
        "CONTANTI":         ingest_contanti,
        "CARTE_BANCARIE":   ingest_pos,
        "SATISPAY":         ingest_satispay,
        "BUONI":            ingest_buoni,
        "carte_petrolifere": ingest_petrolifere,
    }

    for fp in files:
        info = identify_file_type(fp)
        cat  = info["categoria"]
        fname = os.path.basename(fp)
        print(f"  [{cat}] {fname} (conf: {info['confidenza']}%)")

        if cat in handler:
            n = handler[cat](fp, conn)
            summary[cat] = summary.get(cat, 0) + n
        else:
            summary["SCONOSCIUTO"] = summary.get("SCONOSCIUTO", 0) + 1

    conn.close()
    return summary
