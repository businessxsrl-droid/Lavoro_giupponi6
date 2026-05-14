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


def _delete_by_dates(conn, table: str, dates, aliases=None):
    """Cancella le righe della tabella per le date specificate.
    Se aliases è fornito (solo per transazioni_pos), filtra anche per alias_terminale
    così file diversi sulle stesse date non si sovrascrivono a vicenda.
    """
    dates = {d for d in dates if d and str(d) != "nan"}
    if not dates:
        return
    date_list = ", ".join(f"'{d}'" for d in sorted(dates))
    if aliases is not None:
        clean = {str(a) for a in aliases if a and str(a) not in ("None", "nan", "")}
        if clean:
            alias_list = ", ".join(f"'{a}'" for a in sorted(clean))
            conn.execute(f"DELETE FROM {table} WHERE data IN ({date_list}) AND alias_terminale IN ({alias_list})")
            return
    conn.execute(f"DELETE FROM {table} WHERE data IN ({date_list})")


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

    # Marca impianti senza servizio di riconciliazione
    _SENZA_SERVIZIO_PVS = [47831, 45874, 47832, 41118, 42840, 45818]
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
        count += 1
    except Exception:
        pass

    # Inserisci Codogno se non presente
    try:
        conn.execute('''
            INSERT INTO impianti (codice_pv, nome, comune, indirizzo, tipo_gestione)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(codice_pv) DO NOTHING
        ''', (43721, 'CODOGNO - Via Gorizia, 4', 'Codogno', 'Via Gorizia, 4', 'PRESIDIATO'))
        count += 1
    except Exception:
        pass

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

    # Formato alternativo: l'alias è già il codice_pv numerico (Ghislandi/Rovetta/Taleggio)
    alias_stripped = str(alias).strip()
    if alias_stripped.isdigit():
        pv_int = int(alias_stripped)
        if any(imp["pv"] == pv_int for imp in impianti):
            return pv_int

    kw = alias_stripped.upper().replace(" SELF", "").replace(" CORDLESS", "").strip()

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
        "ROVETTA": 42840, "BERGAMO": 45874, "GHISLANDI": 45874,
        "TALEGGIO": 49788, "FAMAGOSTA": 45818, "CODOGNO": 43721,
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
               r["buoni"], r["satispay"], r["petrolifere"],
               r.get("prove_erogazione", 0), r.get("clienti_fine_mese", 0), r.get("diversi", 0))
              for r in records]

    conn.executemany('''
        INSERT INTO transazioni_fortech
            (codice_pv, data, totale_contante, totale_pos, totale_buoni,
             totale_satispay, totale_petrolifere, prove_erogazione, clienti_fine_mese, diversi)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(codice_pv, data) DO UPDATE SET
            totale_contante    = EXCLUDED.totale_contante,
            totale_pos         = EXCLUDED.totale_pos,
            totale_buoni       = EXCLUDED.totale_buoni,
            totale_satispay    = EXCLUDED.totale_satispay,
            totale_petrolifere = EXCLUDED.totale_petrolifere,
            prove_erogazione   = EXCLUDED.prove_erogazione,
            clienti_fine_mese  = EXCLUDED.clienti_fine_mese,
            diversi            = EXCLUDED.diversi
    ''', params)
    count = len(params)

    conn.commit()
    if close:
        conn.close()
    print(f"  [OK] Fortech: {count} record da {os.path.basename(file_path)}")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  CARTE BANCARIE (POS / Numia)
# ═══════════════════════════════════════════════════════════════════════════════

def ingest_pos(file_path: str, conn=None) -> int:
    """Legge un file Carte Bancarie (Numia) e popola transazioni_pos."""
    close = conn is None
    if conn is None:
        conn = get_connection()

    # Trova la riga header cercando "Importo"/"Data e ora" (Numia) o "Importo Transazioni" (alt)
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
        # Formato alternativo Ghislandi/Rovetta/Taleggio
        if "importo transazioni" in vals and "data operazione" in vals:
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



    # Identifica colonne — formato standard Numia
    col_importo = col_data = col_alias = col_circuito = None
    # Formato alternativo Ghislandi/Rovetta/Taleggio (HTML Intesa)
    col_importo_alt = col_data_alt = col_circuito_alt = col_pv_alt = col_mittente_alt = None

    for c in df.columns:
        # Normalizza: rimuove newline, spazi extra, lowercase
        cl = str(c).strip().replace("\n", " ").replace("\r", "").lower()
        cl = " ".join(cl.split())  # collassa spazi multipli
        if cl == "importo":
            col_importo = c
        elif cl == "data e ora":
            col_data = c
        elif cl == "alias terminale":
            col_alias = c
        elif cl == "circuito":
            col_circuito = c
        elif cl == "importo transazioni":
            col_importo_alt = c
        elif cl == "data operazione":
            col_data_alt = c
        elif cl == "tipo carta":
            col_circuito_alt = c
        elif cl == "pv" and col_pv_alt is None:
            col_pv_alt = c
        elif cl == "mittente":
            col_mittente_alt = c

    print(f"  [DEBUG POS] Colonne: {[repr(str(c)) for c in df.columns]}")

    # Formato alternativo: Data operazione + Importo Transazioni (PV opzionale)
    # Gestisce file Ghislandi/Rovetta/Taleggio che non hanno colonna PV
    if col_importo_alt and col_data_alt:
        df["_data"]    = pd.to_datetime(df[col_data_alt], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
        df["_importo"] = pd.to_numeric(df[col_importo_alt], errors="coerce").fillna(0.0)
        df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

        valid_pvs = {r[0] for r in conn.execute("SELECT codice_pv FROM impianti").fetchall()}

        # Se non c'è colonna PV, prova a ricavare l'impianto dal nome del file
        impianti_list = _build_alias_to_pv(conn)
        pv_from_filename = _trova_pv_da_alias(os.path.basename(file_path), impianti_list)

        params = []
        for _, row in df.iterrows():
            codice_pv = None

            if col_pv_alt:
                pv_raw = str(row[col_pv_alt]).strip().lstrip("0")
                try:
                    codice_pv = int(pv_raw) if pv_raw.isdigit() else None
                except Exception:
                    codice_pv = None
                if codice_pv not in valid_pvs:
                    codice_pv = None

            # Fallback: PV dal nome file
            if codice_pv is None:
                codice_pv = pv_from_filename

            # Fallback: PV dalla colonna Mittente
            if codice_pv is None and col_mittente_alt:
                mittente = str(row[col_mittente_alt]).strip()
                codice_pv = _trova_pv_da_alias(mittente, impianti_list)

            # Usa codice_pv come alias per compatibilità con la colonna alias_terminale
            alias    = str(codice_pv) if codice_pv else ""
            circuito = str(row[col_circuito_alt]).strip() if col_circuito_alt else ""
            params.append((row["_data"], alias, row["_importo"], circuito))

        print(f"  [INFO] POS alt: pv_da_file={pv_from_filename}, col_pv={col_pv_alt}, col_mittente={col_mittente_alt}")
        _delete_by_dates(conn, "transazioni_pos", {p[0] for p in params}, aliases={p[1] for p in params})
        conn.executemany('''
            INSERT INTO transazioni_pos (data, alias_terminale, importo, circuito)
            VALUES (?, ?, ?, ?)
        ''', params)
        count = len(params)
        conn.commit()
        if close:
            conn.close()
        print(f"  [OK] POS/Carte (alt): {count} righe da {os.path.basename(file_path)}")
        return count

    # Formato standard
    if not col_importo or not col_data:
        print(f"  [!] Colonne mancanti in {os.path.basename(file_path)}")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
    df["_importo"] = pd.to_numeric(df[col_importo], errors="coerce").fillna(0.0)
    df = df[df["_importo"] != 0.0].dropna(subset=["_data"])

    params = []
    for _, row in df.iterrows():
        alias    = str(row[col_alias]).strip() if col_alias else ""
        circuito = str(row[col_circuito]).strip() if col_circuito else ""
        params.append((row["_data"], alias, row["_importo"], circuito))

    _delete_by_dates(conn, "transazioni_pos", {p[0] for p in params}, aliases={p[1] for p in params})
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

    col_importo = col_data = col_negozio = col_tipo = None
    for c in df.columns:
        cl = str(c).strip().lower()
        if cl == "importo totale":
            col_importo = c
        elif cl == "data transazione":
            col_data = c
        elif cl == "codice negozio":
            col_negozio = c
        elif cl == "tipo transazione":
            col_tipo = c

    # Filtra storni, rimborsi e transazioni rifiutate/annullate
    if col_tipo is not None:
        _TIPI_DA_ESCLUDERE = {"REFUND_TO_BUSINESS", "REFUND", "REJECTED", "CANCELED", "CANCELLED", "STORNO", "RIMBORSO"}
        mask = df[col_tipo].astype(str).str.strip().str.upper().isin(_TIPI_DA_ESCLUDERE)
        escluse = mask.sum()
        if escluse:
            print(f"  [i] Satispay: {escluse} righe escluse (storni/rimborsi/rifiutate)")
        df = df[~mask]

    if not col_importo or not col_data:
        print(f"  [!] Colonne mancanti Satispay in {os.path.basename(file_path)}")
        if close:
            conn.close()
        return 0

    df["_data"]    = pd.to_datetime(df[col_data], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
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

    _delete_by_dates(conn, "transazioni_satispay", {p[0] for p in params})
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

    col_importo = col_data = col_esercente = col_pv = None
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
        elif "punto" in cl and "vendita" in cl:
            col_pv = c

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
        pv_val    = str(row[col_pv]).strip() if col_pv else ""

        # Priorità 1: Colonna "Punto vendita" — legge gli ultimi 5 cifre (es. 0000046273 → 46273)
        if pv_val:
            val_clean = pv_val.strip()[-5:]
            if val_clean in pv_list:
                codice_pv = int(val_clean)
        
        # Priorità 2: Cerca il codice dentro "Esercente"
        if codice_pv is None and esercente:
            for pv in pv_list:
                if pv in esercente:
                    codice_pv = int(pv)
                    break
            
            # Fallback esercente digit
            if codice_pv is None and esercente.lstrip("0").isdigit():
                stripped = esercente.lstrip("0")
                if stripped in pv_list:
                    codice_pv = int(stripped)

        params.append((row["_data"], codice_pv, row["_importo"], esercente))

    _delete_by_dates(conn, "transazioni_buoni", {p[0] for p in params})
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
        df = _leggi_excel_multi_engine(file_path, header=header_row)
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

    df["_data"]    = pd.to_datetime(df[col_data], dayfirst=True, errors="coerce").dt.strftime("%Y-%m-%d")
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

    _delete_by_dates(conn, "transazioni_petrolifere", {p[0] for p in params})
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

    # Carica anagrafica impianti all'inizio
    ingest_impianti(conn)

    summary = {"files_found": len(files), "FORTECH": 0,
               "CARTE_BANCARIE": 0, "SATISPAY": 0, "BUONI": 0,
               "carte_petrolifere": 0, "ANAGRAFICA": 0, "SCONOSCIUTO": 0}

    handler = {
        "FORTECH":          ingest_fortech,
        "CARTE_BANCARIE":   ingest_pos,
        "SATISPAY":         ingest_satispay,
        "BUONI":            ingest_buoni,
        "carte_petrolifere": ingest_petrolifere,
    }

    for fp in files:
        fname = os.path.basename(fp)
        try:
            info = identify_file_type(fp)
            cat  = info["categoria"]
            print(f"  [{cat}] {fname} (conf: {info['confidenza']}%)")

            if cat in handler:
                try:
                    n = handler[cat](fp, conn)
                    summary[cat] = summary.get(cat, 0) + n
                except Exception as e:
                    print(f"  [ERR] Errore elaborando {fname}: {e}")
                    summary["SCONOSCIUTO"] = summary.get("SCONOSCIUTO", 0) + 1
            else:
                summary["SCONOSCIUTO"] = summary.get("SCONOSCIUTO", 0) + 1
        except Exception as e:
            print(f"  [ERR] Classificazione fallita per {fname}: {e}")
            summary["SCONOSCIUTO"] = summary.get("SCONOSCIUTO", 0) + 1

    conn.close()
    return summary
