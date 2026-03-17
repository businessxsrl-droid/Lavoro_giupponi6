"""
Classificatore file Excel — Lavoro Giupponi6
Port migliorato di Step1_riconoscimento_excel/riconoscimento_excel.py.
Identifica il tipo di file in base al matching degli header con le ground-truth note.
"""
import pandas as pd
import os

# ── Ground Truth Header per categoria ────────────────────────────────────────

GROUND_TRUTH: dict[str, list[str]] = {
    "FORTECH": [
        "CodicePV", "DataContabile", "DataInizio", "DataFine", "StatoGiornata",
        "BANCOMAT GESTORE", "CARTA CREDITO GESTORE", "CONTANTI", "CODICERESTO",
        "DKV", "BUONI", "CLIENTI CON FATTURA FINE MESE", "CARTA CREDITO GENERICA",
        "PAGOBANCOMAT", "MANCATO EROGATO", "CARTAMAXIMA", "UTA", "CARTAPETROLIFERA",
        "AMEX", "TBS", "PAGAMENTIINNOVATIVI",
    ],
    "CONTANTI": [
        "Gruppo", "Azienda", "Banca", "Rbn", "Desc. RBN", "Nr Conto Corr.",
        "Dt Operaz.", "Importo", "Divisa", "Dt Valuta", "Saldo Liquido",
        "Saldo Liquido Calc.", "Saldo Contabile", "Saldo Contabile Calc.",
        "Caus. Rendic.", "Descr. Caus. ABI", "Assegno / Pratica", "Nr Rif. Banca",
        "Nr Rif. Distinta", "Note", "Anno", "Nr movim.", "Id Operaz.",
        "Caus. Banca", "Stato", "Dt Supporto", "Nr Supporto",
    ],
    "SATISPAY": [
        "id transazione", "data transazione", "negozio", "codice negozio",
        "importo totale", "totale commissioni", "tipo transazione",
        "codice transazione", "id gruppo",
    ],
    "BUONI": [
        "Datadocumento", "Codice cliente", "Ragione socialecliente",
        "Numero documento", "Numero documentoriferimento", "CodiceRete",
        "Data registrazionedocumento", "Importo totale", "Data operazione",
        "Ora operazione", "Terminale", "Esercente", "Descrizione esercente",
        "Pan", "Serial number", "Importo", "Quantita", "Prodotto",
        "Prezzo unit.", "Punto vendita", "Valuta", "Auth code",
    ],
    "CARTE_BANCARIE": [
        "Data e ora", "Codice autorizzazione", "Numero carta", "Importo",
        "Circuito", "Tipo transazione", "Stato operazione",
        "Importo in valuta originale", "Valuta originale", "Importo Cashback",
        "Punto vendita", "ID Punto vendita", "MID", "ID Terminale / TML",
        "Alias Terminale", "ID Transazione", "Codice ordine",
    ],
    "carte_petrolifere": [
        "Gestore", "PV", "Dataoperazione", "Oraoperazione", "Circuito",
        "Cod. Prod.", "Prodotto", "RiferimentoScontrino", "Quantità",
        "Prezzo", "Importo", "Segno", "Numero Fattura", "Data Fattura",
        "dkv", "uta", "card",
    ],
    "ANAGRAFICA": [
        "COD. PV", "COMUNE", "INDIRIZZO",
        "IDENTIFICATIVO MOVIMENTO DI ACCREDITO",
    ],
}

# Pre-calcola versione clean (senza spazi, minuscolo) una volta sola
_GT_CLEAN: dict[str, list[str]] = {
    cat: [h.replace(" ", "").lower() for h in heads]
    for cat, heads in GROUND_TRUTH.items()
}


# ── Caricamento robusto file Excel/HTML ──────────────────────────────────────

def _carica_excel(file_path: str, **kwargs) -> pd.DataFrame | None:
    """Carica un file Excel dando priorità al foglio 'Incassi' se presente."""
    try:
        with pd.ExcelFile(file_path) as xls:
            target = 0
            for s in xls.sheet_names:
                if s.strip().lower() == 'incassi':
                    target = s
                    break
            return pd.read_excel(xls, sheet_name=target, **kwargs)
    except Exception:
        try:
            dfs = pd.read_html(file_path)
            if dfs:
                df = max(dfs, key=len)
                return df.dropna(how='all').reset_index(drop=True)
        except Exception:
            pass
    return None


# ── Identificazione tipo file ────────────────────────────────────────────────

def identify_file_type(file_path: str) -> dict:
    """
    Identifica il tipo di file Excel confrontando gli header con le ground-truth.
    Ritorna dict con: categoria, confidenza (0-100), ragione.
    """
    try:
        df_head = _carica_excel(file_path, header=None, nrows=12)
        if df_head is None or df_head.empty:
            return {"categoria": "SCONOSCIUTO", "confidenza": 0, "ragione": "File non leggibile"}

        best_cat   = "SCONOSCIUTO"
        best_conf  = 0.0
        best_reason = ""

        for idx, row in df_head.iterrows():
            row_vals = [
                str(v).replace(" ", "").lower()
                for v in row.values
                if pd.notna(v) and str(v).strip()
            ]
            if not row_vals:
                continue

            for category, headers_clean in _GT_CLEAN.items():
                matches = sum(1 for h in headers_clean if h in row_vals)
                if matches == 0:
                    continue

                total = len(headers_clean)
                conf  = (matches / total) * 100.0

                # Boost deterministici
                if matches >= 5:
                    conf = max(conf, 85.0)
                if matches >= 10:
                    conf = 100.0
                if category in ("SATISPAY", "carte_petrolifere", "ANAGRAFICA") and matches >= 3:
                    conf = 100.0

                if conf > best_conf:
                    best_conf   = conf
                    best_cat    = category
                    best_reason = f"Trovati {matches}/{total} match su riga {idx}"

        if best_conf < 15:
            return {"categoria": "SCONOSCIUTO", "confidenza": 0, "ragione": "Nessun match significativo"}

        return {
            "categoria":  best_cat,
            "confidenza": round(min(best_conf, 100.0), 2),
            "ragione":    best_reason,
        }
    except Exception as e:
        return {"categoria": "ERRORE", "confidenza": 0, "ragione": str(e)}


# ── Lettura totali Fortech ────────────────────────────────────────────────────

_FORTECH_MAPPING = {
    "pos":          ["BANCOMAT GESTORE", "CARTA CREDITO GESTORE", "AMEX",
                     "CARTA CREDITO GENERICA", "PAGOBANCOMAT", "TBS"],
    "petrolifere":  ["DKV", "UTA", "CARTAMAXIMA"],
    "buoni":        ["CARTAPETROLIFERA", "BUONI"],
    "satispay":     ["PAGAMENTIINNOVATIVI"],
    "contanti":     ["CONTANTI"],
}


def get_fortech_records(file_path: str) -> list[dict] | None:
    """
    Legge un file Fortech e ritorna lista di record per (CodicePV, DataContabile)
    con i totali per categoria.
    """
    try:
        df = _carica_excel(file_path)
        if df is None:
            return None

        if "CodicePV" not in df.columns or "DataContabile" not in df.columns:
            return None

        df["DataContabile"] = pd.to_datetime(df["DataContabile"], errors="coerce")
        df = df.dropna(subset=["DataContabile"])
        df["DataContabile"] = df["DataContabile"].dt.strftime("%Y-%m-%d")

        # Converti colonne numeriche
        all_cols = [c for cols in _FORTECH_MAPPING.values() for c in cols]
        for col in all_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

        records = []
        for (pv, data), grp in df.groupby(["CodicePV", "DataContabile"]):
            rec = {"codice_pv": int(pv), "data": data}
            for cat, cols in _FORTECH_MAPPING.items():
                present = [c for c in cols if c in df.columns]
                rec[cat] = round(float(grp[present].sum().sum()) if present else 0.0, 2)
            records.append(rec)

        return records
    except Exception as e:
        print(f"[classifier] get_fortech_records error: {e}")
        return None


# ── Standalone ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    target = sys.argv[1] if len(sys.argv) > 1 else "."

    if os.path.isfile(target):
        files = [target]
    else:
        files = [os.path.join(target, f) for f in os.listdir(target)
                 if f.lower().endswith((".xlsx", ".xls"))]

    print(f"{'FILE':<50} | {'CATEGORIA':<20} | {'CONF%':<8}")
    print("-" * 85)
    for fp in files:
        res = identify_file_type(fp)
        print(f"{os.path.basename(fp)[:50]:<50} | {res['categoria']:<20} | {res['confidenza']}%")
