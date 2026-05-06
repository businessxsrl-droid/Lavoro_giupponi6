"""
Motore di riconciliazione — Lavoro Giupponi6
Legge le transazioni dal DB e produce riconciliazione_risultati.
"""
import pandas as pd
from datetime import datetime, timedelta

from database import get_connection, get_config
from ingestion import _build_alias_to_pv, _trova_pv_da_alias

# ── Costanti stato ────────────────────────────────────────────────────────────
ST_QUADRATO       = "QUADRATO"
ST_QUADRATO_ARROT = "QUADRATO_ARROT"
ST_ANOMALIA_LIEVE = "ANOMALIA_LIEVE"
ST_ANOMALIA_GRAVE = "ANOMALIA_GRAVE"
ST_NON_TROVATO    = "NON_TROVATO"

_ANOMALIA_GRAVE_THRESHOLD = 50.0   # EUR oltre il quale è grave

_SQL_UPSERT = '''
    INSERT INTO riconciliazione_risultati
        (codice_pv, data, categoria, valore_teorico, valore_reale, differenza, stato, tipo_match, note)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(codice_pv, data, categoria) DO UPDATE SET
        valore_teorico = excluded.valore_teorico,
        valore_reale   = excluded.valore_reale,
        differenza     = excluded.differenza,
        stato          = excluded.stato,
        tipo_match     = excluded.tipo_match,
        note           = COALESCE(riconciliazione_risultati.note, excluded.note)


'''


def _calcola_stato(teorico: float, reale: float, tolleranza: float) -> str | None:
    if teorico == 0 and reale == 0:
        return None          # record inutile, skip
    if teorico > 0 and reale == 0:
        return ST_NON_TROVATO
    diff = abs(teorico - reale)
    if diff <= tolleranza:
        return ST_QUADRATO
    if diff <= tolleranza * 3:
        return ST_QUADRATO_ARROT
    if diff <= _ANOMALIA_GRAVE_THRESHOLD:
        return ST_ANOMALIA_LIEVE
    return ST_ANOMALIA_GRAVE


def _inserisci_risultato(conn, pv: int, data: str, cat: str,
                         teorico: float, reale: float, tolleranza: float, 
                         tipo_match: str = "nessuno", note: str = "") -> int:
    """Calcola stato e inserisce/aggiorna un record. Ritorna 1 se inserito, 0 se saltato."""
    stato = _calcola_stato(teorico, reale, tolleranza)
    if stato is None:
        return 0
    diff: float = round(reale - teorico, 2)  # type: ignore[call-overload]
    conn.execute(_SQL_UPSERT, (pv, data, cat, teorico, reale, diff, stato, tipo_match, note))
    return 1



# ═══════════════════════════════════════════════════════════════════════════════
#  FORTECH — caricamento dati base
# ═══════════════════════════════════════════════════════════════════════════════

def _to_df(conn, query, columns) -> pd.DataFrame:
    """Utility per convertire risultati Supabase in DataFrame."""
    try:
        rows = conn.execute(query).fetchall()
        if not rows:
            return pd.DataFrame(columns=columns)
        
        # Estraiamo i valori dalle righe (visto che Supabase ritorna dict/DualAccessRow)
        data = []
        for r in rows:
            if hasattr(r, "_values"):
                data.append(r._values)
            elif isinstance(r, dict):
                data.append(list(r.values()))
            else:
                data.append(r)
                
        return pd.DataFrame(data, columns=columns)
    except Exception as e:
        print(f"[RECONCILER ERROR] Fallito caricamento DataFrame: {e}")
        return pd.DataFrame(columns=columns)

# Impianti senza servizio riconciliazione (hardcoded come fallback)
_SENZA_SERVIZIO_HARDCODED = {47831, 45874, 47832, 41118, 42840, 45818, 49788}


def _get_impianti_senza_servizio(conn) -> set:
    """Ritorna set di codice_pv degli impianti senza servizio riconciliazione."""
    try:
        rows = conn.execute(
            "SELECT codice_pv FROM impianti WHERE senza_servizio_riconciliazione = TRUE"
        ).fetchall()
        pvs = {int(r["codice_pv"]) for r in rows if r["codice_pv"] is not None}
        return pvs if pvs else _SENZA_SERVIZIO_HARDCODED
    except Exception as e:
        print(f"[WARN] Errore caricamento impianti senza servizio: {e}")
        return _SENZA_SERVIZIO_HARDCODED


def _carica_fortech(conn) -> pd.DataFrame:
    """Legge tutte le giornate Fortech dal DB."""
    cols = ["codice_pv", "data", "totale_contante", "totale_pos", "totale_buoni",
            "totale_satispay", "totale_petrolifere",
            "prove_erogazione", "clienti_fine_mese", "diversi"]
    return _to_df(conn, f"SELECT {', '.join(cols)} FROM transazioni_fortech", cols)


# ═══════════════════════════════════════════════════════════════════════════════
#  CARTE BANCARIE (POS)
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_carte_bancarie(conn, df_f: pd.DataFrame, tol: float) -> int:
    impianti   = _build_alias_to_pv(conn)
    cols = ["data", "alias_terminale", "importo"]
    df_pos_raw = _to_df(conn,
        "SELECT data, alias_terminale, SUM(importo) AS importo "
        "FROM transazioni_pos GROUP BY data, alias_terminale", cols)

    if not df_pos_raw.empty:
        df_pos_raw["codice_pv"] = df_pos_raw["alias_terminale"].apply(
            lambda a: _trova_pv_da_alias(a, impianti))
        df_reale = (df_pos_raw.dropna(subset=["codice_pv"])
                    .groupby(["codice_pv", "data"])["importo"]
                    .sum().reset_index()
                    .rename(columns={"importo": "reale"}))
        df_reale["codice_pv"] = df_reale["codice_pv"].astype(int)
    else:
        df_reale = pd.DataFrame(columns=["codice_pv", "data", "reale"])

    m = df_f[["codice_pv", "data", "totale_pos"]].copy()
    if not df_reale.empty:
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["totale_pos"])
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(reale - teorico, 2)
            params.append((int(row["codice_pv"]), row["data"], "carte_bancarie", teorico, reale, diff, stato, "nessuno", ""))


    
    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [carte_bancarie] {count} record")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  SATISPAY
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_satispay(conn, df_f: pd.DataFrame, tol: float) -> int:
    """Riconcilia le transazioni Satispay vs Fortech."""
    cols = ["codice_pv", "data", "reale"]
    df_reale = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_satispay WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    m = df_f[["codice_pv", "data", "totale_satispay"]].copy()
    if not df_reale.empty:
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["totale_satispay"])
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(reale - teorico, 2)
            params.append((int(row["codice_pv"]), row["data"], "satispay", teorico, reale, diff, stato, "nessuno", ""))



    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [satispay]      {count} record")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  BUONI / VOUCHER
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_buoni(conn, df_f: pd.DataFrame, tol: float, exclude_pvs: set = None) -> int:
    """Riconcilia i buoni/voucher (iP Portal) vs Fortech.
    Teorico = totale_buoni.
    Reale   = somma Importo dal file iPortal, raggruppato per data e codice PV.
    exclude_pvs: impianti senza servizio (gestiti da _reconcile_buoni_petrolifere_combined).
    """
    cols = ["codice_pv", "data", "reale"]
    df_reale = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_buoni WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    df_src = df_f[~df_f["codice_pv"].isin(exclude_pvs)].copy() if exclude_pvs else df_f.copy()
    m = df_src[["codice_pv", "data", "totale_buoni"]].copy()
    if not df_reale.empty:
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["totale_buoni"])
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(reale - teorico, 2)
            params.append((int(row["codice_pv"]), row["data"], "buoni",
                           teorico, reale, diff, stato, "nessuno", ""))

    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [buoni]         {count} record")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  CARTE PETROLIFERE
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_buoni_petrolifere_combined(conn, df_f: pd.DataFrame, tol: float, pvs: set) -> int:
    """Per impianti senza servizio: reconcilia buoni+petrolifere come unica categoria.
    Prove_erogazione, clienti_fine_mese e diversi sono gestiti da _reconcile_prove_clienti_diversi.
    """
    if not pvs:
        return 0
    df_ss = df_f[df_f["codice_pv"].isin(pvs)].copy()
    if df_ss.empty:
        return 0

    df_ss["teorico"] = df_ss["totale_buoni"] + df_ss["totale_petrolifere"]

    cols = ["codice_pv", "data", "reale"]
    df_buoni = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_buoni WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)
    df_petro = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_petrolifere WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    if not df_buoni.empty and not df_petro.empty:
        df_reale = (pd.concat([df_buoni, df_petro])
                    .groupby(["codice_pv", "data"])["reale"].sum().reset_index())
    elif not df_buoni.empty:
        df_reale = df_buoni
    elif not df_petro.empty:
        df_reale = df_petro
    else:
        df_reale = pd.DataFrame(columns=["codice_pv", "data", "reale"])

    if not df_reale.empty:
        df_reale["codice_pv"] = df_reale["codice_pv"].astype(int)

    m = df_ss[["codice_pv", "data", "teorico"]].copy()
    if not df_reale.empty:
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["teorico"])
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(reale - teorico, 2)
            params.append((int(row["codice_pv"]), row["data"], "buoni_petrolifere",
                           teorico, reale, diff, stato, "nessuno", ""))

    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [buoni_petrolifere] {count} record (impianti senza servizio)")
    return count


def _reconcile_prove_clienti_diversi(conn, df_f: pd.DataFrame) -> int:
    """Crea record informativi per prove_erogazione, clienti_fine_mese, diversi.
    Non esiste fonte esterna: valore_teorico=0, valore_reale=importo Fortech.
    tipo_match='informativo' le distingue dalle categorie riconciliate.
    """
    cats = [
        ("prove_erogazione",  "prove_erogazione"),
        ("clienti_fine_mese", "clienti_fine_mese"),
        ("diversi",           "diversi"),
    ]
    params = []
    for col, cat in cats:
        if col not in df_f.columns:
            continue
        for _, row in df_f.iterrows():
            val = round(float(pd.to_numeric(row[col], errors="coerce") or 0), 2)
            if val <= 0:
                continue
            params.append((int(row["codice_pv"]), row["data"], cat,
                           0.0, val, 0.0, "QUADRATO", "informativo", ""))
    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [prove/clienti/diversi] {count} record")
    return count


# PV per cui totale_petrolifere in Fortech include già clienti_fine_mese (da sottrarre)
_PVS_PETRO_SUBTRACT_CLIENTI = {49788}  # Taleggio


def _reconcile_petrolifere(conn, df_f: pd.DataFrame, tol: float, exclude_pvs: set = None) -> int:
    """Riconcilia le carte petrolifere vs Fortech."""
    cols = ["codice_pv", "data", "reale"]
    df_reale = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_petrolifere WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    df_src = df_f[~df_f["codice_pv"].isin(exclude_pvs)].copy() if exclude_pvs else df_f.copy()
    m = df_src[["codice_pv", "data", "totale_petrolifere", "clienti_fine_mese"]].copy()
    if not df_reale.empty:
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["totale_petrolifere"])
        if int(row["codice_pv"]) in _PVS_PETRO_SUBTRACT_CLIENTI:
            teorico = max(0.0, teorico - float(row["clienti_fine_mese"]))
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(reale - teorico, 2)
            params.append((int(row["codice_pv"]), row["data"], "carte_petrolifere", teorico, reale, diff, stato, "nessuno", ""))

    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [petrolifere]   {count} record")
    return count
# ═══════════════════════════════════════════════════════════════════════════════
#  RICONCILIAZIONE PRINCIPALE
# ═══════════════════════════════════════════════════════════════════════════════

def reconcile(conn=None) -> int:
    """
    Esegue la riconciliazione completa per tutte le categorie.
    Svuota riconciliazione_risultati e la ripopola.
    Ritorna il numero di righe inserite.
    """
    close = conn is None
    if conn is None:
        conn = get_connection()

    cfg = get_config(conn)

    tol = {
        "carte_bancarie": float(cfg.get("tolleranza_carte_fisiologica",       1.00)),
        "satispay":       float(cfg.get("tolleranza_satispay",                0.01)),
        "buoni":          float(cfg.get("tolleranza_buoni",                   0.01)),
        "carte_petrolifere": float(cfg.get("tolleranza_petrolifere",          0.01)),
    }

    conn.execute("DELETE FROM riconciliazione_risultati WHERE TRUE")

    df_f = _carica_fortech(conn)
    if df_f.empty:
        conn.commit()
        if close:
            conn.close()
        return 0

    # Carica impianti senza servizio (logica riconciliazione combinata buoni+petrolifere)
    senza_servizio_pvs = _get_impianti_senza_servizio(conn)

    print("[reconcile] Avvio riconciliazione per categoria:")
    inserted = 0
    try:
        inserted += _reconcile_carte_bancarie(conn, df_f, tol["carte_bancarie"])
    except Exception as e:
        print(f"  [ERR] carte_bancarie: {e}")

    try:
        inserted += _reconcile_satispay(conn, df_f, tol["satispay"])
    except Exception as e:
        print(f"  [ERR] satispay: {e}")

    try:
        inserted += _reconcile_buoni(conn, df_f, tol["buoni"], exclude_pvs=senza_servizio_pvs)
    except Exception as e:
        print(f"  [ERR] buoni: {e}")

    try:
        inserted += _reconcile_petrolifere(conn, df_f, tol["carte_petrolifere"], exclude_pvs=senza_servizio_pvs)
    except Exception as e:
        print(f"  [ERR] petrolifere: {e}")

    try:
        inserted += _reconcile_buoni_petrolifere_combined(conn, df_f, tol["buoni"], pvs=senza_servizio_pvs)
    except Exception as e:
        print(f"  [ERR] buoni_petrolifere_combined: {e}")

    try:
        inserted += _reconcile_prove_clienti_diversi(conn, df_f)
    except Exception as e:
        print(f"  [ERR] prove_clienti_diversi: {e}")



    conn.commit()
    conn.commit()
    print(f"[reconcile] Totale inseriti: {inserted} record in riconciliazione_risultati")

    if close:
        conn.close()
    return inserted


# ═══════════════════════════════════════════════════════════════════════════════
#  Standalone
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    n = reconcile()
    print(f"Riconciliazione completata: {n} record.")
