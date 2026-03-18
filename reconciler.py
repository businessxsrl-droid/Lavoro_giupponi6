"""
Motore di riconciliazione — Lavoro Giupponi6
Legge le transazioni dal DB e produce riconciliazione_risultati e contanti_matching.
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
    diff: float = round(teorico - reale, 2)  # type: ignore[call-overload]
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

def _carica_fortech(conn) -> pd.DataFrame:
    """Legge tutte le giornate Fortech dal DB."""
    cols = ["codice_pv", "data", "totale_contante", "totale_pos", "totale_buoni", "totale_satispay", "totale_petrolifere"]
    return _to_df(conn, f"SELECT {', '.join(cols)} FROM transazioni_fortech", cols)


# ═══════════════════════════════════════════════════════════════════════════════
#  CONTANTI
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_contanti(conn, df_f: pd.DataFrame, tol: float) -> int:
    """
    Sincronizza i risultati della logica Look-Ahead FIFO (da contanti_matching)
    nella tabella generale riconciliazione_risultati.
    """
    rows = conn.execute('''
        SELECT codice_pv, data, contanti_teorico, contanti_versato, differenza, stato, tipo_match, note
        FROM contanti_matching
    ''').fetchall()

    params = []
    for r in rows:
        params.append((
            int(r["codice_pv"]), 
            r["data"], 
            "contanti", 
            float(r["contanti_teorico"]), 
            float(r["contanti_versato"]), 
            float(r["differenza"]), 
            r["stato"],
            r["tipo_match"],
            r["note"]
        ))


    
    if params:
        conn.executemany(_SQL_UPSERT, params)
        
    count = len(params)
    print(f"  [contanti]      {count} record (sincronizzati da rolling)")
    return count


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
            diff = round(teorico - reale, 2)
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
            diff = round(teorico - reale, 2)
            params.append((int(row["codice_pv"]), row["data"], "satispay", teorico, reale, diff, stato, "nessuno", ""))



    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [satispay]      {count} record")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  BUONI / VOUCHER
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_buoni(conn, df_f: pd.DataFrame, tol: float) -> int:
    """Riconcilia i buoni/voucher (iP Portal) vs Fortech."""
    cols = ["codice_pv", "data", "reale"]
    df_reale = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_buoni WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    m = df_f[["codice_pv", "data", "totale_buoni"]].copy()
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
            diff = round(teorico - reale, 2)
            params.append((int(row["codice_pv"]), row["data"], "buoni", teorico, reale, diff, stato, "nessuno", ""))


    
    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [buoni]         {count} record")
    return count


# ═══════════════════════════════════════════════════════════════════════════════
#  CARTE PETROLIFERE
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_petrolifere(conn, df_f: pd.DataFrame, tol: float) -> int:
    """Riconcilia le carte petrolifere vs Fortech."""
    cols = ["codice_pv", "data", "reale"]
    df_reale = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_petrolifere WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    m = df_f[["codice_pv", "data", "totale_petrolifere"]].copy()
    if not df_reale.empty:
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["totale_petrolifere"])
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(teorico - reale, 2)
            params.append((int(row["codice_pv"]), row["data"], "carte_petrolifere", teorico, reale, diff, stato, "nessuno", ""))


    
    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [carte_petrolifere]   {count} record")
    return count
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
        "contanti":       float(cfg.get("tolleranza_contanti_arrotondamento", 2.00)),
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

    # Il matching contanti deve girare PRIMA delle categorie:
    # _reconcile_contanti lo usa per gestire date sfasate e versamenti cumulativi
    _reconcile_contanti_matching(conn, cfg)

    print("[reconcile] Avvio riconciliazione per categoria:")
    inserted = 0
    inserted += _reconcile_contanti(conn, df_f, tol["contanti"])
    inserted += _reconcile_carte_bancarie(conn, df_f, tol["carte_bancarie"])
    inserted += _reconcile_satispay(conn, df_f, tol["satispay"])
    inserted += _reconcile_buoni(conn, df_f, tol["buoni"])
    inserted += _reconcile_petrolifere(conn, df_f, tol["carte_petrolifere"])

    conn.commit()
    print(f"[reconcile] Totale inseriti: {inserted} record in riconciliazione_risultati")

    if close:
        conn.close()
    return inserted


# ═══════════════════════════════════════════════════════════════════════════════
#  MATCHING CONTANTI (Vista Simona)
# ═══════════════════════════════════════════════════════════════════════════════

def _reconcile_contanti_matching(conn, cfg: dict):
    tolleranza  = float(cfg.get("tolleranza_contanti_arrotondamento", 2.00))

    cols_fort = ["codice_pv", "data", "totale_contante"]
    df_fort = _to_df(conn,
        "SELECT codice_pv, data, totale_contante FROM transazioni_fortech "
        "ORDER BY codice_pv, data", cols_fort)
        
    cols_as4 = ["codice_pv", "data", "importo"]
    df_as400 = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS importo "
        "FROM transazioni_contanti WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data ORDER BY codice_pv, data", cols_as4)

    conn.execute("DELETE FROM contanti_matching WHERE TRUE")

    if df_fort.empty and df_as400.empty:
        conn.commit()
        return

    fort_data = {}
    for _, r in df_fort.iterrows():
        fort_data[(int(r["codice_pv"]), r["data"])] = float(r["totale_contante"])
        
    as400_data = {}
    for _, r in df_as400.iterrows():
        as400_data[(int(r["codice_pv"]), r["data"])] = float(r["importo"])


    pvs = sorted(list(set(df_fort["codice_pv"].unique()) | set(df_as400["codice_pv"].unique())))
    all_dates = sorted(list(set(df_fort["data"].unique()) | set(df_as400["data"].unique())))

    results = []
    
    for pv in pvs:
        pv_int = int(pv)
        scarto_precedente = 0.0
        
        # Facciamo una copia locale dei versamenti perché li "consumeremo" andando in avanti (Look-Ahead FIFO)
        local_r = {d: as400_data.get((pv_int, d), 0.0) for d in all_dates}
        
        for i, d_str in enumerate(all_dates):
            t = fort_data.get((pv_int, d_str), 0.0)
            v = local_r[d_str]
            
            # Saltiamo i giorni dove non succede nulla per questo PV se lo scarto è quasi zero
            if t == 0 and v == 0 and abs(scarto_precedente) < 0.01:
                continue
                
            # Calcolo differenza inziale = (Teorico + Scarto Precedente) - Versato del giorno
            differenza_giorno = round(t + scarto_precedente - v, 2)
            versato_mostrato = v
            local_r[d_str] = 0.0 # Consumato
            
            # Look-Ahead: Se manca contante (Diff > 0), peschiamo dai giorni successivi
            j = i + 1
            pescati_futuro = 0
            while differenza_giorno > tolleranza and j < len(all_dates):
                next_date = all_dates[j]
                next_v = local_r[next_date]
                if next_v > 0:
                    versato_mostrato = round(versato_mostrato + next_v, 2)
                    differenza_giorno = round(differenza_giorno - next_v, 2)
                    local_r[next_date] = 0.0 # Soldi del futuro consumati oggi!
                    pescati_futuro += 1
                j += 1
                
            tipo_match = "look_ahead_fifo"
            
            # Generazione Note Automatica
            note_list = []
            if scarto_precedente > 0:
                note_list.append(f"Include mancanza gg precedenti (€{scarto_precedente:.2f})")
            elif scarto_precedente < -0.01:
                note_list.append(f"Coperto da eccedenza gg precedenti (€{abs(scarto_precedente):.2f})")
            
            if pescati_futuro > 0:
                note_list.append(f"Recuperati versamenti da {pescati_futuro} gg successivi")
            
            # Determiniamo lo stato
            if abs(differenza_giorno) <= tolleranza:
                stato = ST_QUADRATO
                scarto_precedente = 0.0
            else:
                if differenza_giorno > _ANOMALIA_GRAVE_THRESHOLD:
                    stato = ST_ANOMALIA_GRAVE
                elif differenza_giorno > 0:
                    stato = ST_ANOMALIA_LIEVE
                else: 
                    # Negativo (Surplus): Trasferiamo tutto lo scarto al giorno dopo ed è quadrato!
                    stato = ST_QUADRATO 
                scarto_precedente = differenza_giorno
                if scarto_precedente < -0.01:
                    note_list.append(f"Eccedenza di €{abs(scarto_precedente):.2f} portata al gg successivo")

            note_str = " | ".join(note_list)
            results.append((pv_int, d_str, t, versato_mostrato, differenza_giorno, stato, tipo_match, note_str))

    if results:

        conn.executemany('''
            INSERT INTO contanti_matching
                (codice_pv, data, contanti_teorico, contanti_versato, differenza, stato, tipo_match, note)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', results)


    conn.commit()
    print(f"[reconcile] contanti_matching (rolling): {len(results)} record")


# ═══════════════════════════════════════════════════════════════════════════════
#  Standalone
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    n = reconcile()
    print(f"Riconciliazione completata: {n} record.")
