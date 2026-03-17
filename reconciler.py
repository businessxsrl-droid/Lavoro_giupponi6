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
        (codice_pv, data, categoria, valore_teorico, valore_reale, differenza, stato)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ON CONFLICT(codice_pv, data, categoria) DO UPDATE SET
        valore_teorico = excluded.valore_teorico,
        valore_reale   = excluded.valore_reale,
        differenza     = excluded.differenza,
        stato          = excluded.stato
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
                         teorico: float, reale: float, tolleranza: float) -> int:
    """Calcola stato e inserisce/aggiorna un record. Ritorna 1 se inserito, 0 se saltato."""
    stato = _calcola_stato(teorico, reale, tolleranza)
    if stato is None:
        return 0
    diff: float = round(teorico - reale, 2)  # type: ignore[call-overload]
    conn.execute(_SQL_UPSERT, (pv, data, cat, teorico, reale, diff, stato))
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
    """Riconcilia i contanti confrontando i totali per data esatta."""
    cols = ["codice_pv", "data", "reale"]
    df_reale = _to_df(conn, 
        "SELECT codice_pv, data, SUM(importo) AS reale "
        "FROM transazioni_contanti WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data", cols)

    m = df_f[["codice_pv", "data", "totale_contante"]].copy()
    if not df_reale.empty:
        df_reale["codice_pv"] = df_reale["codice_pv"].astype(int)
        m = m.merge(df_reale, on=["codice_pv", "data"], how="left")
    else:
        m["reale"] = 0.0
    m.fillna(0.0, inplace=True)

    params = []
    for _, row in m.iterrows():
        teorico = float(row["totale_contante"])
        reale   = float(row["reale"])
        stato   = _calcola_stato(teorico, reale, tol)
        if stato:
            diff = round(teorico - reale, 2)
            params.append((int(row["codice_pv"]), row["data"], "contanti", teorico, reale, diff, stato))
    
    conn.executemany(_SQL_UPSERT, params)
    count = len(params)
    print(f"  [contanti]      {count} record")
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
            params.append((int(row["codice_pv"]), row["data"], "carte_bancarie", teorico, reale, diff, stato))
    
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
            params.append((int(row["codice_pv"]), row["data"], "satispay", teorico, reale, diff, stato))

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
            params.append((int(row["codice_pv"]), row["data"], "buoni", teorico, reale, diff, stato))
    
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
            params.append((int(row["codice_pv"]), row["data"], "carte_petrolifere", teorico, reale, diff, stato))
    
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
    """
    Algoritmo di matching contanti per la vista Simona.
    Per ogni giornata Fortech con contante > 0, cerca il versamento bancario
    corrispondente nell'intervallo di giorni configurato.
    Gestisce:
      - Match 1:1 esatto (entro tolleranza)
      - Match 1:1 arrotondato (entro tolleranza*3)
      - Match cumulativo 2-4gg (somma di giorni Fortech = un versamento AS400)
    """
    tolleranza  = float(cfg.get("tolleranza_contanti_arrotondamento", 2.00))
    giorni_inf  = int(float(cfg.get("scarto_giorni_contanti_inf", 3)))
    giorni_sup  = int(float(cfg.get("scarto_giorni_contanti_sup", 7)))

    cols_fort = ["codice_pv", "data", "totale_contante"]
    df_fort = _to_df(conn,
        "SELECT codice_pv, data, totale_contante FROM transazioni_fortech "
        "WHERE totale_contante > 0 ORDER BY codice_pv, data", cols_fort)
        
    cols_as4 = ["codice_pv", "data", "importo"]
    df_as400 = _to_df(conn,
        "SELECT codice_pv, data, SUM(importo) AS importo "
        "FROM transazioni_contanti WHERE codice_pv IS NOT NULL "
        "GROUP BY codice_pv, data ORDER BY codice_pv, data", cols_as4)

    conn.execute("DELETE FROM contanti_matching WHERE TRUE")

    if df_fort.empty:
        conn.commit()
        return

    # Dizionario AS400 per lookup veloce: (pv, data) → importo
    as400_dict: dict[tuple, float] = {}
    if not df_as400.empty:
        for _, r in df_as400.iterrows():
            as400_dict[(int(r["codice_pv"]), r["data"])] = float(r["importo"])

    results = []
    used_as400 = set()  # versamenti già utilizzati in match cumulativi

    pv_list = df_fort["codice_pv"].unique()

    for pv in pv_list:
        pv = int(pv)
        fort_pv = df_fort[df_fort["codice_pv"] == pv].sort_values("data")

        for _, frow in fort_pv.iterrows():
            data_str  = frow["data"]
            teorico   = float(frow["totale_contante"])
            try:
                data_dt = datetime.strptime(data_str, "%Y-%m-%d")
            except ValueError:
                continue

            versato    = 0.0
            tipo_match = "nessuno"

            # Finestra di ricerca
            date_from = (data_dt - timedelta(days=giorni_inf)).strftime("%Y-%m-%d")
            date_to   = (data_dt + timedelta(days=giorni_sup)).strftime("%Y-%m-%d")

            # Tutte le AS400 entry nel range per questo PV
            as400_in_range = [
                (d, imp) for (p, d), imp in as400_dict.items()
                if p == pv and date_from <= d <= date_to and (p, d) not in used_as400
            ]

            # 1. Prova match 1:1 esatto sulla stessa data
            same_day = as400_dict.get((pv, data_str))
            if same_day is not None and (pv, data_str) not in used_as400:
                diff = abs(teorico - same_day)
                if diff <= tolleranza:
                    versato    = same_day
                    tipo_match = "1:1_esatto"
                    used_as400.add((pv, data_str))
                elif diff <= tolleranza * 3:
                    versato    = same_day
                    tipo_match = "1:1_arrotondato"
                    used_as400.add((pv, data_str))

            # 2. Se non trovato in data esatta, prova in range
            if tipo_match == "nessuno":
                for (d, imp) in sorted(as400_in_range, key=lambda x: abs(datetime.strptime(x[0], "%Y-%m-%d") - data_dt)):
                    diff = abs(teorico - imp)
                    if diff <= tolleranza:
                        versato    = imp
                        tipo_match = "1:1_esatto"
                        used_as400.add((pv, d))
                        break
                    elif diff <= tolleranza * 3:
                        versato    = imp
                        tipo_match = "1:1_arrotondato"
                        used_as400.add((pv, d))
                        break

            # 3. Prova match cumulativo 2-4gg
            if tipo_match == "nessuno":
                for window in (2, 3, 4):
                    window_rows = fort_pv[
                        (fort_pv["data"] >= data_str) &
                        (fort_pv["data"] <= (data_dt + timedelta(days=window - 1)).strftime("%Y-%m-%d"))
                    ]
                    if len(window_rows) < 2:
                        continue
                    sum_teorico = float(window_rows["totale_contante"].sum())

                    for (d, imp) in as400_in_range:
                        diff = abs(sum_teorico - imp)
                        if diff <= tolleranza * window:  # type: ignore[operator]
                            versato    = imp
                            tipo_match = f"cumulativo_{window}gg"
                            used_as400.add((pv, d))
                            break
                    if tipo_match != "nessuno":
                        break

            diff_finale = round(teorico - versato, 2)
            if tipo_match == "1:1_esatto":
                stato = ST_QUADRATO
            elif tipo_match == "1:1_arrotondato" or tipo_match.startswith("cumulativo"):
                stato = ST_QUADRATO_ARROT if abs(diff_finale) <= tolleranza * 3 else ST_ANOMALIA_LIEVE
            elif versato == 0 and teorico == 0:
                stato = None  # skip
            elif versato == 0:
                stato = ST_NON_TROVATO
            else:
                stato = ST_ANOMALIA_GRAVE if abs(diff_finale) > _ANOMALIA_GRAVE_THRESHOLD else ST_ANOMALIA_LIEVE

            if stato is None:
                continue

            results.append((pv, data_str, teorico, versato, diff_finale, stato, tipo_match))

    conn.executemany('''
        INSERT INTO contanti_matching
            (codice_pv, data, contanti_teorico, contanti_versato, differenza, stato, tipo_match)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(codice_pv, data) DO UPDATE SET
            contanti_teorico  = EXCLUDED.contanti_teorico,
            contanti_versato  = EXCLUDED.contanti_versato,
            differenza        = EXCLUDED.differenza,
            stato             = EXCLUDED.stato,
            tipo_match        = EXCLUDED.tipo_match
    ''', results)

    conn.commit()
    print(f"[reconcile] contanti_matching: {len(results)} record")


# ═══════════════════════════════════════════════════════════════════════════════
#  Standalone
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    n = reconcile()
    print(f"Riconciliazione completata: {n} record.")
