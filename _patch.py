import re

def main():
    with open('reconciler.py', 'r', encoding='utf-8') as f:
        code = f.read()
        
    start_str = "def _reconcile_contanti_matching(conn, cfg: dict):"
    end_str = "\n# ═══════════════════════════════════════════════════════════════════════════════\n#  Standalone\n# ═══════════════════════════════════════════════════════════════════════════════\n"
    
    start_idx = code.find(start_str)
    end_idx = code.find(end_str)
    
    if start_idx == -1 or end_idx == -1:
        print("Could not find boundaries")
        return
        
    new_func = """def _reconcile_contanti_matching(conn, cfg: dict):
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
        
        for d_str in all_dates:
            t = fort_data.get((pv_int, d_str), 0.0)
            v = as400_data.get((pv_int, d_str), 0.0)
            
            if t == 0 and v == 0 and abs(scarto_precedente) < 0.01:
                continue
                
            differenza_giorno = round(t + scarto_precedente - v, 2)
            tipo_match = "bilancio_continuo"
            
            if abs(differenza_giorno) <= tolleranza:
                stato = ST_QUADRATO
                scarto_precedente = 0.0
            else:
                if abs(differenza_giorno) > _ANOMALIA_GRAVE_THRESHOLD:
                    stato = ST_ANOMALIA_GRAVE
                else:
                    stato = ST_ANOMALIA_LIEVE
                scarto_precedente = differenza_giorno
            
            results.append((pv_int, d_str, t, v, differenza_giorno, stato, tipo_match))

    if results:
        conn.executemany('''
            INSERT INTO contanti_matching
                (codice_pv, data, contanti_teorico, contanti_versato, differenza, stato, tipo_match)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', results)

    conn.commit()
    print(f"[reconcile] contanti_matching (rolling): {len(results)} record")

"""
    
    new_code = code[:start_idx] + new_func + code[end_idx:]
    with open('reconciler.py', 'w', encoding='utf-8') as f:
        f.write(new_code)
        
    print("Patched successfully")

if __name__ == "__main__":
    main()
