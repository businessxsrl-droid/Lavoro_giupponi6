import re

def main():
    with open('reconciler.py', 'r', encoding='utf-8') as f:
        code = f.read()

    # The existing block starts at: all_dates = sorted(...)
    # and ends at: if results:
    
    new_loop_logic = """
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
            while differenza_giorno > tolleranza and j < len(all_dates):
                next_date = all_dates[j]
                next_v = local_r[next_date]
                if next_v > 0:
                    versato_mostrato = round(versato_mostrato + next_v, 2)
                    differenza_giorno = round(differenza_giorno - next_v, 2)
                    local_r[next_date] = 0.0 # Soldi del futuro consumati oggi!
                j += 1
                
            tipo_match = "look_ahead_fifo"
            
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
            
            results.append((pv_int, d_str, t, versato_mostrato, differenza_giorno, stato, tipo_match))

    if results:
"""

    # We need to find the start and end of the block to replace.
    # It looks like:
    #     pvs = sorted(list(set(df_fort["codice_pv"].unique()) | set(df_as400["codice_pv"].unique())))
    #     ...
    #     if results:
    
    start_str = '    pvs = sorted(list(set(df_fort["codice_pv"].unique()) | set(df_as400["codice_pv"].unique())))'
    end_str = '    if results:'
    
    start_idx = code.find(start_str)
    end_idx = code.find(end_str)
    
    if start_idx == -1 or end_idx == -1:
        print("Could not find boundaries")
        return
        
    new_code = code[:start_idx] + new_loop_logic + code[end_idx+len(end_str):]
    with open('reconciler.py', 'w', encoding='utf-8') as f:
        f.write(new_code)
        
    print("Patched successfully")

if __name__ == "__main__":
    main()
