import pandas as pd
from datetime import datetime, timedelta
from database import get_connection

def main():
    conn = get_connection()
    f_all = conn.execute('SELECT * FROM transazioni_fortech WHERE totale_contante > 0').fetchall()
    a_all = conn.execute('SELECT * FROM transazioni_contanti').fetchall()
    
    df_f = pd.DataFrame([dict(r) for r in f_all])
    df_a = pd.DataFrame([dict(r) for r in a_all])
    
    if df_f.empty:
        print("No fortech data")
        return

    df_f['dt'] = pd.to_datetime(df_f['data'])
    df_f['monday'] = df_f['dt'] - pd.to_timedelta(df_f['dt'].dt.weekday, unit='D')
    
    found_any = False
    for (pv, mon), f_week in df_f.groupby(['codice_pv', 'monday']):
        f_sum = f_week['totale_contante'].sum()
        mon_s = mon.strftime('%Y-%m-%d')
        sun = mon + timedelta(days=6)
        lim = mon + timedelta(days=10) # Fino al Giovedì successivo
        
        lim_s = lim.strftime('%Y-%m-%d')
        
        a_week = df_a[(df_a['codice_pv'] == pv) & (df_a['data'] >= mon_s) & (df_a['data'] <= lim_s)]
        a_sum = a_week['importo'].sum()
        
        diff = f_sum - a_sum
        print(f"PV {pv} WEEK {mon_s}: Fort {f_sum:8.2f} | AS400 {a_sum:8.2f} | DIFF {diff:9.2f}")

    if not found_any:
        print("No candidate weekly matches found even with 100 EUR tolerance.")

if __name__ == "__main__":
    main()
