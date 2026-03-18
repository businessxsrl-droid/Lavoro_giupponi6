import pandas as pd
from database import get_connection

def main():
    conn = get_connection()
    rows = conn.execute("SELECT codice_pv, data, contanti_teorico, contanti_versato, differenza, stato, tipo_match FROM contanti_matching ORDER BY codice_pv, data").fetchall()
    
    df = pd.DataFrame([dict(r) for r in rows])
    output_path = "risultato_contanti_rolling.xlsx"
    df.to_excel(output_path, index=False)
    print(f"Exported to {output_path}")

if __name__ == "__main__":
    main()
