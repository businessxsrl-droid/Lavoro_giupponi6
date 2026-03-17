import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\Elenco impianti.xlsx"
if os.path.exists(path):
    df = pd.read_excel(path)
    print("Columns:", df.columns.tolist())
    print("\nRows for target PVs:")
    target_pvs = [43809, 46273]
    # COD. PV might be string or float in Excel
    df_filtered = df[df["COD. PV"].astype(str).str.contains("43809|46273")]
    print(df_filtered[["COD. PV", "COMUNE", "IDENTIFICATIVO MOVIMENTO DI ACCREDITO"]])
else:
    print("File not found")
