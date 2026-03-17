import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    df = pd.read_excel(path)
    for col in df.columns:
        print(f"Col: '{col}' | Type: {df[col].dtype} | Sample: {df[col].iloc[0] if not df[col].empty else 'N/A'}")
else:
    print("File not found")
