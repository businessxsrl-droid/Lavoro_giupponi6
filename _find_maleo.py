import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    df = pd.read_excel(path)
    found_any = False
    for col in df.columns:
        col_str = df[col].astype(str)
        if col_str.str.contains("MALEO", case=False).any():
            print(f"FOUND 'MALEO' in column: '{col}' | Type: {df[col].dtype}")
            # Show the first row that matches
            match = df[col_str.str.contains("MALEO", case=False)].iloc[0]
            print(f"  Value: {match}")
            found_any = True
    if not found_any:
        print("MALEO not found in any column")
else:
    print("File not found")
