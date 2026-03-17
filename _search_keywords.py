import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    df = pd.read_excel(path)
    keywords = ["46273", "MALEO", "VERSAMENTO", "00486"]
    
    print("Searching for keywords in all columns...")
    for col in df.columns:
        # Check if any row in this column contains any of the keywords
        col_series = df[col].astype(str)
        found = col_series.str.contains("|".join(keywords), case=False, na=False).any()
        if found:
            print(f"Keywords found in column: '{col}'")
            # Show first 3 non-null values containing keywords
            matches = col_series[col_series.str.contains("|".join(keywords), case=False, na=False)].head(3)
            print("  Samples:", matches.tolist())

else:
    print("File not found")
