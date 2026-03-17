import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    df = pd.read_excel(path)
    text_cols = [c for c in df.columns if df[c].dtype == object]
    print("Detected text_cols:", text_cols)
    
    # Try with a row that we know has MALEO
    row_with_maleo = df[df.astype(str).apply(lambda r: r.str.contains("MALEO", case=False).any(), axis=1)].iloc[0]
    all_text = " ".join(
        str(row_with_maleo[c]) for c in text_cols if pd.notna(row_with_maleo[c]) and str(row_with_maleo[c]) != "nan"
    )
    print("\nResulting all_text for MALEO row:")
    print(all_text)
else:
    print("File not found")
