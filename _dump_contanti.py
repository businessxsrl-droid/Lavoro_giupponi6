import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    # For .xls files, we might need xlrd
    df = pd.read_excel(path)
    print("Columns:", df.columns.tolist())
    print("\nTypes:\n", df.dtypes)
    print("\nFirst 5 rows:")
    print(df.head())
else:
    print("File not found")
