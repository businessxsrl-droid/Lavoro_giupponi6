import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    df = pd.read_excel(path)
    for i, col in enumerate(df.columns):
        print(f"{i}: '{col}'")
else:
    print("File not found")
