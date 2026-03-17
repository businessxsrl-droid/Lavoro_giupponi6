import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    df = pd.read_excel(path)
    print(f"Column 'Note' dtype: {df['Note'].dtype}")
    print(f"Number of non-null 'Note' values: {df['Note'].count()}")
else:
    print("File not found")
