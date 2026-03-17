import pandas as pd
import os

path = r"C:\Users\Utente\Desktop\Lavoro\Lavoro_Giupponi6\data\db_schema\INPUT\CONTANTI\movimenti doc finance 02.03_09.03.xls"
if os.path.exists(path):
    xls = pd.ExcelFile(path)
    print("Sheets:", xls.sheet_names)
else:
    print("File not found")
