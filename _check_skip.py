from database import get_connection
conn = get_connection()
res = conn.execute("SELECT data, contanti_teorico, contanti_versato, differenza, note FROM contanti_matching LIMIT 20").fetchall()
for r in res:
    print(f"{r['data']} | T:{r['contanti_teorico']} | V:{r['contanti_versato']} | D:{r['differenza']} | {r['note']}")
conn.close()
