"""
verifica.py
-----------
Fase di verifica post-riconciliazione: individua e applica compensazioni incrociate.

Logica: all'interno della stessa coppia (codice_pv, categoria), una anomalia con
differenza +X e una con differenza -X si annullano a vicenda → stato QUADRATO_COMPENSATO.
"""

from collections import defaultdict


def find_compensazioni(conn, tolleranza: float = 1.0, codice_pv=None, categoria=None):
    """
    Individua coppie di anomalie che si compensano.
    Ritorna lista di dict con i match proposti (senza modificare il DB).
    """
    where = ["r.stato IN ('ANOMALIA_GRAVE','ANOMALIA_LIEVE','NON_TROVATO')"]
    params = []
    if codice_pv:
        where.append("r.codice_pv = ?")
        params.append(int(codice_pv))
    if categoria:
        where.append("r.categoria = ?")
        params.append(str(categoria))

    rows = conn.execute(f"""
        SELECT r.id, r.codice_pv, r.data, r.categoria,
               r.differenza, r.stato, i.nome AS impianto
        FROM riconciliazione_risultati r
        LEFT JOIN impianti i ON r.codice_pv = i.codice_pv
        WHERE {' AND '.join(where)}
        ORDER BY r.codice_pv, r.categoria, r.data
    """, params if params else None).fetchall()

    # Raggruppa per (codice_pv, categoria)
    groups = defaultdict(list)
    for r in rows:
        groups[(r['codice_pv'], r['categoria'])].append(dict(r))

    matches = []
    for (pv, cat), items in groups.items():
        positivi = sorted(
            [r for r in items if float(r['differenza']) > 0],
            key=lambda x: -abs(float(x['differenza']))
        )
        negativi = sorted(
            [r for r in items if float(r['differenza']) < 0],
            key=lambda x: abs(float(x['differenza'])),
            reverse=True
        )

        used_neg = set()
        for pos in positivi:
            best = None
            best_residuo = tolleranza + 0.0001
            for neg in negativi:
                if neg['id'] in used_neg:
                    continue
                residuo = abs(float(pos['differenza']) + float(neg['differenza']))
                if residuo < best_residuo:
                    best_residuo = residuo
                    best = neg
            if best is not None:
                used_neg.add(best['id'])
                matches.append({
                    'id_pos':    pos['id'],
                    'id_neg':    best['id'],
                    'impianto':  pos['impianto'] or 'N/D',
                    'codice_pv': pv,
                    'categoria': cat,
                    'data_pos':  pos['data'],
                    'data_neg':  best['data'],
                    'diff_pos':  round(float(pos['differenza']), 2),
                    'diff_neg':  round(float(best['differenza']), 2),
                    'residuo':   round(float(pos['differenza']) + float(best['differenza']), 2),
                    'stato_pos': pos['stato'],
                    'stato_neg': best['stato'],
                })

    return matches


def applica_compensazioni(conn, id_pairs: list[tuple] | None = None, tolleranza: float = 1.0):
    """
    Applica le compensazioni aggiornando lo stato a QUADRATO_COMPENSATO.

    id_pairs: lista di (id_pos, id_neg) da applicare. Se None → applica tutte.
    Ritorna (n_aggiornati, matches_applicati).
    """
    matches = find_compensazioni(conn, tolleranza)

    if id_pairs is not None:
        pair_set = {(a, b) for a, b in id_pairs} | {(b, a) for a, b in id_pairs}
        matches = [m for m in matches if (m['id_pos'], m['id_neg']) in pair_set]

    updated = 0
    for m in matches:
        conn.execute(
            "UPDATE riconciliazione_risultati SET stato='QUADRATO_COMPENSATO', note=? WHERE id=?",
            (f"Compensato con {m['data_neg']} (€{m['diff_neg']:+.2f})", m['id_pos'])
        )
        conn.execute(
            "UPDATE riconciliazione_risultati SET stato='QUADRATO_COMPENSATO', note=? WHERE id=?",
            (f"Compensato con {m['data_pos']} (€{m['diff_pos']:+.2f})", m['id_neg'])
        )
        updated += 2

    conn.commit()
    return updated, matches


def reset_compensazioni(conn, codice_pv=None):
    """
    Ripristina le compensazioni (QUADRATO_COMPENSATO → da rivalutare).
    Utile se si vuole rifare la verifica da zero dopo nuovi upload.
    """
    where = "stato = 'QUADRATO_COMPENSATO'"
    if codice_pv:
        where += f" AND codice_pv = {int(codice_pv)}"
    conn.execute(f"UPDATE riconciliazione_risultati SET stato='ANOMALIA_GRAVE', note=NULL WHERE {where}")
    conn.commit()
