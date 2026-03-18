"""
Flask Backend — Lavoro Giupponi6
API completa per il frontend Calor Systems Riconciliazione.
Serve il frontend da Lavoro_Giupponi5/frontend.
"""
import os
import sys
import hashlib
import datetime
import io
import json
import urllib.request
import urllib.error
from collections import Counter

import pandas as pd
from flask import Flask, request, jsonify, render_template, send_from_directory, send_file
from flask_jwt_extended import (
    JWTManager, create_access_token, create_refresh_token,
    jwt_required, get_jwt_identity,
)
from flask_cors import CORS

# ── Path setup ────────────────────────────────────────────────────────────────
ROOT_DIR      = os.path.dirname(os.path.abspath(__file__))
FRONTEND_DIR  = os.path.join(ROOT_DIR, "frontend")
TEMPLATES_DIR = os.path.join(FRONTEND_DIR, "templates")
STATIC_DIR    = os.path.join(FRONTEND_DIR, "static")
EXCEL_DIR     = os.path.join(ROOT_DIR, "excel")

# ── Imports locali ────────────────────────────────────────────────────────────
from database   import get_connection, init_db, get_config
from classifier import identify_file_type
from ingestion  import ingest_folder
from reconciler import reconcile, _calcola_stato

# ── Inizializzazione DB ───────────────────────────────────────────────────────
init_db()
os.makedirs(EXCEL_DIR, exist_ok=True)

# ── Flask app ─────────────────────────────────────────────────────────────────
app = Flask(__name__, template_folder=TEMPLATES_DIR, static_folder=STATIC_DIR)
app.config["JWT_SECRET_KEY"]             = os.environ.get("JWT_SECRET", "calor-systems-secret-2025")
app.config["JWT_ACCESS_TOKEN_EXPIRES"]   = datetime.timedelta(hours=8)
app.config["JWT_REFRESH_TOKEN_EXPIRES"]  = datetime.timedelta(days=7)

CORS(app)
jwt = JWTManager(app)


# ═══════════════════════════════════════════════════════════════════════════════
#  TEMPLATE / STATIC
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/login")
def login_page():
    return render_template("login.html")

@app.route("/static/<path:path>")
def send_static(path):
    return send_from_directory(STATIC_DIR, path)


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTH
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/auth/login", methods=["POST"])
def login():
    data     = request.get_json(force=True)
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    conn = get_connection()
    row  = conn.execute(
        "SELECT password_hash FROM users WHERE username = ?", (username,)
    ).fetchone()
    conn.close()

    if row and row["password_hash"] == hashlib.sha256(password.encode()).hexdigest():
        access  = create_access_token(identity=username)
        refresh = create_refresh_token(identity=username)
        return jsonify(access_token=access, refresh_token=refresh), 200

    return jsonify(msg="Credenziali non valide"), 401


@app.route("/api/auth/refresh", methods=["POST"])
@jwt_required(refresh=True)
def refresh_token():
    identity = get_jwt_identity()
    access   = create_access_token(identity=identity)
    return jsonify(access_token=access), 200


# ═══════════════════════════════════════════════════════════════════════════════
#  CLASSIFY — identifica un singolo file senza salvarlo
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/classify", methods=["POST"])
@jwt_required()
def classify():
    if "file" not in request.files:
        return jsonify(error="Nessun file inviato"), 400

    f         = request.files["file"]
    tmp_path  = os.path.join(ROOT_DIR, "tmp_" + f.filename)
    f.save(tmp_path)

    try:
        res = identify_file_type(tmp_path)
        return jsonify(res)
    except Exception as e:
        return jsonify(categoria="ERRORE", confidenza=0, ragione=str(e)), 500
    finally:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass


# ═══════════════════════════════════════════════════════════════════════════════
#  UPLOAD + PIPELINE COMPLETA
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/upload", methods=["POST"])
@jwt_required()
def upload():
    files = request.files.getlist("files[]")
    if not files:
        return jsonify(error="Nessun file selezionato"), 400

    os.makedirs(EXCEL_DIR, exist_ok=True)

    # Rimuovi i file Excel precedenti così si elaborano SOLO i file appena caricati
    for old_fn in os.listdir(EXCEL_DIR):
        if old_fn.lower().endswith((".xlsx", ".xls")) and not old_fn.startswith("~$"):
            try:
                os.remove(os.path.join(EXCEL_DIR, old_fn))
            except OSError:
                pass

    saved = 0
    for f in files:
        if f.filename and not f.filename.startswith("~$"):
            dest = os.path.join(EXCEL_DIR, f.filename)
            f.save(dest)
            saved += 1

    if saved == 0:
        return jsonify(error="Nessun file salvato"), 400

    logs = [f"Salvati {saved} file in {EXCEL_DIR}"]

    # ── Ingestion ─────────────────────────────────────────────────────────────
    try:
        summary = ingest_folder(EXCEL_DIR)
        logs.append(f"Fortech: {summary.get('FORTECH', 0)} righe")
        logs.append(f"Contanti: {summary.get('CONTANTI', 0)} righe")
        logs.append(f"Carte Bancarie: {summary.get('CARTE_BANCARIE', 0)} righe")
        logs.append(f"Satispay: {summary.get('SATISPAY', 0)} righe")
        logs.append(f"Buoni: {summary.get('BUONI', 0)} righe")
        logs.append(f"Carte Petrolifere: {summary.get('carte_petrolifere', 0)} righe")
    except Exception as e:
        logs.append(f"[ERR] Ingestion: {e}")
        return jsonify(error=str(e), logs=logs), 500

    # ── Riconciliazione ───────────────────────────────────────────────────────
    try:
        n_ric = reconcile()
        logs.append(f"Riconciliazione: {n_ric} record elaborati")
    except Exception as e:
        logs.append(f"[WARN] Riconciliazione: {e}")

    # Conta giornate elaborate
    conn = get_connection()
    days = conn.execute(
        "SELECT COUNT(DISTINCT data) FROM transazioni_fortech").fetchone()[0]
    conn.close()

    return jsonify({
        "files_imported": saved,
        "days_analyzed":  days,
        "status":         "success",
        "logs":           logs,
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD — STATS
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/stats", methods=["GET"])
@jwt_required()
def get_stats():
    conn = get_connection()
    c    = conn.cursor()

    total_impianti = c.execute("SELECT COUNT(*) FROM impianti").fetchone()[0]
    total_giornate = c.execute(
        "SELECT COUNT(DISTINCT data) FROM transazioni_fortech").fetchone()[0]

    quadrate = c.execute(
        "SELECT COUNT(*) FROM riconciliazione_risultati "
        "WHERE stato IN ('QUADRATO','QUADRATO_ARROT')").fetchone()[0]
    anomalie = c.execute(
        "SELECT COUNT(*) FROM riconciliazione_risultati "
        "WHERE stato IN ('ANOMALIA_LIEVE','ANOMALIA_GRAVE','NON_TROVATO')").fetchone()[0]
    anomalie_gravi = c.execute(
        "SELECT COUNT(*) FROM riconciliazione_risultati "
        "WHERE stato = 'ANOMALIA_GRAVE'").fetchone()[0]
    fortech_records = c.execute(
        "SELECT COUNT(*) FROM transazioni_fortech").fetchone()[0]

    conn.close()
    return jsonify({
        "total_impianti":  total_impianti,
        "total_giornate":  total_giornate,
        "quadrate":        quadrate,
        "anomalie_aperte": anomalie,
        "anomalie_gravi":  anomalie_gravi,
        "fortech_records": fortech_records,
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  DASHBOARD — STATO VERIFICHE PER IMPIANTO
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/stato-verifiche", methods=["GET"])
@jwt_required()
def get_stato_verifiche():
    conn = get_connection()

    impianti = conn.execute(
        "SELECT id, codice_pv, nome, tipo_gestione FROM impianti ORDER BY nome").fetchall()

    results = []
    for imp in impianti:
        rows = conn.execute('''
            SELECT categoria, stato
            FROM riconciliazione_risultati
            WHERE codice_pv = ?
            ORDER BY data DESC
        ''', (imp["codice_pv"],)).fetchall()

        # Per ogni categoria: prendi lo stato peggiore più recente
        cat_stato: dict[str, str] = {}
        _priority = {
            "ANOMALIA_GRAVE": 5, "ANOMALIA_LIEVE": 4, "NON_TROVATO": 3,
            "IN_ATTESA": 2, "QUADRATO_ARROT": 1, "QUADRATO": 0,
        }
        for r in rows:
            cat = r["categoria"]
            s   = r["stato"]
            if cat not in cat_stato:
                cat_stato[cat] = s
            elif _priority.get(s, 0) > _priority.get(cat_stato[cat], 0):
                cat_stato[cat] = s

        results.append({
            "id":            imp["id"],
            "nome":          imp["nome"] or f"PV {imp['codice_pv']}",
            "tipo_gestione": imp["tipo_gestione"] or "PRESIDIATO",
            "categorie":     {cat: {"stato": stato} for cat, stato in cat_stato.items()},
        })

    conn.close()
    return jsonify(results)


# ═══════════════════════════════════════════════════════════════════════════════
#  RICONCILIAZIONI — TABELLA
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/riconciliazioni", methods=["GET"])
@jwt_required()
def get_riconciliazioni():
    da = request.args.get("da")
    a  = request.args.get("a")
    pv = request.args.get("pv")

    query  = '''
        SELECT r.id, r.codice_pv, r.data, r.categoria,
               r.valore_teorico, r.valore_reale, r.differenza, r.stato, r.note, r.tipo_match,
               i.nome AS nome_pv

        FROM riconciliazione_risultati r
        LEFT JOIN impianti i ON r.codice_pv = i.codice_pv
        WHERE 1=1
    '''
    params = []
    if da:
        query  += " AND r.data >= ?"
        params.append(da)
    if a:
        query  += " AND r.data <= ?"
        params.append(a)
    if pv:
        query  += " AND r.codice_pv = ?"
        params.append(int(pv))
    query += " ORDER BY r.data DESC, r.codice_pv, r.categoria"

    conn = get_connection()
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return jsonify([{
        "id":            r["id"],
        "data":          r["data"],
        "impianto":      f"{r['codice_pv']} – {r['nome_pv'] or 'N/D'}",
        "categoria":     r["categoria"],
        "valore_fortech": r["valore_teorico"],
        "valore_reale":  r["valore_reale"],
        "differenza":    r["differenza"],
        "stato":         r["stato"],
        "tipo_match":    r["tipo_match"] or "nessuno",
        "note":          r["note"] or "",

    } for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
#  RICONCILIAZIONI — MODIFICA INLINE
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/riconciliazioni/edit", methods=["POST"])
@jwt_required()
def edit_riconciliazione():
    data = request.get_json(force=True)
    rid          = data.get("id")
    valore_reale = data.get("valore_reale")
    note         = data.get("note", "")

    if rid is None:
        return jsonify(error="id mancante"), 400

    conn = get_connection()
    row  = conn.execute(
        "SELECT valore_teorico FROM riconciliazione_risultati WHERE id = ?", (rid,)
    ).fetchone()

    if not row:
        conn.close()
        return jsonify(error="Record non trovato"), 404

    try:
        valore_reale = float(valore_reale)
    except (TypeError, ValueError):
        conn.close()
        return jsonify(error="valore_reale non valido"), 400

    teorico    = float(row["valore_teorico"])
    differenza = round(teorico - valore_reale, 2)

    cfg       = get_config(conn)
    # Determina categoria per scegliere la tolleranza giusta
    cat_row   = conn.execute(
        "SELECT categoria FROM riconciliazione_risultati WHERE id = ?", (rid,)
    ).fetchone()
    cat       = cat_row["categoria"] if cat_row else "contanti"
    tol_map   = {
        "contanti":       float(cfg.get("tolleranza_contanti_arrotondamento", 2.0)),
        "carte_bancarie": float(cfg.get("tolleranza_carte_fisiologica",       1.0)),
        "satispay":       float(cfg.get("tolleranza_satispay",                0.01)),
        "buoni":          float(cfg.get("tolleranza_buoni",                   0.01)),
        "carte_petrolifere": float(cfg.get("tolleranza_petrolifere",             0.01)),
    }
    tol = tol_map.get(cat, 0.01)

    nuovo_stato = _calcola_stato(teorico, valore_reale, tol) or "IN_ATTESA"

    conn.execute('''
        UPDATE riconciliazione_risultati
        SET valore_reale = ?, differenza = ?, stato = ?, note = ?
        WHERE id = ?
    ''', (valore_reale, differenza, nuovo_stato, note, rid))
    conn.commit()
    conn.close()

    return jsonify(differenza=differenza, nuovo_stato=nuovo_stato)


# ═══════════════════════════════════════════════════════════════════════════════
#  RICONCILIAZIONI — EXPORT EXCEL
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/riconciliazioni/export/excel", methods=["GET"])
@jwt_required()
def export_excel():
    da = request.args.get("da")
    a  = request.args.get("a")
    pv = request.args.get("pv")

    query  = '''
        SELECT r.data, i.nome AS impianto, r.categoria,
               r.valore_teorico, r.valore_reale, r.differenza, r.stato, r.note, r.tipo_match

        FROM riconciliazione_risultati r
        LEFT JOIN impianti i ON r.codice_pv = i.codice_pv
        WHERE 1=1
    '''
    params = []
    if da:
        query  += " AND r.data >= ?"
        params.append(da)
    if a:
        query  += " AND r.data <= ?"
        params.append(a)
    if pv:
        query  += " AND r.codice_pv = ?"
        params.append(int(pv))
    query += " ORDER BY r.data DESC, r.categoria"

    conn = get_connection()
    df   = pd.read_sql_query(query, conn, params=params)
    conn.close()

    df.rename(columns={
        "data": "Data", "impianto": "Impianto", "categoria": "Categoria",
        "valore_teorico": "Fortech (€)", "valore_reale": "Reale (€)",
        "differenza": "Diff (€)", "stato": "Stato", "note": "Note",
    }, inplace=True)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Riconciliazioni")
    buf.seek(0)

    fname = f"Riconciliazioni_{datetime.date.today().isoformat()}.xlsx"
    return send_file(buf, as_attachment=True, download_name=fname,
                     mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")


# ═══════════════════════════════════════════════════════════════════════════════
#  CHART DATA
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/chart-data", methods=["GET"])
@jwt_required()
def get_chart_data():
    conn = get_connection()
    rows = conn.execute('''
        SELECT categoria,
               SUM(valore_teorico) AS tot_fortech,
               SUM(valore_reale)   AS tot_reale
        FROM riconciliazione_risultati
        GROUP BY categoria
        ORDER BY tot_fortech DESC
    ''').fetchall()
    conn.close()

    return jsonify([{
        "categoria":   r["categoria"],
        "tot_fortech": round(r["tot_fortech"] or 0, 2),
        "tot_reale":   round(r["tot_reale"] or 0, 2),
    } for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
#  CONTANTI BANCA (Vista Simona)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/contanti-banca", methods=["GET"])
@jwt_required()
def get_contanti_banca():
    conn = get_connection()
    rows = conn.execute('''
        SELECT cm.id, cm.codice_pv, cm.data,
               cm.contanti_teorico, cm.contanti_versato, cm.differenza,
               cm.stato, cm.tipo_match, cm.risolto,
               cm.verificato_da, cm.data_verifica, cm.note,
               i.nome AS nome_pv
        FROM contanti_matching cm
        LEFT JOIN impianti i ON cm.codice_pv = i.codice_pv
        ORDER BY cm.data DESC, cm.codice_pv
    ''').fetchall()
    conn.close()

    return jsonify([{
        "id":               r["id"],
        "data":             r["data"],
        "impianto":         f"{r['codice_pv']} – {r['nome_pv'] or 'N/D'}",
        "contanti_teorico": r["contanti_teorico"],
        "contanti_versato": r["contanti_versato"],
        "differenza":       r["differenza"],
        "stato":            r["stato"],
        "tipo_match":       r["tipo_match"] or "nessuno",
        "risolto":          bool(r["risolto"]),
        "verificato_da":    r["verificato_da"] or "",
        "data_verifica":    r["data_verifica"] or "",
        "note":             r["note"] or "",
    } for r in rows])


# ═══════════════════════════════════════════════════════════════════════════════
#  CONTANTI — CONFERMA / SEGNALAZIONE
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/contanti-conferma", methods=["POST"])
@jwt_required()
def contanti_conferma():
    data   = request.get_json(force=True)
    rid    = data.get("id")
    azione = data.get("azione")  # "conferma" | "rifiuta"
    nota   = data.get("nota", "")

    if rid is None or azione not in ("conferma", "rifiuta"):
        return jsonify(error="Parametri non validi"), 400

    identity = get_jwt_identity()
    now      = datetime.date.today().isoformat()

    conn = get_connection()
    if azione == "conferma":
        conn.execute('''
            UPDATE contanti_matching
            SET risolto=1, verificato_da=?, data_verifica=?
            WHERE id=?
        ''', (identity, now, rid))
    else:
        conn.execute('''
            UPDATE contanti_matching
            SET stato='ANOMALIA_GRAVE', risolto=0,
                verificato_da=?, data_verifica=?, note=?
            WHERE id=?
        ''', (identity, now, nota, rid))

    conn.commit()
    conn.close()
    return jsonify(ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  IMPIANTI — LISTA
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/impianti", methods=["GET"])
@jwt_required()
def get_impianti():
    conn = get_connection()
    rows = conn.execute(
        "SELECT id, codice_pv, nome, comune, indirizzo, tipo_gestione FROM impianti ORDER BY nome"
    ).fetchall()

    results = []
    for r in rows:
        stats = conn.execute('''
            SELECT
                SUM(CASE WHEN stato IN ('QUADRATO','QUADRATO_ARROT') THEN 1 ELSE 0 END) AS cnt_ok,
                SUM(CASE WHEN stato = 'ANOMALIA_LIEVE'               THEN 1 ELSE 0 END) AS cnt_warn,
                SUM(CASE WHEN stato = 'ANOMALIA_GRAVE'               THEN 1 ELSE 0 END) AS cnt_grave
            FROM riconciliazione_risultati WHERE codice_pv = ?
        ''', (r["codice_pv"],)).fetchone()

        results.append({
            "id":           r["id"],
            "codice_pv":    r["codice_pv"],
            "nome":         r["nome"] or f"PV {r['codice_pv']}",
            "comune":       r["comune"],
            "indirizzo":    r["indirizzo"],
            "tipo":         r["tipo_gestione"] or "PRESIDIATO",
            "cnt_ok":       stats["cnt_ok"] or 0,
            "cnt_warn":     stats["cnt_warn"] or 0,
            "cnt_grave":    stats["cnt_grave"] or 0,
        })

    conn.close()
    return jsonify(results)


# ═══════════════════════════════════════════════════════════════════════════════
#  IMPIANTI — ANDAMENTO GIORNALIERO (per Modal)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/impianti/<int:imp_id>/andamento", methods=["GET"])
@jwt_required()
def get_andamento(imp_id):
    conn = get_connection()
    imp  = conn.execute(
        "SELECT id, codice_pv, nome, tipo_gestione FROM impianti WHERE id = ?", (imp_id,)
    ).fetchone()

    if not imp:
        conn.close()
        return jsonify(error="Impianto non trovato"), 404

    rows = conn.execute('''
        SELECT data, categoria, valore_teorico, valore_reale, differenza, stato
        FROM riconciliazione_risultati
        WHERE codice_pv = ?
        ORDER BY data DESC, categoria
    ''', (imp["codice_pv"],)).fetchall()
    conn.close()

    # Raggruppa per data
    _priority = {
        "ANOMALIA_GRAVE": 5, "ANOMALIA_LIEVE": 4, "NON_TROVATO": 3,
        "IN_ATTESA": 2, "QUADRATO_ARROT": 1, "QUADRATO": 0,
    }
    giorni_dict: dict[str, dict] = {}
    for r in rows:
        d = r["data"]
        if d not in giorni_dict:
            giorni_dict[d] = {"data": d, "categorie": {}, "totale_diff": 0.0, "stato_peggiore": "QUADRATO"}
        giorni_dict[d]["categorie"][r["categoria"]] = {
            "teorico":    r["valore_teorico"],
            "reale":      r["valore_reale"],
            "differenza": r["differenza"],
            "stato":      r["stato"],
        }
        giorni_dict[d]["totale_diff"] = round(
            giorni_dict[d]["totale_diff"] + (r["differenza"] or 0), 2)
        if _priority.get(r["stato"], 0) > _priority.get(giorni_dict[d]["stato_peggiore"], 0):
            giorni_dict[d]["stato_peggiore"] = r["stato"]

    giorni = list(giorni_dict.values())

    # Statistiche aggregate
    stati_cnt = Counter(g["stato_peggiore"] for g in giorni)

    return jsonify({
        "impianto":     dict(imp),
        "totale_giorni": len(giorni),
        "stats":        dict(stati_cnt),
        "giorni":       giorni,
    })


# ═══════════════════════════════════════════════════════════════════════════════
#  SICUREZZA (Placeholder — Taleggio IoT non ancora integrato)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/sicurezza", methods=["GET"])
@jwt_required()
def get_sicurezza():
    return jsonify([])


# ═══════════════════════════════════════════════════════════════════════════════
#  AI REPORT (OpenRouter / GPT-4o Mini)
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/ai-report", methods=["POST"])
@jwt_required()
def ai_report():
    import urllib.request
    import json

    conn = get_connection()
    cfg  = get_config(conn)
    api_key = cfg.get("openrouter_api_key", "").strip()

    if not api_key:
        conn.close()
        return jsonify(error="Chiave API OpenRouter non configurata. Vai in Impostazioni."), 400

    # Prepara il contesto con i dati di riconciliazione
    rows = conn.execute('''
        SELECT r.data, i.nome, r.categoria, r.valore_teorico, r.valore_reale,
               r.differenza, r.stato
        FROM riconciliazione_risultati r
        LEFT JOIN impianti i ON r.codice_pv = i.codice_pv
        WHERE r.stato NOT IN ('QUADRATO')
        ORDER BY ABS(r.differenza) DESC
        LIMIT 50
    ''').fetchall()
    conn.close()

    if not rows:
        return jsonify(report="Nessuna anomalia rilevata. Tutti i dati sono quadrati."), 200

    context_lines = ["Data | Impianto | Categoria | Fortech | Reale | Diff | Stato"]
    for r in rows:
        context_lines.append(
            f"{r['data']} | {r['nome'] or 'N/D'} | {r['categoria']} | "
            f"€{r['valore_teorico']:.2f} | €{r['valore_reale']:.2f} | "
            f"€{r['differenza']:.2f} | {r['stato']}"
        )
    context = "\n".join(context_lines)

    prompt = (
        "Sei un esperto di riconciliazione contabile per stazioni di servizio. "
        "Analizza le seguenti anomalie rilevate nel sistema e fornisci:\n"
        "1. Un riepilogo delle anomalie più critiche\n"
        "2. Possibili cause per le discrepanze più gravi\n"
        "3. Azioni consigliate per la risoluzione\n\n"
        f"Dati anomalie:\n{context}"
    )

    payload = json.dumps({
        "model": "openai/gpt-4o-mini",
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 1500,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
            "HTTP-Referer":  "https://calor-systems.local",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read().decode("utf-8"))
        report_text = result["choices"][0]["message"]["content"]
        return jsonify(report=report_text)
    except Exception as e:
        return jsonify(error=f"Errore API OpenRouter: {e}"), 502



# ═══════════════════════════════════════════════════════════════
#  CONTANTI — RIEPILOGO AGGREGATO PER PV
# ═══════════════════════════════════════════════════════════════

@app.route("/api/contanti-riepilogo", methods=["GET"])
@jwt_required()
def get_contanti_riepilogo():
    conn = get_connection()
    # Calcola somme per PV
    query = '''
        SELECT 
            i.codice_pv, 
            i.nome,
            (SELECT COALESCE(SUM(totale_contante), 0) FROM transazioni_fortech WHERE codice_pv = i.codice_pv) as tot_teorico,
            (SELECT COALESCE(SUM(importo), 0) FROM transazioni_contanti WHERE codice_pv = i.codice_pv) as tot_reale
        FROM impianti i
        ORDER BY i.nome
    '''
    rows = conn.execute(query).fetchall()
    conn.close()

    results = []
    for r in rows:
        teorico = r["tot_teorico"]
        reale = r["tot_reale"]
        diff = round(teorico - reale, 2)
        results.append({
            "codice_pv": r["codice_pv"],
            "nome": r["nome"] or f"PV {r['codice_pv']}",
            "tot_teorico": teorico,
            "tot_reale": reale,
            "differenza": diff
        })
    return jsonify(results)


# ═══════════════════════════════════════════════════════════════════════════════
#  IMPOSTAZIONI — CONFIG
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/settings/config", methods=["GET", "POST"])
@jwt_required()
def settings_config():
    conn = get_connection()
    if request.method == "GET":
        cfg = get_config(conn)
        conn.close()
        # Ritorna solo le chiavi non sensibili
        safe_keys = [
            "tolleranza_contanti_arrotondamento", "tolleranza_carte_fisiologica",
            "tolleranza_satispay", "scarto_giorni_buoni",
            "scarto_giorni_contanti_inf", "scarto_giorni_contanti_sup",
        ]
        return jsonify({k: float(cfg[k]) if k in cfg else 0 for k in safe_keys})

    data = request.get_json(force=True)
    for k, v in data.items():
        conn.execute(
            "INSERT INTO config (chiave, valore) VALUES (?, ?) "
            "ON CONFLICT(chiave) DO UPDATE SET valore = excluded.valore",
            (k, str(v))
        )
    conn.commit()
    conn.close()
    return jsonify(ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  IMPOSTAZIONI — PASSWORD
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/settings/password", methods=["POST"])
@jwt_required()
def settings_password():
    identity = get_jwt_identity()
    data     = request.get_json(force=True)
    old_pw   = data.get("old_password", "")
    new_pw   = data.get("new_password", "")

    if len(new_pw) < 8:
        return jsonify(msg="La nuova password deve avere almeno 8 caratteri"), 400

    conn = get_connection()
    row  = conn.execute(
        "SELECT password_hash FROM users WHERE username = ?", (identity,)
    ).fetchone()

    if not row or row["password_hash"] != hashlib.sha256(old_pw.encode()).hexdigest():
        conn.close()
        return jsonify(msg="Vecchia password non corretta"), 400

    new_hash = hashlib.sha256(new_pw.encode()).hexdigest()
    conn.execute(
        "UPDATE users SET password_hash = ? WHERE username = ?", (new_hash, identity)
    )
    conn.commit()
    conn.close()
    return jsonify(ok=True)


# ═══════════════════════════════════════════════════════════════════════════════
#  IMPOSTAZIONI — API KEY
# ═══════════════════════════════════════════════════════════════════════════════

@app.route("/api/settings/apikey", methods=["GET", "POST"])
@jwt_required()
def settings_apikey():
    conn = get_connection()
    if request.method == "GET":
        cfg = get_config(conn)
        conn.close()
        key = cfg.get("openrouter_api_key", "")
        if key:
            masked = key[:8] + "…" + key[-4:]
            return jsonify(has_key=True, masked=masked)
        return jsonify(has_key=False, masked="")

    data    = request.get_json(force=True)
    api_key = data.get("api_key", "").strip()
    if not api_key:
        conn.close()
        return jsonify(error="Chiave vuota"), 400

    conn.execute(
        "INSERT INTO config (chiave, valore) VALUES ('openrouter_api_key', ?) "
        "ON CONFLICT(chiave) DO UPDATE SET valore = excluded.valore", (api_key,)
    )
    conn.commit()
    conn.close()
    return jsonify(message="Chiave API salvata con successo")


@app.route("/api/settings/apikey/test", methods=["POST"])
@jwt_required()
def settings_apikey_test():
    import urllib.request
    import json

    data    = request.get_json(force=True)
    api_key = data.get("api_key", "").strip()

    if not api_key:
        conn = get_connection()
        cfg  = get_config(conn)
        conn.close()
        api_key = cfg.get("openrouter_api_key", "").strip()

    if not api_key:
        return jsonify(error="Nessuna chiave API configurata"), 400

    payload = json.dumps({
        "model": "openai/gpt-4o-mini",
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 5,
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://openrouter.ai/api/v1/chat/completions",
        data=payload,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type":  "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
        return jsonify(message="✅ Connessione OpenRouter OK")
    except urllib.error.HTTPError as e:
        if e.code == 401:
            return jsonify(error="❌ Chiave non valida (401 Unauthorized)"), 400
        return jsonify(error=f"❌ HTTP {e.code}: {e.reason}"), 502
    except Exception as e:
        return jsonify(error=f"❌ Connessione fallita: {e}"), 502


# ═══════════════════════════════════════════════════════════════════════════════
#  AVVIO
# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
