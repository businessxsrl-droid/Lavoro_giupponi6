/* ═══════════════════════════════════════════════════════════════
   CALOR SYSTEMS — Dashboard Application
   ═══════════════════════════════════════════════════════════════ */

// ── State ──
let selectedFiles = [];
const CATEGORY_LABELS = {
    carte_bancarie: { label: '💳 Carte Bancarie', color: '#3fb950' },
    carte_petrolifere: { label: '⛽ Carte Petrolifere', color: '#58a6ff' },
    satispay: { label: '📱 Satispay', color: '#8b949e' },
    crediti: { label: '🟣 Crediti', color: '#bc8cff' },
};

const STATUS_MAP = {
    QUADRATO: { label: '✅ Quadrato', css: 'status-quadrato' },
    QUADRATO_ARROT: { label: '✅ Quadrato (≈)', css: 'status-quadrato-arrot' },
    QUADRATO_COMPENSATO: { label: '🔁 Compensato', css: 'status-quadrato-arrot' },
    ANOMALIA_LIEVE: { label: '⚠️ Anomalia lieve', css: 'status-anomalia-lieve' },
    ANOMALIA_GRAVE: { label: '🔴 Anomalia grave', css: 'status-anomalia-grave' },
    NON_TROVATO: { label: '❓ Non trovato', css: 'status-non-trovato' },
    IN_ATTESA: { label: '⏳ In attesa', css: 'status-in-attesa' },
    INCOMPLETO: { label: '❓ Incompleto', css: 'status-incompleto' },
};

// File type detection (mirrors backend FileClassifier)
function classifyFile(name) {
    const n = name.toLowerCase();
    if (n.includes('fortech') || n.includes('file generale') || n.includes('a_file'))
        return { type: 'FORTECH', label: 'Fortech', css: 'badge-fortech' };
    if (n.includes('as400') || n.includes('giallo'))
        return { type: 'AS400', label: 'AS400', css: 'badge-as400' };
    if (n.includes('numia') || n.includes('carte bancarie') || n.includes('verde'))
        return { type: 'NUMIA', label: 'Numia', css: 'badge-numia' };
    if (n.includes('carte petrolifere') || n.includes('azzurro') || (n.includes('ip') && n.includes('carte')))
        return { type: 'IP_CARTE', label: 'iP Carte', css: 'badge-ip-carte' };
    if (n.includes('buoni') || n.includes('rosso'))
        return { type: 'IP_BUONI', label: 'iP Buoni', css: 'badge-ip-buoni' };
    if (n.includes('satispay') || n.includes('grigio'))
        return { type: 'SATISPAY', label: 'Satispay', css: 'badge-satispay' };
    return { type: 'UNKNOWN', label: '???', css: 'badge-unknown' };
}

// ═══════════════════════════════════════════════════════════════
// NAVIGATION
// ═══════════════════════════════════════════════════════════════

const navItems = document.querySelectorAll('.nav-item');
const views = document.querySelectorAll('.view');

function switchView(viewName) {
    // Update nav
    navItems.forEach(item => {
        item.classList.toggle('active', item.dataset.view === viewName);
    });
    // Update views
    views.forEach(v => {
        v.classList.toggle('active', v.id === `view-${viewName}`);
    });
    // Update title
    const titles = {
        dashboard: '📊 Dashboard',
        upload: '📂 Carica File Excel',
        riconciliazioni: '🔄 Riconciliazioni',
        impianti: '🏢 Impianti',
        sicurezza: '🔐 Sicurezza',
        verifica: '✅ Verifica Compensazioni',
        'ai-report': '🤖 Report AI',
        settings: '⚙️ Impostazioni Sistema',
    };
    document.getElementById('pageTitle').textContent = titles[viewName] || viewName;

    // Load data for the view
    if (viewName === 'dashboard') loadDashboard();
    if (viewName === 'riconciliazioni') { populatePvFilter(); loadRiconciliazioni(); }
    if (viewName === 'impianti') loadImpianti();
    if (viewName === 'sicurezza') loadSicurezza();
    if (viewName === 'settings') loadConfig();
    if (viewName === 'verifica') loadVerifica();
    if (viewName === 'ai-report') loadAIModels();

    // Close mobile sidebar
    document.getElementById('sidebar').classList.remove('open');
}

navItems.forEach(item => {
    item.addEventListener('click', () => switchView(item.dataset.view));
});

// Mobile menu toggle
document.getElementById('menuToggle').addEventListener('click', () => {
    document.getElementById('sidebar').classList.toggle('open');
});

// ═══════════════════════════════════════════════════════════════
// FILE UPLOAD
// ═══════════════════════════════════════════════════════════════

const uploadZone = document.getElementById('uploadZone');
const fileInput = document.getElementById('fileInput');

// Click to select
uploadZone.addEventListener('click', () => fileInput.click());

// Drag & Drop
uploadZone.addEventListener('dragover', (e) => {
    e.preventDefault();
    uploadZone.classList.add('drag-over');
});
uploadZone.addEventListener('dragleave', () => {
    uploadZone.classList.remove('drag-over');
});
uploadZone.addEventListener('drop', (e) => {
    e.preventDefault();
    uploadZone.classList.remove('drag-over');
    addFiles(e.dataTransfer.files);
});

// File input change
fileInput.addEventListener('change', () => {
    addFiles(fileInput.files);
    fileInput.value = ''; // Reset so same file can be selected again
});

function addFiles(fileList) {
    for (const file of fileList) {
        // Only accept Excel/CSV
        if (!file.name.match(/\.(xlsx|xls|csv)$/i)) continue;
        // Avoid duplicates
        if (selectedFiles.some(f => f.name === file.name && f.size === file.size)) continue;
        selectedFiles.push(file);
    }
    renderFileList();
}

function removeFile(index) {
    selectedFiles.splice(index, 1);
    renderFileList();
}

function clearFiles() {
    selectedFiles = [];
    renderFileList();
}

function formatSize(bytes) {
    if (bytes < 1024) return bytes + ' B';
    if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB';
    return (bytes / (1024 * 1024)).toFixed(1) + ' MB';
}

function renderFileList() {
    const container = document.getElementById('fileList');
    const actions = document.getElementById('uploadActions');

    if (selectedFiles.length === 0) {
        container.innerHTML = '';
        actions.style.display = 'none';
        return;
    }

    actions.style.display = 'flex';
    container.innerHTML = selectedFiles.map((file, i) => {
        // Mostriamo uno stato di caricamento/identificazione
        const fileId = `file-item-${i}`;
        const badgeId = `badge-${i}`;
        
        // Eseguiamo l'identificazione asincrona
        setTimeout(() => identifyFileRemote(file, badgeId), 0);

        return `
            <div class="file-item" id="${fileId}">
                <span id="${badgeId}" class="file-type-badge badge-unknown">⏳ Identificazione...</span>
                <span class="file-name">${file.name}</span>
                <span class="file-size">${formatSize(file.size)}</span>
                <button class="file-remove" onclick="removeFile(${i})" title="Rimuovi">✕</button>
            </div>
        `;
    }).join('');
}

/**
 * Chiama l'endpoint /api/classify del backend per identificare il file in base al contenuto.
 */
async function identifyFileRemote(file, badgeId) {
    const badge = document.getElementById(badgeId);
    if (!badge) return;

    try {
        const formData = new FormData();
        formData.append('file', file);

        const data = await apiFetch('/api/classify', {
            method: 'POST',
            body: formData
        });

        if (!data) throw new Error("Errore classificazione");
        
        // Mappa le categorie del backend ai badge del frontend
        const categoryMap = {
            'FORTECH': { label: 'Fortech', css: 'badge-fortech' },
            'carte_bancarie': { label: '💳 Carte Bancarie', css: 'badge-numia' },
            'carte_petrolifere': { label: '⛽ Carte Petrolifere', css: 'badge-ip-carte' },
            'buoni': { label: '🎟️ Buoni / iP', css: 'badge-ip-buoni' },
            'satispay': { label: '📱 Satispay', css: 'badge-satispay' },
            'ANAGRAFICA': { label: '🏢 Anagrafica PV', css: 'badge-fortech' },
            'SCONOSCIUTO': { label: '❓ Sconosciuto', css: 'badge-unknown' }
        };

        const theme = categoryMap[data.categoria] || { label: data.categoria, css: 'badge-unknown' };
        
        badge.textContent = theme.label;
        badge.className = `file-type-badge ${theme.css}`;
        badge.title = `${data.ragione} (Confidenza: ${data.confidenza}%)`;

    } catch (err) {
        console.error("Identificazione fallita:", err);
        // Fallback alla logica locale basata su nome file se il server fallisce
        const cls = classifyFile(file.name);
        badge.textContent = cls.label;
        badge.className = `file-type-badge ${cls.css}`;
    }
}

// ═══════════════════════════════════════════════════════════════
// PROCESS FILES (Upload + Import + Analyze)
// ═══════════════════════════════════════════════════════════════

async function processFiles() {
    if (selectedFiles.length === 0) return;

    const btn = document.getElementById('btnProcess');
    btn.disabled = true;

    // Show progress
    document.getElementById('progressSection').style.display = 'block';
    document.getElementById('resultsSummary').style.display = 'none';

    const fill = document.getElementById('progressFill');
    const phase = document.getElementById('progressPhase');
    const pct = document.getElementById('progressPct');
    const log = document.getElementById('progressLog');

    phase.textContent = 'Caricamento file...';
    pct.textContent = '10%';
    fill.style.width = '10%';
    log.innerHTML = '';
    logLine(log, 'Preparazione upload...');

    // Build FormData
    const formData = new FormData();
    selectedFiles.forEach(f => formData.append('files[]', f));

    try {
        logLine(log, `Invio di ${selectedFiles.length} file al server...`);
        phase.textContent = 'Elaborazione sul server...';
        pct.textContent = '30%';
        fill.style.width = '30%';

        const token = localStorage.getItem("access_token");
        const resp = await fetch('/api/upload', {
            method: 'POST',
            headers: token ? { 'Authorization': `Bearer ${token}` } : {},
            body: formData,
        });

        pct.textContent = '90%';
        fill.style.width = '90%';

        // Controlla se la risposta è HTML (errore del server) prima di parsare JSON
        const contentType = resp.headers.get('content-type') || '';
        if (!contentType.includes('application/json')) {
            const text = await resp.text();
            throw new Error(`Il server ha restituito un errore (HTTP ${resp.status}). Controlla i log di Render.`);
        }

        const data = await resp.json();

        if (!resp.ok) {
            throw new Error(data.error || 'Errore sconosciuto');
        }

        // Show logs from server
        if (data.logs) {
            data.logs.forEach(msg => logLine(log, msg));
        }

        pct.textContent = '100%';
        fill.style.width = '100%';
        phase.textContent = '✅ Completato!';

        logLine(log, `Importati ${data.files_imported} file, analizzate ${data.days_analyzed} giornate.`);

        // Show results summary
        document.getElementById('resultsSummary').style.display = 'block';
        document.getElementById('resultsStats').textContent =
            `${data.files_imported} file importati — ${data.days_analyzed} giornate elaborate`;

        // Clear selection
        selectedFiles = [];
        renderFileList();

    } catch (err) {
        phase.textContent = '❌ Errore';
        logLine(log, `ERRORE: ${err.message}`);
        pct.textContent = 'Errore';
        fill.style.width = '100%';
        fill.style.background = 'var(--status-danger)';
    } finally {
        btn.disabled = false;
    }
}

function logLine(container, msg) {
    const time = new Date().toLocaleTimeString('it-IT');
    container.innerHTML += `[${time}] ${msg}\n`;
    container.scrollTop = container.scrollHeight;
}

// ═══════════════════════════════════════════════════════════════
// API CALLS & RENDERING
// ═══════════════════════════════════════════════════════════════

async function apiFetch(endpoint, options = {}) {
    // Prevent aggressive browser caching for all our API endpoints
    const defaultOptions = { cache: 'no-store', ...options };
    let url = endpoint;

    // Manual cache-busting query parameter solo per GET
    if (!options.method || options.method.toUpperCase() === 'GET') {
        url = endpoint.includes('?')
            ? `${endpoint}&_t=${Date.now()}`
            : `${endpoint}?_t=${Date.now()}`;
    }

    const resp = await fetch(url, defaultOptions);
    if (!resp.ok) {
        // Prova a leggere il JSON di errore dal server
        let errData = {};
        try { errData = await resp.json(); } catch(e) {}
        throw new Error(errData.error || errData.msg || `HTTP ${resp.status}`);
    }
    return await resp.json();
}

// ── Chart Incassi ──
let _chartFortechInst = null;
let _chartRealeInst = null;

async function loadChartData() {
    const data = await apiFetch('/api/chart-data');
    if (!data || !data.length) {
        document.getElementById('chartPanel').style.display = 'none';
        return;
    }
    document.getElementById('chartPanel').style.display = 'block';

    const CAT_COLORS = {
        carte_bancarie: '#3fb950',
        carte_petrolifere: '#58a6ff',
        satispay: '#8b949e',
        crediti: '#bc8cff',
    };
    const DEFAULT_CLR = ['#f78166', '#79c0ff', '#56d364', '#e3b341', '#d2a8ff', '#ffa657'];

    const labels = data.map(r => (CATEGORY_LABELS[r.categoria]?.label ?? r.categoria).replace(/^.\s/, ''));
    const fortech = data.map(r => r.tot_fortech);
    const reale = data.map(r => r.tot_reale);
    const colors = data.map((r, i) => CAT_COLORS[r.categoria] ?? DEFAULT_CLR[i % DEFAULT_CLR.length]);

    const makeDataset = values => ({
        data: values,
        backgroundColor: colors.map(c => c + 'cc'),
        borderColor: colors,
        borderWidth: 2,
        hoverOffset: 14,
    });

    const chartOpts = {
        type: 'doughnut',
        options: {
            responsive: true,
            maintainAspectRatio: false,
            cutout: '58%',
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: ctx => {
                            const v = ctx.parsed;
                            const tot = ctx.dataset.data.reduce((a, b) => (parseFloat(a) || 0) + (parseFloat(b) || 0), 0);
                            const val = parseFloat(v) || 0;
                            const pct = (tot > 0) ? ((val / tot) * 100).toFixed(1) : 0;
                            return ` € ${val.toLocaleString('it-IT', { minimumFractionDigits: 2 })}  (${pct}%)`;
                        }
                    }
                }
            },
            animation: { animateRotate: true, duration: 900 }
        }
    };

    const buildLegend = (elId, values) => {
        const tot = values.reduce((a, b) => (parseFloat(a) || 0) + (parseFloat(b) || 0), 0);
        document.getElementById(elId).innerHTML = data.map((r, i) => {
            const lbl = (CATEGORY_LABELS[r.categoria]?.label ?? r.categoria);
            const val = parseFloat(values[i]) || 0;
            const pct = (tot > 0) ? ((val / tot) * 100).toFixed(1) : 0;
            const fmt = val.toLocaleString('it-IT', { minimumFractionDigits: 2 });
            return `<div class="chart-legend-item">
                <span class="chart-legend-dot" style="background:${colors[i]}"></span>
                <span class="chart-legend-label">${lbl}</span>
                <span class="chart-legend-val">€ ${fmt}</span>
                <span class="chart-legend-pct">${pct}%</span>
            </div>`;
        }).join('');
    };

    if (_chartFortechInst) _chartFortechInst.destroy();
    if (_chartRealeInst) _chartRealeInst.destroy();

    _chartFortechInst = new Chart(document.getElementById('chartFortech'), {
        ...chartOpts, data: { labels, datasets: [makeDataset(fortech)] }
    });
    _chartRealeInst = new Chart(document.getElementById('chartReale'), {
        ...chartOpts, data: { labels, datasets: [makeDataset(reale)] }
    });

    buildLegend('legendFortech', fortech);
    buildLegend('legendReale', reale);
}

function renderStatus(stato) {
    const s = STATUS_MAP[stato] || { label: stato, css: 'status-non-trovato' };
    return `<span class="status-badge ${s.css}">${s.label}</span>`;
}

function renderDiff(val) {
    if (val === null || val === undefined) return '—';
    const v = parseFloat(val);
    if (v === 0) return `<span class="diff-zero">€ 0.00</span>`;
    const cls = v > 0 ? 'diff-positive' : 'diff-negative';
    return `<span class="${cls}">€ ${v >= 0 ? '+' : ''}${v.toFixed(2)}</span>`;
}

function renderMoney(val) {
    if (val === null || val === undefined) return '—';
    return `€ ${parseFloat(val).toLocaleString('it-IT', { minimumFractionDigits: 2 })}`;
}

function renderCatBadge(cat) {
    const c = CATEGORY_LABELS[cat] || { label: cat };
    return `<span class="cat-badge cat-${cat}">${c.label || cat}</span>`;
}

// ── Dashboard ──
async function loadDashboard() {
    const stats = await apiFetch('/api/stats');
    if (stats) {
        setStatValue('statImpianti', stats.total_impianti ?? '—');
        setStatValue('statGiornate', stats.total_giornate ?? '—');
        setStatValue('statQuadrate', stats.quadrate ?? '—');
        setStatValue('statAnomalie', stats.anomalie_aperte ?? '—');
        setStatValue('statGravi', stats.anomalie_gravi ?? '—');
        setStatValue('statRecords', stats.fortech_records ?? '—');
    }
    loadStatoVerifiche();
}

function setStatValue(id, val) {
    const el = document.getElementById(id);
    if (!el) return;
    const valEl = el.querySelector('.stat-value');
    if (valEl) {
        valEl.textContent = val;
        valEl.style.animation = 'none';
        valEl.offsetHeight; // trigger reflow
        valEl.style.animation = 'fadeIn 0.3s ease';
    }
}

async function loadStatoVerifiche() {
    const data = await apiFetch('/api/stato-verifiche');
    const grid = document.getElementById('verificheGrid');

    if (!data || data.length === 0) {
        grid.innerHTML = '<div class="empty-state">Nessun dato disponibile. Carica i file Excel dalla sezione "Carica File".</div>';
        return;
    }

    grid.innerHTML = data.map(imp => {
        const cats = Object.entries(imp.categorie || {}).map(([cat, det]) => {
            const c = CATEGORY_LABELS[cat] || { color: '#8b949e' };
            const sMap = {
                QUADRATO: '#3fb950', QUADRATO_ARROT: '#56d364',
                ANOMALIA_LIEVE: '#d29922', ANOMALIA_GRAVE: '#f85149',
                IN_ATTESA: '#58a6ff', NON_TROVATO: '#8b949e',
            };
            const dotColor = sMap[det.stato] || '#8b949e';
            return `
                <div class="verifiche-row">
                    <div class="verifiche-cat">
                        <span class="verifiche-dot" style="background:${dotColor}"></span>
                        <span>${CATEGORY_LABELS[cat]?.label || cat}</span>
                    </div>
                    ${renderStatus(det.stato)}
                </div>
            `;
        }).join('');

        return `
            <div class="verifiche-card">
                <div class="verifiche-card-header">
                    <h4>${imp.nome}</h4>
                    <span class="verifiche-tipo">${imp.tipo_gestione || 'PRESIDIATO'}</span>
                </div>
                <div class="verifiche-body">
                    ${cats || '<div style="color:var(--text-muted);font-size:12px">Nessuna verifica</div>'}
                </div>
            </div>
        `;
    }).join('');
}

// ── Riconciliazioni ──
async function populatePvFilter() {
    const select = document.getElementById('filterPv');
    const current = select.value;
    const data = await apiFetch('/api/impianti');
    if (!data) return;
    // Mantieni opzione "Tutti" e ricostruisci le opzioni
    select.innerHTML = '<option value="">Tutti i punti vendita</option>';
    data.forEach(imp => {
        const opt = document.createElement('option');
        opt.value = imp.codice_pv;
        opt.textContent = imp.nome || `PV ${imp.codice_pv}`;
        select.appendChild(opt);
    });
    // Ripristina selezione precedente se ancora valida
    if (current) select.value = current;
}

async function loadRiconciliazioni() {
    const da = document.getElementById('filterDa').value;
    const a = document.getElementById('filterA').value;
    const pv = document.getElementById('filterPv').value;

    let url = '/api/riconciliazioni?';
    if (da) url += `da=${da}&`;
    if (a) url += `a=${a}&`;
    if (pv) url += `pv=${pv}&`;

    const data = await apiFetch(url);
    const container = document.getElementById('riconciliazioniTablesContainer');

    if (!data || data.length === 0) {
        container.innerHTML = '<div class="table-container"><table class="data-table"><tr><td class="empty-state" style="padding: 40px;">Nessun dato. Carica i file Excel per generare i risultati.</td></tr></table></div>';
        return;
    }

    const CATEGORIE_INFORMATIVE = ['prove_erogazione', 'clienti_fine_mese', 'diversi'];
    const categorie = [...new Set(data.map(r => r.categoria))];
    let html = '';

    for (const cat of categorie) {
        if (CATEGORIE_INFORMATIVE.includes(cat)) continue;
        const catData = data.filter(r => r.categoria === cat);
        const catLabel = CATEGORY_LABELS[cat] ? CATEGORY_LABELS[cat].label : cat;

        html += `
        <div class="category-section" id="section-${cat}">
            <h4 style="margin: 30px 0 10px 0; color: var(--text-primary); border-bottom: 2px solid var(--border-color); padding-bottom: 8px; display: flex; align-items: center; justify-content: space-between;">
                <div style="display: flex; align-items: center; gap: 8px;">
                    ${CATEGORY_LABELS[cat] ? '<span style="color:' + CATEGORY_LABELS[cat].color + '">●</span>' : ''} ${catLabel}
                </div>
                <button class="btn-toggle-section" onclick="toggleSection('wrapper-${cat}', this)" title="Nascondi/Mostra">
                    👁️
                </button>
            </h4>
            <div class="collapsible-wrapper" id="wrapper-${cat}">
                <div class="table-container" style="margin-bottom: 20px; max-height: 400px; overflow-y: auto;">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th style="width: 120px;">Data</th>
                                <th>Impianto</th>
                                <th style="width: 120px; text-align: right;">Fortech (€)</th>
                                <th style="width: 120px; text-align: right;">Reale (€)</th>
                                <th style="width: 120px; text-align: right;">Diff (€)</th>
                                <th style="width: 130px;">Stato</th>
                                <th style="width: 200px;">Note</th>
                                <th style="width: 70px; text-align: center;">Azioni</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${catData.map(r => `
                                <tr id="row-ric-${r.id}">
                                    <td>${r.data || '—'}</td>
                                    <td style="font-weight: 500;">${r.impianto || '—'}</td>
                                    <td style="text-align: right;">${renderMoney(r.valore_fortech)}</td>

                                    <td style="text-align: right;">
                                        <span class="val-reale-text" id="reale-txt-${r.id}">${renderMoney(r.valore_reale)}</span>
                                        <input type="number" step="0.01" class="edit-input edit-reale" id="reale-inp-${r.id}" value="${r.valore_reale !== null ? r.valore_reale : ''}" style="display:none; width:80px; text-align: right;">
                                    </td>

                                    <td style="text-align: right; font-weight: bold;" id="diff-cell-${r.id}">${renderDiff(r.differenza)}</td>
                                    <td id="stato-cell-${r.id}" style="white-space: nowrap;">
                                        ${renderStatus(r.stato)}
                                        ${r.tipo_match && r.tipo_match !== 'nessuno' ? `<span style="margin-left:5px; cursor:help;" title="${TIPO_MATCH_LABELS[r.tipo_match]?.label || r.tipo_match}">${TIPO_MATCH_LABELS[r.tipo_match]?.icon || ''}</span>` : ''}
                                    </td>
                                    
                                    <td style="font-size: 11px; color: var(--text-secondary); line-height: 1.4;">
                                        <span class="val-note-text" id="note-txt-${r.id}">${r.note || ''}</span>
                                        <input type="text" class="edit-input edit-note" id="note-inp-${r.id}" value="${r.note || ''}" style="display:none; width:100%">
                                    </td>

                                    <td style="text-align: center;">
                                        <button class="btn-action btn-edit" id="btn-edit-${r.id}" onclick="toggleEditRic(${r.id})" title="Modifica">✏️</button>
                                        <button class="btn-action btn-save" id="btn-save-${r.id}" onclick="salvaModificheRic(${r.id})" style="display:none" title="Salva">💾</button>
                                        <button class="btn-action btn-cancel" id="btn-cancel-${r.id}" onclick="toggleEditRic(${r.id})" style="display:none" title="Annulla">❌</button>
                                    </td>
                                </tr>
                            `).join('')}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        `;
    }

    // ── Sezione Altro Fortech — pivot per (data, impianto) ──
    window._infoPivotData = {};
    const pivotMap = {};
    for (const r of data) {
        if (!CATEGORIE_INFORMATIVE.includes(r.categoria)) continue;
        const key = `${r.data}||${r.impianto}`;
        if (!pivotMap[key]) {
            pivotMap[key] = { data: r.data, impianto: r.impianto,
                prove: 0, clienti: 0, diversi: 0, note: '',
                proveId: null, proveVal: 0,
                clientiId: null, clientiVal: 0,
                diversiId: null, diversiVal: 0 };
        }
        const p = pivotMap[key];
        if (r.categoria === 'prove_erogazione')  { p.prove   = r.valore_reale; p.proveId   = r.id; p.proveVal   = r.valore_reale; }
        if (r.categoria === 'clienti_fine_mese') { p.clienti = r.valore_reale; p.clientiId = r.id; p.clientiVal = r.valore_reale; }
        if (r.categoria === 'diversi')           { p.diversi = r.valore_reale; p.diversiId = r.id; p.diversiVal = r.valore_reale; }
        if (!p.note && r.note) p.note = r.note;
    }
    const infoRows = Object.values(pivotMap)
        .filter(r => r.prove > 0 || r.clienti > 0 || r.diversi > 0)
        .sort((a, b) => b.data.localeCompare(a.data));

    if (infoRows.length > 0) {
        const infoRowsHtml = infoRows.map((r, idx) => {
            window._infoPivotData[idx] = [
                r.proveId   ? { id: r.proveId,   val: r.proveVal   } : null,
                r.clientiId ? { id: r.clientiId, val: r.clientiVal } : null,
                r.diversiId ? { id: r.diversiId, val: r.diversiVal } : null,
            ].filter(Boolean);
            return `
            <tr>
                <td>${r.data || '—'}</td>
                <td style="font-weight: 500;">${r.impianto || '—'}</td>
                <td style="text-align: right; color: var(--text-secondary);">${r.prove   > 0 ? renderMoney(r.prove)   : '—'}</td>
                <td style="text-align: right; color: var(--text-secondary);">${r.clienti > 0 ? renderMoney(r.clienti) : '—'}</td>
                <td style="text-align: right; color: var(--text-secondary);">${r.diversi > 0 ? renderMoney(r.diversi) : '—'}</td>
                <td style="font-size: 11px; color: var(--text-secondary); line-height: 1.4;">
                    <span id="info-note-txt-${idx}">${r.note || ''}</span>
                    <input type="text" id="info-note-inp-${idx}" value="${r.note || ''}" style="display:none; width:100%">
                </td>
                <td style="text-align: center;">
                    <button class="btn-action btn-edit"   id="info-btn-edit-${idx}"   onclick="toggleEditInfo(${idx})" title="Modifica">✏️</button>
                    <button class="btn-action btn-save"   id="info-btn-save-${idx}"   onclick="salvaInfoNote(${idx})"  style="display:none" title="Salva">💾</button>
                    <button class="btn-action btn-cancel" id="info-btn-cancel-${idx}" onclick="toggleEditInfo(${idx})" style="display:none" title="Annulla">❌</button>
                </td>
            </tr>`;
        }).join('');

        html += `
        <div class="category-section" id="section-deduzioni">
            <h4 style="margin: 30px 0 10px 0; color: var(--text-primary); border-bottom: 2px solid var(--border-color); padding-bottom: 8px; display: flex; align-items: center; justify-content: space-between;">
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="color:#e8a838;">●</span> Altro Fortech
                </div>
                <button class="btn-toggle-section" onclick="toggleSection('wrapper-deduzioni', this)" title="Nascondi/Mostra">👁️</button>
            </h4>
            <div class="collapsible-wrapper" id="wrapper-deduzioni">
                <div class="table-container" style="margin-bottom: 20px; max-height: 400px; overflow-y: auto;">
                    <table class="data-table">
                        <thead>
                            <tr>
                                <th style="width: 120px;">Data</th>
                                <th>Impianto</th>
                                <th style="width: 140px; text-align: right;" title="Prove di erogazione">Prove Erogazione</th>
                                <th style="width: 120px; text-align: right;" title="Clienti con fattura fine mese">Clienti F.M.</th>
                                <th style="width: 100px; text-align: right;">Diversi</th>
                                <th style="width: 200px;">Note</th>
                                <th style="width: 70px; text-align: center;">Azioni</th>
                            </tr>
                        </thead>
                        <tbody>${infoRowsHtml}</tbody>
                    </table>
                </div>
            </div>
        </div>`;
    }

    container.innerHTML = html;
}

// ── Modifica inline e Esportazioni ──
function toggleSection(wrapperId, btn) {
    const wrapper = document.getElementById(wrapperId);
    if (!wrapper) return;
    
    const isCollapsed = wrapper.classList.toggle('collapsed');
    
    // Cambiamo l'icona del bottone
    if (btn) {
        btn.textContent = isCollapsed ? '👁️‍🗨️' : '👁️';
        btn.title = isCollapsed ? 'Mostra sezione' : 'Nascondi sezione';
    }
}

function toggleEditInfo(idx) {
    const inp = document.getElementById(`info-note-inp-${idx}`);
    const isEditing = inp.style.display === 'block';
    document.getElementById(`info-note-txt-${idx}`).style.display = isEditing ? 'inline' : 'none';
    inp.style.display = isEditing ? 'none' : 'block';
    document.getElementById(`info-btn-edit-${idx}`).style.display = isEditing ? 'inline-block' : 'none';
    document.getElementById(`info-btn-save-${idx}`).style.display = isEditing ? 'none' : 'inline-block';
    document.getElementById(`info-btn-cancel-${idx}`).style.display = isEditing ? 'none' : 'inline-block';
}

async function salvaInfoNote(idx) {
    const newNote = document.getElementById(`info-note-inp-${idx}`).value;
    const items = window._infoPivotData[idx] || [];
    if (!items.length) return;
    try {
        for (const { id, val } of items) {
            await apiFetch('/api/riconciliazioni/edit', {
                method: 'POST',
                body: JSON.stringify({ id, valore_reale: val, note: newNote })
            });
        }
        document.getElementById(`info-note-txt-${idx}`).textContent = newNote;
        toggleEditInfo(idx);
        showToast("Nota aggiornata", "success");
    } catch(e) {
        showToast("Errore salvataggio nota", "error");
    }
}

function toggleEditRic(id) {
    const isEditing = document.getElementById(`reale-inp-${id}`).style.display === 'block';

    document.getElementById(`reale-txt-${id}`).style.display = isEditing ? 'inline' : 'none';
    document.getElementById(`reale-inp-${id}`).style.display = isEditing ? 'none' : 'block';

    document.getElementById(`note-txt-${id}`).style.display = isEditing ? 'inline' : 'none';
    document.getElementById(`note-inp-${id}`).style.display = isEditing ? 'none' : 'block';

    document.getElementById(`btn-edit-${id}`).style.display = isEditing ? 'inline-block' : 'none';
    document.getElementById(`btn-save-${id}`).style.display = isEditing ? 'none' : 'inline-block';
    document.getElementById(`btn-cancel-${id}`).style.display = isEditing ? 'none' : 'inline-block';
}

async function salvaModificheRic(id) {
    const newValoreReale = document.getElementById(`reale-inp-${id}`).value;
    const newNote = document.getElementById(`note-inp-${id}`).value;

    const token = localStorage.getItem("access_token");
    if (!token) {
        showToast("Errore di sessione: esegui nuovamente il login", "error");
        return;
    }

    try {
        const result = await apiFetch('/api/riconciliazioni/edit', {
            method: 'POST',
            body: JSON.stringify({
                id: id,
                valore_reale: newValoreReale,
                note: newNote
            })
        });

        if (result) {
            // Aggiorna UI inline per un feedback istantaneo
            document.getElementById(`reale-txt-${id}`).textContent = renderMoney(newValoreReale);
            document.getElementById(`note-txt-${id}`).textContent = newNote;

            if (result && result.differenza !== undefined) {
                document.getElementById(`diff-cell-${id}`).innerHTML = renderDiff(result.differenza);
            }
            if (result && result.nuovo_stato !== undefined) {
                document.getElementById(`stato-cell-${id}`).innerHTML = renderStatus(result.nuovo_stato);
            }

            // Chiudi l'edit mode
            toggleEditRic(id);
            showToast("Record aggiornato", "success");
        } else {
            console.error("Server Error:", response.status, isJson ? result : await response.text());
            const errMsg = (isJson && result && result.error) ? result.error : `Errore server: ${response.status}`;
            showToast(errMsg, "error");
        }
    } catch (e) {
        showToast("Errore di rete/comunicazione col server", "error");
        console.error(e);
    }
}

function esportaExcel() {
    const token = localStorage.getItem("access_token");
    if (!token) {
        showToast("Sessione scaduta: effettua di nuovo il login", "error");
        return;
    }
    const da = document.getElementById('filterDa') ? document.getElementById('filterDa').value : '';
    const a  = document.getElementById('filterA')  ? document.getElementById('filterA').value  : '';
    const pv = document.getElementById('filterPv') ? document.getElementById('filterPv').value : '';

    let url = '/api/riconciliazioni/export/excel';
    let params = [];
    if (da) params.push(`da=${da}`);
    if (a)  params.push(`a=${a}`);
    if (pv) params.push(`pv=${pv}`);
    if (params.length > 0) url += '?' + params.join('&');

    showToast('⏳ Generazione Excel in corso...', 'info');

    fetch(url, {
        cache: 'no-store',
        headers: { 'Authorization': 'Bearer ' + token }
    })
    .then(response => {
        if (!response.ok) {
            return response.text().then(txt => {
                throw new Error(`HTTP ${response.status} — ${txt.substring(0, 200)}`);
            });
        }
        return response.blob();
    })
    .then(blob => {
        const blobUrl = window.URL.createObjectURL(blob);
        const aEl = document.createElement('a');
        aEl.style.display = 'none';
        aEl.href = blobUrl;
        aEl.download = `Riconciliazioni_${new Date().toISOString().split('T')[0]}.xlsx`;
        document.body.appendChild(aEl);
        aEl.click();
        document.body.removeChild(aEl);
        setTimeout(() => window.URL.revokeObjectURL(blobUrl), 2000);
        showToast('✅ Excel scaricato!', 'success');
    })
    .catch(err => {
        console.error('Excel export error:', err);
        showToast('Errore Excel: ' + err.message, 'error');
    });
}

function esportaPDF() {
    const element = document.getElementById('riconciliazioniTablesContainer');
    if (!element) {
        showToast("Nessun dato da esportare in PDF", "error");
        return;
    }
    if (!element.children.length) {
        showToast("Carica prima le riconciliazioni", "error");
        return;
    }

    showToast("Generazione PDF in corso...", "info");

    // Configura html2pdf per non mostrare bottoni azione
    var opt = {
        margin: [10, 10, 10, 10],
        filename: `Riconciliazioni_${new Date().toISOString().split('T')[0]}.pdf`,
        image: { type: 'jpeg', quality: 0.95 },
        html2canvas: { scale: 1.5, useCORS: true, logging: false, scrollY: 0 },
        jsPDF: { unit: 'mm', format: 'a4', orientation: 'landscape' },
        pagebreak: { mode: ['avoid-all', 'css', 'legacy'] }
    };

    // Nascondi temporaneamente i bottoni azione per il PDF
    const btnActions = element.querySelectorAll('.btn-action, .btn-toggle-section');
    btnActions.forEach(btn => btn.style.display = 'none');

    html2pdf().set(opt).from(element).save()
        .then(() => {
            showToast("PDF esportato con successo!", "success");
        })
        .catch(err => {
            console.error("Errore PDF:", err);
            showToast("Errore durante la generazione del PDF: " + err.message, "error");
        })
        .finally(() => {
            // Ripristina i bottoni
            btnActions.forEach(btn => btn.style.display = '');
        });
}

// ── Contanti / Banca (Simona — Conferma Matching) ──

const TIPO_MATCH_LABELS = {
    '1:1_esatto': { label: 'Match Esatto 1:1', icon: '✅', css: 'tm-esatto' },
    '1:1_arrotondato': { label: 'Arrotondamento', icon: '≈', css: 'tm-arrotondato' },
    'cumulativo_2gg': { label: 'Cumulativo 2gg', icon: '📦', css: 'tm-cumulativo' },
    'cumulativo_3gg': { label: 'Cumulativo 3gg', icon: '📦', css: 'tm-cumulativo' },
    'cumulativo_4gg': { label: 'Cumulativo 4gg', icon: '📦', css: 'tm-cumulativo' },
    'settimanale': { label: 'Settimanale', icon: '📅', css: 'tm-cumulativo' },
    'look_ahead_fifo': { label: 'Bilancio C.', icon: '🔄', css: 'tm-cumulativo' },
    'nessuno': { label: 'Nessun Match', icon: '❌', css: 'tm-nessuno' },
    'zero': { label: 'Niente Contanti', icon: '—', css: 'tm-zero' },
    '': { label: 'Legacy', icon: '📋', css: 'tm-legacy' },
};


function renderTipoMatch(tipo) {
    const t = TIPO_MATCH_LABELS[tipo] || TIPO_MATCH_LABELS[''];
    return `<span class="tipo-match-badge ${t.css}">${t.icon} ${t.label}</span>`;
}

// ── Impianti ──
async function loadImpianti() {
    const data = await apiFetch('/api/impianti');
    const grid = document.getElementById('impiantiGrid');

    if (!data || data.length === 0) {
        grid.innerHTML = '<div class="empty-state">Nessun impianto registrato</div>';
        return;
    }

    grid.innerHTML = data.map(imp => {
        const safeName = (imp.nome || 'N/A').replace(/'/g, "\\'");
        return `
        <div class="impianto-card" onclick="openAndamento(${imp.id}, '${safeName}')" style="cursor:pointer">
            <div class="impianto-name">${imp.nome || 'N/A'}</div>
            <div class="impianto-code">PV: ${imp.codice_pv || '—'} · ${imp.tipo || 'PRESIDIATO'}</div>
            <div class="impianto-stats">
                <span class="impianto-stat" style="color:var(--status-ok)">✅ ${imp.cnt_ok || 0}</span>
                <span class="impianto-stat" style="color:var(--status-warn)">⚠️ ${imp.cnt_warn || 0}</span>
                <span class="impianto-stat" style="color:var(--status-danger)">🔴 ${imp.cnt_grave || 0}</span>
            </div>
            <div style="font-size:10px;color:var(--text-muted);margin-top:8px">🔍 Clicca per andamento</div>
        </div>`;
    }).join('');
}

// ── Sicurezza (Taleggio) ──
async function loadSicurezza() {
    const data = await apiFetch('/api/sicurezza');
    const tbody = document.getElementById('sicurezzaBody');

    if (!data || data.length === 0) {
        tbody.innerHTML = '<tr><td colspan="8" class="empty-state">Nessun evento di sicurezza registrato</td></tr>';
        return;
    }

    tbody.innerHTML = data.map(r => `
        < tr >
            <td style="white-space:nowrap">${r.timestamp || '—'}</td>
            <td>${r.giorno || '—'}</td>
            <td>${r.impianto || '—'}</td>
            <td>${renderMoney(r.importo_fortech)}</td>
            <td>${renderMoney(r.importo_atteso)}</td>
            <td>${renderDiff(r.differenza)}</td>
            <td>${r.autorizzata === true ? '✅ Si' : r.autorizzata === false ? '❌ No' : '—'}</td>
            <td style="font-size:11px;color:var(--text-secondary)">${r.note || ''}</td>
        </tr >
        `).join('');
}

// ═══════════════════════════════════════════════════════════════
// MODAL / ANDAMENTO
// ═══════════════════════════════════════════════════════════════

function closeModal(event) {
    if (event && event.target !== event.currentTarget) return;
    document.getElementById('modalOverlay').style.display = 'none';
    document.body.style.overflow = '';
}

// Close on Escape key
document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') closeModal();
});

async function openAndamento(id, nome) {
    const overlay = document.getElementById('modalOverlay');
    const body = document.getElementById('modalBody');
    const title = document.getElementById('modalTitle');
    const subtitle = document.getElementById('modalSubtitle');

    title.textContent = `🏢 ${nome}`;
    subtitle.textContent = 'Caricamento andamento...';
    body.innerHTML = '<div class="empty-state" style="padding:40px">⏳ Caricamento dati...</div>';
    overlay.style.display = 'flex';
    document.body.style.overflow = 'hidden';

    try {
        const data = await apiFetch(`/api/impianti/${id}/andamento`);
        const imp = data.impianto;
        subtitle.textContent = `PV: ${imp.codice_pv || '—'} · ${imp.tipo || 'PRESIDIATO'} · ${data.totale_giorni} giornate analizzate`;

        renderAndamento(body, data);
    } catch (err) {
        body.innerHTML = '<div class="empty-state" style="color:var(--status-danger)">Errore: ' + err.message + '</div>';
    }
}

function renderAndamento(container, data) {
    const stats = data.stats || {};
    const giorni = data.giorni || [];

    // Summary stats
    const totalOk = (stats.QUADRATO || 0) + (stats.QUADRATO_ARROT || 0);
    const totalWarn = stats.ANOMALIA_LIEVE || 0;
    const totalGrave = stats.ANOMALIA_GRAVE || 0;
    const totalMissing = (stats.NON_TROVATO || 0) + (stats.IN_ATTESA || 0);

    let html = '<div class="trend-summary">';
    html += '<div class="trend-stat"><div class="trend-stat-value" style="color:var(--status-ok)">' + totalOk + '</div><div class="trend-stat-label">Quadrature</div></div>';
    html += '<div class="trend-stat"><div class="trend-stat-value" style="color:var(--status-warn)">' + totalWarn + '</div><div class="trend-stat-label">Anomalie Lievi</div></div>';
    html += '<div class="trend-stat"><div class="trend-stat-value" style="color:var(--status-danger)">' + totalGrave + '</div><div class="trend-stat-label">Anomalie Gravi</div></div>';
    html += '<div class="trend-stat"><div class="trend-stat-value" style="color:var(--text-muted)">' + totalMissing + '</div><div class="trend-stat-label">Dati Mancanti</div></div>';
    html += '<div class="trend-stat"><div class="trend-stat-value">' + data.totale_giorni + '</div><div class="trend-stat-label">Giorni Totali</div></div>';
    html += '</div>';

    // Bar chart: one row per day
    const stateColors = {
        QUADRATO: '#3fb950', QUADRATO_ARROT: '#56d364',
        ANOMALIA_LIEVE: '#d29922', ANOMALIA_GRAVE: '#f85149',
        NON_TROVATO: '#8b949e', IN_ATTESA: '#58a6ff', INCOMPLETO: '#8b949e'
    };
    const stateIcons = {
        QUADRATO: '\u2705', QUADRATO_ARROT: '\u2705', ANOMALIA_LIEVE: '\u26a0\ufe0f',
        ANOMALIA_GRAVE: '\ud83d\udd34', NON_TROVATO: '\u2753', IN_ATTESA: '\u23f3'
    };

    if (giorni.length > 0) {
        html += '<div class="trend-chart"><div class="trend-chart-title">Andamento Giornaliero</div>';

        const maxCats = Math.max(...giorni.map(g => Object.keys(g.categorie).length), 1);

        for (const g of giorni) {
            const cats = Object.entries(g.categorie);
            const dateShort = (g.data || '').substring(0, 10);
            const statusIcon = stateIcons[g.stato_peggiore] || '\u2753';

            const segWidth = 100 / maxCats;
            let segments = '';
            cats.forEach(([cat, det]) => {
                const color = stateColors[det.stato] || '#8b949e';
                segments += '<div class="trend-bar-segment" style="width:' + segWidth + '%;background:' + color + '" title="' + cat + ': ' + det.stato + '"></div>';
            });

            html += '<div class="trend-bar-row" onclick="this.nextElementSibling.style.display=this.nextElementSibling.style.display===\'none\'?\'block\':\'none\'">';
            html += '<span class="trend-bar-date">' + dateShort + '</span>';
            html += '<div class="trend-bar-track">' + segments + '</div>';
            html += '<span class="trend-bar-value">' + renderDiff(g.totale_diff) + '</span>';
            html += '<span class="trend-bar-status">' + statusIcon + '</span>';
            html += '</div>';

            // Expandable detail card
            html += '<div class="trend-day-card" style="display:none;margin:0 0 8px 102px"><div class="trend-day-cats">';
            cats.forEach(([cat, det]) => {
                html += '<div class="trend-cat-row">';
                html += '<span class="trend-cat-name">' + renderCatBadge(cat) + '</span>';
                html += '<span class="trend-cat-vals">';
                html += '<span>Teorico: ' + renderMoney(det.teorico) + '</span>';
                html += '<span>Reale: ' + renderMoney(det.reale) + '</span>';
                html += '<span>' + renderDiff(det.differenza) + '</span>';
                html += '</span>';
                html += '<span class="trend-cat-status">' + renderStatus(det.stato) + '</span>';
                html += '</div>';
            });
            html += '</div></div>';
        }
        html += '</div>';
    } else {
        html += '<div class="empty-state">Nessun dato disponibile per questo impianto</div>';
    }

    container.innerHTML = html;
}

// ═══════════════════════════════════════════════════════════════
// AI REPORT
// ═══════════════════════════════════════════════════════════════

function simpleMarkdownToHtml(md) {
    if (!md) return '';
    if (typeof marked !== 'undefined') {
        marked.setOptions({ breaks: true, gfm: true });
        return marked.parse(md);
    }
    // Fallback minimale se marked non è caricato
    return '<p>' + md.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/\n/g,'<br>') + '</p>';
}

// ═══════════════════════════════════════════════════════════════
// VERIFICA COMPENSAZIONI
// ═══════════════════════════════════════════════════════════════

let _verificaMatches = [];

async function loadVerifica() {
    // Popola filtro impianti
    const impSel = document.getElementById('verificaImpiantoFilter');
    if (impSel && impSel.options.length <= 1) {
        const impianti = await apiFetch('/api/impianti');
        if (impianti) {
            impianti.forEach(i => {
                const opt = document.createElement('option');
                opt.value = i.codice_pv;
                opt.textContent = i.nome || i.codice_pv;
                impSel.appendChild(opt);
            });
        }
    }
    await _aggiornaVerificaStats();
}

async function _aggiornaVerificaStats() {
    const data = await apiFetch('/api/verifica/stats');
    if (!data) return;
    const bar = document.getElementById('verificaStatsBar');
    if (!bar) return;

    const items = [
        { stato: 'ANOMALIA_GRAVE',      label: 'Anomalie Gravi',   color: '#f85149' },
        { stato: 'ANOMALIA_LIEVE',      label: 'Anomalie Lievi',   color: '#d29922' },
        { stato: 'NON_TROVATO',         label: 'Non Trovate',      color: '#8b949e' },
        { stato: 'QUADRATO_COMPENSATO', label: 'Compensate',       color: '#56d364' },
        { stato: 'QUADRATO',            label: 'Quadrate',         color: '#3fb950' },
    ];

    bar.innerHTML = items.map(item => {
        const d = data[item.stato] || { count: 0, esposizione: 0 };
        return `<div style="background:var(--bg-card);border:1px solid var(--border-color);border-radius:8px;padding:12px 18px;flex:1;min-width:140px;">
            <div style="font-size:11px;color:var(--text-muted);margin-bottom:4px;">${item.label}</div>
            <div style="font-size:22px;font-weight:700;color:${item.color};">${d.count}</div>
            <div style="font-size:11px;color:var(--text-muted);">€${d.esposizione.toFixed(2)}</div>
        </div>`;
    }).join('');
}

async function verificaAnteprima() {
    const tolleranza = parseFloat(document.getElementById('verificaTolleranza').value) || 1.0;
    const codice_pv  = document.getElementById('verificaImpiantoFilter').value || null;
    const categoria  = document.getElementById('verificaCategoriaFilter').value || null;

    const data = await apiFetch('/api/verifica/anteprima', {
        method: 'POST',
        body: JSON.stringify({ tolleranza, codice_pv, categoria })
    });
    if (!data) return;

    _verificaMatches = data.matches || [];
    const risultati = document.getElementById('verificaRisultati');
    const empty     = document.getElementById('verificaEmpty');
    const summary   = document.getElementById('verificaSummary');
    const tbody     = document.getElementById('verificaBody');
    const btnApplica = document.getElementById('btnApplicaVerifica');

    if (_verificaMatches.length === 0) {
        risultati.style.display = 'none';
        empty.style.display = 'block';
        btnApplica.style.display = 'none';
        return;
    }

    empty.style.display = 'none';
    risultati.style.display = 'block';
    btnApplica.style.display = 'inline-block';

    summary.innerHTML = `
        <div style="display:flex;gap:24px;flex-wrap:wrap;">
            <div><span style="font-size:24px;font-weight:700;color:#56d364;">${data.totale_coppie}</span><br><small style="color:var(--text-muted);">coppie compensabili</small></div>
            <div><span style="font-size:24px;font-weight:700;color:#56d364;">${data.righe_compensabili}</span><br><small style="color:var(--text-muted);">righe da aggiornare</small></div>
            <div><span style="font-size:24px;font-weight:700;color:#f85149;">€${data.esposizione_compensata.toFixed(2)}</span><br><small style="color:var(--text-muted);">esposizione compensata</small></div>
        </div>`;

    tbody.innerHTML = _verificaMatches.map((m, idx) => `
        <tr>
            <td><input type="checkbox" class="chk-match" data-idx="${idx}" checked onchange="aggiornaSelCount()"></td>
            <td style="font-weight:500;">${m.impianto}</td>
            <td>${renderCatBadge ? renderCatBadge(m.categoria) : m.categoria}</td>
            <td style="text-align:right;">${m.data_pos}</td>
            <td style="text-align:right;color:#f85149;font-weight:600;">+€${Math.abs(m.diff_pos).toFixed(2)}</td>
            <td style="text-align:right;">${m.data_neg}</td>
            <td style="text-align:right;color:#3fb950;font-weight:600;">−€${Math.abs(m.diff_neg).toFixed(2)}</td>
            <td style="text-align:right;font-weight:600;color:${Math.abs(m.residuo) < 0.01 ? '#56d364' : '#d29922'};">€${Math.abs(m.residuo).toFixed(2)}</td>
            <td><span class="status-badge status-anomalia-grave" style="font-size:11px;">${m.stato_pos}</span></td>
        </tr>`).join('');

    aggiornaSelCount();
}

function toggleSelAll(chk) {
    document.querySelectorAll('.chk-match').forEach(c => { c.checked = chk.checked; });
    aggiornaSelCount();
}

function aggiornaSelCount() {
    const sel = document.querySelectorAll('.chk-match:checked').length;
    const tot = document.querySelectorAll('.chk-match').length;
    document.getElementById('verificaSelCount').textContent = `${sel} di ${tot} coppie selezionate`;
}

async function verificaApplica() {
    const tolleranza = parseFloat(document.getElementById('verificaTolleranza').value) || 1.0;
    const data = await apiFetch('/api/verifica/applica', {
        method: 'POST',
        body: JSON.stringify({ tolleranza })
    });
    if (!data) return;
    alert(`✅ ${data.message}`);
    await verificaAnteprima();
    await _aggiornaVerificaStats();
}

async function verificaApplicaSelezionati() {
    const tolleranza = parseFloat(document.getElementById('verificaTolleranza').value) || 1.0;
    const checked = Array.from(document.querySelectorAll('.chk-match:checked'));
    if (!checked.length) { alert('Seleziona almeno una coppia.'); return; }

    const id_pairs = checked.map(c => {
        const m = _verificaMatches[parseInt(c.dataset.idx)];
        return [m.id_pos, m.id_neg];
    });

    const data = await apiFetch('/api/verifica/applica', {
        method: 'POST',
        body: JSON.stringify({ tolleranza, id_pairs })
    });
    if (!data) return;
    alert(`✅ ${data.message}`);
    await verificaAnteprima();
    await _aggiornaVerificaStats();
}

async function verificaReset() {
    if (!confirm('Annullare tutte le compensazioni già applicate? Le righe torneranno ad ANOMALIA_GRAVE.')) return;
    const data = await apiFetch('/api/verifica/reset', { method: 'POST', body: '{}' });
    if (!data) return;
    alert('↩ ' + data.message);
    document.getElementById('verificaRisultati').style.display = 'none';
    document.getElementById('verificaEmpty').style.display = 'none';
    document.getElementById('btnApplicaVerifica').style.display = 'none';
    await _aggiornaVerificaStats();
}

let _aiAbortController = null;

async function loadAIModels() {
    try {
        const data = await apiFetch('/api/ai-report/models');
        if (!data) return;
        const sel = document.getElementById('aiModelSelect');
        if (!sel) return;
        sel.innerHTML = data.models.map(m =>
            `<option value="${m.id}" ${m.id === data.current ? 'selected' : ''}>${m.label}</option>`
        ).join('');
        // Popola anche il select impianti
        const impData = await apiFetch('/api/impianti');
        const impSel = document.getElementById('aiImpiantoSelect');
        if (impSel && impData) {
            impData.forEach(i => {
                const opt = document.createElement('option');
                opt.value = i.codice_pv;
                opt.textContent = i.nome || i.codice_pv;
                impSel.appendChild(opt);
            });
        }
    } catch(e) { console.error('loadAIModels:', e); }
}

async function salvaModelloAI() {
    const model = document.getElementById('aiModelSelect')?.value;
    if (!model) return;
    await apiFetch('/api/ai-report/model', { method: 'POST', body: JSON.stringify({ model }) });
}

function stopAIReport() {
    if (_aiAbortController) _aiAbortController.abort();
}

async function generateAIReport() {
    const btn       = document.getElementById('btnGenerateAI');
    const btnStop   = document.getElementById('btnStopAI');
    const status    = document.getElementById('aiStatus');
    const container = document.getElementById('aiReportContainer');
    const reportBody = document.getElementById('aiReportBody');
    const timestamp = document.getElementById('aiTimestamp');

    const model    = document.getElementById('aiModelSelect')?.value || 'openai/gpt-4o-mini';
    const dataFrom = document.getElementById('aiDataFrom')?.value || null;
    const dataTo   = document.getElementById('aiDataTo')?.value || null;
    const codicePv = document.getElementById('aiImpiantoSelect')?.value || null;

    btn.disabled = true;
    if (btnStop) btnStop.style.display = 'inline-block';
    status.textContent = 'Analisi in corso…';
    status.className = 'ai-status loading';
    reportBody.innerHTML = '';
    container.style.display = 'block';
    timestamp.textContent = new Date().toLocaleString('it-IT');

    _aiAbortController = new AbortController();
    let rawText = '';

    try {
        const token = localStorage.getItem('token');
        const resp = await fetch('/api/ai-report/stream', {
            method: 'POST',
            headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' },
            body: JSON.stringify({ model, data_from: dataFrom, data_to: dataTo, codice_pv: codicePv }),
            signal: _aiAbortController.signal,
        });

        if (!resp.ok) {
            const err = await resp.json().catch(() => ({}));
            throw new Error(err.error || `HTTP ${resp.status}`);
        }

        // Leggi testo completo (funziona sia con streaming sia con risposta bufferizzata)
        const text = await resp.text();

        // Parsa tutti gli eventi SSE
        const lines = text.split('\n');
        for (const line of lines) {
            if (!line.startsWith('data: ')) continue;
            const payload = line.slice(6).trim();
            if (!payload || payload === '[DONE]') continue;
            let chunk;
            try { chunk = JSON.parse(payload); } catch(e) { continue; }
            if (chunk.error) {
                const msg = typeof chunk.error === 'object' ? (chunk.error.message || JSON.stringify(chunk.error)) : String(chunk.error);
                throw new Error(msg);
            }
            if (chunk.content) {
                rawText += chunk.content;
                reportBody.innerHTML = simpleMarkdownToHtml(rawText);
            }
        }

        if (!rawText) throw new Error('Nessun contenuto ricevuto dal modello AI.');

        status.textContent = 'Report completato!';
        status.className = 'ai-status';

    } catch (err) {
        if (err.name === 'AbortError') {
            status.textContent = 'Generazione interrotta.';
            if (rawText) { reportBody.innerHTML = simpleMarkdownToHtml(rawText); }
        } else {
            status.textContent = 'Errore: ' + err.message;
            status.style.color = 'var(--status-danger)';
        }
    } finally {
        btn.disabled = false;
        if (btnStop) btnStop.style.display = 'none';
        _aiAbortController = null;
    }
}

// ═══════════════════════════════════════════════════════════════
// IMPOSTAZIONI (PASSWORD & CONFIG)
// ═══════════════════════════════════════════════════════════════

async function loadConfig() {
    try {
        const data = await apiFetch('/api/settings/config');
        if (data) {
            document.getElementById('cfg_carte').value = data.tolleranza_carte_fisiologica || 1.00;
            document.getElementById('cfg_satispay').value = data.tolleranza_satispay || 0.01;
            document.getElementById('cfg_giorni').value = data.scarto_giorni_buoni || 1;
        }
    } catch (e) { console.error('Errore caricamento config:', e); }
}

async function updateConfig(e) {
    e.preventDefault();
    const status = document.getElementById('cfg-status');
    status.textContent = 'Salvataggio...';
    status.className = 'status-message show';
    status.style.color = 'var(--text-secondary)';

    const payload = {
        tolleranza_carte_fisiologica: parseFloat(document.getElementById('cfg_carte').value),
        tolleranza_satispay: parseFloat(document.getElementById('cfg_satispay').value),
        scarto_giorni_buoni: parseInt(document.getElementById('cfg_giorni').value),
    };

    try {
        const res = await apiFetch('/api/settings/config', {
            method: 'POST',
            body: JSON.stringify(payload)
        });

        if (res) {
            status.textContent = '✔ Tolleranze salvate con successo!';
            status.style.color = 'var(--status-ok)';
            setTimeout(() => {
                status.classList.remove('show');
            }, 3000);
        } else {
            status.textContent = '✖ Errore durante il salvataggio.';
            status.style.color = 'var(--status-danger)';
        }
    } catch (err) {
        status.textContent = '✖ Errore di connessione.';
        status.style.color = 'var(--status-danger)';
    }
}

async function updatePassword(e) {
    e.preventDefault();
    const status = document.getElementById('pw-status');
    const old_pw = document.getElementById('old_pw').value;
    const new_pw = document.getElementById('new_pw').value;

    status.textContent = 'Verifica in corso...';
    status.className = 'status-message show';
    status.style.color = 'var(--text-secondary)';

    try {
        const data = await apiFetch('/api/settings/password', {
            method: 'POST',
            body: JSON.stringify({ old_password: old_pw, new_password: new_pw })
        });

        if (data && data.ok) {
            status.textContent = '✔ Password aggiornata. Effettua il login.';
            status.style.color = 'var(--status-ok)';
            document.getElementById('formPassword').reset();
            setTimeout(() => {
                status.classList.remove('show');
                if (typeof Auth !== 'undefined') Auth.clear();
            }, 2000);
        } else {
            status.textContent = '✖ ' + (data.msg || 'Errore');
            status.style.color = 'var(--status-danger)';
        }
    } catch (err) {
        status.textContent = '✖ Server irraggiungibile.';
        status.style.color = 'var(--status-danger)';
    }
}

// ═══════════════════════════════════════════════════════════════
// INIT
// ═══════════════════════════════════════════════════════════════

document.addEventListener('DOMContentLoaded', () => {
    loadDashboard();
    loadChartData();

    const btnLogout = document.getElementById('btnLogout');
    if (btnLogout) {
        btnLogout.addEventListener('click', () => {
            if (typeof Auth !== 'undefined') Auth.clear();
        });
    }
});

// ═══════════════════════════════════════════════════════════════
// SETTINGS — API KEY (OpenRouter)
// ═══════════════════════════════════════════════════════════════

function _apikeyStatus(msg, isError = false) {
    const el = document.getElementById('apikey-status');
    if (!el) return;
    el.textContent = msg;
    el.style.color = isError ? 'var(--accent-red)' : 'var(--accent-green)';
    el.classList.add('show');
    setTimeout(() => el.classList.remove('show'), 5000);
}

function toggleApiKeyVisibility() {
    const inp = document.getElementById('openrouter_key');
    const btn = document.getElementById('btnToggleKey');
    if (!inp) return;
    if (inp.type === 'password') { inp.type = 'text'; btn.textContent = '🙈'; }
    else { inp.type = 'password'; btn.textContent = '👁️'; }
}

async function loadApiKeyStatus() {
    const data = await apiFetch('/api/settings/apikey');
    if (!data) return;
    const inp = document.getElementById('openrouter_key');
    if (data.has_key && inp) {
        inp.placeholder = `Chiave attuale: ${data.masked}`;
        _apikeyStatus('✅ Chiave API configurata');
    } else if (inp) {
        inp.placeholder = 'sk-or-v1-…';
        _apikeyStatus('⚠️ Nessuna chiave configurata', true);
    }
}

async function salvaApiKey() {
    const inp = document.getElementById('openrouter_key');
    const key = inp ? inp.value.trim() : '';
    if (!key) { _apikeyStatus('Inserisci la chiave prima di salvare.', true); return; }

    const resp = await apiFetch('/api/settings/apikey', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ api_key: key })
    });

    if (resp && resp.message) {
        showToast(resp.message, 'success');
        _apikeyStatus(resp.message);
        inp.value = '';
        loadApiKeyStatus();
    } else {
        const err = (resp && resp.error) || 'Errore nel salvataggio';
        showToast(err, 'error');
        _apikeyStatus(err, true);
    }
}

async function testApiKey() {
    const inp = document.getElementById('openrouter_key');
    const key = inp ? inp.value.trim() : '';
    _apikeyStatus('⏳ Test in corso…');

    const payload = key ? { api_key: key } : {};
    const resp = await apiFetch('/api/settings/apikey/test', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
    });

    if (resp && resp.message) { _apikeyStatus(resp.message); showToast(resp.message, 'success'); }
    else { const e = (resp && resp.error) || 'Connessione fallita'; _apikeyStatus(e, true); showToast(e, 'error'); }
}

// ═══════════════════════════════════════════════════════════════
// TOAST NOTIFICATION
// ═══════════════════════════════════════════════════════════════

function showToast(message, type = 'info') {
    const existing = document.getElementById('toast-container');
    if (!existing) {
        const style = document.createElement('style');
        style.textContent = `
            #toast-container { position:fixed; bottom:24px; right:24px; z-index:9999; display:flex; flex-direction:column; gap:8px; }
            .toast { padding:12px 18px; border-radius:8px; font-size:13px; font-weight:500;
                     color:#fff; min-width:220px; max-width:360px; box-shadow:0 4px 16px rgba(0,0,0,.4);
                     animation: toastIn .25s ease; }
            .toast-success { background:#238636; }
            .toast-error   { background:#da3633; }
            .toast-info    { background:#1f6feb; }
            @keyframes toastIn { from{opacity:0;transform:translateY(12px)} to{opacity:1;transform:none} }
        `;
        document.head.appendChild(style);
        const container = document.createElement('div');
        container.id = 'toast-container';
        document.body.appendChild(container);
    }
    const container = document.getElementById('toast-container');
    const el = document.createElement('div');
    el.className = `toast toast-${type}`;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(() => { el.style.opacity = '0'; el.style.transition = 'opacity .3s'; setTimeout(() => el.remove(), 300); }, 3500);
}

