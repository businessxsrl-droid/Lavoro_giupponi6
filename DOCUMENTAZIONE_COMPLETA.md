# Calor Systems — Documentazione Tecnica Completa e Passaggio di Consegne

Questa guida è stata pensata per accompagnarti "dalla A alla Z" all'interno del progetto **Calor Systems** (Sistema di Riconciliazione Incassi). L'obiettivo è permettere a un nuovo sviluppatore di prendere in mano il progetto e capirne logiche, flussi, regole di business e insidie (casi particolari o debiti tecnici affrontati in passato).

---

## 1. Introduzione: Cosa fa l'Applicazione?

Le stazioni di servizio (pompe di benzina) gestiscono incassi da decine di fonti diverse ogni giorno: contanti, carte di credito, app come Satispay, buoni cartacei ed elettronici, carte petrolifere (es. DKV, UTA).
Il gestionale centrale (chiamato **Fortech** o AS400 in vari contesti) registra il **teorico** (quanto carburante è uscito e come "sarebbe dovuto" essere pagato). 
A fine giornata queste stime devono essere confrontate (riconciliate) con il **reale**, ovvero i soldi effettivamente accreditati sul conto per le varie tipologie o i movimenti del POS.

**Scopo dell'App:** Invece di controllare tutto a mano con decine di fogli Excel, l'utente carica l'Excel estratto da Fortech e *tutti* gli altri Excel (estratti da Satispay, dalle banche, POS, ecc.). L'applicazione automatizza il `matching` e restituisce in una dashboard le anomalie.

---

## 2. Stack Tecnologico

- **Linguaggio Principale**: Python 3
- **Framework Web**: Flask 3.0 (fornisce sia le API backend che il rendering della dashboard base)
- **Database**: PostgreSQL 15 (usato su Render.com o Docker in dev). Esiste o è stato testato un fallback su *Supabase HTTP API*
- **Elaborazione Dati/Excel**: Pandas 2.1 (il motore di ingestion e classificazione)
- **Frontend**: HTML5, CSS3, e Vanilla JavaScript. Il framework UI è puro JS e DOM manipulation. Librerie extra: Chart.js per le grafiche e html2pdf.js per l'esportazione.
- **Autenticazione**: JWT tramite la libreria `Flask-JWT-Extended`
- **Infrastruttura**: Docker-compose per dev, e Web Service Render.com per la produzione. 

---

## 3. L'Architettura e il Data Flow

Come viaggiano i dati dall'utente al Database e viceversa?
Tutto inizia con l'**Upload**. L'utente apre l'interfaccia e carica svariati file Excel contemporaneamente. 

1. **`app.py` (Il controller)**: Riceve i file in multiparte sulla route `/api/upload`.
2. **`classifier.py` (Il Caleidoscopio)**: Prende un file Excel caricato temporaneamente in RAM, prova a leggerne gli header colonna e le prime righe. Grazie a un dizionario di mappe, identifica di che tipo di file si stratta (È un file Fortech? È un file Satispay? È il POS Numia?). Questo passaggio è vitale perché i file di alcuni gestori cambiano formato di frequente.
3. **`ingestion.py` (L'Ingestore)**: Una volta capito il tipo, converte i dati dal CSV/Excel in record compatibili coi nostri modelli DB. Normalizza date, importi, associa i codici dei PV (Punti Vendita).
4. **`reconciler.py` (Il Motore di Riconciliazione)**: Quando tutti i file di una sessione d'upload sono ingestati nel DB, l'app lancia una riconciliazione. Il processo confronta la riga "teorica Fortech" contro le somme "reali" appena importate. Controlla le discrepanze, stila l'esito (es. "Squadrato di X €") e salva il risultato sulla tabella `riconciliazione_risultati`. 
5. **Frontend `index.html` (La Dashboard)**: Il frontend fa una fetch all'endpoint `GET /api/riconciliazioni`, modella i dati formatta le tabelle con gli indicatori in base allo stato (`QUADRATO`, `ANOMALIA_GRAVE`...). 

---

## 4. Panoramica sul Database 

Le tabelle principali e a cosa servono:

| Tabella | Cosa fa |
| -- | -- |
| `impianti` | Anagrafica base: `codice_pv` e nome dell'impianto. Contiene anche flag importanti come `senza_servizio_riconciliazione`. |
| `transazioni_fortech` | La "fonte di verità teorica". Ogni riga indica un impianto, una data, e l'atteso per ogni categoria: `totale_pos`, `totale_buoni`, ecc. Contiene anche colonne puramente visuali (`prove_erogazione`, `clienti_fine_mese`). |
| Le "Reali" | Varie tabelle (`transazioni_pos`, `transazioni_satispay`, `transazioni_petrolifere`, ecc.) che contengono i record reali ingestati dai report Excel esterni. Spesso hanno granularità maggiore del fortech giornaliero. |
| `riconciliazione_risultati` | Tabella conclusiva. Ha colonne come `stato_pos`, `dif_pos`, ecc. che definiscono l'output della matematica calcolata in `reconciler.py`. |
| `config` | Costanti globali di tolleranze in euro e opzioni dell'app. |

---

## 5. Dettaglio dei Moduli Principali

### `app.py`
Il backend Flask. Gestisce API di utilità, configurazione JWT, le callback dell'interfaccia UI. Serve l'HTML principale e si occupa di gestire gli iter/loop per invocare il motore di classificazione e salvataggio quando parte un file upload bulk.

### `classifier.py`
Questo file è fondamentale. Contiene `identify_file_type()`, che distingue i vari file. Se all'improvviso un file della banca non viene più letto, è perché la banca ha cambiato nome a una colonna (es. da "DATA TRANSAZ." a "Data operazione"). Bisogna correggere le tuple di lookup all'interno di questo file. Un'altra peculiarità è la gestione dei file salvati come HTML ma rinominati con estensione `.xls` (gestiti qui ad hoc, per filiali come Ghislandi o Rovetta).

### `ingestion.py`
La parte legata a pandas che parsa e "ripulisce" i dati, uniformandoli. Elabora tipi di dati errati (date malformate, virgole non riconosciute come separatore decimale, codici testuali al posto che interi). Le varie funzioni `ingest_fortech()`, `ingest_pos()` prendono il df dal classifier e lo caricano pulito verso il DB.

### `reconciler.py`
Riceve l'ordine per impianto e data. Interroga il database e tira le somme:
- Applica i threshold di errore. 
- Definisce i 5 stati logici principali: `QUADRATO`, `QUADRATO_ARROT`, `ANOMALIA_LIEVE`, `ANOMALIA_GRAVE`, `NON_TROVATO`

---

## 6. Casi Particolari e Regole di Business Strane 🚨 (Da Sapere Assolutamente)

Nel corso del tempo, sono arrivate "eccezioni" di business che si riflettono come regole speciali nel codice.
Fai attenzione a non rimouvere queste regole se andrai a toccare il sistema:

1. **Il file Alias mapping (`alias_mapping.json`)**
Molti gestori POS emettono Excel che contengono solo il "Codice Terminale" senza specificare a quale distributore appartiene. Nel progetto c'è questo file `.json` che serve come chiave di lookup per capire a quale `codice_pv` (Impianto) assegnare una transazione basandosi unicamente sul "IdTerminale". Se si aggiunge un terminale nuovo va inserito lì altrimenti l'ingestion non troverà mai un impianto e fallirà l'import per quel transato.

2. **Impianti "Senza Servizio di Riconciliazione"**
Alcuni impianti (es. Famagosta, Ghislandi, Belfiore, Marmirolo, Oltre il Colle, Rovetta) generano file Fortech dove Buoni e Carte Petrolifere non sono distinti (messi sotto la stessa colonna generica "Card Petrolifera"). 
Nel database la loro voce in `impianti` ha il flag `senza_servizio_riconciliazione = TRUE`. In `reconciler.py` se un impianto ha questo flag viene eseguita una **riconciliazione combinata** (`_reconcile_buoni_petrolifere_combined` o logica affine). Non divide il teorico, ma unisce i reali di entrambe le categorie. Non modificare questa logica o i conti si disalineranno e appariranno falsi negativi.

3. **Colonne Solo-Mostra (Display-only)**
Nel file Fortech figurano delle voci puramente descrittive (nessun movimento monetario), come:
- `prove_erogazione`
- `clienti_fine_mese` (corrispettivo post-pagato)
- `diversi`
Se le vedi saltare fuori nell'app o nel db, sappi che non influenzano lo stato della Riconciliazione nè la cassa pos. Vanno solo servite nelle API e visualizzate nella pagina d'uso dell'anagrafica Riconciliazioni.

4. **Sospensione dei Contanti**
Il calcolo e matching sui contanti via "look-ahead FIFO" (che cercava di legare versamenti bancari differiti di giorni rispetto al teorico del giorno di cassa) soffriva di troppe variabili imprevedibili. La logica è disattivata per questi ultimi aggiornamenti.

---

## 7. Deploy e Sviluppo Locale

### Come Sviluppare in Locale
Troverai il file `docker-compose.yml`. E' sufficiente lanciarlo per tirare su il DB PostgreSQL.
1. Crea un virtual environment `python -m venv venv` 
2. `pip install -r requirements.txt`
3. Rinomina `.env.example` in `.env` e accertati che `DATABASE_URL` punti a locale (come esposto dal docker, tipicamente `postgresql://calor:calor@localhost:5432/calor`).
4. Avvia il server tramite `python app.py`.

### Produzione (Render.com)
In file come `render.yaml` o dal pannello Render il deploy è automatico a seguito di un push sul main branch. 
Usa Gunicorn come entrypoint: `gunicorn app:app`. In Render devono essere settate le corrette varibali d'ambiente (il DB Postgres as a Service sempre su Render, la `JWT_SECRET_KEY`, ecc).

---

## 8. Guida Rapida alla Manutenzione
Cosa fare se un utente si lamenta che c'è un errore e la pagina non "quadra"?

- **Fallito Ingestion**: Apri il file. Controlla quale colonna è cambiata da parte della banca emittente, aggiungila nei dizionari dentro `classifier.py`.
- **Riconciliazione "Non Trovato"**: Probabilmente la banca invia i soldi per il terminare `XYZ` col POS ma nel file `alias_mapping.json` non è censito da che impianto provengono.
- **Eccezione Python (Impossibile caricare / Pandas error)**: Se un excel di Satispay o di altri sistemi scaricato da un utente ha per la prima volta delle prime righe vuote usate come header inutile, Pandas prova a convertire dei NaN. Sistemare il parametro `skiprows` della libreria excel dove necessario, tramite l'estensione adeguata in `ingestion.py`.

*Buon lavoro.* 🚀
