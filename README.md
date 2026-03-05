# ✉ Email Scraper

Script Python per estrarre automaticamente indirizzi email da una lista di URL.  
Cerca nei tag `mailto:`, nel testo visibile, nell'HTML grezzo, nelle email offuscate (tipo `info [at] sito [dot] com`) e nelle pagine di contatto più comuni.

---

## Requisiti

- Python 3.10+
- Le dipendenze vengono **installate automaticamente** al primo avvio:
  - `requests`
  - `beautifulsoup4`
  - `rich`
  - `textual`

---

## Utilizzo

```bash
python main.py <input.txt> [opzioni]
```

### Modalita TUI (Textual)

Puoi avviare l'interfaccia testuale in due modi:

```bash
python main.py --tui
```

oppure senza argomenti:

```bash
python main.py
```

Nella TUI puoi:

- impostare i parametri principali dello scraping da form
- vedere un'anteprima live della configurazione (input, URL rilevati, filtri attivi)
- avviare/interrompere il run
- seguire progresso, KPI (con/senza email, errori, saltati) e risultati in tempo reale
- vedere i path dei file salvati a fine esecuzione

Le impostazioni vengono salvate automaticamente in `.mailcrawler_tui_settings.json` e riproposte al prossimo avvio.

### Argomenti

| Argomento | Descrizione |
|-----------|-------------|
| `input.txt` | File di testo con un URL per riga **(obbligatorio)** |

### Opzioni

| Opzione | Descrizione |
|---------|-------------|
| `--no-contact-pages` | Cerca le pagine contatti **solo** se la homepage non ha trovato email (default: le cerca sempre) |
| `--delay N` | Pausa aggiuntiva in secondi tra richieste nello stesso thread (default: `0`) |
| `--workers N` | Numero di siti analizzati in parallelo (default: `5`) |
| `--exclude DOMINIO ...` | Esclude uno o più domini dalla ricerca |
| `--exclude file.txt` | Esclude i domini elencati in un file `.txt` (uno per riga) |
| `--output-dir CARTELLA` | Cartella base per i risultati (default: `risultati`) |
| `--tld-whitelist TLD ...` | Accetta solo email con TLD in whitelist (o da file `.txt`) |
| `--use-common-tlds` | Attiva una whitelist integrata di TLD comuni (`it`, `com`, `org`, `net`, ...) |
| `--max-tld-length N` | Scarta email con TLD piu lungo di `N` caratteri (es. `6`) |
| `--non-email-domain-blacklist DOMINIO ...` | Scarta email appartenenti a domini noti non-contatto (o da file `.txt`) |
| `--use-default-non-email-domains` | Attiva blacklist integrata (`example.com`, `schema.org`, `google.com`, ...) |
| `--local-prefix-blacklist PREFISSO ...` | Scarta email con prefissi locali non utili (o da file `.txt`) |
| `--use-default-system-local-prefixes` | Attiva blacklist integrata (`noreply`, `postmaster`, `mailer-daemon`, ...) |
| `--min-local-length N` | Impone lunghezza minima della parte locale prima della `@` |
| `--ignore-non-content` | Ignora estrazioni da `script/style/meta/commenti` e attributi `data-*` |
| `--split-confidence` | Salva nel JSON anche `emails_reliable` e `emails_uncertain` |
| `--add-source-type` | Aggiunge nel JSON `source_type` per email e `domain_distribution` per sito |
| `--max-frequency N` | Scarta email ripetute almeno `N` volte nella stessa pagina |

---

## Esempi

```bash
# Uso base
python email_scraper.py urls.txt

# 10 thread paralleli
python email_scraper.py urls.txt --workers 10

# Cerca solo nella homepage se trova già email lì
python email_scraper.py urls.txt --no-contact-pages

# Escludi domini specifici
python email_scraper.py urls.txt --exclude facebook.com google.com

# Escludi domini da file
python email_scraper.py urls.txt --exclude esclusi.txt

# Cartella di output personalizzata
python email_scraper.py urls.txt --output-dir /miei/risultati

# Whitelist TLD + limite lunghezza TLD
python email_scraper.py urls.txt --use-common-tlds --max-tld-length 6

# Blacklist domini non-email (integrata + custom)
python email_scraper.py urls.txt --use-default-non-email-domains --non-email-domain-blacklist sentry.io w3.org

# Blacklist prefissi locali di sistema + lunghezza minima local-part
python email_scraper.py urls.txt --use-default-system-local-prefixes --min-local-length 2

# Ignora contenuto non utile e separa email affidabili/incerte
python email_scraper.py urls.txt --ignore-non-content --split-confidence

# Aggiunge metadati source_type e filtra email troppo ripetute
python email_scraper.py urls.txt --add-source-type --max-frequency 5
```

Nota: negli esempi sopra puoi sostituire `python email_scraper.py` con `python main.py` se usi il file del workspace corrente.

---

## Output

Ad ogni esecuzione viene creata automaticamente una sottocartella con timestamp:

```
risultati/
└── 2026-03-05_14-30-00/
    ├── output.json      ← tutti i risultati completi
    ├── all_emails.txt   ← tutte le mail trovate
    ├── no_email.txt     ← URL raggiungibili ma senza email trovata
    └── errori.txt       ← URL non raggiungibili, con motivo
```

In questo modo le esecuzioni successive non sovrascrivono mai i risultati precedenti.

### output.json

```json
{
  "generated_at": "2026-03-05T14:30:00+00:00",
  "total_urls": 10,
  "urls_with_emails": 6,
  "total_emails_found": 14,
  "results": [
    {
      "url": "https://www.esempio.it",
      "emails": ["info@esempio.it", "contatti@esempio.it"],
      "pages_checked": [
        { "url": "https://www.esempio.it", "status": 200 },
        { "url": "https://www.esempio.it/contatti", "status": 200 }
      ],
      "status": "ok",
      "error": null,
      "timestamp": "2026-03-05T14:30:01+00:00"
    }
  ]
}
```

Con i flag avanzati attivi, ogni risultato puo includere anche:

- `emails_reliable`: email trovate in fonti ad alta affidabilita (es. `mailto:`, testo visibile, testo offuscato)
- `emails_uncertain`: email trovate solo in contesti meno affidabili (es. HTML grezzo)
- `email_details`: mappa per-email con:
  - `sources`: sorgenti in cui e stata trovata
  - `frequency`: quante volte compare
  - `confidence`: `reliable` oppure `uncertain`
  - `source_type`: `site_domain`, `external_domain`, `external_freemail` (se usi `--add-source-type`)
- `domain_distribution`: conteggio email per dominio nello stesso sito (se usi `--add-source-type`)

### Filtri avanzati (nuovi)

I seguenti filtri sono tutti attivabili da CLI e combinabili tra loro:

1. **Whitelist TLD validi**
  - `--tld-whitelist it com org` oppure `--use-common-tlds`
  - opzionale: `--max-tld-length 6`

2. **Blacklist domini noti non-email**
  - `--non-email-domain-blacklist dominio1 dominio2`
  - oppure `--use-default-non-email-domains`

3. **Blacklist prefissi locali falsi/sistema**
  - `--local-prefix-blacklist noreply postmaster`
  - oppure `--use-default-system-local-prefixes`

4. **Lunghezza minima local-part**
  - `--min-local-length 2`

5. **Contesto HTML prioritario (affidabile vs incerto)**
  - `--split-confidence`

6. **Esclusione da tag non-contenuto**
  - `--ignore-non-content`

7. **Deduplicazione/analisi per dominio con `source_type`**
  - `--add-source-type`

8. **Soglia di frequenza**
  - `--max-frequency 5`

### no_email.txt

Un URL per riga — siti raggiungibili dove non è stata trovata alcuna email.

### errori.txt

Un URL per riga con il motivo — siti non raggiungibili o con errore HTTP.

```
https://sito-offline.it  →  Impossibile raggiungere l'URL
```

### Valori di `status`

| Valore | Significato |
|--------|-------------|
| `ok` | Email trovate |
| `no_emails_found` | Pagina raggiunta ma nessuna email trovata |
| `error` | Impossibile raggiungere l'URL |
| `skipped` | Dominio escluso tramite `--exclude` |

---

## Come funziona

1. Carica la lista di URL dal file di input
2. Filtra subito i domini presenti nella lista di esclusione
3. I siti vengono processati in parallelo con `N` thread (`--workers`)
4. Per ogni URL lo script:
   - Esegue una GET sulla homepage
   - Cerca email nei tag `<a href="mailto:...">`, nel testo visibile e nell'HTML grezzo
   - Cerca email **offuscate** nel testo (es. `info [at] sito [dot] com`)
  - Applica (se attivi) i filtri su TLD, domini blacklist, prefissi locali, lunghezza minima local-part e soglia di frequenza
  - Puo ignorare contenuti non utili (`script/style/meta/commenti/data-*`) con `--ignore-non-content`
   - Visita le pagine contatti più comuni (`/contatti`, `/contact`, `/chi-siamo`, `/about`, ecc.) — sempre di default, oppure solo se la homepage non ha prodotto risultati con `--no-contact-pages`
5. Deduplica e ordina le email trovate
6. Salva i file di output nella cartella cronologica

---

## Note

- I certificati SSL non validi vengono gestiti automaticamente con un retry senza verifica
- L'esclusione domini supporta sottodomini: escludere `facebook.com` esclude anche `www.facebook.com`
- Con `--workers 10` su liste lunghe la velocità è ~5-10x rispetto all'elaborazione seriale
- Premi `Ctrl+C` in qualsiasi momento per interrompere senza errori
