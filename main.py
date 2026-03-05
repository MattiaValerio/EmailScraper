#!/usr/bin/env python3
"""
Email Scraper - Estrae indirizzi email da una lista di URL
Uso: python email_scraper.py <file_urls.txt> [opzioni]
"""

import sys
import json
import re
import os
import time
import warnings
import argparse
import subprocess
import threading
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from pathlib import Path


# ── Auto-install dipendenze ────────────────────────────────────────────────────
def _install(pkg):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", pkg, "--break-system-packages", "-q"]
    )

for _pkg in ("requests", "beautifulsoup4", "rich"):
    try:
        __import__(_pkg.replace("-", "_").split(".")[0])
    except ImportError:
        print(f"  Installazione {_pkg}...")
        _install(_pkg)

import requests
import urllib3
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning, Comment
from rich.console import Console
from rich.table import Table
from rich.progress import (
    Progress, SpinnerColumn, BarColumn,
    TextColumn, TimeElapsedColumn, TaskProgressColumn
)
from rich.panel import Panel
from rich import box
from rich.rule import Rule

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Alcuni endpoint (es. sitemap/feed) possono essere XML: evitiamo warning verbosi
# quando li analizziamo comunque con parser HTML per estrarre email dal testo.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

console = Console()

# ── Costanti ───────────────────────────────────────────────────────────────────
EMAIL_REGEX = re.compile(
    r'[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

DEFAULT_NON_EMAIL_DOMAINS = {
    "example.com", "sentry.io", "githubusercontent.com", "w3.org",
    "schema.org", "google.com", "facebook.com",
}

DEFAULT_SYSTEM_LOCAL_PREFIXES = {
    "noreply", "no-reply", "donotreply", "mailer-daemon",
    "postmaster", "webmaster", "bounce", "notification",
}

COMMON_VALID_TLDS = {
    "it", "com", "org", "net", "eu", "edu", "gov", "biz", "info",
    "co", "io", "app", "dev", "ai", "uk", "de", "fr", "es", "ch",
    "nl", "be", "at", "pt", "us", "ca", "au", "jp",
}

RELIABLE_SOURCES = {"mailto", "visible_text", "obfuscated_text"}
UNCERTAIN_SOURCES = {"raw_html", "any_at_text"}

# Pattern più permissivo per testi offuscati tipo "info [at] sito [dot] com"
OBFUSCATED_REGEX = re.compile(
    r'[a-zA-Z0-9._%+\-]+'           # parte locale
    r'\s*(?:@|\[\s*at\s*\]|\(\s*at\s*\))\s*'  # @ oppure [at] o (at)
    r'[a-zA-Z0-9.\-]+'              # dominio
    r'\s*(?:\.|\[\s*dot\s*\]|\(\s*dot\s*\))\s*'  # . oppure [dot] o (dot)
    r'[a-zA-Z]{2,}',                # TLD
    re.IGNORECASE
)

CONTACT_PATHS = [
    "/contatti", "/contattaci", "/contact", "/contact-us", "/contacts",
    "/about", "/about-us", "/chi-siamo", "/info", "/informazioni",
    "/support", "/supporto", "/help", "/aiuto", "/assistenza",
    "/privacy", "/team", "/staff",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "it-IT,it;q=0.9,en-US;q=0.8,en;q=0.7",
}

TIMEOUT = 10

OUTPUT_DIR = "risultati"


# ── Utility ────────────────────────────────────────────────────────────────────
def normalize_url(url: str) -> str:
    url = url.strip()
    if not url or url.startswith("#"):
        return ""
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def short_url(url: str, max_len: int = 48) -> str:
    parsed = urlparse(url)
    s = parsed.netloc + parsed.path.rstrip("/")
    return s if len(s) <= max_len else s[:max_len - 1] + "…"


def normalize_obfuscated(raw: str) -> str:
    """Normalizza email offuscate tipo 'info [at] sito [dot] com' → 'info@sito.com'"""
    s = raw.lower()
    s = re.sub(r'\s*[\[\(]?\s*at\s*[\]\)]?\s*', '@', s)
    s = re.sub(r'\s*[\[\(]?\s*dot\s*[\]\)]?\s*', '.', s)
    s = re.sub(r'\s+', '', s)
    return s


def normalize_tld_token(tld: str) -> str:
    return tld.lower().strip().lstrip(".")


def split_email(email: str):
    if "@" not in email:
        return "", "", ""
    local, domain = email.rsplit("@", 1)
    tld = domain.rsplit(".", 1)[-1] if "." in domain else ""
    return local, domain.lower(), tld.lower()


def email_passes_filters(email: str, cfg: dict) -> bool:
    local, domain, tld = split_email(email)

    if not local or not domain or not tld:
        return False
    if len(local) < cfg["min_local_length"]:
        return False
    if cfg["max_tld_length"] and len(tld) > cfg["max_tld_length"]:
        return False
    if cfg["tld_whitelist"] and tld not in cfg["tld_whitelist"]:
        return False

    local_fold = local.replace("_", "").replace(".", "").lower()
    for bad_prefix in cfg["local_prefix_blacklist"]:
        if local_fold == bad_prefix or local_fold.startswith(bad_prefix):
            return False

    for bad_domain in cfg["non_email_domain_blacklist"]:
        if domain == bad_domain or domain.endswith("." + bad_domain):
            return False

    if any(email.endswith(x) for x in [
        ".png", ".jpg", ".jpeg", ".gif", ".svg",
        ".css", ".js", ".woff", ".ttf", ".eot", ".ico", ".webp"
    ]):
        return False
    if ".." in email or email.startswith(".") or len(email) >= 120:
        return False
    return True


def extract_at_tokens(text: str) -> set:
    """Estrae token grezzi che contengono '@' (modalita permissiva)."""
    raw_tokens = re.findall(r"\S+@\S+", text)
    return {
        t.strip("\"'()[]{}<>,;:!?\\")
        for t in raw_tokens
        if "@" in t.strip("\"'()[]{}<>,;:!?\\")
    }


def _add_matches(matches, source: str, email_sources: dict, email_counter: Counter):
    for raw in matches:
        if not raw:
            continue
        email = raw.lower().strip()
        if not email:
            continue
        email_sources[email].add(source)
        email_counter[email] += 1


def _prepare_html_for_content_only(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    for tag_name in ("script", "style", "meta", "noscript", "template"):
        for tag in soup.find_all(tag_name):
            tag.decompose()

    for comment in soup.find_all(string=lambda t: isinstance(t, Comment)):
        comment.extract()

    for tag in soup.find_all(True):
        data_keys = [k for k in tag.attrs.keys() if str(k).lower().startswith("data-")]
        for k in data_keys:
            del tag.attrs[k]

    return str(soup)


def extract_emails_from_html(html: str, cfg: dict) -> dict:
    email_sources = defaultdict(set)
    email_counter = Counter()

    html_to_scan = _prepare_html_for_content_only(html) if cfg["ignore_non_content"] else html
    soup = BeautifulSoup(html_to_scan, "html.parser")

    # 1. Tag mailto: fonte piu affidabile
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if href.lower().startswith("mailto:"):
            email = href[7:].split("?")[0].strip().lower()
            if EMAIL_REGEX.match(email):
                email_sources[email].add("mailto")
                email_counter[email] += 1

    text = soup.get_text(separator=" ")

    # 2. Regex standard su testo visibile e HTML
    _add_matches(EMAIL_REGEX.findall(text), "visible_text", email_sources, email_counter)
    _add_matches(EMAIL_REGEX.findall(html_to_scan), "raw_html", email_sources, email_counter)

    # 3. Modalita permissiva: qualsiasi token che contiene '@'
    if cfg["include_any_at_text"]:
        _add_matches(extract_at_tokens(text), "any_at_text", email_sources, email_counter)
        _add_matches(extract_at_tokens(html_to_scan), "any_at_text", email_sources, email_counter)

    # 4. Email offuscate nel testo visibile
    normalized_obf = []
    for raw in OBFUSCATED_REGEX.findall(text):
        normalized = normalize_obfuscated(raw)
        if EMAIL_REGEX.match(normalized):
            normalized_obf.append(normalized)
    _add_matches(normalized_obf, "obfuscated_text", email_sources, email_counter)

    selected_emails = set()
    selected_reliable = set()
    selected_uncertain = set()
    details = {}

    for email, sources in email_sources.items():
        if cfg["max_frequency"] and email_counter[email] >= cfg["max_frequency"]:
            continue
        if not email_passes_filters(email, cfg):
            continue

        selected_emails.add(email)
        reliability = "reliable" if any(s in RELIABLE_SOURCES for s in sources) else "uncertain"
        if reliability == "reliable":
            selected_reliable.add(email)
        else:
            selected_uncertain.add(email)

        details[email] = {
            "sources": sorted(sources),
            "frequency": email_counter[email],
            "confidence": reliability,
        }

    return {
        "emails": selected_emails,
        "reliable": selected_reliable,
        "uncertain": selected_uncertain,
        "details": details,
    }


def fetch_page(url: str, session: requests.Session):
    try:
        r = session.get(url, headers=HEADERS, timeout=TIMEOUT, allow_redirects=True)
        r.raise_for_status()
        return r.text, r.status_code
    except requests.exceptions.SSLError:
        try:
            r = session.get(url, headers=HEADERS, timeout=TIMEOUT,
                            allow_redirects=True, verify=False)
            return r.text, r.status_code
        except Exception:
            return None, None
    except requests.exceptions.RequestException:
        return None, None


def classify_email_source_type(email_domain: str, site_netloc: str) -> str:
    site = site_netloc.lower().removeprefix("www.")
    domain = email_domain.lower().removeprefix("www.")

    if domain == site or domain.endswith("." + site) or site.endswith("." + domain):
        return "site_domain"
    if domain in {"gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "icloud.com", "proton.me", "protonmail.com"}:
        return "external_freemail"
    return "external_domain"


# ── Scraping ───────────────────────────────────────────────────────────────────
def scrape_url(base_url: str, session: requests.Session,
               check_contact_pages: bool,
               filter_cfg: dict,
               split_confidence: bool,
               add_source_type: bool) -> dict:

    result = {
        "url": base_url,
        "emails": [],
        "email_details": {},
        "pages_checked": [],
        "status": "ok",
        "error": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    all_emails = set()
    reliable_emails = set()
    uncertain_emails = set()
    email_details = {}
    parsed = urlparse(base_url)
    base_domain = f"{parsed.scheme}://{parsed.netloc}"

    # Scarica homepage
    html, status = fetch_page(base_url, session)
    if html is None:
        result["status"] = "error"
        result["error"] = "Impossibile raggiungere l'URL"
        return result

    result["pages_checked"].append({"url": base_url, "status": status})
    extracted = extract_emails_from_html(html, filter_cfg)
    all_emails.update(extracted["emails"])
    reliable_emails.update(extracted["reliable"])
    uncertain_emails.update(extracted["uncertain"])
    email_details.update(extracted["details"])

    # Cerca sempre nelle pagine contatti (a prescindere da check_contact_pages)
    # Se check_contact_pages=False cerca solo se la homepage non ha trovato nulla
    should_check = check_contact_pages or len(all_emails) == 0

    if should_check:
        for path in CONTACT_PATHS:
            contact_url = urljoin(base_domain, path)
            if contact_url == base_url:
                continue
            html_c, status_c = fetch_page(contact_url, session)
            if html_c:
                result["pages_checked"].append({"url": contact_url, "status": status_c})
                extracted_c = extract_emails_from_html(html_c, filter_cfg)
                all_emails.update(extracted_c["emails"])
                reliable_emails.update(extracted_c["reliable"])
                uncertain_emails.update(extracted_c["uncertain"])
                for email, info in extracted_c["details"].items():
                    if email not in email_details:
                        email_details[email] = info
                    else:
                        merged_sources = sorted(set(email_details[email]["sources"]) | set(info["sources"]))
                        email_details[email]["sources"] = merged_sources
                        email_details[email]["frequency"] += info["frequency"]
                        if email_details[email]["confidence"] != "reliable" and info["confidence"] == "reliable":
                            email_details[email]["confidence"] = "reliable"
            time.sleep(0.15)

    result["emails"] = sorted(all_emails)
    if split_confidence:
        result["emails_reliable"] = sorted(reliable_emails)
        result["emails_uncertain"] = sorted(uncertain_emails)

    if add_source_type and all_emails:
        domain_counts = Counter(split_email(e)[1] for e in all_emails)
        result["domain_distribution"] = dict(sorted(domain_counts.items(), key=lambda kv: (-kv[1], kv[0])))

    for email in sorted(all_emails):
        local, domain, _ = split_email(email)
        detail = email_details.get(email, {"sources": [], "frequency": 0, "confidence": "uncertain"})
        entry = {
            "email": email,
            "domain": domain,
            "local": local,
            "frequency": detail["frequency"],
            "confidence": detail["confidence"],
            "source_type": classify_email_source_type(domain, parsed.netloc),
            "sources": detail["sources"],
        }
        result["email_details"][email] = entry

    if not all_emails:
        result["status"] = "no_emails_found"
    return result


# ── I/O ────────────────────────────────────────────────────────────────────────
def load_urls(filepath: str) -> list:
    with open(filepath, "r", encoding="utf-8") as f:
        return [u for line in f if (u := normalize_url(line))]


def load_token_set(items: list, normalize_domain: bool = False) -> set:
    values = set()
    if not items:
        return values

    for token in items:
        if os.path.isfile(token):
            with open(token, "r", encoding="utf-8") as f:
                candidates = [line.strip() for line in f if line.strip() and not line.strip().startswith("#")]
        else:
            candidates = [token.strip()]

        for c in candidates:
            v = c.lower().strip().strip("/")
            if normalize_domain:
                v = v.removeprefix("http://").removeprefix("https://").strip("/").removeprefix("www.")
            if v:
                values.add(v)
    return values


def load_excluded_domains(args_exclude: list) -> set:
    return load_token_set(args_exclude, normalize_domain=True)


def is_excluded(url: str, excluded: set) -> bool:
    if not excluded:
        return False
    netloc = urlparse(url).netloc.lower()
    bare = netloc.removeprefix("www.")
    for ex in excluded:
        ex_bare = ex.removeprefix("www.")
        if bare == ex_bare or bare.endswith("." + ex_bare):
            return True
    return False


def build_filter_config(args) -> dict:
    tld_whitelist = load_token_set(args.tld_whitelist)
    if args.use_common_tlds:
        tld_whitelist |= COMMON_VALID_TLDS
    tld_whitelist = {normalize_tld_token(t) for t in tld_whitelist if t}

    non_email_domains = set()
    if args.use_default_non_email_domains:
        non_email_domains |= DEFAULT_NON_EMAIL_DOMAINS
    non_email_domains |= load_token_set(args.non_email_domain_blacklist, normalize_domain=True)

    local_blacklist = set()
    if args.use_default_system_local_prefixes:
        local_blacklist |= DEFAULT_SYSTEM_LOCAL_PREFIXES
    local_blacklist |= load_token_set(args.local_prefix_blacklist)
    local_blacklist = {
        t.replace("_", "").replace(".", "").lower()
        for t in local_blacklist if t
    }

    return {
        "include_any_at_text": args.include_any_at_text,
        "ignore_non_content": args.ignore_non_content,
        "max_tld_length": args.max_tld_length,
        "tld_whitelist": tld_whitelist,
        "non_email_domain_blacklist": non_email_domains,
        "local_prefix_blacklist": local_blacklist,
        "min_local_length": args.min_local_length,
        "max_frequency": args.max_frequency,
    }


def make_run_dir() -> Path:
    """Crea la cartella risultati/YYYY-MM-DD_HH-MM-SS/ per questa esecuzione."""
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    run_dir = Path(OUTPUT_DIR) / ts
    run_dir.mkdir(parents=True, exist_ok=True)
    return run_dir


def save_outputs(results: list, run_dir: Path) -> dict:
    """
    Salva nella cartella di run:
      - output.json          → tutti i risultati
      - no_email.txt         → siti raggiungibili ma senza email
      - errori.txt           → siti non raggiungibili
            - all_emails.txt       → tutte le email trovate (solo email, una per riga)
    Restituisce un dict con i path usati.
    """
    # output.json
    json_path = run_dir / "output.json"
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "total_urls": len(results),
            "urls_with_emails": sum(1 for r in results if r["emails"]),
            "total_emails_found": sum(len(r["emails"]) for r in results),
            "results": results,
        }, f, ensure_ascii=False, indent=2)

    # no_email.txt
    no_email_path = run_dir / "no_email.txt"
    no_email = [r["url"] for r in results if r["status"] == "no_emails_found"]
    with open(no_email_path, "w", encoding="utf-8") as f:
        f.write("\n".join(no_email) + ("\n" if no_email else ""))

    # errori.txt
    errors_path = run_dir / "errori.txt"
    errors = [f"{r['url']}  →  {r['error']}" for r in results if r["status"] == "error"]
    with open(errors_path, "w", encoding="utf-8") as f:
        f.write("\n".join(errors) + ("\n" if errors else ""))

    # all_emails.txt
    all_emails_path = run_dir / "all_emails.txt"
    all_emails = sorted({e for r in results for e in r["emails"]})
    with open(all_emails_path, "w", encoding="utf-8") as f:
        f.write("\n".join(all_emails) + ("\n" if all_emails else ""))

    return {
        "json": json_path,
        "no_email": no_email_path,
        "errors": errors_path,
        "all_emails": all_emails_path,
        "dir": run_dir,
    }


# ── UI helpers ─────────────────────────────────────────────────────────────────
def print_banner():
    console.print()
    console.print(Panel.fit(
        "[bold cyan]  Email Scraper[/bold cyan]\n"
        "[dim]Estrae indirizzi email da una lista di URL[/dim]",
        border_style="cyan",
        padding=(0, 6),
    ))
    console.print()


def print_config(input_file, run_dir, n_urls, contact_pages, excluded, workers,
                 filter_cfg, split_confidence, add_source_type):
    t = Table(box=box.SIMPLE, show_header=False, padding=(0, 2))
    t.add_column(style="dim", justify="right")
    t.add_column(style="white")
    t.add_row("Input",             f"[bold]{input_file}[/bold]")
    t.add_row("Cartella output",   f"[bold]{run_dir}[/bold]")
    t.add_row("URL da analizzare", f"[bold cyan]{n_urls}[/bold cyan]")
    t.add_row("Pagine contatti",   "[green]sempre[/green]" if contact_pages else "[yellow]solo se homepage vuota[/yellow]")
    t.add_row("Interpretazione @", "[yellow]permissiva (qualsiasi token con @)[/yellow]" if filter_cfg["include_any_at_text"] else "[green]rigorosa (solo email valide)[/green]")
    t.add_row("Thread paralleli",  f"[bold cyan]{workers}[/bold cyan]")
    t.add_row("Ignora script/style/meta", "[green]attivo[/green]" if filter_cfg["ignore_non_content"] else "[dim]disattivo[/dim]")
    t.add_row("Whitelist TLD", f"[cyan]{len(filter_cfg['tld_whitelist'])}[/cyan]" if filter_cfg["tld_whitelist"] else "[dim]disattiva[/dim]")
    t.add_row("Max lunghezza TLD", str(filter_cfg["max_tld_length"]) if filter_cfg["max_tld_length"] else "[dim]disattivo[/dim]")
    t.add_row("Min local-part", f"[cyan]{filter_cfg['min_local_length']}[/cyan]")
    t.add_row("Blacklist domini non-email", f"[red]{len(filter_cfg['non_email_domain_blacklist'])}[/red]" if filter_cfg["non_email_domain_blacklist"] else "[dim]disattiva[/dim]")
    t.add_row("Blacklist prefissi locali", f"[red]{len(filter_cfg['local_prefix_blacklist'])}[/red]" if filter_cfg["local_prefix_blacklist"] else "[dim]disattiva[/dim]")
    t.add_row("Soglia frequenza", f"[yellow]>= {filter_cfg['max_frequency']} scartata[/yellow]" if filter_cfg["max_frequency"] else "[dim]disattiva[/dim]")
    t.add_row("Split affidabilita", "[green]attivo[/green]" if split_confidence else "[dim]disattivo[/dim]")
    t.add_row("source_type nel JSON", "[green]attivo[/green]" if add_source_type else "[dim]disattivo[/dim]")
    if excluded:
        ex_str = "  ".join(f"[dim red]{d}[/dim red]" for d in sorted(excluded))
        t.add_row("Domini esclusi", f"[red]{len(excluded)}[/red]  {ex_str}")
    console.print(t)
    console.print(Rule(style="dim"))
    console.print()


def _result_line(i: int, total: int, result: dict) -> str:
    w = len(str(total))
    idx  = f"[dim][{i:>{w}}/{total}][/dim]"
    host = f"[white bold]{short_url(result['url'])}[/white bold]"

    if result["status"] == "skipped":
        tag   = "[on bright_black] SKIP [/on bright_black]"
        extra = f"  [dim]{result['error']}[/dim]"
    elif result["status"] == "error":
        tag   = "[on red] ERRORE [/on red]"
        extra = f"  [dim red]{result['error']}[/dim red]"
    elif not result["emails"]:
        tag   = "[on yellow black] VUOTO [/on yellow black]"
        extra = ""
    else:
        count = len(result["emails"])
        tag   = f"[on green black] {count} email [/on green black]"
        mails = "  ".join(f"[cyan]{e}[/cyan]" for e in result["emails"])
        extra = f"  {mails}"

    return f"  {idx}  {host}  {tag}{extra}"


def print_summary(results: list, paths: dict, elapsed: float):
    total      = len(results)
    with_mail  = sum(1 for r in results if r["emails"])
    no_mail    = sum(1 for r in results if r["status"] == "no_emails_found")
    errors     = sum(1 for r in results if r["status"] == "error")
    skipped    = sum(1 for r in results if r["status"] == "skipped")
    tot_emails = sum(len(r["emails"]) for r in results)

    console.print()
    console.print(Rule(style="dim"))
    console.print()

    # Tabella riepilogo
    t = Table(box=box.SIMPLE_HEAVY, show_header=False, padding=(0, 3))
    t.add_column(justify="right", style="dim")
    t.add_column(justify="left")
    t.add_row("URL analizzati",  f"[bold white]{total}[/bold white]")
    t.add_row("Con email",       f"[bold green]{with_mail}[/bold green]")
    t.add_row("Senza email",     f"[yellow]{no_mail}[/yellow]")
    t.add_row("Saltati",         f"[dim]{skipped}[/dim]" if skipped else "[dim]0[/dim]")
    t.add_row("Errori",          f"[red]{errors}[/red]" if errors else "[dim]0[/dim]")
    t.add_row("Email trovate",   f"[bold cyan]{tot_emails}[/bold cyan]")
    t.add_row("Tempo",           f"[dim]{elapsed:.1f}s[/dim]")

    border = "green" if not errors else "yellow"
    console.print(Panel(t, title="[bold]Riepilogo[/bold]",
                        border_style=border, padding=(0, 2)))

    # Email uniche
    all_unique = sorted({e for r in results for e in r["emails"]})
    if all_unique:
        console.print()
        body = "\n".join(f"  [cyan]{e}[/cyan]" for e in all_unique)
        console.print(Panel(
            body,
            title=f"[bold]Email uniche trovate  ·  {len(all_unique)}[/bold]",
            border_style="cyan",
            padding=(0, 1),
        ))

    # File salvati
    console.print()
    run_dir = paths["dir"]
    console.print(Panel(
        f"  [dim]output completo  [/dim] [bold]{paths['json'].name}[/bold]\n"
        f"  [dim]tutte le email   [/dim] [bold]{paths['all_emails'].name}[/bold]"
        + (f"  [cyan]({len(all_unique)} email)[/cyan]" if all_unique else "  [dim]vuoto[/dim]") + "\n"
        f"  [dim]senza email      [/dim] [bold]{paths['no_email'].name}[/bold]"
        + (f"  [yellow]({no_mail} siti)[/yellow]" if no_mail else "  [dim]vuoto[/dim]") + "\n"
        f"  [dim]errori           [/dim] [bold]{paths['errors'].name}[/bold]"
        + (f"  [red]({errors} siti)[/red]" if errors else "  [dim]vuoto[/dim]"),
        title=f"[bold]File salvati in  [cyan]{run_dir}[/cyan][/bold]",
        border_style="dim",
        padding=(0, 1),
    ))
    console.print()


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    global OUTPUT_DIR

    parser = argparse.ArgumentParser(
        description="Scrapa email da una lista di URL in un file .txt"
    )
    parser.add_argument("input_file",
                        help="File .txt con un URL per riga")
    parser.add_argument("--no-contact-pages", action="store_true",
                        help="Cerca le pagine contatti SOLO se la homepage non ha email")
    parser.add_argument("--delay", type=float, default=0.0,
                        help="Pausa aggiuntiva in secondi tra richieste (default: 0)")
    parser.add_argument("--exclude", metavar="DOMINIO", nargs="+",
                        help="Domini da escludere: file .txt o lista diretta")
    parser.add_argument("--workers", type=int, default=5,
                        help="Numero di thread paralleli (default: 5)")
    parser.add_argument("--output-dir", default=OUTPUT_DIR,
                        help=f"Cartella base per i risultati (default: {OUTPUT_DIR})")
    parser.add_argument("--include-any-at-text", action="store_true",
                        help="Modalita permissiva: include qualsiasi testo/token che contiene '@'")
    parser.add_argument("--tld-whitelist", metavar="TLD", nargs="+",
                        help="Whitelist TLD (es: it com org) o file .txt con un TLD per riga")
    parser.add_argument("--use-common-tlds", action="store_true",
                        help="Attiva una whitelist integrata di TLD comuni")
    parser.add_argument("--max-tld-length", type=int, default=0,
                        help="Scarta email con TLD piu lungo di N caratteri (0=disattivo)")
    parser.add_argument("--non-email-domain-blacklist", metavar="DOMINIO", nargs="+",
                        help="Domini da scartare nelle email (es: example.com), o file .txt")
    parser.add_argument("--use-default-non-email-domains", action="store_true",
                        help="Attiva blacklist domini noti non-email (example.com, schema.org, ...)")
    parser.add_argument("--local-prefix-blacklist", metavar="PREFISSO", nargs="+",
                        help="Prefissi local-part da scartare (es: noreply postmaster), o file .txt")
    parser.add_argument("--use-default-system-local-prefixes", action="store_true",
                        help="Attiva blacklist prefissi di sistema (noreply, mailer-daemon, ...)")
    parser.add_argument("--min-local-length", type=int, default=1,
                        help="Lunghezza minima della parte locale prima della @ (default: 1)")
    parser.add_argument("--split-confidence", action="store_true",
                        help="Salva anche emails_reliable ed emails_uncertain nel JSON")
    parser.add_argument("--ignore-non-content", action="store_true",
                        help="Ignora script/style/meta/commenti e attributi data-* durante l'estrazione")
    parser.add_argument("--add-source-type", action="store_true",
                        help="Aggiunge source_type e domain_distribution nel JSON")
    parser.add_argument("--max-frequency", type=int, default=0,
                        help="Scarta email ripetute >= N volte nella stessa pagina (0=disattivo)")
    args = parser.parse_args()

    print_banner()

    try:
        urls = load_urls(args.input_file)
    except FileNotFoundError:
        console.print(f"[red bold]✗  File non trovato:[/red bold] {args.input_file}")
        sys.exit(1)

    if not urls:
        console.print("[yellow]⚠  Nessun URL trovato nel file.[/yellow]")
        sys.exit(1)

    # Prepara output dir
    OUTPUT_DIR = args.output_dir
    run_dir = make_run_dir()

    check_contacts = not args.no_contact_pages
    filter_cfg = build_filter_config(args)
    excluded = load_excluded_domains(args.exclude)
    split_confidence = args.split_confidence
    add_source_type = args.add_source_type

    urls_to_scan = [u for u in urls if not is_excluded(u, excluded)]
    urls_skipped = [u for u in urls if is_excluded(u, excluded)]

    print_config(args.input_file, run_dir,
                 len(urls_to_scan), check_contacts, excluded, args.workers,
                 filter_cfg, split_confidence, add_source_type)

    results = []
    for u in urls_skipped:
        results.append({
            "url": u, "emails": [], "pages_checked": [],
            "status": "skipped", "error": "dominio escluso",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    _print_lock = threading.Lock()
    _counter = [0]
    t_start = time.time()

    with Progress(
        SpinnerColumn(style="cyan"),
        BarColumn(bar_width=24, style="dim cyan", complete_style="bold cyan"),
        TaskProgressColumn(),
        TextColumn("{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        main_task = progress.add_task(
            f"[dim]0/{len(urls_to_scan)} completati[/dim]",
            total=len(urls_to_scan)
        )

        def _worker(url: str) -> dict:
            session = requests.Session()
            result = scrape_url(url, session, check_contacts, filter_cfg,
                                split_confidence, add_source_type)
            session.close()
            if args.delay > 0:
                time.sleep(args.delay)
            return result

        executor = ThreadPoolExecutor(max_workers=args.workers)
        interrupted = False
        try:
            futures = {executor.submit(_worker, url): url for url in urls_to_scan}
            for future in as_completed(futures):
                result = future.result()
                with _print_lock:
                    _counter[0] += 1
                    n = _counter[0]
                    results.append(result)
                    progress.console.print(_result_line(n, len(urls_to_scan), result))
                    progress.advance(main_task)
                    progress.update(
                        main_task,
                        description=f"[dim]{n}/{len(urls_to_scan)} completati[/dim]"
                    )
        except KeyboardInterrupt:
            interrupted = True
            for f in futures:
                f.cancel()
            progress.update(main_task, description="[yellow]Interruzione in corso...[/yellow]")
            raise
        finally:
            # Evita traceback in uscita: su Ctrl+C non attendere il join dei worker.
            executor.shutdown(wait=not interrupted, cancel_futures=interrupted)

        progress.update(main_task, description="[green]Completato[/green]")

    elapsed = time.time() - t_start
    paths = save_outputs(results, run_dir)
    print_summary(results, paths, elapsed)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n\n  [yellow]Interrotto dall'utente.[/yellow]\n")
        sys.exit(0)