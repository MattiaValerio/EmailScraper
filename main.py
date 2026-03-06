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
import queue
import logging
from typing import Any
from types import SimpleNamespace
import concurrent.futures.thread as cf_thread
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED
from urllib.parse import urljoin, urlparse
from datetime import datetime, timezone
from pathlib import Path


# ── Auto-install dipendenze ────────────────────────────────────────────────────
def _install(pkg):
    subprocess.check_call(
        [sys.executable, "-m", "pip", "install", pkg, "--break-system-packages", "-q"]
    )

for _pkg in ("requests", "beautifulsoup4", "rich", "textual"):
    try:
        __import__(_pkg.replace("-", "_").split(".")[0])
    except ImportError:
        # print(f"  Installazione {_pkg}...")
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
from rich.markup import escape
from rich.text import Text

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
TUI_SETTINGS_FILE = ".mailcrawler_tui_settings.json"
ERROR_LOG_FILE = "scraping_errors.log"
TUI_DEBUG_LOG_FILE = "tui_debug.log"
APP_DIR = Path(__file__).resolve().parent

DEFAULT_TUI_SETTINGS = {
    "input_file": "websites.txt",
    "output_dir": OUTPUT_DIR,
    "workers": 5,
    "delay": 0.0,
    "exclude": "",
    "no_contact_pages": False,
    "include_any_at_text": False,
    "tld_whitelist": "",
    "use_common_tlds": False,
    "max_tld_length": 0,
    "non_email_domain_blacklist": "",
    "use_default_non_email_domains": False,
    "local_prefix_blacklist": "",
    "use_default_system_local_prefixes": False,
    "min_local_length": 1,
    "split_confidence": False,
    "ignore_non_content": False,
    "add_source_type": False,
    "max_frequency": 0,
}


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
    file_path = Path(filepath).expanduser()
    if not file_path.is_absolute():
        file_path = APP_DIR / file_path
    with open(file_path, "r", encoding="utf-8") as f:
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


def parse_tokens(text: str) -> list:
    if not text:
        return []
    return [tok for tok in re.split(r"[\s,;]+", text.strip()) if tok]


def _coerce_int(value, default: int, minimum: int = 0) -> int:
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


def _coerce_float(value, default: float, minimum: float = 0.0) -> float:
    try:
        parsed = float(str(value).strip())
    except (TypeError, ValueError):
        return default
    return max(minimum, parsed)


def load_tui_settings() -> dict:
    settings = dict(DEFAULT_TUI_SETTINGS)
    settings_path = Path(TUI_SETTINGS_FILE)
    if not settings_path.exists():
        return settings

    try:
        with open(settings_path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return settings

    if not isinstance(raw, dict):
        return settings

    for key in settings:
        if key in raw:
            settings[key] = raw[key]

    settings["workers"] = _coerce_int(settings["workers"], 5, minimum=1)
    settings["delay"] = _coerce_float(settings["delay"], 0.0, minimum=0.0)
    settings["max_tld_length"] = _coerce_int(settings["max_tld_length"], 0, minimum=0)
    settings["min_local_length"] = _coerce_int(settings["min_local_length"], 1, minimum=1)
    settings["max_frequency"] = _coerce_int(settings["max_frequency"], 0, minimum=0)

    return settings


def save_tui_settings(settings: dict) -> None:
    payload = dict(DEFAULT_TUI_SETTINGS)
    payload.update(settings)
    with open(TUI_SETTINGS_FILE, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def settings_to_args(settings: dict):
    input_file_raw = str(settings.get("input_file", "")).strip()
    output_dir_raw = str(settings.get("output_dir", OUTPUT_DIR)).strip() or OUTPUT_DIR

    input_file = Path(input_file_raw).expanduser() if input_file_raw else Path("")
    if input_file_raw and not input_file.is_absolute():
        input_file = APP_DIR / input_file

    output_dir = Path(output_dir_raw).expanduser()
    if not output_dir.is_absolute():
        output_dir = APP_DIR / output_dir

    return SimpleNamespace(
        input_file=str(input_file),
        output_dir=str(output_dir),
        workers=_coerce_int(settings.get("workers"), 5, minimum=1),
        delay=_coerce_float(settings.get("delay"), 0.0, minimum=0.0),
        exclude=parse_tokens(str(settings.get("exclude", ""))),
        no_contact_pages=bool(settings.get("no_contact_pages", False)),
        include_any_at_text=bool(settings.get("include_any_at_text", False)),
        tld_whitelist=parse_tokens(str(settings.get("tld_whitelist", ""))),
        use_common_tlds=bool(settings.get("use_common_tlds", False)),
        max_tld_length=_coerce_int(settings.get("max_tld_length"), 0, minimum=0),
        non_email_domain_blacklist=parse_tokens(str(settings.get("non_email_domain_blacklist", ""))),
        use_default_non_email_domains=bool(settings.get("use_default_non_email_domains", False)),
        local_prefix_blacklist=parse_tokens(str(settings.get("local_prefix_blacklist", ""))),
        use_default_system_local_prefixes=bool(settings.get("use_default_system_local_prefixes", False)),
        min_local_length=_coerce_int(settings.get("min_local_length"), 1, minimum=1),
        split_confidence=bool(settings.get("split_confidence", False)),
        ignore_non_content=bool(settings.get("ignore_non_content", False)),
        add_source_type=bool(settings.get("add_source_type", False)),
        max_frequency=_coerce_int(settings.get("max_frequency"), 0, minimum=0),
    )


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


def setup_run_error_logger(run_dir: Path) -> tuple[logging.Logger, Path]:
    """Crea un logger dedicato al run corrente per tracciare errori/exception."""
    log_path = run_dir / ERROR_LOG_FILE
    logger_name = f"mailcrawler.run.{run_dir.name}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.ERROR)
    logger.propagate = False

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(file_handler)
    return logger, log_path


def setup_tui_debug_logger() -> tuple[logging.Logger, Path]:
    """Logger persistente per diagnosticare avvio/stop e flusso eventi TUI."""
    log_path = APP_DIR / TUI_DEBUG_LOG_FILE
    logger_name = "mailcrawler.tui"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    for handler in list(logger.handlers):
        handler.close()
        logger.removeHandler(handler)

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    )
    logger.addHandler(file_handler)
    return logger, log_path


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

def detach_executor_threads_from_atexit(executor: ThreadPoolExecutor):
    """
    Evita che l'atexit di concurrent.futures faccia join bloccante dei worker
    dopo Ctrl+C. Usa API private in modo difensivo solo nel percorso di stop.
    """
    try:
        for t in list(getattr(executor, "_threads", ())):
            cf_thread._threads_queues.pop(t, None)
        executor._threads.clear()
    except Exception:
        # In caso di differenze tra versioni Python, non bloccare l'uscita.
        pass


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


def print_summary(results: list, paths: dict, elapsed: float, error_log_path: Path | None = None):
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

    all_unique = sorted({e for r in results for e in r["emails"]})

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
        + (f"  [red]({errors} siti)[/red]" if errors else "  [dim]vuoto[/dim]")
        + (f"\n  [dim]log tecnico      [/dim] [bold]{error_log_path.name}[/bold]" if error_log_path else ""),
        title=f"[bold]File salvati in  [cyan]{run_dir}[/cyan][/bold]",
        border_style="dim",
        padding=(0, 1),
    ))
    console.print()


def scrape_with_callbacks(args, on_event, stop_event: threading.Event | None = None):
    global OUTPUT_DIR

    try:
        urls = load_urls(args.input_file)
    except FileNotFoundError:
        on_event({"type": "error", "message": f"File non trovato: {args.input_file}"})
        return

    if not urls:
        on_event({"type": "error", "message": "Nessun URL trovato nel file indicato."})
        return

    OUTPUT_DIR = args.output_dir
    run_dir = make_run_dir()
    error_logger, error_log_path = setup_run_error_logger(run_dir)

    check_contacts = not args.no_contact_pages
    filter_cfg = build_filter_config(args)
    excluded = load_excluded_domains(args.exclude)
    split_confidence = args.split_confidence
    add_source_type = args.add_source_type

    urls_to_scan = [u for u in urls if not is_excluded(u, excluded)]
    urls_skipped = [u for u in urls if is_excluded(u, excluded)]

    results = []
    for u in urls_skipped:
        results.append({
            "url": u,
            "emails": [],
            "pages_checked": [],
            "status": "skipped",
            "error": "dominio escluso",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    on_event({
        "type": "start",
        "run_dir": str(run_dir),
        "error_log": str(error_log_path),
        "total": len(urls_to_scan),
        "skipped": len(urls_skipped),
        "workers": args.workers,
        "input_file": args.input_file,
    })

    if not urls_to_scan:
        elapsed = 0.0
        paths = save_outputs(results, run_dir)
        on_event({
            "type": "done",
            "results": results,
            "paths": paths,
            "error_log": str(error_log_path),
            "elapsed": elapsed,
            "interrupted": False,
            "total_scanned": 0,
        })
        return

    _counter = 0
    t_start = time.time()
    executor = ThreadPoolExecutor(max_workers=args.workers)
    interrupted = False

    def _worker(url: str) -> dict:
        session = requests.Session()
        try:
            try:
                result = scrape_url(url, session, check_contacts, filter_cfg,
                                    split_confidence, add_source_type)
            except Exception as exc:
                error_logger.exception("Errore non gestito nel worker per URL %s", url)
                result = {
                    "url": url,
                    "emails": [],
                    "email_details": {},
                    "pages_checked": [],
                    "status": "error",
                    "error": str(exc),
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
            if args.delay > 0:
                time.sleep(args.delay)
            return result
        finally:
            session.close()

    futures = {executor.submit(_worker, url): url for url in urls_to_scan}
    pending = set(futures.keys())

    try:
        while pending:
            if stop_event and stop_event.is_set():
                interrupted = True
                break

            done, pending = wait(pending, timeout=0.2, return_when=FIRST_COMPLETED)
            for future in done:
                url = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    error_logger.exception("Errore future.result() per URL %s", url)
                    result = {
                        "url": url,
                        "emails": [],
                        "email_details": {},
                        "pages_checked": [],
                        "status": "error",
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }

                _counter += 1
                results.append(result)
                on_event({
                    "type": "result",
                    "index": _counter,
                    "total": len(urls_to_scan),
                    "result": result,
                })

        if interrupted:
            for fut in pending:
                fut.cancel()

    finally:
        executor.shutdown(wait=not interrupted, cancel_futures=interrupted)
        if interrupted:
            detach_executor_threads_from_atexit(executor)

    elapsed = time.time() - t_start
    paths = save_outputs(results, run_dir)
    on_event({
        "type": "done",
        "results": results,
        "paths": paths,
        "error_log": str(error_log_path),
        "elapsed": elapsed,
        "interrupted": interrupted,
        "total_scanned": len(urls_to_scan),
    })


def run_tui():
    from textual.app import App, ComposeResult
    from textual.containers import Container, Horizontal, Vertical, VerticalScroll
    from textual.widgets import Header, Footer, Input, Checkbox, Button, RichLog, Static, ProgressBar

    class ScraperTuiApp(App):
        TITLE = "MailCrawler TUI"
        SUB_TITLE = "Configura, capisci cosa fa, monitora in tempo reale"

        CSS = """
        Screen {
            background: #090d14;
            color: #d6deeb;
        }
        #root { height: 1fr; }
        #layout { height: 1fr; }
        #settings {
            width: 56%;
            min-width: 64;
            border: round #ff9f1c;
            background: #0c111a;
            padding: 1 2;
        }
        #runtime {
            width: 44%;
            min-width: 56;
            border: round #2ec4b6;
            background: #0b1320;
            padding: 1 2;
        }
        .line {
            margin: 0 0 1 0;
        }
        .field-label {
            color: #9aa4b2;
            margin: 0;
        }
        .title {
            text-style: bold;
            color: #ffbf69;
            margin-bottom: 1;
        }
        .subtitle {
            text-style: bold;
            color: #b8c4d9;
            margin: 1 0 0 0;
        }
        .hint {
            color: #9aa4b2;
            margin-bottom: 1;
        }
        #actions {
            margin: 1 0;
            height: auto;
        }
        #start {
            width: 1fr;
            min-width: 22;
            margin-right: 1;
            text-style: bold;
        }
        #stop {
            width: 1fr;
            min-width: 16;
            text-style: bold;
        }
        #progress { margin: 1 0; }
        #status {
            margin: 0 0 1 0;
            color: #d6deeb;
        }
        #preview {
            border: round #334155;
            background: #0b1421;
            padding: 1;
            margin-top: 1;
            height: 14;
        }
        #kpi {
            border: round #334155;
            background: #0c1727;
            padding: 1;
            margin-bottom: 1;
        }
        #quick_help {
            border: round #334155;
            background: #0c1727;
            padding: 1;
            margin-top: 1;
        }
        Input {
            border: round #2a3342;
            background: #121926;
            color: #e5ecf6;
        }
        Input:focus {
            border: heavy #ff9f1c;
            background: #161f2e;
        }
        """

        BINDINGS = [
            ("ctrl+c", "request_stop", "Interrompi scraping"),
            ("f5", "start_scraping", "Start scraping"),
            ("f6", "request_stop", "Stop scraping"),
            ("q", "quit", "Esci"),
        ]

        def __init__(self):
            super().__init__()
            self._debug_logger, self._debug_log_path = setup_tui_debug_logger()
            self._debug_logger.debug("Inizializzazione ScraperTuiApp")
            self._settings = load_tui_settings()
            self._worker_thread = None
            self._stop_event = threading.Event()
            self._event_queue: queue.Queue[dict] = queue.Queue()
            self._scrape_running = False
            self._stop_requested = False
            self._stats = {
                "processed": 0,
                "total": 0,
                "with_email": 0,
                "no_email": 0,
                "errors": 0,
                "skipped": 0,
            }
            self._preview_cache: dict[str, Any] = {
                "path": None,
                "total_urls": None,
                "error": None,
            }

        def compose(self) -> ComposeResult:
            yield Header(show_clock=True)
            with Container(id="root"):
                with Horizontal(id="layout"):
                    with VerticalScroll(id="settings"):
                        yield Static("Configurazione scraping", classes="title")
                        yield Static(
                            "Compila i campi e premi START. TAB passa al campo successivo.",
                            classes="hint",
                        )

                        yield Static("Controlli run", classes="subtitle")
                        with Horizontal(id="actions"):
                            yield Button("START SCRAPING  [F5]", id="start", variant="success", action="start_scraping")
                            yield Button("STOP  [F6]", id="stop", variant="error", disabled=True, action="request_stop")

                        yield Static("Input e prestazioni", classes="subtitle")
                        yield Static("File URL (.txt)", classes="field-label")
                        yield Input(value=str(self._settings["input_file"]), placeholder="es. websites.txt", id="input_file", classes="line")
                        yield Static("Cartella output", classes="field-label")
                        yield Input(value=str(self._settings["output_dir"]), placeholder="es. risultati", id="output_dir", classes="line")
                        yield Static("Workers", classes="field-label")
                        yield Input(value=str(self._settings["workers"]), placeholder="es. 5", id="workers", classes="line")
                        yield Static("Delay (secondi)", classes="field-label")
                        yield Input(value=str(self._settings["delay"]), placeholder="es. 0", id="delay", classes="line")

                        yield Static("Esclusioni e filtri", classes="subtitle")
                        yield Static("Exclude domini", classes="field-label")
                        yield Input(value=str(self._settings["exclude"]), placeholder="spazio o virgola", id="exclude", classes="line")
                        yield Static("TLD whitelist", classes="field-label")
                        yield Input(value=str(self._settings["tld_whitelist"]), placeholder="it, com, org", id="tld_whitelist", classes="line")
                        yield Static("Max lunghezza TLD (0=off)", classes="field-label")
                        yield Input(value=str(self._settings["max_tld_length"]), placeholder="es. 6", id="max_tld_length", classes="line")
                        yield Static("Blacklist domini non-email", classes="field-label")
                        yield Input(value=str(self._settings["non_email_domain_blacklist"]), placeholder="es. example.com", id="non_email_domain_blacklist", classes="line")
                        yield Static("Blacklist prefissi locali", classes="field-label")
                        yield Input(value=str(self._settings["local_prefix_blacklist"]), placeholder="es. noreply", id="local_prefix_blacklist", classes="line")
                        yield Static("Min local-part", classes="field-label")
                        yield Input(value=str(self._settings["min_local_length"]), placeholder="es. 1", id="min_local_length", classes="line")
                        yield Static("Max frequency (0=off)", classes="field-label")
                        yield Input(value=str(self._settings["max_frequency"]), placeholder="es. 5", id="max_frequency", classes="line")

                        yield Checkbox("No contact pages", value=bool(self._settings["no_contact_pages"]), id="no_contact_pages")
                        yield Checkbox("Include any @ text", value=bool(self._settings["include_any_at_text"]), id="include_any_at_text")
                        yield Checkbox("Use common TLDs", value=bool(self._settings["use_common_tlds"]), id="use_common_tlds")
                        yield Checkbox("Use default non-email domains", value=bool(self._settings["use_default_non_email_domains"]), id="use_default_non_email_domains")
                        yield Checkbox("Use default system local prefixes", value=bool(self._settings["use_default_system_local_prefixes"]), id="use_default_system_local_prefixes")
                        yield Checkbox("Split confidence", value=bool(self._settings["split_confidence"]), id="split_confidence")
                        yield Checkbox("Ignore non content", value=bool(self._settings["ignore_non_content"]), id="ignore_non_content")
                        yield Checkbox("Add source type", value=bool(self._settings["add_source_type"]), id="add_source_type")

                        yield Static("Anteprima configurazione", classes="subtitle")
                        yield Static("Pronto", id="preview")

                    with Vertical(id="runtime"):
                        yield Static("Monitor esecuzione", classes="title")
                        yield Static(
                            "Stato in tempo reale: avanzamento, qualita risultati e percorsi output.",
                            classes="hint",
                        )
                        yield Static("Pronto per l'avvio", id="status")
                        yield Static("Completati: 0/0 | Con email: 0 | Senza email: 0 | Errori: 0 | Saltati: 0", id="kpi")
                        yield ProgressBar(total=1, show_eta=False, id="progress")
                        yield RichLog(id="log", wrap=True, auto_scroll=True, highlight=True)
                        yield Static(
                            "Scorciatoie:\n"
                            "- Avvia: bottone Avvia\n"
                            "- Stop sicuro: Ctrl+C o bottone Stop\n"
                            "- Uscita: q",
                            id="quick_help",
                        )
            yield Footer()

        def on_mount(self) -> None:
            self._debug_logger.debug("on_mount eseguito")
            # Focus immediato sul primo input: scrittura disponibile senza click.
            self.query_one("#input_file", Input).focus()
            # Bridge stabile thread-worker -> UI thread.
            self.set_interval(0.1, self._drain_events)
            self._refresh_preview()
            self._append_log(f"[dim]Debug log: {self._debug_log_path}[/dim]")

        def on_unmount(self) -> None:
            self._debug_logger.debug("on_unmount eseguito")
            if self._worker_thread and self._worker_thread.is_alive():
                self._stop_event.set()
                self._worker_thread.join(timeout=2.0)

        def action_start_scraping(self) -> None:
            self._debug_logger.debug("action_start_scraping invocata")
            self._start_run()

        def _drain_events(self) -> None:
            while True:
                try:
                    payload = self._event_queue.get_nowait()
                except queue.Empty:
                    break
                try:
                    self._debug_logger.debug("Evento worker ricevuto: %s", payload.get("type"))
                    self._handle_worker_event(payload)
                except Exception as exc:
                    self._debug_logger.exception("Errore in _drain_events")
                    self.query_one("#status", Static).update(f"Errore monitor UI: {exc}")
                    self._append_log(f"Errore monitor UI: {exc}", allow_markup=False)
                    self._set_running_ui(False)
                    break

        def on_input_changed(self, _: Input.Changed) -> None:
            if not self._scrape_running:
                self._refresh_preview()

        def on_checkbox_changed(self, _: Checkbox.Changed) -> None:
            if not self._scrape_running:
                self._refresh_preview()

        def action_request_stop(self) -> None:
            if self._scrape_running and not self._stop_requested:
                self._debug_logger.debug("Interruzione richiesta dall'utente")
                self._stop_requested = True
                self._stop_event.set()
                self._append_log("[yellow]Interruzione richiesta...[/yellow]")

        def on_button_pressed(self, event: Button.Pressed) -> None:
            # Fallback: alcune versioni/theme possono avere comportamenti diversi sui bottoni.
            self._debug_logger.debug("Bottone premuto: id=%s", event.button.id)
            if event.button.id == "start":
                self._start_run()
            elif event.button.id == "stop":
                self.action_request_stop()

        def _collect_settings(self) -> dict:
            data = {
                "input_file": self.query_one("#input_file", Input).value.strip(),
                "output_dir": self.query_one("#output_dir", Input).value.strip() or OUTPUT_DIR,
                "workers": _coerce_int(self.query_one("#workers", Input).value, 5, minimum=1),
                "delay": _coerce_float(self.query_one("#delay", Input).value, 0.0, minimum=0.0),
                "exclude": self.query_one("#exclude", Input).value.strip(),
                "tld_whitelist": self.query_one("#tld_whitelist", Input).value.strip(),
                "max_tld_length": _coerce_int(self.query_one("#max_tld_length", Input).value, 0, minimum=0),
                "non_email_domain_blacklist": self.query_one("#non_email_domain_blacklist", Input).value.strip(),
                "local_prefix_blacklist": self.query_one("#local_prefix_blacklist", Input).value.strip(),
                "min_local_length": _coerce_int(self.query_one("#min_local_length", Input).value, 1, minimum=1),
                "max_frequency": _coerce_int(self.query_one("#max_frequency", Input).value, 0, minimum=0),
                "no_contact_pages": self.query_one("#no_contact_pages", Checkbox).value,
                "include_any_at_text": self.query_one("#include_any_at_text", Checkbox).value,
                "use_common_tlds": self.query_one("#use_common_tlds", Checkbox).value,
                "use_default_non_email_domains": self.query_one("#use_default_non_email_domains", Checkbox).value,
                "use_default_system_local_prefixes": self.query_one("#use_default_system_local_prefixes", Checkbox).value,
                "split_confidence": self.query_one("#split_confidence", Checkbox).value,
                "ignore_non_content": self.query_one("#ignore_non_content", Checkbox).value,
                "add_source_type": self.query_one("#add_source_type", Checkbox).value,
            }
            return data

        def _set_running_ui(self, running: bool) -> None:
            self._scrape_running = running
            start_btn = self.query_one("#start", Button)
            stop_btn = self.query_one("#stop", Button)
            start_btn.disabled = running
            stop_btn.disabled = not running
            start_btn.label = "RUN IN CORSO..." if running else "START SCRAPING  [F5]"
            stop_btn.label = "STOP  [F6]"
            if not running:
                self._stop_requested = False

        def _append_log(self, line: str, allow_markup: bool = True) -> None:
            log = self.query_one("#log", RichLog)
            if allow_markup:
                try:
                    log.write(Text.from_markup(line))
                    return
                except Exception:
                    # Fallback sicuro: mostra comunque il messaggio se il markup e invalido.
                    pass
            log.write(escape(str(line)))

        def _estimate_urls(self, input_path: str):
            if not input_path:
                return 0, "input mancante"

            if self._preview_cache["path"] == input_path:
                return self._preview_cache["total_urls"], self._preview_cache["error"]

            try:
                urls = load_urls(input_path)
                total_urls = len(urls)
                err = "file vuoto" if total_urls == 0 else None
            except FileNotFoundError:
                total_urls = 0
                err = "file non trovato"
            except OSError:
                total_urls = 0
                err = "file non leggibile"

            self._preview_cache["path"] = input_path
            self._preview_cache["total_urls"] = total_urls
            self._preview_cache["error"] = err
            return total_urls, err

        def _update_kpi(self) -> None:
            s = self._stats
            self.query_one("#kpi", Static).update(
                f"Completati: {s['processed']}/{s['total']} | "
                f"Con email: {s['with_email']} | Senza email: {s['no_email']} | "
                f"Errori: {s['errors']} | Saltati: {s['skipped']}"
            )

        def _refresh_preview(self) -> None:
            settings = self._collect_settings()
            input_file = settings["input_file"]
            total_urls, input_err = self._estimate_urls(input_file)

            check_contacts = not settings["no_contact_pages"]
            mode_contacts = "sempre" if check_contacts else "solo se homepage vuota"
            mode_at = "permissivo" if settings["include_any_at_text"] else "rigoroso"

            active_filters = []
            if settings["use_common_tlds"] or settings["tld_whitelist"]:
                active_filters.append("TLD whitelist")
            if settings["max_tld_length"]:
                active_filters.append(f"max_tld={settings['max_tld_length']}")
            if settings["use_default_non_email_domains"] or settings["non_email_domain_blacklist"]:
                active_filters.append("blacklist domini")
            if settings["use_default_system_local_prefixes"] or settings["local_prefix_blacklist"]:
                active_filters.append("blacklist prefissi")
            if settings["min_local_length"] > 1:
                active_filters.append(f"min_local={settings['min_local_length']}")
            if settings["ignore_non_content"]:
                active_filters.append("ignora script/style")
            if settings["split_confidence"]:
                active_filters.append("split confidence")
            if settings["add_source_type"]:
                active_filters.append("source_type")
            if settings["max_frequency"]:
                active_filters.append(f"max_freq={settings['max_frequency']}")

            filters_label = ", ".join(active_filters) if active_filters else "nessun filtro avanzato"
            warn = f"Attenzione: {input_err}" if input_err else "Input pronto"

            self.query_one("#preview", Static).update(
                "\n".join([
                    f"Input: {input_file or '-'}",
                    f"URL rilevati: {total_urls}",
                    f"Output base: {settings['output_dir']}",
                    f"Parallelismo: {settings['workers']} worker | delay {settings['delay']}s",
                    f"Pagine contatti: {mode_contacts}",
                    f"Interpretazione '@': {mode_at}",
                    f"Filtri attivi: {filters_label}",
                    warn,
                ])
            )

        def _start_run(self) -> None:
            if self._scrape_running:
                self._debug_logger.debug("_start_run ignorato: run gia in corso")
                return

            try:
                self._debug_logger.debug("_start_run iniziato")
                settings = self._collect_settings()
                if not settings["input_file"]:
                    self._debug_logger.warning("Blocco avvio: input_file mancante")
                    self.query_one("#status", Static).update("Errore: input_file obbligatorio")
                    self._append_log("Errore: input_file obbligatorio", allow_markup=False)
                    return

                total_urls, input_err = self._estimate_urls(settings["input_file"])
                if input_err:
                    self._debug_logger.warning(
                        "Blocco avvio: input non valido (%s), url_rilevati=%s",
                        input_err,
                        total_urls,
                    )
                    self.query_one("#status", Static).update(f"Errore configurazione: {input_err}")
                    self._append_log(
                        f"Errore configurazione: {input_err} (input={settings['input_file']}, url_rilevati={total_urls})",
                        allow_markup=False,
                    )
                    return

                try:
                    save_tui_settings(settings)
                except OSError as exc:
                    self._debug_logger.exception("Errore salvataggio settings")
                    self.query_one("#status", Static).update(f"Errore salvataggio settings: {exc}")
                    self._append_log(f"Errore salvataggio settings: {exc}", allow_markup=False)
                    return

                self._stop_event.clear()
                self._stop_requested = False
                self._set_running_ui(True)
                progress = self.query_one("#progress", ProgressBar)
                progress.update(total=1, progress=0)

                log = self.query_one("#log", RichLog)
                if hasattr(log, "clear"):
                    log.clear()
                else:
                    self._append_log("[dim]Nuova esecuzione[/dim]")

                self.query_one("#status", Static).update("Validazione configurazione e avvio scraping...")

                self._stats = {
                    "processed": 0,
                    "total": 0,
                    "with_email": 0,
                    "no_email": 0,
                    "errors": 0,
                    "skipped": 0,
                }
                self._update_kpi()

                args = settings_to_args(settings)
                self._debug_logger.debug(
                    "Avvio worker: input=%s output=%s workers=%s",
                    args.input_file,
                    args.output_dir,
                    args.workers,
                )

                def _on_event(payload: dict):
                    # Strategia robusta cross-versione Textual: il worker push-a solo in coda.
                    self._event_queue.put(payload)

                def _run_worker():
                    try:
                        self._debug_logger.debug("Thread worker partito")
                        scrape_with_callbacks(args, _on_event, self._stop_event)
                        self._debug_logger.debug("Thread worker terminato normalmente")
                    except Exception as exc:
                        self._debug_logger.exception("Errore nel thread worker")
                        _on_event({"type": "error", "message": f"Errore worker: {exc}"})

                self._worker_thread = threading.Thread(
                    target=_run_worker,
                    daemon=True,
                )
                self._worker_thread.start()
                self._append_log("[cyan]Start ricevuto: preparo i worker...[/cyan]")
            except Exception as exc:
                self._debug_logger.exception("Errore avvio run")
                self.query_one("#status", Static).update(f"Errore avvio: {exc}")
                self._append_log(f"Errore avvio: {exc}", allow_markup=False)
                self._set_running_ui(False)

        def _handle_worker_event(self, payload: dict) -> None:
            kind = payload.get("type")
            self._debug_logger.debug("Gestione evento: %s", kind)

            if kind == "error":
                self._debug_logger.error("Evento errore: %s", payload.get("message"))
                self.query_one("#status", Static).update(f"Errore: {payload['message']}")
                self._append_log(f"Errore: {payload['message']}", allow_markup=False)
                self._set_running_ui(False)
                return

            if kind == "start":
                total = payload["total"]
                skipped = payload["skipped"]
                self._stats["total"] = total
                self._stats["skipped"] = skipped
                self._update_kpi()
                self.query_one("#progress", ProgressBar).update(total=max(1, total), progress=0)
                self.query_one("#status", Static).update(
                    f"In esecuzione: {total} URL da analizzare ({skipped} saltati)"
                )
                self._append_log(
                    "[bold cyan]Run avviato[/bold cyan] "
                    f"| output={payload['run_dir']} | workers={payload['workers']} | input={payload['input_file']}"
                    f" | error_log={payload.get('error_log', '-') }"
                )
                return

            if kind == "result":
                idx = payload["index"]
                total = payload["total"]
                result = payload["result"]
                self._stats["processed"] = idx
                if result["status"] == "error":
                    self._stats["errors"] += 1
                elif result["status"] == "no_emails_found":
                    self._stats["no_email"] += 1
                elif result["status"] == "ok":
                    self._stats["with_email"] += 1
                self._update_kpi()
                self.query_one("#progress", ProgressBar).update(progress=idx)
                self.query_one("#status", Static).update(f"Completati {idx}/{total} URL")
                self._append_log(_result_line(idx, total, result))
                return

            if kind == "done":
                elapsed = payload["elapsed"]
                interrupted = payload["interrupted"]
                results = payload["results"]
                paths = payload["paths"]
                total_scanned = payload["total_scanned"]

                progress = self.query_one("#progress", ProgressBar)
                progress.update(total=max(1, total_scanned), progress=total_scanned)

                with_mail = sum(1 for r in results if r["emails"])
                errors = sum(1 for r in results if r["status"] == "error")
                no_mail = sum(1 for r in results if r["status"] == "no_emails_found")
                self._stats["processed"] = total_scanned
                self._stats["with_email"] = with_mail
                self._stats["errors"] = errors
                self._stats["no_email"] = no_mail
                self._update_kpi()

                state = "Interrotto" if interrupted else "Completato"
                self.query_one("#status", Static).update(
                    f"{state} in {elapsed:.1f}s | con email={with_mail} | senza email={no_mail} | errori={errors}"
                )
                self._append_log(
                    f"[green]Output salvati[/green] | {paths['json']} | {paths['all_emails']} | {paths['no_email']} | {paths['errors']}"
                    f" | error_log={payload.get('error_log', '-') }"
                )
                self._debug_logger.debug(
                    "Run concluso: interrupted=%s elapsed=%.2fs with_email=%s no_email=%s errors=%s",
                    interrupted,
                    elapsed,
                    with_mail,
                    no_mail,
                    errors,
                )
                self._set_running_ui(False)
                self._worker_thread = None

    app = ScraperTuiApp()
    app.run()
    return 0


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    global OUTPUT_DIR

    parser = argparse.ArgumentParser(
        description="Scrapa email da una lista di URL in un file .txt"
    )
    parser.add_argument("input_file", nargs="?",
                        help="File .txt con un URL per riga")
    parser.add_argument("--tui", action="store_true",
                        help="Avvia la TUI (Textual) per configurare e monitorare lo scraping")
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

    if args.tui or not args.input_file:
        return run_tui()

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
    error_logger, error_log_path = setup_run_error_logger(run_dir)

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
            try:
                try:
                    result = scrape_url(url, session, check_contacts, filter_cfg,
                                        split_confidence, add_source_type)
                except Exception as exc:
                    error_logger.exception("Errore non gestito nel worker CLI per URL %s", url)
                    result = {
                        "url": url,
                        "emails": [],
                        "email_details": {},
                        "pages_checked": [],
                        "status": "error",
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
                if args.delay > 0:
                    time.sleep(args.delay)
                return result
            finally:
                session.close()

        executor = ThreadPoolExecutor(max_workers=args.workers)
        interrupted = False
        futures = {}
        try:
            futures = {executor.submit(_worker, url): url for url in urls_to_scan}
            for future in as_completed(futures):
                url = futures[future]
                try:
                    result = future.result()
                except Exception as exc:
                    error_logger.exception("Errore future.result() CLI per URL %s", url)
                    result = {
                        "url": url,
                        "emails": [],
                        "email_details": {},
                        "pages_checked": [],
                        "status": "error",
                        "error": str(exc),
                        "timestamp": datetime.now(timezone.utc).isoformat(),
                    }
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
        finally:
            # Evita traceback in uscita: su Ctrl+C non attendere il join dei worker.
            executor.shutdown(wait=not interrupted, cancel_futures=interrupted)
            if interrupted:
                detach_executor_threads_from_atexit(executor)

        if interrupted:
            progress.update(main_task, description="[yellow]Interrotto[/yellow]")
        else:
            progress.update(main_task, description="[green]Completato[/green]")

    elapsed = time.time() - t_start
    paths = save_outputs(results, run_dir)
    print_summary(results, paths, elapsed, error_log_path=error_log_path)

    if interrupted:
        console.print("\n  [yellow]Interrotto dall'utente.[/yellow]\n")
        return 0

    return 0


if __name__ == "__main__":
    sys.exit(main())
