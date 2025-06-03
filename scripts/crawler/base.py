"""
Basis‑Utilities für alle Quell‑Crawler des Newsletter‑Agenten.

Hier werden wiederkehrende Funktionen (HTTP‑Fetch mit Retry, GZip‑Support,
Whitespace‑Cleaning und Bulk‑JSON‑Speichern) zentral bereitgestellt, damit
einzelne Crawler‑Module wie `spiegel.py`, `ifun.py` etc. keinen Boilerplate
duplizieren müssen.
"""

from __future__ import annotations

import gzip
from io import BytesIO
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from datetime import datetime
import pytz

import requests
from bs4 import BeautifulSoup, Tag
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------#
# Konstante HTTP‑Header & Session mit Retry                                  #
# ---------------------------------------------------------------------------#

HEADERS = {
    "User-Agent": (
    "Mozilla/5.0 (compatible; MinimalCrawler/0.2; "
    "+https://github.com/vitellinho)"
)
}

def _make_session(backoff: float = 1.5, retries: int = 5) -> requests.Session:
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    sess = requests.Session()
    sess.headers.update(HEADERS)
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)
    return sess

SESSION = _make_session()
TIMEOUT = 6  # Sekunden

# ---------------------------------------------------------------------------#
# Fetch‑Utility mit GZip‑Unterstützung                                       #
# ---------------------------------------------------------------------------#

def fetch_html(url: str, timeout: int = TIMEOUT) -> str:
    """
    HTTP‑GET mit kurzem Timeout und automatischem Retry.
    Erkennt .gz‑Sitemaps und entpackt sie transparent.
    """
    resp = SESSION.get(url, timeout=timeout, stream=True)
    resp.raise_for_status()

    if url.endswith(".gz"):
        raw = BytesIO(resp.content)
        with gzip.GzipFile(fileobj=raw) as gz:
            return gz.read().decode("utf‑8", errors="ignore")
    return resp.text

# ---------------------------------------------------------------------------#
# Text‑Helfer                                                                #
# ---------------------------------------------------------------------------#

def clean_text(text: str) -> str:
    """Fasst mehrfachen Whitespace zusammen und trimmt."""
    return " ".join(text.split())

def extract_article_text(article_tag: Tag) -> str:
    """
    Wandelt einen <article>‑Tag in reinen Text um (Zeilenumbruch = ' ').
    Kann von Quell‑Crawlern genutzt werden, um Boilerplate zu vermeiden.
    """
    if not article_tag:
        return ""
    paragraphs = [p.get_text(" ", strip=True) for p in article_tag.find_all("p")]
    return clean_text(" ".join(paragraphs))

# ---------------------------------------------------------------------------#
# Bulk‑JSON speichern                                                        #
# ---------------------------------------------------------------------------#

BASE_DIR = Path(__file__).resolve().parent.parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

def save_bulk_json(prefix: str, records: List[Dict]) -> Path:
    """
    Speichert eine Liste von Dicts als pretty‑printed JSON:
    data/raw/<prefix>_raw_YYYYMMDDTHHMMSS.json
    """
    ts = datetime.now().strftime("%Y%m%dT%H%M%S")
    fp = RAW_DIR / f"{prefix}_raw_{ts}.json"
    fp.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding="utf‑8")
    return fp

# ---------------------------------------------------------------------------#
# Soup‑Factory (optional, aber bequem)                                       #
# ---------------------------------------------------------------------------#

def make_soup(html: str, parser: str = "lxml") -> BeautifulSoup:
    """Erzeugt einen BeautifulSoup mit lxml‑Parser (fällt auf html.parser zurück)."""
    try:
        return BeautifulSoup(html, parser)
    except Exception:
        return BeautifulSoup(html, "html.parser")