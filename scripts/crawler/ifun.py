#!/usr/bin/env python3
"""
Crawler und Parser für SPIEGEL.de – holt alle Artikel der letzten 7 Tage

1. `fetch_html(url)` lädt den Sitemap-Index https://www.spiegel.de/sitemap.xml.
2. `extract_article_sitemaps()` wählt die jüngsten *max_chunks* Article-Sitemaps (neueste zuerst).
3. `get_recent_article_links()` durchsucht diese Sitemaps, sammelt URLs, deren `<lastmod>` im
   7-Tage-Fenster liegt, entfernt Duplikate und liefert (URL, ISO-Datum)-Tupel.
4. Für jede URL lädt `fetch_article()` das HTML und `parse_article()` extrahiert
   Titel, Autor, Text; `clean_text()` normalisiert Leerzeichen.
5. `save_bulk_json()` legt alle Datensätze gemeinsam unter
   data/raw/spiegel_raw_YYYYMMDDTHHMMSSZ.json ab.
6. `build_argparser()` definiert die CLI-Parameter --days, --limit, --sleep.
7. `main()` orchestriert den Ablauf und schreibt Fortschritts-Logs.

"""

from __future__ import annotations

# --- utilities aus base.py ----------------------------------------------------
from scripts.crawler.base import fetch_html, clean_text, save_bulk_json

import argparse
import json
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any
import itertools

from bs4 import BeautifulSoup, Tag

# ---------------------------------------------------------------------------
# Globale Einstellungen
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"

SITEMAP_INDEX_URL = "https://www.ifun.de/sitemap.xml"


# ---------------------------------------------------------------------------
# Sitemap-Handling
# ---------------------------------------------------------------------------

def extract_article_sitemaps(index_xml: str,
                             max_chunks: int = 5,
                             days_back: int = 7) -> list[str]:
    """
    Liefert nur Teil-Sitemaps, deren <lastmod> im gewünschten Zeitfenster liegt,
    sortiert nach Aktualität.
    """
    soup = BeautifulSoup(index_xml, "xml")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    candidates: list[tuple[datetime, str]] = []
    for sm in soup.find_all("sitemap"):
        loc = sm.loc.text.strip()
        if "post-sitemap" not in loc and "article" not in loc:
            continue

        try:
            lm = datetime.fromisoformat(sm.lastmod.text.replace("Z", "+00:00"))
        except Exception:
            continue

        if lm >= cutoff:
            candidates.append((lm, loc))

    # neueste zuerst
    candidates.sort(reverse=True, key=lambda t: t[0])
    return [loc for _, loc in candidates[:max_chunks]]


def get_recent_article_links(days_back: int = 7,
                             limit: int | None = None) -> List[tuple[str, str]]:
    """
    Liefert eine Liste von Tupeln  (url, sitemap_lastmod_iso).
    """
    idx_xml = fetch_html(SITEMAP_INDEX_URL)
    chunk_urls = extract_article_sitemaps(
        idx_xml,
        max_chunks=50,
        days_back=days_back,
    )

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    links: list[tuple[str, str]] = []

    for chunk_url in chunk_urls:
        chunk_xml = fetch_html(chunk_url)
        soup = BeautifulSoup(chunk_xml, "xml")

        for url_tag in soup.find_all("url"):
            loc = url_tag.loc.text.strip()
            last_text = url_tag.lastmod.text
            try:
                art_dt = datetime.fromisoformat(last_text.replace("Z", "+00:00"))
            except ValueError:
                continue

            if art_dt >= cutoff:
                links.append((loc, art_dt.isoformat()))

        if limit and len(links) >= limit:
            break

    # Duplikate entfernen, Reihenfolge behalten
    seen = set()
    ordered: list[tuple[str, str]] = []
    for loc, lm in links:
        if loc not in seen:
            seen.add(loc)
            ordered.append((loc, lm))

    return ordered[:limit] if limit else ordered


# ---------------------------------------------------------------------------
# Artikel-Fetching & Parsing
# ---------------------------------------------------------------------------


def fetch_article(url: str) -> str:
    return fetch_html(url)


def parse_article(html: str, url: str, sitemap_lastmod: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("meta", property="og:title") or soup.find("title")
    title = clean_text(title_tag.get("content") if title_tag else "")

    # Veröffentlichungsdatum direkt aus der Sitemap übernehmen
    published = sitemap_lastmod

    author_tag = soup.find("meta", {"name": "author"})
    author = author_tag["content"].strip() if author_tag and author_tag.get("content") else ""

    article_tag = soup.find("article")
    paragraphs = (
        [p.get_text(" ", strip=True) for p in article_tag.find_all("p") if isinstance(p, Tag)]
        if article_tag
        else []
    )
    text = clean_text("\n".join(paragraphs))

    return {
        "url": url,
        "title": title,
        "published": published,
        "author": author,
        "text": text,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }

# ---------------------------------------------------------------------------
# Public helper for batch-runs (importiert von crawl_all.py)
# ---------------------------------------------------------------------------

def crawl_ifun(
    days_back: int = 7,
    limit: int | None = None,
    sleep: float = 0,
) -> List[Dict[str, Any]]:
    """
    Crawlt ifun.de-Artikel und gibt nur die Datensätze zurück
    (kein Speichern).  So kann ein Sammelskript alles aggregieren.
    """
    print(f"[INFO] Sammle Artikel der letzten {days_back} Tage …")
    links = get_recent_article_links(days_back=days_back, limit=limit)
    print(f"[INFO] {len(links)} Links gefunden – starte Download/Parsing")

    records: List[Dict[str, Any]] = []
    for idx, (link, lastmod) in enumerate(links, 1):
        try:
            print(f"  [{idx}/{len(links)}] {link}")
            html = fetch_article(link)
            records.append(parse_article(html, link, lastmod))
            time.sleep(sleep)
        except Exception as exc:
            print(f"      ✖ Fehler bei {link}: {exc}", file=sys.stderr)

    return records

# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="crawler_parser",
        description="Crawlt alle Artikel der letzten X Tage über den Sitemap-Index",
    )
    parser.add_argument("--days", type=int, default=7, help="Zeitraum in Tagen (Standard: 7)")
    parser.add_argument("--limit", type=int, default=0, help="Max. Artikel (0 = unbegrenzt)")
    parser.add_argument("--sleep", type=float, default=0, help="Pause zwischen Requests (s)")
    return parser


if __name__ == "__main__":
    articles = crawl_ifun()
    path = save_bulk_json("articles", articles)
    print(f"[ifun] ✔ {len(articles)} Artikel gespeichert unter: {path}")