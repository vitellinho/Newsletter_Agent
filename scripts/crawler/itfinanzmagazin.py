#!/usr/bin/env python3
"""
Crawler und Parser für IT-Finanzmagazin (it‑finanzmagazin.de) – holt alle Artikel
der letzten 7 Tage und liefert sie als Datensätze, kompatibel mit crawl_all.py.

Voraussetzung: base.py liegt im gleichen Verzeichnis und stellt bereit:
fetch_html(), clean_text(), save_bulk_json().
"""

from __future__ import annotations
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any
from bs4 import BeautifulSoup, Tag
import re  # ensure regex available

# base importierbar machen
sys.path.append(str(Path(__file__).resolve().parent))
from base import fetch_html, clean_text, save_bulk_json

SOURCE = "itfinanzmagazin"
SITEMAP_INDEX_URL = "https://www.it-finanzmagazin.de/sitemap_index.xml"

# ---------------------------------------------------------------------------
# Sitemap-Handling
# ---------------------------------------------------------------------------
def extract_article_sitemaps(
    index_xml: str, max_chunks: int = 5, days_back: int = 7
) -> list[str]:
    """
    Sammelt bis zu max_chunks Sitemap-URLs mit 'post-sitemap' unabhängig von Sitemap-Lastmod.
    """
    soup = BeautifulSoup(index_xml, "xml")
    all_sitemaps = [
        sm.loc.text.strip()
        for sm in soup.find_all("sitemap")
        if "post-sitemap" in sm.loc.text
    ]
    # neueste zuerst
    all_sitemaps.reverse()
    return all_sitemaps[:max_chunks]

def get_recent_article_links(
    days_back: int = 7, limit: int | None = None
) -> List[tuple[str, str]]:
    """
    Durchsucht die ausgewählten Sitemaps, liefert Liste von (url, lastmod_iso).
    """
    idx_xml = fetch_html(SITEMAP_INDEX_URL)
    sitemap_urls = extract_article_sitemaps(idx_xml, days_back=days_back)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    links: list[tuple[str, str]] = []

    for sm_url in sitemap_urls:
        xml = fetch_html(sm_url)
        soup = BeautifulSoup(xml, "xml")
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

    # Duplikate entfernen
    seen = set()
    unique: list[tuple[str, str]] = []
    for loc, lm in links:
        if loc not in seen:
            seen.add(loc)
            unique.append((loc, lm))
    return unique[:limit] if limit else unique

# ---------------------------------------------------------------------------
# Artikel-Fetching & Parsing
# ---------------------------------------------------------------------------
def fetch_article(url: str) -> str:
    return fetch_html(url)

def parse_article(html: str, url: str, sitemap_lastmod: str) -> Dict[str, Any]:
    """
    Extrahiert Titel, Autor, Text und Metadaten aus dem HTML-Dokument.
    """
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("meta", property="og:title") or soup.find("title")
    title = clean_text(title_tag.get("content") if title_tag else "")

    published = sitemap_lastmod

    author_tag = soup.find("meta", {"name": "author"})
    author = author_tag["content"].strip() if author_tag and author_tag.get("content") else ""

    # Suche gängige Content-Container
    paragraphs = []
    for sel in ("article", ".post-content", ".entry-content", ".content"):
        for container in soup.select(sel):
            paragraphs.extend(
                p.get_text(" ", strip=True) for p in container.find_all("p") if isinstance(p, Tag)
            )
    # Filter Audio-/Download-Links oder „Preview:“‑Absätze
    paragraphs = [
        para
        for para in paragraphs
        if (
            ".mp3" not in para.lower()
            and not para.lower().startswith("https://")
            and not re.match(r"^\s*preview\s*:", para.lower())
        )
    ]
    text = clean_text("\n".join(paragraphs))

    return {
        "url": url,
        "title": title,
        "published": published,
        "author": author,
        "text": text,
        "source": SOURCE,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }

# ---------------------------------------------------------------------------
# Haupt-Crawler-Funktion
# ---------------------------------------------------------------------------
def crawl_itfinanzmagazin(
    days_back: int = 7, limit: int = 0, sleep: float = 0.0
) -> List[Dict[str, Any]]:
    print(f"[itfinanzmagazin] Sammle Artikel der letzten {days_back} Tage …")
    links = get_recent_article_links(days_back=days_back, limit=limit or None)
    print(f"[itfinanzmagazin] {len(links)} Links gefunden – starte Parsing")

    records: List[Dict[str, Any]] = []
    for idx, (loc, lm) in enumerate(links, 1):
        try:
            print(f"[itfinanzmagazin] [{idx}/{len(links)}] {loc}")
            html = fetch_article(loc)
            records.append(parse_article(html, loc, lm))
            time.sleep(sleep)
        except Exception as exc:
            print(f"[itfinanzmagazin] ✖ Fehler bei {loc}: {exc}")
    return records

if __name__ == "__main__":
    articles = crawl_itfinanzmagazin()
    path = save_bulk_json("articles", articles)
    print(f"[itfinanzmagazin] ✔ {len(articles)} Artikel gespeichert unter: {path}")