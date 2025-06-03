

#!/usr/bin/env python3
"""
Crawler und Parser für Finance Forward (financefwd.com) – holt alle Artikel
der letzten 7 Tage und speichert sie einzeln oder via crawl_all.py.

Voraussetzung: base.py ist im selben Verzeichnis und stellt die Funktionen
fetch_html, clean_text und save_bulk_json bereit.
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any

from bs4 import BeautifulSoup, Tag

# lokales base-Modul importieren
sys.path.append(str(Path(__file__).resolve().parent))
from base import fetch_html, clean_text, save_bulk_json

SOURCE = "financefwd"
SITEMAP_INDEX_URL = "https://financefwd.com/sitemap_index.xml"


def extract_article_sitemaps(
    index_xml: str, max_chunks: int = 5, days_back: int = 7
) -> list[str]:
    """Liefert bis zu max_chunks Sitemaps mit post-sitemap im letzten days_back-Zeitraum."""
    soup = BeautifulSoup(index_xml, "xml")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    candidates: list[tuple[datetime, str]] = []

    for sm in soup.find_all("sitemap"):
        loc = sm.loc.text.strip()
        if "post-sitemap" not in loc:
            continue
        try:
            lm = datetime.fromisoformat(sm.lastmod.text.replace("Z", "+00:00"))
        except Exception:
            continue
        if lm >= cutoff:
            candidates.append((lm, loc))

    # neueste zuerst, begrenzt auf max_chunks
    candidates.sort(reverse=True, key=lambda x: x[0])
    return [loc for _, loc in candidates[:max_chunks]]


def get_recent_article_links(
    days_back: int = 7, limit: int | None = None
) -> List[tuple[str, str]]:
    """Sammelt Artikel-URLs + LastMod aus den ausgewählten Sitemaps."""
    idx_xml = fetch_html(SITEMAP_INDEX_URL)
    sitemap_urls = extract_article_sitemaps(idx_xml, days_back=days_back)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    links: list[tuple[str, str]] = []

    for sm_url in sitemap_urls:
        xml = fetch_html(sm_url)
        soup = BeautifulSoup(xml, "xml")
        for url_tag in soup.find_all("url"):
            loc = url_tag.loc.text.strip()
            lastmod = url_tag.lastmod.text
            try:
                art_dt = datetime.fromisoformat(lastmod.replace("Z", "+00:00"))
            except ValueError:
                continue
            if art_dt >= cutoff:
                links.append((loc, art_dt.isoformat()))
        if limit and len(links) >= limit:
            break

    # Duplikate entfernen, Reihenfolge behalten
    seen = set()
    unique: list[tuple[str, str]] = []
    for loc, lm in links:
        if loc not in seen:
            seen.add(loc)
            unique.append((loc, lm))
    return unique[:limit] if limit else unique


def fetch_article(url: str) -> str:
    return fetch_html(url)


def parse_article(html: str, url: str, sitemap_lastmod: str) -> Dict[str, Any]:
    """Extrahiert Titel, Autor, Text und Metadaten aus dem HTML."""
    soup = BeautifulSoup(html, "html.parser")
    title_tag = soup.find("meta", property="og:title") or soup.find("title")
    title = clean_text(title_tag.get("content") if title_tag else "")

    # Datum aus Sitemap übernehmen
    published = sitemap_lastmod

    # Author (falls vorhanden)
    author_tag = soup.find("meta", {"name": "author"})
    author = author_tag["content"].strip() if author_tag and author_tag.get("content") else ""

    # Content container suchen
    paragraphs = []
    for sel in ("article", ".post-content", ".entry-content", ".content"):
        for container in soup.select(sel):
            paragraphs.extend(
                p.get_text(" ", strip=True) for p in container.find_all("p") if isinstance(p, Tag)
            )
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


def crawl_financefwd(
    days_back: int = 7, limit: int = 0, sleep: float = 0.0
) -> List[Dict[str, Any]]:
    """Hauptfunktion zum Sammeln der Artikel-Records."""
    print(f"[financefwd] Sammle Artikel der letzten {days_back} Tage …")
    links = get_recent_article_links(days_back=days_back, limit=limit or None)
    print(f"[financefwd] {len(links)} Links gefunden …")

    records: List[Dict[str, Any]] = []
    for idx, (loc, lm) in enumerate(links, 1):
        try:
            print(f"[financefwd] [{idx}/{len(links)}] {loc}")
            html = fetch_article(loc)
            records.append(parse_article(html, loc, lm))
            time.sleep(sleep)
        except Exception as e:
            print(f"[financefwd] ✖ Fehler bei {loc}: {e}", file=sys.stderr)
    return records

if __name__ == "__main__":
    articles = crawl_financefwd()
    path = save_bulk_json("articles", articles)
    print(f"[financefwd] ✔ {len(articles)} Artikel gespeichert unter: {path}")