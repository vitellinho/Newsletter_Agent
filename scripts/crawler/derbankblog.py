from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any
import re
from bs4 import BeautifulSoup, Tag

from scripts.crawler.base import fetch_html, clean_text, save_bulk_json

# -----------------------------------------------------------------------------
# Quelle & Metadaten
# -----------------------------------------------------------------------------

SOURCE = "derbankblog"
SITEMAP_INDEX_URL = "https://www.der-bank-blog.de/sitemap.xml"

# -----------------------------------------------------------------------------
# Sitemap-Handling
# -----------------------------------------------------------------------------


def extract_article_sitemaps(index_xml: str, max_chunks: int = 5, days_back: int = 7) -> list[str]:
    soup = BeautifulSoup(index_xml, "xml")
    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)

    candidates: list[tuple[datetime, str]] = []
    for sm in soup.find_all("sitemap"):
        loc = sm.find("loc").text.strip()
        if "post-sitemap" not in loc:
            continue

        try:
            lm = datetime.fromisoformat(sm.find("lastmod").text.replace("Z", "+00:00"))
        except Exception:
            continue

        if lm >= cutoff:
            candidates.append((lm, loc))

    candidates.sort(reverse=True, key=lambda t: t[0])
    return [loc for _, loc in candidates[:max_chunks]]


def get_recent_article_links(days_back: int = 7, limit: int | None = None) -> List[tuple[str, str]]:
    idx_xml = fetch_html(SITEMAP_INDEX_URL)
    chunk_urls = extract_article_sitemaps(idx_xml, max_chunks=20, days_back=days_back)

    cutoff = datetime.now(timezone.utc) - timedelta(days=days_back)
    links: list[tuple[str, str]] = []

    for chunk_url in chunk_urls:
        chunk_xml = fetch_html(chunk_url)
        soup = BeautifulSoup(chunk_xml, "xml")

        for url_tag in soup.find_all("url"):
            loc = url_tag.find("loc").text.strip()
            last_text = url_tag.find("lastmod").text
            try:
                art_dt = datetime.fromisoformat(last_text.replace("Z", "+00:00"))
            except ValueError:
                continue

            if art_dt >= cutoff:
                links.append((loc, art_dt.isoformat()))

        if limit and len(links) >= limit:
            break

    seen = set()
    ordered: list[tuple[str, str]] = []
    for loc, lm in links:
        if loc not in seen:
            seen.add(loc)
            ordered.append((loc, lm))

    return ordered[:limit] if limit else ordered

# -----------------------------------------------------------------------------
# Artikel-Fetching & Parsing
# -----------------------------------------------------------------------------


def fetch_article(url: str) -> str:
    return fetch_html(url)


def parse_article(html: str, url: str, sitemap_lastmod: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("meta", property="og:title") or soup.find("title")
    title = clean_text(title_tag.get("content") if title_tag else "")

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
        "source": SOURCE,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
    }

# -----------------------------------------------------------------------------
# Hauptfunktion
# -----------------------------------------------------------------------------


def crawl_derbankblog(days_back: int = 7, limit: int = 0) -> List[Dict[str, Any]]:
    print(f"[INFO] Sammle Artikel der letzten {days_back} Tage …")
    links = get_recent_article_links(days_back=days_back, limit=limit or None)
    print(f"[INFO] {len(links)} Links gefunden – starte Download/Parsing")

    records: List[Dict[str, Any]] = []
    for idx, (link, lastmod) in enumerate(links, 1):
        try:
            print(f"  [{idx}/{len(links)}] {link}")
            html = fetch_article(link)
            records.append(parse_article(html, link, lastmod))
        except Exception as exc:
            print(f"      ✖ Fehler bei {link}: {exc}", file=sys.stderr)

    return records


if __name__ == "__main__":
    articles = crawl_derbankblog()
    path = save_bulk_json("articles", articles)
    print(f"[derbankblog] ✔ {len(articles)} Artikel gespeichert unter: {path}")