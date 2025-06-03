from __future__ import annotations

import sys
import time
import json
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Dict, Any

from bs4 import BeautifulSoup, Tag

sys.path.append(str(Path(__file__).resolve().parent))  # base importierbar machen
from scripts.crawler.base import fetch_html, clean_text, save_bulk_json

SOURCE = "cio"
SITEMAP_INDEX_URL = "https://www.cio.de/sitemap.xml"

def extract_article_sitemaps(index_xml: str, max_chunks: int = 10, days_back: int = 7) -> list[str]:
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

    candidates.sort(reverse=True, key=lambda t: t[0])
    return [loc for _, loc in candidates[:max_chunks]]

def get_recent_article_links(days_back: int = 7, limit: int | None = None) -> List[tuple[str, str]]:
    idx_xml = fetch_html(SITEMAP_INDEX_URL)
    chunk_urls = extract_article_sitemaps(idx_xml, max_chunks=50, days_back=days_back)

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

    seen = set()
    ordered: list[tuple[str, str]] = []
    for loc, lm in links:
        if loc not in seen:
            seen.add(loc)
            ordered.append((loc, lm))

    return ordered[:limit] if limit else ordered

def fetch_article(url: str) -> str:
    return fetch_html(url)

def parse_article(html: str, url: str, sitemap_lastmod: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")

    title_tag = soup.find("meta", property="og:title") or soup.find("title")
    title = clean_text(title_tag.get("content") if title_tag else "")

    published = sitemap_lastmod

    author_tag = soup.find("meta", {"name": "author"})
    author = author_tag["content"].strip() if author_tag and author_tag.get("content") else ""

    candidates = soup.select("article, .post-content, .entry-content, .content, .single-content, .inner-content")
    paragraphs = []
    for container in candidates:
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

def crawl_cio(days_back: int = 7, limit: int = 0, sleep: float = 0) -> list[Dict[str, Any]]:
    links = get_recent_article_links(days_back=days_back, limit=limit or None)
    print(f"[cio] {len(links)} Links gefunden")

    records: List[Dict[str, Any]] = []
    for idx, (link, lastmod) in enumerate(links, 1):
        try:
            print(f"[cio] [{idx}/{len(links)}] {link}")
            html = fetch_article(link)
            records.append(parse_article(html, link, lastmod))
            time.sleep(sleep)
        except Exception as exc:
            print(f"[cio] ✖ Fehler bei {link}: {exc}", file=sys.stderr)

    return records

if __name__ == "__main__":
    articles = crawl_cio()
    path = save_bulk_json("articles", articles)
    print(f"[cio] ✔ {len(articles)} Artikel gespeichert unter: {path}")