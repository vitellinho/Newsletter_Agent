#!/usr/bin/env python3
"""
Sammelt Artikel aus allen Quell‑Crawlern (Spiegel, ifun, …),
führt sie zu einer Liste zusammen und speichert genau eine
JSON-Datei unter data/raw/articles_raw_<timestamp>.json.
"""

import sys
from pathlib import Path

# Aktuellen Ordner (scripts/crawler) zum Python‑Pfad hinzufügen, damit lokale Module ohne Paketkontext importierbar sind
sys.path.append(str(Path(__file__).resolve().parent))

from crawler.base import save_bulk_json
from crawler.spiegel import crawl_spiegel
from crawler.ifun import crawl_ifun
from crawler.iphonetricks import crawl_iphonetricks
from crawler.bankingclub import crawl_bankingclub
from crawler.cio import crawl_cio
from crawler.derbankblog import crawl_derbankblog
from crawler.financefwd import crawl_financefwd
from crawler.itfinanzmagazin import crawl_itfinanzmagazin
from crawler.netzpolitik import crawl_netzpolitik
from crawler.paymentandbanking import crawl_paymentandbanking

def main() -> None:
    records = []
    days_back = 7

    print("\n=== CRAWLE SPIEGEL ===")
    records += crawl_spiegel(days_back=days_back)

    print("\n=== CRAWLE IFUN ===")
    records += crawl_ifun(days_back=days_back)

    print("\n=== CRAWLE IPHONETRICKS ===")
    records += crawl_iphonetricks(days_back=days_back)

    print("\n=== CRAWLE BANKINGCLUB ===")
    records += crawl_bankingclub(days_back=days_back)

    print("\n=== CRAWLE CIO ===")
    records += crawl_cio(days_back=days_back)

    print("\n=== CRAWLE DERBANKBLOG ===")
    records += crawl_derbankblog(days_back=days_back)

    print("\n=== CRAWLE FINANCEFWD ===")
    records += crawl_financefwd(days_back=days_back)

    print("\n=== CRAWLE ITFINANZMAGAZIN ===")
    records += crawl_itfinanzmagazin(days_back=days_back)

    print("\n=== CRAWLE NETZPOLITIK ===")
    records += crawl_netzpolitik(days_back=days_back)

    print("\n=== CRAWLE PAYMENTANDBANKING ===")
    records += crawl_paymentandbanking(days_back=days_back)

    outfile = save_bulk_json("articles", records)
    print(f"[INFO] Artikel in {outfile} gespeichert")

if __name__ == "__main__":
    main()