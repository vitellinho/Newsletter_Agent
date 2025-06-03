#!/usr/bin/env python3
"""
Streamlit-Frontend für den SPIEGEL-Newsletter-Agenten.
Zeigt die besten thematischen Artikel der letzten 7 Tage.

> cd /Users/S0097439/my_projects/GitHub/Spiegel_Agent/app
> streamlit run streamlit_app.py
"""

import sys
from pathlib import Path

# Basisverzeichnis zur Python-Path hinzufügen (um preprocess_rag importieren zu können)
sys.path.append(str(Path(__file__).resolve().parent.parent))

import streamlit as st

# Imports Skripte
from scripts.preprocess_rag import run_preprocess, ask_rag, _latest_raw_file, load_records
import re

# Import crawl_all
from scripts.crawl_all import main as run_all_crawlers

if "pipeline_done" not in st.session_state:
    st.session_state.pipeline_done = False

st.set_page_config(page_title="Newsletter-Agent Demo", layout="centered")

st.title("📰 NEWSLETTER-AGENT \n – Top-Artikel der Woche -")

def run_full_pipeline():

    # Crawl alle Quellen und speichere in einer JSON
    st.info("Crawle Artikel aus allen Quellen …")
    run_all_crawlers()
    latest = _latest_raw_file()
    st.success(f"Artikel gespeichert unter: {latest.name}")

    # Preprocessing starten
    st.info("Starte Pre-Processing & Indexaufbau …")
    run_preprocess(latest)
    st.success("FAISS-Index erfolgreich aktualisiert.")

if not st.session_state.pipeline_done:
    if st.button("🔄 Kompletten Prozess starten"):
        run_full_pipeline()
        st.session_state.pipeline_done = True
        st.rerun()
else:
    # Eingabefeld anzeigen, wenn Prozess abgeschlossen ist
    n_articles = st.number_input(
        "Wie viele Artikel sollen ausgewählt werden?",
        min_value=1, max_value=20, value=7, step=1
    )

    if st.button("Artikel generieren"):
        with st.spinner("Suche beste Artikel …"):
            combined_query = (
                "bank finanzen zinsen börse aktien künstliche intelligenz "
                "digitalisierung blockchain bitcoin ethereum"
            )
            hits = ask_rag(combined_query, n=n_articles)

        if not hits:
            st.warning("Keine Artikel gefunden – hast du schon den Index gebaut?")
        else:
            from datetime import date, timedelta
            today = date.today()
            last_week = today - timedelta(days=7)
            kw = last_week.isocalendar().week

            header = (
                f"Top News (KW {kw:02d}): Digitaler Wandel und Innovationen im Finanzmarkt\n\n"
                "In diesem Newsletter teile ich meine persönliche Auswahl der spannendsten "
                "Artikel, die ich letzte Woche gelesen habe.\n\n"
            )

            md_lines = []
            for i, art in enumerate(hits, 1):
                summary = art.get("summary", "").strip()
                summary_line = f"{summary}\n" if summary else "(keine Zusammenfassung)\n"

                # Lade den Volltext aus der Roh-JSON, um die ersten drei Sätze zu zeigen
                raw_records = load_records(_latest_raw_file())
                url_to_text = {r["url"]: r.get("text", "") for r in raw_records}
                full_text = url_to_text.get(art["url"], "")
                # Splitte in Sätze und nimm die ersten drei
                sentences = re.split(r'(?<=[.!?])\s+', full_text.strip())
                preview = " ".join(sentences[:3]) if sentences else "" # Erste 3 Sätze
                preview_line = f"{preview}\n\n" if preview else ""

                line = (
                    f"**{i}. [{art['title']}]({art['url']})**  \n"
                    f"KI-generiert: {summary_line}\n\n"
                    f"Preview: {preview_line}"
                    f"veröffentlicht bei {art['url']}\n\n"
                )
                md_lines.append(line)

            result_md = header + "\n".join(md_lines)

            st.subheader("Kopierfertiger Newsletter‑Text")
            st.markdown(result_md)