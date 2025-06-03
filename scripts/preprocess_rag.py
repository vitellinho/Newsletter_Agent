#!/usr/bin/env python3
"""
Pre-Processing & Retrieval-Augmented Generation (RAG) für Newsletter

Dieses Skript verarbeitet die von 'crawl_all.py' erzeugte JSON-Datei mit Artikeln:
Es indexiert alle Artikel, reinigt und chunked die Texte, erzeugt Embeddings und speichert diese
in einem lokalen FAISS-Vektorindex zur schnellen Ähnlichkeitssuche.

Zusätzlich generiert es über OpenAI (Modell: o4-mini) kurze Zusammenfassungen
für die jeweils relevantesten Artikel-Chunks.

Funktionen im Überblick:
1. `load_records(path)` – lädt die Rohdaten (Liste von Dicts)
2. `clean_and_chunk(recs)` – normalisiert Texte & erzeugt Chunks (~200 Wörter)
3. `embed_chunks(chunks)` – erzeugt Vektoren mit Sentence-Transformers
4. `build_faiss(emb, meta)` – speichert Vektoren + Metadaten als FAISS-Index
5. `_summarize(text)` – ruft das OpenAI-Modell o4-mini auf, um Chunks zu verdichten
6. `ask_rag(query, n)` – sucht n relevante Chunks zu einer Query, liefert Titel/URL/Summary

Speicherorte:
- Rohdaten:       data/raw/
- FAISS-Index:    data/vectorstore/articles.index
- Metadaten:      data/vectorstore/articles.meta.pkl
"""

from __future__ import annotations
import os as _os_
_os_.environ.setdefault("OMP_NUM_THREADS", "1")
_os_.environ.setdefault("MKL_NUM_THREADS", "1")

import argparse
import json
import os
import pickle
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
import faiss  # type: ignore
import numpy as np
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv
import math

import openai
from openai import OpenAI

# ---------------------------------------------------------------------------
# Globale Einstellungen
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent.parent
RAW_DIR = BASE_DIR / "data" / "raw"
PROC_DIR = BASE_DIR / "data" / "processed"
VEC_DIR = BASE_DIR / "data" / "vectorstore"

CHUNK_SIZE = 200        # ~Wörter pro Chunk
EMB_MODEL = "sentence-transformers/all-mpnet-base-v2"

# ---------------------------------------------------------------------------
# System‑Prompt (semantische Relevanzbeschreibung)
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "Finde Artikel, die für eine regionale Sparkasse relevant sind – "
    "insbesondere Themen rund um Banken, Digitalisierung, Cloud‑Computing, "
    "Künstliche Intelligenz und Kryptowährungen."
)

# ---------------------------------------------------------------------------
# LLM
# ---------------------------------------------------------------------------

dotenv_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
load_dotenv(dotenv_path)

api_key = os.getenv("OPENAI_API_KEY")

if not api_key:
    raise ValueError("OPENAI_API_KEY ist nicht gesetzt")

client = OpenAI(api_key=api_key)
OPENAI_MODEL = "o4-mini"

def _summarize(text: str, max_sentences: int = 1) -> str:
    """Kurzzusammenfassung via OpenAI (o4-mini) – ohne Zusatzparameter."""
    if not text:
        return ""

    prompt = (
        f"Fasse den folgenden Text prägnant in höchstens {max_sentences} Sätzen "
        f"zusammen:\n\n{text[:2000]}"
    )
    try:
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[{"role": "user", "content": prompt}],
        )
        return resp.choices[0].message.content.strip()
    except Exception as exc:
        print("[ERR] OpenAI-Call fehlgeschlagen:", exc)
        return ""

# ---------------------------------------------------------------------------
# Hilfsfunktionen
# ---------------------------------------------------------------------------


def load_records(path: Path) -> List[Dict[str, Any]]:
    """Lädt JSON‐Datei voller Artikel."""
    return json.loads(path.read_text(encoding="utf-8"))


def _clean(text: str) -> str:
    """Whitespace & Unicode normalisieren."""
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _chunk(text: str, size: int = CHUNK_SIZE) -> List[str]:
    """Naiver Wort-Chunker."""
    words = text.split()
    return [" ".join(words[i : i + size]) for i in range(0, len(words), size)]


def clean_and_chunk(recs: List[Dict[str, Any]]) -> Tuple[List[str], List[Dict[str, Any]]]:
    """Erzeugt Text-Chunks & parallele Metadaten‐Liste."""
    chunks, meta = [], []
    for r in recs:
        for chunk in _chunk(_clean(r["text"])):
            chunks.append(chunk)
            meta.append(
                {
                    "url": r["url"],
                    "title": r["title"],
                    "published": r["published"],
                    "source": r.get("source", ""),  # ← neu
                    "chunk": chunk,
                }
            )
    return chunks, meta


def embed_chunks(chunks: List[str], batch_size: int = 16) -> np.ndarray:
    """Embeddings batchweise erzeugen, um Speicherprobleme zu vermeiden."""
    model = SentenceTransformer(EMB_MODEL, device="cpu")
    emb = model.encode(
        chunks,
        batch_size=min(batch_size, len(chunks)),
        convert_to_numpy=True,
        show_progress_bar=True,
        normalize_embeddings=True,
    )
    return emb.astype("float32")


def build_faiss(emb: np.ndarray, meta: List[Dict[str, Any]]) -> None:
    """Schreibt Vektorindex + Metadaten mit FAISS."""
    VEC_DIR.mkdir(parents=True, exist_ok=True)

    faiss.normalize_L2(emb)
    index = faiss.IndexFlatIP(emb.shape[1])
    index.add(emb)
    faiss.write_index(index, str(VEC_DIR / "articles.index"))
    print(f"[INFO] FAISS-Index geschrieben ({index.ntotal} Vektoren).")

    with open(VEC_DIR / "articles.meta.pkl", "wb") as fh:
        pickle.dump(meta, fh)


def _load_vectors() -> Tuple[faiss.IndexFlatIP, List[Dict[str, Any]]]:
    """Lädt FAISS-Index + Metadaten."""
    with open(VEC_DIR / "articles.meta.pkl", "rb") as fh:
        meta = pickle.load(fh)
    index = faiss.read_index(str(VEC_DIR / "articles.index"))
    return index, meta


# ---------------------------------------------------------------------------
# RAG-Query
# ---------------------------------------------------------------------------


def ask_rag(query: str, n: int = 7, ratio: float = 0.33) -> List[Dict[str, Any]]:
    """
    Liefert genau n eindeutige Artikel-Treffer (Titel, URL, published, summary, snippet).
    - ratio: Maximaler Anteil einer einzelnen Quelle (z. B. 0.33 ⇒ höchstens ein Drittel).
    """
    if not query:
        query = SYSTEM_PROMPT

    vectors, meta = _load_vectors()
    model = SentenceTransformer(EMB_MODEL, device="cpu")

    # Query-Embedding
    q_vec = model.encode([query], convert_to_numpy=True).astype("float32")
    faiss.normalize_L2(q_vec)

    # Großzügig viele Chunks abrufen, um trotz Filter genügend Artikel zu sammeln
    k = min(n * 30, vectors.ntotal)          # z. B. n=7 → 210
    sims, idxs = vectors.search(q_vec, k)
    sims, idxs = sims[0], idxs[0]

    allowed_per_source = max(1, math.ceil(n * ratio))
    per_source_count: dict[str, int] = {}
    seen_urls: set[str] = set()
    results: List[Dict[str, Any]] = []

    for score, idx in zip(sims, idxs):
        m         = meta[idx]
        url       = m["url"]
        src       = m.get("source", "unknown")

        # Quellen-Obergrenze & Duplikate
        if per_source_count.get(src, 0) >= allowed_per_source:
            continue
        if url in seen_urls:
            continue

        per_source_count[src] = per_source_count.get(src, 0) + 1
        seen_urls.add(url)

        chunk_text = m.get("chunk", "")
        summary    = _summarize(chunk_text) if chunk_text else ""

        # Die ersten drei Sätze als Snippet
        sentences  = re.split(r"(?<=[.!?])\s+", chunk_text)
        snippet    = " ".join(sentences[:3]).strip()

        results.append(
            {
                "title":     m["title"],
                "url":       url,
                "published": m["published"],
                "source":    src,
                "summary":   summary,
                "snippet":   snippet,
            }
        )
        if len(results) >= n:
            break

    return results


# ---------------------------------------------------------------------------
# CLI & Main-Workflow
# ---------------------------------------------------------------------------


def _latest_raw_file() -> Path:
    '''Gibt die neueste JSON-Datei aus dem Verzeichnis data/raw zurück,
    die mit articles_raw_ beginnt, und bricht ab, wenn keine vorhanden ist.'''
    files = sorted(RAW_DIR.glob("articles_raw_*.json"))
    if not files:
        sys.exit("Keine Roh-JSON gefunden – bitte zuerst crawl_all.py ausführen.")
    return files[-1]  # neueste


def run_preprocess(raw_path: Path) -> None:
    """Kompletter Pre-Processing-Flow."""
    dest_index = VEC_DIR / "articles.index"
    if dest_index.exists():
        print("[INFO] Überschreibe bestehenden Index.")

    print(f"[INFO] Lade Rohdaten aus {raw_path.name}")
    records = load_records(raw_path)
    filtered = records  # keine Keyword‑Filterung mehr
    print(f"[INFO] Verarbeite {len(filtered)} Artikel (ohne Themenfilter).")

    chunks, meta = clean_and_chunk(filtered)
    print(f"[INFO] {len(chunks)} Text-Chunks erzeugt – starte Embedding…")
    emb = embed_chunks(chunks)
    build_faiss(emb, meta)


def build_argparser() -> argparse.ArgumentParser:
    '''Definiert einen Argumentparser für die Kommandozeile,
    mit dem optional ein Pfad zur Rohdaten-JSON (--raw) und eine
    Testabfrage für die RAG-Funktion (--query) übergeben werden können.'''
    p = argparse.ArgumentParser(description="Pre-Processing & FAISS-Build")
    p.add_argument("--raw", type=Path, help="Pfad zur Roh-JSON")
    p.add_argument("--query", type=str, help="Testabfrage für ask_rag()")
    return p


def main(argv: List[str] | None = None) -> None:
    args = build_argparser().parse_args(argv)

    raw_file = args.raw or _latest_raw_file()
    run_preprocess(raw_file)

    if args.query:
        print("\n>>> ask_rag:", args.query)
        for r in ask_rag(args.query):
            print(f"- {r['title']}  ({r['score']:.2f})\n  {r['url']}\n")

if __name__ == "__main__":
    main()