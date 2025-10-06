# Knowledge Base (KB) Kickoff Guide

### Purpose

We’re building the **Knowledge Base (KB)** that powers our Retrieval-Augmented Generation (RAG) systems — the same architecture used in the Gmail responder project. The KB will handle ingestion, embedding, and retrieval of source materials, forming the foundation for intelligent responses.
This project reuses existing components to accelerate delivery while maintaining architectural consistency.

---

## 1. Setup & Environment

* Use the shared **development environment** and README from the Gmail responder project.
* Confirm access to:

  * **Google Cloud** (Vertex AI + GCS buckets)
  * **Workato connectors** (Vertex AI connector + Drive utilities)
  * **Authentication**: GCP service account and OAuth for Drive

**Goal:** Be able to run connectors locally and execute test actions successfully.

---

## 2. Ingestion Pipeline

Start with **Google Drive → GCS**.

Each file should be:

* Normalized to plain text (strip formatting, images, links)
* Chunked (~1K tokens per section)
* Enriched with metadata: `doc_id`, `filename`, `updated_at`

**Deliverable:** Recipe that detects new/updated Drive files and stores normalized versions in GCS.

---

## 3. Embedding & Storage

* Use the **Vertex AI connector** (`generate_embeddings`) for vectorization.
* Upsert embeddings into **Vertex Vector Search** under a shared collection (`knowledge_base`).
* Include metadata for traceability.

**Deliverable:** Recipe that turns normalized text → embeddings → stored vectors.

---

## 4. Workato KB Actions

Expose simple, reusable actions:

* **Ingest documents** → Drive/GCS → embeddings → KB
* **Query KB** → question → top-K relevant results

**Goal:** Recipe builders can access KB functions without custom code.

---

## 5. Standards to Follow

* **Schema consistency:** use the shared field structure (`doc_id`, `section`, `content`, `updated_at`)
* **Immutable inputs:** never mutate caller data
* **Observability:** include success flags, timestamps, and trace IDs in outputs
* **UI design:** maintain logical grouping of connector fields for clarity

---

## 6. Documentation & Handoff

* Document setup, authentication, and usage in plain English.
* Keep a small **change log** for any deviations from the Gmail responder design.
* Weekly sync with Emily for review and technical alignment.

---

## 7. Stretch Goals (Post-MVP)

Once core ingestion and retrieval work:

* Add new data sources (Confluence, Slack, PDFs, etc.)
* Implement incremental refresh for updated docs
* Optimize retrieval quality and latency

---

### Success Criteria

* End-to-end pipeline: Drive file → GCS → normalized text → embeddings → stored vectors → retrievable chunks
* Querying returns relevant content with traceable metadata
* Reusable across multiple RAG-based projects

---
