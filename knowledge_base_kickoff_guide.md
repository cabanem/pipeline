# Knowledge Base Kickoff Guide

### Purpose

This guide will help you build the **knowledge base (KB)** component for our Retrieval-Augmented Generation (RAG) system. The KB will power embeddings, retrieval, and responses. We’ll reuse core components already built in the Gmail responder project to speed things up.

---

## Step 1. Get Your Environment Ready

1. **Clone/setup the dev environment** we’ve already standardized (check the README in our repo).

   * Make sure you can run Workato connectors locally with the SDK.
   * Confirm access to Google Cloud (GCP project + Vertex AI + GCS buckets).
2. **Auth setup**:

   * Service account JSON for GCP.
   * OAuth tokens if accessing Google Drive.
   * Verify Workato connector authentication works end-to-end.

---

## Step 2. Build the Ingestion Pipeline

1. **Source**: Start with Google Drive as the data source.
2. **Destination**: Copy files into Google Cloud Storage (GCS).
3. **Normalize**: For each file:

   * Extract metadata (doc_id, filename, owner, last_updated).
   * Convert content to plain text (strip formatting, URLs, images).
   * Chunk large docs into ~1k tokens.

➡️ Deliverable: A Workato recipe that takes “New file in Drive” → stores a normalized version in GCS.

---

## Step 3. Embed & Store

1. **Connector**: Reuse the Vertex AI connector’s `generate_embeddings` action.
2. **Chunked text**: For each text chunk, call the embedding API.
3. **Upsert into Vector Search**:

   * Collection: `knowledge_base` (or similar namespace).
   * Store embeddings + metadata (doc_id, section_id, content, updated_at).

➡️ Deliverable: Recipe that takes normalized text chunks → embeddings → inserts into Vertex AI Vector Store.

---

## Step 4. Expose KB Actions

Implement actions in Workato so recipe builders can easily use the KB:

* **Ingest documents**: Input = source (Drive/GCS), Output = confirmation + metadata.
* **Query KB**: Input = user question, Output = top-k documents/snippets + metadata.

➡️ Deliverable: Connector actions that recipe builders can drag-and-drop without touching code.

---

## Step 5. Maintain Consistency

Follow these principles (already in the Gmail responder project):

* **Schema consistency**: Always use `doc_id`, `section`, `content`, `updated_at`.
* **No mutation of inputs**: Copy before transforming.
* **Observability**: Every output includes success flag, timestamp, and trace ID.
* **UI/UX**: Group connector fields logically for recipe builders.

---

## Step 6. Document & Share

* Write down **setup instructions** (auth, env vars, sample recipes).
* Keep a **change log** of anything that diverges from the email responder pipeline.
* Schedule a **weekly sync with me** for review and alignment.

---

## Step 7. Stretch Goal (Later)

Once the basics work, you can:

* Add new sources (Confluence, PDFs in Drive, Slack exports).
* Add batch ingestion (multiple docs at once).
* Add refresh logic (detect if a doc has changed, re-ingest only that piece).

---

## Success Criteria

* End-to-end flow works: Drive doc → GCS → normalized text → embeddings → stored in Vector Search.
* You can query the KB and get relevant chunks back.
* Recipe builders can use the KB without worrying about internal mechanics.

