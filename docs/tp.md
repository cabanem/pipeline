# Alpha / Shadow-Testing Plan — RAG Email Responder (Vertex AI + Workato)

## 1) Goals, scope, and guardrails

**Primary goals**

* Validate end-to-end correctness on *real* incoming emails in shadow mode.
* Prove baseline **quality**, **latency**, **cost**, and **operability** with strong telemetry.
* Shake out auth/permissions, object shape mismatches, retry/idempotency, and logging.

**In scope**

* Actions: `rag_retrieve_contexts`, `rag_answer`, `gen_generate`, `rank_texts_with_ranking_api`, `gen_categorize_email`, `email_extract_salient_span`.
* Ingestion path (Drive → GCS → chunk/embeds → index upsert) and serving path (query → retrieve → (optional re-rank) → generate).
* Observability via **Cloud Logging** tail calls (one log per outcome), correlation IDs, sampling.

**Out of scope (alpha)**

* Managed rollout to production inboxes.
* Cost optimization beyond obvious quick wins.
* Corpus-wide taxonomy/ontology work.

**Guardrails**

* Never auto-send. Shadow only: log proposed reply + confidence + citations.
* Strict PII handling: redact before logs; no payloads with secrets in Cloud Logging.

---

## 2) Entry & exit criteria

**Entry**

* Service account scopes validated; Vertex & Logging APIs enabled.
* Connectors load cleanly; required `object_definitions` present; sample outputs render pills.
* Tail logger wired (`tail_log_begin!` / `tail_log_end!` or equivalent) and **does not raise** on failure.

**Exit (ship to limited beta if all true)**

* ≥ **85%** of golden emails scored **Good or better** by rubric below.
* **P50 latency ≤ 3.0s**, **P95 ≤ 7.0s** end-to-end per email (receive → candidate reply).
* **Hallucination rate ≤ 5%** (any non-supported factual claim).
* **No data-loss bugs**, **no auth regressions**, **no schema breakages** for one week.
* **Cost per shadowed email ≤ $0.015** (rough ceiling; adjust once measured).

---

## 3) Test data & evaluation artifacts

**Golden set (truth data)**

* 150–250 real historical emails (or synthetic mirrors) across top 8–10 categories you care about.
* For each: a **golden reply** + **allowed sources** (doc IDs) + **must-cite items** (facts/links).
* Negative controls: 15–25 emails with **no answerable content** (expect refusal/clarifying question).

**Shadow stream**

* Live emails from selected inbox(es) copied to a “shadow” label or forwarding rule.
* System produces candidate reply + confidence + citations, but **does not send**.

**Annotation rubric (per email)**

* **Factuality** (Attributable to sources) 0–2
* **Coverage** (addresses user intent) 0–2
* **Style fit** (concise, actionable) 0–1
* **Safety/Policy** (no leaks/toxicity) pass/fail
  Overall: **Excellent (5), Good (4), Borderline (3), Poor (≤2)**

---

## 4) Metrics to track (daily roll-ups)

**Quality**

* Golden accuracy: % Excellent/Good.
* **Faithfulness**: % claims verifiably supported by cited contexts.
* **Hallucination**: % with unsupported claims.
* **Refusal correctness** on negatives.

**Retrieval**

* **Recall@k** vs golden sources (did required doc IDs appear in retrieved set?).
* **MRR / nDCG** on doc ranking when a ranker is used.

**Latency (ms)**

* Breakdown: retrieve → rank → generate → total. Track **P50/P90/P95/MAX**.

**Cost**

* Per email: embedding + retrieval + ranking + generation token costs (approximate from model usage).
* **Cost per correct** reply.

**Ops**

* Error rate (by category), retry rate, timeout rate.
* Tail-logger success rate (should be ≥99.9% and never block).

---

## 5) Test matrix (what to actually run)

**A. Functional**

* Each action executes with valid inputs; schema emits expected shapes (including `emit_parts`).
* Retry/idempotency doesn’t duplicate logs or GCS transfers.
* Permissions: shared drives, nested folders, MIME types (PDF, DOCX, email threads), large files.

**B. Retrieval quality**

* Vary `top_k`, threshold (`vector_distance_threshold` or `vector_similarity_threshold`) independently.
* Constrain by `restrict_to_file_ids` and confirm limits respected.

**C. Ranking**

* Compare **no ranker** vs **ranking API** vs **LLM ranker** on golden recall and nDCG.
* Measure ranker overhead (ms) and net quality delta.

**D. Generation**

* System prompt variants, citation styles (inline vs footnote), max tokens.
* **Confidence pill** emitted and sensible (e.g., calibrated to retrieval scores + heuristics).

**E. Categorization & extraction**

* `gen_categorize_email` accuracy on policy labels.
* `email_extract_salient_span` correctness for fields you’ll later automate.

**F. Robustness**

* Timeouts from Vertex; upstream 429/5xx; Workato transient faults.
* Oversized inputs; malformed HTML; empty/ambiguous emails; multi-language.
* Backpressure: queue 50–100 emails rapidly → no meltdown, graceful tail latency.

**G. Safety**

* Toxic content, prompt injection attempts, “answer beyond corpus” tests → safe refusals.

---

## 6) Shadow-mode flow (wire-level)

1. **Ingestion sanity** (daily): Drive→GCS→chunk→embed→upsert jobs complete; log counts match.
2. **Shadow intake**: Gmail label/forward → Workato recipe triggers.
3. **Pipeline**: categorize → retrieve → (optional rank) → generate → produce **candidate** reply.
4. **Emit**: log envelope (one per outcome), store candidate in Data Table + Sheet row.
5. **Human review**: annotators rate and, if needed, hand-edit reply (logged).
6. **Feedback**: collect scores, reasons, doc gaps → weekly patch to corpus or prompts.

---

## 7) Telemetry & logging (Cloud Logging)

**Envelope (minimum)**

* `correlation_id`, `action_id`, `recipe_id`, `email_msg_id`, `severity`
* `begun_at`, `ended_at`, `latency_ms`
* `component` (ingest/retrieve/rank/generate/respond)
* `model`, `index_endpoint`, `top_k`, `threshold`, `ranker`
* `result_meta` (token counts, retrieved_doc_ids[], citation_doc_ids[])
* `outcome` (success|failure|refusal), `error_class`, `error_message_shrunk`

**Explorer quick filters (paste-ready)**

* `resource.type="global" AND jsonPayload.component="generate" | stats quantile(latency_ms,50), quantile(latency_ms,95)`
* `jsonPayload.outcome="failure" | count by error_class`
* `jsonPayload.component="retrieve" | top 20 jsonPayload.index_endpoint`
* `jsonPayload.component="retrieve" | histogram jsonPayload.latency_ms`
* `jsonPayload.outcome="success" AND jsonPayload.component="generate" | avg jsonPayload.result_meta.token_output`

**Rules**

* **One log per outcome**. Logging must **never raise** or block.
* Non-canonical severities prefixed `NONSTANDARD/…`, but normalize to `DEBUG|INFO|WARNING|ERROR`.

---

## 8) Evaluation harness (fast and lightweight)

**Google Sheet (Shadow Eval) — columns**

* `timestamp`, `email_msg_id`, `from`, `subject`, `category`, `candidate_reply`, `citations`, `confidence`, `golden_reply? (Y/N)`, `annotator_rating (0–5)`, `hallucination? (Y/N)`, `notes`, `correlation_id`, `latency_ms`, `cost_estimate_usd`

**Apps Script**

* Import daily Cloud Logging export (or webhook) → append rows.
* Simple dashboard: counts by rating, latency percentiles, error classes.
* Weekly snapshot tab for trending.

---

## 9) Tuning plan (tight loop)

1. **Week 1 (stabilize)**

   * Focus: schema breaks, permissions, tail logging, timeouts.
   * Tune: `top_k` to 12, add similarity threshold (start 0.75 for cosine-like), no ranker.
   * Target: minimize failures, get P95 < 8s.

2. **Week 2 (quality push)**

   * A/B: Add ranking API vs no ranker; test one LLM ranker pass.
   * Prompt: tighten system preamble, enforce “answer only from citations”.
   * Target: ≥85% Good+, hallucination ≤5%, cost ≤$0.015/email.

(If you need tighter timing, compress to 7–10 days; same sequence.)

---

## 10) Risk register (alpha-relevant)

* **Auth drift** (projects/regions/env) → enforce `ensure_project_id!`, `ensure_regional_location!`.
* **Schema mismatches** (Workato pills) → freeze `object_definitions` and version increment on change.
* **Long-tail formats** (scanned PDFs) → OCR fallback or mark unanswerable.
* **Ranker overhead** inflates latency → keep switchable and sample only 20–30% initially.
* **Prompt injection** → strict refusal if no supporting context; never browse external links at gen time.

---

## 11) Triage, bug bar, and ownership

**Priority**

* P0: data loss, auth failure, sending email by mistake, P95 > 15s sustained.
* P1: hallucination >10%, schema/pill breakage in any action.
* P2: cosmetic, rare edge formats.

**Owners (suggested)**

* Ingestion & indexing: You (+ backup)
* Serving path & ranking: You
* Observability (logs/Sheets): You
* Annotation & rubric: trusted reviewer(s)

---

## 12) What to decide now (so we can execute)

* **Confidence pill** formula (for display): combine normalized retrieval score + ranker agreement + citation density. Start with simple min-max of top-3 similarity and clamp to [0.0–1.0]; label **Low <0.45 / Med 0.45–0.7 / High >0.7**.
* **Ranker sampling**: 30% of emails in Week 2.
* **Refusal policy**: If **no retrieved contexts pass threshold**, output friendly clarification with 0.00 confidence.

---

## 13) Minimal task list to kick off (today)

* Wire tail logs across all listed actions (verify one-per-outcome).
* Export Logging sinks to BigQuery **or** pull to Sheets nightly.
* Finalize the 200-email golden set + rubric.
* Add ranker toggle + threshold fields in actions (with hints that enforce ONE threshold at a time).
* Create the Shadow Eval Sheet + Apps Script stub.
* Start Week-1 run; review daily at fixed time with metrics snapshot.
