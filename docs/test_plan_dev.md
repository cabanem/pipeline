# RAG System — Test Plan

## 0) Scope & Goals

**Goal:** Prove the system can ingest corpora, generate grounded answers, and produce safe draft replies for Gmail—first in **shadow mode** (non-interactive), then in controlled **beta**.

**In-scope:** Workato custom connectors (Drive/GCS Utilities, Vertex AI Adapter, RAG Utilities), recipes (ingest → chunk → embed → upsert → answer), Gmail intake and draft emitter, GCS object storage, Vertex Vector Search / Discovery Engine.

**Out of scope (for this plan):** Org-wide change management, full production SLOs.

---

## 1) Environments & Data

* **Envs:** `DEV` (connector iteration), `STAGE` (realistic data; shadow tests), `PILOT` (limited users; guarded send).
* **Data:** Curated **golden set** (50–200 real emails + ground-truth answers + source URIs). Sanitized PII where possible; keep original for small, access-controlled subset.
* **Secrets/SA:** Service account JSON keys scoped least-privilege; separate per env; audit “x-goog-user-project” billing headers.

---

## 2) Roles

* **Test Lead:** Owns schedule, entry/exit gates, defect triage.
* **Connector Owner(s):** Workato actions/objects, auth, retries, sample_output.
* **Data Owner:** Golden set & evaluation rubric.
* **Observer(s):** Monitor telemetry & cost.
* **Security Reviewer:** SA scopes, email data handling.

---

## 3) Test Phases (with purpose, how, entry/exit)

### Phase A — Static & Unit (DEV)

**Purpose:** Catch dumb mistakes early; keep connectors DRY and predictable.
**How:**

* Lint & static checks on connector Ruby DSL (your internal rules).
* **Unit** recipes for helpers (e.g., `sanitize_feature_vector`, JSON shapes).
* Sample outputs for every action for reliable data pills.
  **Entry:** MVP compiles; actions wired.
  **Exit:** 0 critical lint errors; unit checks green; sample_output present for all actions.

### Phase B — Contract/API Tests (DEV → STAGE)

**Purpose:** Ensure every connector method matches upstream Google specs and Workato conventions.
**How:**

* For Vertex/Discovery Engine & GCS/Drive: request/response **schema assertions** vs discovery/OpenAPI (you’re already converting Discovery → OpenAPI—good).
* Negative tests (4xx/5xx), idempotency (no duplicate upserts), retry/backoff.
  **Entry:** Phase A passed.
  **Exit:** 100% methods tested for happy+sad paths; idempotent actions verified (no duplicate jobs).

### Phase C — Pipeline & Data Quality (STAGE)

**Purpose:** Validate end-to-end ingestion → chunk → embed → upsert.
**How:**

* Ingest a representative corpus from Drive/GCS.
* Verify **chunk counts**, average tokens, and **embedding vector dims** match model.
* Verify **upsert**: point-in-time consistency (read-after-write queries).
* **Data QA:** dedupe, language detection, MIME exclusion, OCR quality thresholds.
  **Entry:** Phase B passed.
  **Exit:**
* 99%+ files ingested without error; failures triaged.
* 100% vectors correct dimensionality; ≤1% malformed chunks.
* Index contains expected doc counts ±2%.

### Phase D — **Alpha (Shadow Mode)** (STAGE) — *your current ask*

**Purpose:** Run alongside real mail, **no user interaction**; capture answers + signals.
**How:**

* Gmail intake pulls messages (labels/filters to limit scope).
* RAG answers generated; **drafts saved to a staging store** (e.g., GCS bucket `drafts/` or Workato Data Table) **not sent**.
* Log **context citations** (source URIs, chunk IDs), latency (p50/p90/p99), cost per email.
  **Entry:** Phase C passed; guardrails/rate limits configured.
  **Exit (shadow gates):**
* **Grounding rate** (answer cites at least 1 source): ≥95%.
* **Citation coverage** (top-1 source relevant): ≥85% on golden set.
* **Hallucination rate** (fact errors per answer by rubric): ≤5%.
* **Latency** end-to-end: p90 ≤ 6s; p99 ≤ 10s.
* **Cost** ≤ target budget per 100 emails (set a dollar cap).

### Phase E — Offline Eval on Golden Set (parallel with D)

**Purpose:** Quantify retrieval & answer quality independent of live variance.
**How:**

* Metrics: **Recall@k**, **nDCG@k**, **MRR**, **Answer quality** (rubric 1–5), **Deflection rate** (auto-answer vs escalate), **Toxicity/PII flags**.
* A/B model configs (embedder variants, chunk sizes, k).
  **Entry:** Golden set ready.
  **Exit:**
* Retrieval: Recall@5 ≥ 0.9, MRR ≥ 0.6.
* Answer rubric average ≥ 4.0; stdev noted.
* Zero toxic content; PII only when present in sources.

### Phase F — Beta (Limited Send) (PILOT)

**Purpose:** Carefully send drafts to a small group, with human-in-the-loop.
**How:**

* Workato recipe writes **Gmail drafts** under label “RAG-Draft”; humans edit/send.
* Collect **edit distance** between draft and final; **time-to-send**; **user satisfaction** (1–5).
  **Entry:** Phase D/E gates passed; sign-off from Security.
  **Exit:**
* Edit distance median ≤ 20%;
* Human correction time median ≤ 60s;
* Satisfaction ≥ 4/5;
* No P0 security/privacy incidents.

### Phase G — Performance/Resilience (STAGE)

**Purpose:** Don’t fall over at scale; degrade gracefully.
**How:**

* Load test bursts (e.g., 500 emails/10 min) with staged GCS payloads.
* Chaos: transient 429/5xx from Vertex/GCS; verify retries/backoff & dead-letter queues.
  **Entry:** D/E passed.
  **Exit:**
* No message loss (exactly-once or at-least-once + replay).
* p95 latency within 25% of baseline under 3× load.
* Backoff respected; queue drains post-incident.

### Phase H — Security/Compliance (STAGE/PILOT)

**Purpose:** Prove least-privilege and safe handling of email content.
**How:**

* SA scopes audit; token lifetime; rotation; secrets storage.
* PII logging review; redaction where not needed.
* Access controls on GCS buckets and Data Tables; Gmail label scoping.
  **Entry:** Any time after B.
  **Exit:** Documented SA matrix; PII handling approved; audit logs verified.

### Phase I — Go/No-Go & Rollback

**Purpose:** Formal decision with safety net.
**How:**

* Checklist (below), dry-run rollback (disable recipes, archive labels, revert routing).
  **Exit:** All green or waived with owner sign-off.

---

## 4) Metrics (authoritative)

* **Retrieval:** Recall@k, nDCG@k, MRR.
* **Answering:** Grounding rate, hallucination rate, citation coverage, edit distance, deflection rate.
* **Ops:** p50/p90/p99 latency; error budget (429/5xx); retry success rate; cost/email.
* **Human factors (beta):** correction time, satisfaction score.

---

## 5) Artifacts (per phase)

* Test cases & checklists in repo/wiki.
* Golden set (CSV/JSONL): `{email_id, question, expected_sources[], expected_facts[], notes}`.
* Telemetry dashboards: latency, cost, error rates, top failure causes.
* Security matrix: SA → resources → permissions.
* Phase reports with gate results and decisions.

---

## 6) Core Test Cases (condensed)

1. **Ingestion:** Drive file with mixed MIME (PDF, DOCX, EML) → filtered, OCR’d where needed → chunk counts sane.
2. **Embedding:** Empty/short docs; oversized docs; non-English; vector dim mismatch rejection.
3. **Upsert:** Duplicate datapoint IDs; idempotent re-runs; partial failures with retry.
4. **Query/Answer:**

   * With/without relevant sources; multi-source answers; time-bounded queries (dates).
   * Citation correctness (URIs resolve; snippet matches).
5. **Gmail Intake:** Label scoping; threading; attachments ignored/handled as configured.
6. **Shadow Output:** Draft JSON schema stable; all fields present (answer, sources[], confidence, latency_ms).
7. **Rate/Quota:** Respect Gmail & Vertex quotas; backoff on 429.
8. **Security:** SA can read only required buckets/labels; redact logs; rotate keys.

---

## 7) Shadow-Mode Runbook (1 week)

* **Day 0:** Freeze configs; seed golden set; enable recipes for target label.
* **Days 1–5:** Collect drafts; daily review: grounding, hallucinations, top failure classes, cost.
* **Day 6:** A/B tweak (k, chunk size) on a subset; re-score golden set.
* **Day 7:** Gate review vs targets; decide beta scope.

---

## 8) Entry/Exit Gate Checklist (quick)

* [ ] Connectors: sample_output present; retries configured; idempotency proven.
* [ ] Index health: expected vector count ±2%; read-after-write OK.
* [ ] Telemetry: correlation IDs; request/response truncation rules; PII safe.
* [ ] Security: SA scopes reviewed; bucket IAM least-privilege; audit logs on.
* [ ] Metrics: meet targets listed in Phases D/E/F.
* [ ] Rollback steps documented and tested.

---

## 9) Risks & Mitigations

* **Hallucinations on weak context:** tighten retrieval (smaller chunks, overlap, better k); enforce “answer only from sources” prompt; abstain when low confidence.
* **Quota/Rate spikes:** queueing + exponential backoff; cap concurrency.
* **Cost creep:** alerting based on spend/email; periodic model/embedding cost review.
* **Data leakage:** strict label scoping; redact logs; encrypt buckets; least-privilege SAs.

---

## 10) What to Communicate to the Team (one-liner per phase)

* **A:** We’re eliminating obvious connector failures.
* **B:** Every API call matches Google/Workato contracts.
* **C:** The pipeline turns raw docs into searchable vectors correctly.
* **D:** In **shadow mode**, we generate answers but **don’t send**—we measure truthfulness and speed.
* **E:** On the golden set, we score retrieval and answers quantitatively.
* **F:** In **beta**, drafts land in Gmail; humans edit/send; we measure edits and time saved.
* **G/H:** It scales safely and stays compliant.
* **I:** We have a clean rollback if anything smells smoky.

---

### Immediate next steps

1. Finalize golden set and rubric
2. Turn on Phase D in **STAGE** with telemetry dashboards;
3. After 5 business days, hold a gate review to decide beta size and guardrails.

