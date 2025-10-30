Short version: you can turn your Cloud Logging stream into a live x-ray of retrieval, ranking, and generation quality, plus reliability, cost, and data hygiene. Here’s what you can *actually* measure with the logs you’re already emitting—and the few small fields I’d add to make it surgical.

# Current

1. Reliability & performance

* P50/P95/P99 latency per action (retrieve, rerank, generate).
* Error rate by HTTP code/class (from `_tl_norm_error`).
* Hot paths: which actions dominate wall time within recipes (correlate by `correlation_id`).
* Regional anomalies (from `request_meta.region`).

2. Traffic & usage patterns

* Volume by action/env/recipe/job.
* Spike detection (simple EWMA over count per minute).

3. Operational hygiene

* “NONSTANDARD/*” severity rate → quickly surfaces weird custom severities builders typed.
* Silent failure inventory: `status="error"` with empty/unknown HTTP code.

# Two tiny additions that unlock RAG-specific insights

Right now `_tl_shrink_meta` drops most of the juicy RAG fields. Add a **compact “facets/metrics” block** to the log payload—sourced from each action’s `out['telemetry']` and known fields—so you can query them directly without bloating logs.

In each action *before* `tail_log_emit!`, compute something like:

```ruby
facets = {
  retrieval_top_k:      out.dig('telemetry','retrieval','top_k'),
  retrieval_filter:     out.dig('telemetry','retrieval','filter','type'),
  retrieval_filter_val: out.dig('telemetry','retrieval','filter','value'),
  rank_mode:            out.dig('telemetry','rank','mode'),
  rank_model:           out.dig('telemetry','rank','model'),
  contexts_returned:    Array(out['contexts'] || out['context_chunks']).length,
  tokens_prompt:        out.dig('usage','promptTokenCount'),
  tokens_candidates:    out.dig('usage','candidatesTokenCount'),
  tokens_total:         out.dig('usage','totalTokenCount'),
  gen_finish_reason:    out.dig('candidates',0,'finishReason'),
  confidence:           out.dig('confidence') # present for rag_with_context
}.compact
```

Then extend `tail_log_emit!`’s payload:

```ruby
(jsonPayload[:facets] = facets) if defined?(facets) && facets.any?
```

That single block lets you answer the important questions below with one query each.

# The RAG questions you’ll want to answer (and how)

**Retrieval quality**

* Zero-hit rate: fraction of calls with `contexts_returned = 0`.
  Why it matters: permissions gaps, bad corpus linkages, or wrong region.
* Context richness distribution: histogram of `contexts_returned` vs `retrieval_top_k`.
  Why it matters: if you always return `≤3` on `top_k=20`, your threshold is too tight.
* Threshold effectiveness: success/latency as a function of `retrieval_filter` & value.
  Why it matters: tune distance/similarity to prune noise without starving the prompt.
* Source coverage: which `metadata.source` dominate citations (if you later add a per-chunk facet like `top_cited_sources`).

**Ranking effectiveness**

* Ranker mix: `% rank_mode=rank_service` vs `llm`.
* Ranking lift proxy: compare average `score` among *cited* chunks vs all retrieved (requires a small facet like `avg_retrieved_score` and `avg_cited_score` you compute in-action).

**Generation outcomes**

* Truncation rate: `gen_finish_reason="MAX_TOKENS"`; bump `reserve_output_tokens` or trim strategy when it climbs.
* Safety blocks rate: `candidates[0].safetyRatings[].blocked=true` (if you add a boolean facet like `safety_blocked`).
* Answerability: answer is “I don’t know.” frequency (add `answered_unknown` boolean when your system prompt enforces abstention).

**Confidence & quality proxies**

* Overall confidence (you already compute for rag-lite): track its distribution and drift.
* Answer length vs confidence matrix: short-but-confident vs long-and-low-confidence smells hallucination or insufficient context.

**Cost & capacity**

* Token usage per action (`tokens_total`) → rough cost curves; couple with embeddings `metadata.billableCharacterCount`.
* Throughput vs latency: saturation curves; indicates when to parallelize retrieval/ranking.

**Governance & hygiene**

* Severity anomalies: count of `severity:"NONSTANDARD/*"` by builder → training need.
* PII/code redaction sanity: scan for accidental large payloads in logs (you’re already redacting; keep it that way).

# Copy-paste queries (Logs Explorer)

Replace `YOUR_PROJECT` and `workato_vertex_rag` if needed.

**P95 latency by action (last 24h)**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
jsonPayload.latency_ms:*
| stats quantile(jsonPayload.latency_ms, 0.95) by labels.action
```

**Error rate by action & HTTP code**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
jsonPayload.status="error"
| parse jsonPayload.error.http_status as code
| stats count() by labels.action, code
```

**Zero-context rate (retrieve + answer)**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
labels.action in ("rag_retrieve_contexts","rag_answer")
| parse jsonPayload.facets.contexts_returned as n
| stats
  count_if(n=0) as zero_hits,
  count(*) as total,
  100.0*count_if(n=0)/count(*) as pct_zero
  by labels.action
```

**Truncation rate (generation)**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
labels.action="rag_answer"
| parse jsonPayload.facets.gen_finish_reason as fr
| stats count_if(fr="MAX_TOKENS")/count(*) * 100 as pct_truncated
```

**Confidence distribution (rag-lite)**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
labels.action="gen_generate"
| parse jsonPayload.facets.confidence as conf
| stats count() by bin(conf, 0.1)
```

**Ranking mode mix**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
labels.action in ("rag_retrieve_contexts","rag_answer")
| parse jsonPayload.facets.rank_mode as mode
| stats count() by mode
```

**Token spend per recipe**

```
logName="projects/YOUR_PROJECT/logs/workato_vertex_rag"
jsonPayload.facets.tokens_total:*
| stats sum(jsonPayload.facets.tokens_total) as tokens by jsonPayload.request_meta.recipe_id, labels.action
```

# Minimal tweaks

* **Add the `facets` block** (above). It costs a few dozen bytes and pays for itself daily.
* **Emit `contexts_returned`** (you already have the array; just count it).
* **Emit `gen_finish_reason`** (already in the response; just surface it).
* **Emit `safety_blocked`** (true if any rating blocked).
* **Emit `answered_unknown`** (detect from your abstention string).
* **Emit `rank_model` & `rank_mode`** (you already attach in `telemetry.rank`).
* **Add a BigQuery sink** for the log and build a view with typed columns over `jsonPayload.facets.*` so you can do longer-horizon analyses without Logs Explorer limits.

# SLOs

* **Availability**: 99.5% of calls end with `status="ok"` and HTTP 2xx/3xx.
* **Latency**: P95 `latency_ms` < 1500ms for retrieve, < 800ms for rank, < 2500ms for generate (tune to reality).
* **Retrieval quality**: zero-hit rate < 2%; median `contexts_returned` ≥ 8 for `top_k=20`.
* **Generation quality proxy**: truncation rate < 3%; `confidence ≥ 0.7` in ≥ 60% of rag-lite calls.
* **Cost guardrail**: tokens_total P95 < target per use case.

# More

* **Ranking lift proxy**: log `avg_retrieved_score` (pre-rank) and `avg_cited_score` (post-answer) to estimate lift without labels.
* **Dedupe effectiveness**: in rag-lite, log `kept_chunks` vs `pool_size` and `dedup_dropped` from `drop_near_duplicates`.
* **Hot sources**: when building `context_chunks`, compute top-N `source` counts and emit `top_sources=["handbook","policy_portal",…]`.
