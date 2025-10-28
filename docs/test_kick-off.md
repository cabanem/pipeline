# Kick off - Shadow Testing

The goal is to build confidence, align on the purpose, and confirm the "blast radius" is zero.

### 1. The "Phase C" Close-Out Report
* **Ingestion Success:** "We successfully ingested $X$ documents, representing $Y\%$ of the target corpus."
* **Data Quality:** "We confirmed 100% of vectors have the correct dimensionality and $\le 1\%$ of content was malformed (and triaged)."
* **Index Fidelity:** "The vector index contains $N$ documents, which matches our expected count."

### 2. The "Shadow Mode" Plan (Simplified)
* **The "One-Sentence" Purpose:** "We are kicking off a 'shadow test' where the AI will *draft* answers to real emails but **will not send them**. The goal is to safely measure answer quality and cost against live data."
* **Simple Architecture Diagram:** A visual showing:
    1.  `Live Email In`
    2.  `RAG System Generates Answer`
    3.  A big **[STOP]** sign showing the answer is *not* sent.
    4.  `Draft Answer Saved to [GCS / Data Table]`
    5.  `Metrics Sent to [Dashboard]`
* **Test Scope:**
    * **Source:** Which emails are you pulling? (e.g., "Only emails with the 'Support' label," "A 5% random sample of all inbound," etc.)
    * **Duration:** How long will the test run? (e.g., "A 2-week period," or "Until we capture 1,000 emails.")

### 3. The "Shadow Gates" (Success Criteria)
| Question We Will Answer | Metric | Target (Exit Gate) |
| :--- | :--- | :--- |
| Does the AI use our docs? | Grounding Rate | $\ge 95\%$ |
| Are the docs it uses *relevant*? | Citation Coverage | $\ge 85\%$ |
| How often is it factually wrong? | Hallucination Rate | $\le 5\%$ |
| Is it fast enough? | Latency (p90) | $\le 6$ seconds |
| Is it affordable? | Cost per 100 Emails | $\le \text{\$TBD}$ |

### 4. The Risk & Safety Plan
* **Cost Controls:** "We have configured hard **rate limits** and a **budget cap** of $X per day/week to prevent runaway costs."
* **Data Security:** "All generated drafts are stored securely in our private GCS bucket/Data Table. No PII or drafts leave our environment. Access is limited to the project team."
* **Failure Mode:** "If the system fails, the *only* impact is that a draft isn't generated. The existing human workflow is not affected in any way."

### 5. The Communication Plan
* **The Dashboard:** A link to the live monitoring dashboard (even if it's empty) where they can see the metrics (latency, cost, grounding) accumulate in real-time.
* **The Cadence:** "We will send a weekly summary of the 'Shadow Gates' metrics every Friday."
* **The "Go/No-Go" Criteria:** "After the test, we will present a final report against these 'Shadow Gates' with a recommendation to proceed to Phase E (Offline Eval) and a future 'Beta' (live test) or to re-evaluate."
