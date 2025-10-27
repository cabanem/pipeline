# Action Sheets

> Headers policy for all actions: only `Authorization: Bearer <token>` from `auth.apply()` and stable `Content-Type: application/json`. No per‑action inputs may influence headers. 

---

## 1A) Categorize email — **Embeddings**

**Plain task:** Turn email text into an embedding for downstream categorization.

**Host & path**

* **Host:** `https://{LOCATION}-aiplatform.googleapis.com`
* **Path:** `POST /v1/projects/{project}/locations/{location}/publishers/google/models/{embeddingModel}:predict`
  Use `gemini-embedding-001` (superset) or `text-embedding-005` / `text-multilingual-embedding-002`. Docs show model list and the `predict` endpoint form. ([Google Cloud Documentation][1])

**Path params (source)**

* `{project}`, `{location}` ⇒ **connection config**
* `{embeddingModel}` ⇒ **action config** (recommend default `gemini-embedding-001`) ([Google Cloud Documentation][1])

**Headers**

* From `apply()`: `Authorization`, plus `Content-Type: application/json`. (No dynamic headers.) 

**Required request body**

```json
{
  "instances": [
    {
      "content": "<EMAIL SUBJECT + BODY>",
      "task_type": "CLASSIFICATION"
    }
  ]
}
```

* `instances[].content` **required**; `task_type` optional but recommended (e.g., `CLASSIFICATION`, `RETRIEVAL_QUERY`, etc.). Optional `parameters` include `autoTruncate` and `outputDimensionality`. ([Google Cloud Documentation][1])

**Key response shape**

* `predictions[0].embeddings.values[]` (vector) and `predictions[0].embeddings.statistics.token_count`. ([Google Cloud Documentation][1])

**Test vectors**

* *Minimal:* one instance, short email text.
* *Typical:* subject + body (~1–2k chars), `task_type="CLASSIFICATION"`.
* *Max-ish:* long body near the model’s input limit; set `"parameters": {"autoTruncate": true}`.
* *Malformed:* missing `instances`, or `instances` not an array. (Expect 400.) ([Google Cloud Documentation][1])

---

## 1B) Categorize email — **Generate (LLM label)**

**Plain task:** Ask Gemini to output a single category label.

**Host & path**

* **Host:** `https://aiplatform.googleapis.com`
* **Path:** `POST /v1/{model}:generateContent`
  Where `{model}` is a publisher path like `projects/{project}/locations/{location}/publishers/google/models/{geminiModel}`. Fields and required `contents` are defined here. ([Google Cloud][2])

**Path params (source)**

* `{project}`, `{location}` ⇒ **connection config**
* `{geminiModel}` ⇒ **action config** (e.g., `gemini-1.5-flash-001` / current equivalent)

**Required request body (enum output)**

```json
{
  "contents": [
    {
      "role": "user",
      "parts": [
        { "text": "Classify this email into one of [Billing, Sales, Support, HR]:\n<EMAIL TEXT>" }
      ]
    }
  ],
  "generationConfig": {
    "responseMimeType": "text/x.enum",
    "responseSchema": {
      "type": "string",
      "enum": ["Billing","Sales","Support","HR"]
    },
    "temperature": 0
  }
}
```

* `contents` **required**.
* If you supply `responseSchema`, you must also set a supported `responseMimeType` (`application/json` or `text/x.enum`; not `text/plain`). ([Google Cloud Documentation][3])

**Key response shape**

* `candidates[].content.parts[].text` (enum value) and `usageMetadata`. ([Google Cloud][2])

**Test vectors**

* *Enum happy path:* clean email pointing to `Billing`.
* *Edge:* ambiguous email—verify model still emits one of the enum values.
* *Malformed:* missing `responseMimeType` while providing `responseSchema` → validation fail (enforce in connector). 

---

## 2) Extract salient span — **Generate (structured JSON)**

**Plain task:** Extract an exact span and optional character offsets from the email.

**Host & path**

* **Host:** `https://aiplatform.googleapis.com`
* **Path:** `POST /v1/{model}:generateContent` (same as above). `contents` is required. ([Google Cloud][2])

**Body (recommended JSON schema)**

```json
{
  "contents": [
    {
      "role": "user",
      "parts": [
        { "text": "From the email below, extract the exact span answering:\n'When does the benefits window close?'\n\n<EMAIL TEXT>" }
      ]
    }
  ],
  "generationConfig": {
    "responseMimeType": "application/json",
    "responseSchema": {
      "type": "object",
      "properties": {
        "span": { "type": "string" },
        "startChar": { "type": "integer" },
        "endChar": { "type": "integer" }
      },
      "required": ["span"]
    },
    "temperature": 0,
    "maxOutputTokens": 256
  }
}
```

* `responseSchema` requires a non‑`text/plain` `responseMimeType`. Docs enumerate supported MIME types and the pairing with schema. ([Google Cloud Documentation][3])

**Test vectors**

* *Minimal:* single date present.
* *Multi‑match:* two candidate dates—confirm model picks the most relevant.
* *Malformed:* schema requires `span` but model returns empty—connector should surface a clear “schema violation” error to caller (application‑level validation).

---

## 3A) Answer with context — **Count tokens**

**Plain task:** Pre‑flight sizing before generation.

**Host & path (publisher or tuned)**

* **Publisher model form (preferred for simplicity in your connector):**
  `POST /v1/{model}:countTokens` where `{model}` is a publisher model path. **Body** carries the same `contents` you’ll send to generate. The REST reference allows `contents` and/or `instances`, plus optional `generationConfig` and `systemInstruction`. 
* **Endpoint/tuned model form:**
  `POST /v1/projects/{project}/locations/{location}/endpoints/{endpoint}:countTokens`. The reference shows the endpoint‑form explicitly and permits including a `model` field in the body when using the endpoint path. ([Google Cloud Documentation][4])

**Minimal body**

```json
{
  "systemInstruction": {
    "role": "system",
    "parts": [{ "text": "Use only the provided context; otherwise say you don't know." }]
  },
  "contents": [
    { "role": "user", "parts": [{ "text": "QUESTION:\n<USER QUESTION>" }] },
    { "role": "user", "parts": [{ "text": "CONTEXT:\n<CONCATENATED CHUNKS>" }] }
  ]
}
```

**Response**: `totalTokens` (+ billable characters and per‑modality details where available). 

**Test vectors**

* *Short prompt* vs *long prompt*—assert budget gating works (e.g., block if projected tokens > model limit).
* *Malformed:* `contents` missing or empty (expect 400). ([Google Cloud Documentation][4])

---

## 3B) Answer with context — **Generate (with config)**

**Plain task:** Answer the question using the supplied contexts.

**Host & path**

* **Host:** `https://aiplatform.googleapis.com`
* **Path:** `POST /v1/{model}:generateContent` (publisher model recommended). Required `contents`; optional `systemInstruction` and `generationConfig`. ([Google Cloud][2])

**Body (steady default)**

```json
{
  "systemInstruction": {
    "role": "system",
    "parts": [{ "text": "Answer using only the provided CONTEXT. If insufficient, say you don't know." }]
  },
  "contents": [
    { "role": "user", "parts": [{ "text": "QUESTION:\n<USER QUESTION>" }] },
    { "role": "user", "parts": [{ "text": "CONTEXT:\n<CHUNK 1>\n---\n<CHUNK 2>\n---\n<CHUNK 3>" }] }
  ],
  "generationConfig": {
    "maxOutputTokens": 512,
    "temperature": 0.2
  }
}
```

**Structured answer (optional):** add `responseMimeType: "application/json"` and a `responseSchema` if you want machine‑readable answers. (When `responseSchema` is present, `responseMimeType` cannot be `text/plain`.) ([Google Cloud Documentation][3])

**Test vectors**

* *Minimal:* one short context chunk.
* *Typical:* 3–5 chunks, 200–800 tokens.
* *Edge:* prompt close to model’s context window → rely on prior 3A count to reject early.

---

## 4) Rank contexts — **Discovery Engine Ranking API**

**Plain task:** Re‑rank your candidate chunks before sending to the LLM.

**Host & path**

* **Host:** `https://discoveryengine.googleapis.com`
* **Path:** `POST /v1beta/{rankingConfig=projects/*/locations/*/rankingConfigs/*}:rank`
  Method and request schema (including default model `semantic-ranker-512@latest`, required `records[]`) are in the v1beta reference. ([Google Cloud][5])

**Path params (source)**

* `{project}`, `{location}` ⇒ **connection config**
* `{rankingConfigs/*}` ⇒ **action config** (often `default_ranking_config`). ([Google Cloud][5])

**Required request body (minimal)**

```json
{
  "query": "benefits enrollment window",
  "records": [
    { "id": "a", "content": "Doc A text..." },
    { "id": "b", "content": "Doc B text..." }
  ],
  "topN": 5
}
```

* Each record must include **at least one of** `title` or `content`; `id` recommended for traceability. Defaults `model` to `semantic-ranker-512@latest`. ([Google Cloud][5])

**Response**

* `{ "records": [ { "id", "title", "content", "score" } ... ] }` sorted by descending `score`. ([Google Cloud][5])

**Test vectors**

* *Minimal:* two short records.
* *Typical:* 20–50 records; verify `topN` truncation.
* *Malformed:* records missing both `title` and `content` → INVALID_ARGUMENT. ([Google Cloud][5])

---

## 5) Retrieve contexts — **Vertex RAG Engine**

**Plain task:** Retrieve top‑k contexts from your RAG corpus/store.

**Host & path (two valid forms)**

* **Location‑level:** `POST https://aiplatform.googleapis.com/v1/{parent}:retrieveContexts` with `parent=projects/{project}/locations/{location}`. ([Google Cloud][6])
* **Corpus‑level:** `POST https://aiplatform.googleapis.com/v1/{parent}:retrieveContexts` with `parent=projects/{project}/locations/{location}/ragCorpora/{corpus}`. (Same method; narrower scope by corpus.) 

**Required request body**

```json
{
  "query": { "text": "<USER QUESTION OR QUERY TEXT>" },
  "dataSource": {
    "vertexRagStore": {
      "ragResources": [
        {
          "ragCorpus": "projects/{project}/locations/{location}/ragCorpora/{corpus}",
          "ragFileIds": ["optional-file-id-1", "optional-file-id-2"]
        }
      ]
    }
  }
}
```

* `query` and `dataSource.vertexRagStore.ragResources[0].ragCorpus` are the critical pieces. Response returns ranked contexts with `text`, `score`, and source metadata. ([Google Cloud][6])

**Test vectors**

* *Minimal:* one corpus, short query.
* *File‑scoped:* include `ragFileIds` to restrict retrieval.
* *Edge:* noisy query; verify response still returns contexts or empty list.
* *Malformed:* missing `query` or `dataSource` → 400. ([Google Cloud][6])

---

# Connector wiring notes that keep things stable

* **Paths & servers:** For Vertex, you can call via the global host or a region host. Keep `{project}`, `{location}`, and `{model}`/`{endpoint}` strictly in **path params**; **don’t** smuggle any of these into headers. 
* **Structured output rule:** if `generationConfig.responseSchema` is present, require a supported `responseMimeType` (e.g., `application/json` or `text/x.enum`), not `text/plain`. Enforce at validation time so action inputs can’t accidentally “tickle” `auth.apply`. ([Google Cloud Documentation][3])
* **CountTokens form:** The docs currently emphasize the **endpoint‑form** (`…/endpoints/{endpoint}:countTokens`) and allow a `model` in the body, while your minimal contract also supports the **publisher‑model path** (`{model}:countTokens`). Support either, but pick one per connector to avoid ambiguity. ([Google Cloud Documentation][4]) 

---

## Copy‑paste “action specs” (platform‑neutral skeletons)

> Use these as the per‑action scaffolds in your connector repo.

### Template fields (repeat per action)

* **Name / Plain task**
* **Host & Path:** `METHOD URL`
* **Path params (source):** connection config vs action input
* **Query params:** (if any)
* **Headers:** *apply‑only allowlist*
* **Body (required fields):** shape + enums
* **Success mapping:** key fields returned
* **Error mapping:** 401/403 refresh, 429 `Retry-After`, 5xx retryable. 
* **Test vectors:** minimal / typical / edge / malformed


[1]: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api "Text embeddings API  |  Generative AI on Vertex AI  |  Google Cloud Documentation"
[2]: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.endpoints/generateContent "Method: endpoints.generateContent  |  Generative AI on Vertex AI  |  Google Cloud"
[3]: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/inference "Generate content with the Gemini API in Vertex AI"
[4]: https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.endpoints/countTokens "Method: endpoints.countTokens | Generative AI on Vertex AI"
[5]: https://cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1beta/projects.locations.rankingConfigs/rank "Method: projects.locations.rankingConfigs.rank  |  Vertex AI Search  |  Google Cloud"
[6]: https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations/retrieveContexts "Method: locations.retrieveContexts  |  Generative AI on Vertex AI  |  Google Cloud"
