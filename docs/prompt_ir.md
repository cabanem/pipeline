# Phases

## Phase 1 — “Analyze → Emit Manifest”

**Goal:** carve the code into components and produce a single manifest JSON with locations, types, and dependency hints.

**Prompt (drop-in template):**

```
You are a code analyst. Ingest the code below (≤1000 lines). 
Task: emit a MANIFEST that lists all logical components with locations and dependencies.

Rules:
- Output ONLY valid JSON. No prose. Use the schema in "Manifest schema".
- Identify components such as: modules/packages, classes, structs/types, interfaces, top-level functions, constants/enums, config blocks.
- Record start_line and end_line (1-based) inclusive.
- For dependencies, use symbol-level references seen in the code (imports, calls, extends/implements, field types).
- If unsure, set the field to null or [] (do not invent data).
- Never include the original code in JSON.

Manifest schema:
{
  "language": "string",                // e.g., "python", "ruby", "js", "go"
  "file_name": "string",
  "summary": "string",
  "components": [
    {
      "id": "string",                  // stable slug: <kind>:<name> (unique)
      "kind": "module|class|interface|function|type|enum|const|config|other",
      "name": "string|null",
      "start_line": 0,
      "end_line": 0,
      "exports": ["string"],           // public symbols this component exposes
      "depends_on": ["string"],        // component ids or symbol names
      "visibility": "public|internal|private|null",
      "notes": "string|null"
    }
  ]
}

Now analyze this code:

<CODE>
{{paste code here}}
</CODE>
```

**What you’ll get:** a clean index of components with line ranges and a first pass at dependencies. This becomes your routing table for Phase 2.

---

## Phase 2 — “Per-Component Analysis → JSON Components”

**Goal:** for each component from the manifest, ask for a *focused* JSON analysis: purpose, API surface, invariants, risks, and test ideas. Keep it short and structured.

**Prompt (run once per component using its slice):**

```
You are a code analyst. Analyze the following component only.

Context:
- language: {{language}}
- file_name: {{file_name}}
- component_id: {{id}}
- lines: {{start_line}}-{{end_line}}

Task: emit ONLY valid JSON conforming to "Component schema". 
Keep answers concise and grounded strictly in the provided snippet. 
If a field is unknown, use null or [].

Component schema:
{
  "id": "string",
  "kind": "module|class|interface|function|type|enum|const|config|other",
  "name": "string|null",
  "purpose": "string",                 // one-sentence headline
  "public_api": {
    "functions": [
      { "name": "string", "params": [{"name":"string","type":"string|null"}], "returns": "string|null", "throws": ["string"], "doc": "string|null" }
    ],
    "fields": [
      { "name": "string", "type": "string|null", "visibility": "public|internal|private|null", "doc": "string|null" }
    ]
  },
  "contracts": {
    "preconditions": ["string"],
    "postconditions": ["string"],
    "invariants": ["string"]
  },
  "complexity": {
    "cyclomatic": "number|null",
    "hotspots": ["string"]             // e.g., "nested branching", "I/O + retries"
  },
  "dependencies": {
    "imports": ["string"],
    "calls": ["string"],
    "uses_types": ["string"]
  },
  "security_and_reliability": {
    "risks": ["string"],
    "inputs_validation_gaps": ["string"],
    "error_handling": "string|null"
  },
  "performance": {
    "big_o": "string|null",
    "allocations": "string|null"
  },
  "test_ideas": {
    "unit": ["string"],
    "property": ["string"],
    "fuzz": ["string"],
    "integration": ["string"]
  },
  "telemetry": {
    "logging": ["string"],
    "metrics": ["string"],
    "tracing": ["string"]
  },
  "notes": "string|null"
}

Component source:
<CODE>
{{paste lines start_line..end_line from original code}}
</CODE>
```

**Why this works:** you’re constraining the model to the exact lines that define the component, which reduces hallucinations and keeps outputs compact and comparable.

---

## Phase 3 — “Join and Emit”

**Goal:** stitch the manifest and component JSONs into a single IR document you can store, diff, and reuse when asking for help.

**Prompt (feed the manifest + all per-component JSONs):**

```
You are an IR assembler. Merge the MANIFEST with N COMPONENT JSON artifacts into a single Intermediate Representation (IR).

Rules:
- Output ONLY valid JSON per the "IR schema".
- Join by component id. Preserve manifest line ranges and kinds.
- Merge dependency sets (imports/calls/uses_types) and normalize to sorted, unique arrays.
- Validate referential integrity: for each dependency that matches a known id, keep it under "depends_on_ids"; otherwise keep the symbol under "depends_on_symbols".
- Do not include code.

IR schema:
{
  "language": "string",
  "file_name": "string",
  "summary": "string",
  "graph": {
    "nodes": [
      {
        "id": "string",
        "kind": "string",
        "name": "string|null",
        "start_line": 0,
        "end_line": 0,
        "public_api": { "functions": [...], "fields": [...] },
        "contracts": { "preconditions":[], "postconditions":[], "invariants":[] },
        "complexity": { "cyclomatic": "number|null", "hotspots": ["string"] },
        "security_and_reliability": { "risks":[], "inputs_validation_gaps":[], "error_handling":"string|null" },
        "performance": { "big_o":"string|null", "allocations":"string|null" },
        "telemetry": { "logging":[], "metrics":[], "tracing":[] },
        "test_ideas": { "unit":[], "property":[], "fuzz":[], "integration":[] },
        "depends_on_ids": ["string"],
        "depends_on_symbols": ["string"],
        "exports": ["string"],
        "visibility": "public|internal|private|null",
        "notes": "string|null"
      }
    ],
    "edges": [ { "from": "string", "to": "string", "type": "import|call|type" } ]
  },
  "meta": {
    "created_at": "ISO-8601 string",
    "generator": "string",
    "version": "1.0.0"
  }
}

Inputs:
- MANIFEST JSON:
<MANIFEST>
{{paste manifest JSON}}
</MANIFEST>

- COMPONENT JSONS (array):
<COMPONENTS>
[
  {{component_1_json}},
  {{component_2_json}},
  ...
]
</COMPONENTS>
```

The assembler can also compute `graph.edges` by translating dependency sets into edges where the target id exists.

---

# Practical guardrails

* **Determinism:** always say “Output ONLY valid JSON.” If the model adds prose, reject and re-ask with “Return JSON only, no commentary.”
* **Line slicing:** in Phase 2, paste only the component’s lines. This keeps the model from drifting to neighbors.
* **Unknowns beat inventions:** explicitly allow `null`/`[]` for unknown fields.
* **IDs:** use `"<kind>:<name>"` or `"<kind>:<file>:<line>"` when names are missing.
* **Size discipline:** ≤1000 lines per file is fine; if you ever exceed, run multiple manifests and later merge.

---

# Tiny worked example (abbreviated)

**Input code (JS):**

```js
export function add(a, b) { return a + b; }
export function sum(arr) { return arr.reduce(add, 0); }
```

**Phase 1 manifest (model output):**

```json
{
  "language": "js",
  "file_name": "math.js",
  "summary": "Two exported functions.",
  "components": [
    { "id":"function:add","kind":"function","name":"add","start_line":1,"end_line":1,"exports":["add"],"depends_on":[],"visibility":"public","notes":null },
    { "id":"function:sum","kind":"function","name":"sum","start_line":2,"end_line":2,"exports":["sum"],"depends_on":["function:add"],"visibility":"public","notes":null }
  ]
}
```

**Phase 2 component (for `sum`):**

```json
{
  "id":"function:sum",
  "kind":"function",
  "name":"sum",
  "purpose":"Aggregate an array by summing using add.",
  "public_api":{"functions":[{"name":"sum","params":[{"name":"arr","type":"Array<number>|null"}],"returns":"number","throws":[],"doc":null}],"fields":[]},
  "contracts":{"preconditions":["arr is iterable"],"postconditions":["result equals sum of items"],"invariants":[]},
  "complexity":{"cyclomatic":1,"hotspots":[]},
  "dependencies":{"imports":[],"calls":["add"],"uses_types":[]},
  "security_and_reliability":{"risks":[],"inputs_validation_gaps":["no type checks"],"error_handling":null},
  "performance":{"big_o":"O(n)","allocations":"reduce iterator"},
  "test_ideas":{"unit":["sum([1,2,3])==6","sum([])==0"],"property":["sum(xs.concat(ys))==sum(xs)+sum(ys)"],"fuzz":["random arrays numbers"],"integration":[]},
  "telemetry":{"logging":[],"metrics":[],"tracing":[]},
  "notes":null
}
```

**Phase 3 IR (abbrev):**

```json
{
  "language":"js",
  "file_name":"math.js",
  "summary":"Two exported functions.",
  "graph":{
    "nodes":[ /* merged nodes for add and sum */ ],
    "edges":[ {"from":"function:sum","to":"function:add","type":"call"} ]
  },
  "meta":{"created_at":"2025-10-28T00:00:00Z","generator":"ir-assembler","version":"1.0.0"}
}
```

---

## How this improves “help me code” requests

When you ask for help later, include the IR (not the raw file) and say: “You may rely on these contracts, APIs, and dependencies.” The model can reason faster, avoid hallucinating structure, and jump straight to tests, refactors, or bug hunts. You can even ask it to “write unit tests for nodes with risks” or “propose a refactor plan for the subgraph rooted at function:sum.”


# Global system instructions (apply to every phase)

* **Output contract:** Return **ONLY valid JSON**, no prose, no markdown. If something is unknown, use `null` or `[]`—**never invent** values.
* **Schema fidelity:** Conform exactly to the provided schema for the current phase. Reject extra keys. Keep field types correct (numbers as numbers, booleans as booleans).
* **Determinism:** Sort all arrays lexicographically unless the schema states otherwise. Use stable IDs of the form `"<kind>:<name>"`; if no name, use `"<kind>:<file>:<start_line>"`.
* **Line discipline:** All line numbers are **1-based, inclusive**. Do not exceed the snippet’s bounds.
* **No code echoes:** Do not reproduce source code outside the given `<CODE>` block in Phase 2. Never include full code in outputs.
* **Conservatism:** If the input is ambiguous, prefer `null`/`[]` and add a brief machine-readable note in the `notes` field (string, not an essay).
* **No hidden reasoning:** Do not include explanations, analysis, or chain-of-thought. Only final JSON.
* **Units & locale:** Use plain SI/ASCII; ISO-8601 timestamps; no locale-specific formatting.
* **Security:** Do not emit secrets, keys, tokens, or personally identifying data even if present in the code; replace with `"REDACTED"` and note it.

# Phase-specific add-ons

## Phase 1 — Analyze → Emit Manifest

* **Goal:** enumerate components with locations and dependency hints.
* **Scope:** Entire file (≤1000 lines).
* **Dependencies:** Use observed symbols only (imports, extends/implements, function calls, type references). Do not guess hidden modules.
* **Fields:** Always set `start_line`, `end_line`, `kind`, `name` (or `null`), `exports`, `depends_on`, `visibility` (best effort), `notes` (optional).

## Phase 2 — Per-component analysis → JSON Components

* **Goal:** describe API surface, contracts, risks, deps, and test ideas for **one** component.
* **Scope:** Only lines `start_line..end_line` from the manifest; ignore neighbors.
* **Precision:** Derive `public_api`, `contracts`, `dependencies` from the snippet. If metrics like `cyclomatic` cannot be computed confidently, set `null`.
* **Testing fields:** Keep `test_ideas` concise and directly tied to observed behavior; avoid generic filler.

## Phase 3 — Join and Emit

* **Goal:** merge the manifest and N component JSONs into a single IR with a dependency graph.
* **Integrity:**

  * Join by `id`. If a dependency matches a known `id`, place it in `depends_on_ids`; otherwise keep it in `depends_on_symbols`.
  * Create `edges` with `type` ∈ `{"import","call","type"}` based on merged dependency sets.
* **Normalization:** Deduplicate and sort all arrays. Preserve original `start_line`/`end_line`.

# Quality gates (self-check before emitting)

* JSON parses? ✔️
* All required fields present per schema? ✔️
* Arrays sorted/deduped? ✔️
* No prose or code blocks outside JSON? ✔️
* Line ranges within bounds? ✔️
* Unknowns use `null`/`[]`, not fabricated values? ✔️

# Failure mode contract (use when inputs are malformed)

Return a minimal JSON error object (and nothing else):

```json
{ "error": { "code": "INPUT_VALIDATION", "message": "Brief machine-readable reason", "details": { "phase": "P1|P2|P3", "field": "name.of.field.or.null" } } }
```

# Stable ID rules (to avoid drift)

* Functions: `function:<name>`; Classes: `class:<name>`; Types/Interfaces: `type:<name>`/`interface:<name>`.
* Unnamed items: append file and line: `function:<file>:<start_line>`.
* IDs are case-sensitive and must remain identical across phases.

# Minimal system prompt (template)

```
SYSTEM:
Follow these global rules:
- Output ONLY valid JSON, no prose/markdown.
- Conform exactly to the provided schema (no extra keys).
- Use 1-based inclusive line numbers; never exceed snippet bounds.
- Prefer null/[] over invention; never fabricate values.
- Sort/deduplicate arrays; keep numbers/booleans correctly typed.
- Use stable IDs "<kind>:<name>" or "<kind>:<file>:<start_line>".
- No code echoes outside provided snippets; no hidden reasoning.
- Never emit secrets; replace with "REDACTED" if encountered.

Phase-specific rules:
- (Add the Phase 1/2/3 section from above corresponding to this run.)
- On malformed input, emit only the error JSON object.

Proceed using the schema provided in the user message.
```
