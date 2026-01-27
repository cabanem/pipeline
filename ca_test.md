## SYSTEM
You are a world-class Workato custom connector engineer (Ruby DSL). You build enterprise-grade connectors: stable I/O contracts, defensive input handling, clear errors, and Workato-safe Ruby (standard libs only, no monkey patches).

## TASK
Create a **Utility custom connector** that helps recipe builders generate **Workflow Apps “new request” URLs** that prefill form components using Workato’s documented `prefilled_values` URL parameter.

## OBJECTIVE (must match Workato docs)
Workflow Apps supports prefilled form values by appending `prefilled_values` to a request page URL, where the parameter value is **URL-encoded JSON**. Keys must use the component **Title** (builder-visible), not the component Label (user-visible). Prefilled fields are **read-only by default** unless `"disabled": false` is included. There is a **maximum URL length of 8,000 characters**.

## DELIVERABLES
Return the **full Workato custom connector Ruby DSL** in one block, including:
- `title`, `subtitle`, `description`, `help`
- `connection` (no auth)
- `test`
- `actions` (at minimum the action below)
- helper methods (URL building, encoding, JSON parsing, normalization)
Also include:
- 2–3 example inputs and outputs (including dropdown manual + dropdown table formats)
- Notes for recipe builders on what to paste/share and what values must match (component Title)

---

# CONNECTOR REQUIREMENTS

## Action: build_prefilled_request_url

### Inputs
- `request_url` (string, required)
  - The Workflow Apps **new request page** URL from the Workflow apps portal.
- `prefilled_values` (object OR string, required)
  - If **object**: a hash keyed by **component Title** → `{ "value": <...>, "disabled": <bool optional> }`
  - If **string**: JSON text that parses to the same object shape.
- `default_editable` (boolean, optional, default: false)
  - If true: for any component missing `disabled`, automatically set `disabled: false`.
  - If false: do **not** add `disabled` unless explicitly provided.
- `on_url_limit` (string enum, optional, default: "error")
  - Allowed: `"error"`, `"return_error_object"`
  - If final URL length exceeds 8000 characters:
    - `"error"`: raise a clear error
    - `"return_error_object"`: return `within_limit=false` and populate `error`

### Behavior rules (must implement exactly)
1. Validate:
   - `request_url` is present and looks like a URL
   - `prefilled_values` is present
   - If string JSON is invalid: raise a clear, user-friendly error
   - The parsed `prefilled_values` must be a Hash/Object (not array)
2. Enforce that keys represent component **Title** (builder-visible). Include a warning note in docs/output.
3. JSON shape:
   - Top-level object keyed by component Title
   - Each value is an object containing:
     - `"value": <any supported type>`
     - `"disabled": false` only when explicitly specified OR when `default_editable=true`
4. Supported component value shapes:
   - Checkbox: boolean
   - Date: `"YYYY-MM-DD"`
   - DateTime: `"YYYY-MM-DD HH:MM"` (string)
   - Decimal/Integer: numbers
   - Text/Description: strings
   - Dropdown (manual): string value (e.g., `"Sales"`)
   - Dropdown (table): object value:
     - `{ "record_id": "<uuid>", "value": "<display>" }`
5. Encode & append:
   - Serialize JSON with `JSON.generate(...)` (no pretty-print)
   - URL-encode the JSON (e.g., `URI.encode_www_form_component`)
   - Append to `request_url` as query parameter named **`prefilled_values`**
   - Correctly handle:
     - URL already has query params (`?`) → append with `&`
     - URL has no query params → append with `?`
6. URL length limit:
   - Compute final URL length
   - If > 8000 chars, handle per `on_url_limit`
7. Return rich debug outputs:
   - `prefilled_url` (string)
   - `prefilled_values_json` (string)
   - `prefilled_values_encoded` (string)
   - `url_length` (integer)
   - `within_limit` (boolean)
   - `error` (string, optional)

### Outputs
Return the fields listed above.

---

## Optional Action (only if you can do it cleanly): json_to_csv
Add a second action that converts JSON → CSV and returns:
- `csv_string` (string)
- `csv_binary` (base64 string)
- `csv_file` (Workato file object: `{ content, content_type, original_filename }`)

Rules:
- Accept JSON as object or array; object becomes single-row array
- Flatten nested hashes with dotted keys (`a.b.c`)
- Flatten arrays with indexed keys (`items[0].id`)
- Stable header order (first-seen across rows)
- If a cell value remains Hash/Array after flattening, stringify as JSON
- Use standard libs only: `json`, `csv`, `base64`

---

## OUTPUT FORMAT
1) Provide the full connector code.
2) Provide examples:
- Example A: checkbox/date/integer/text
- Example B: dropdown manual
- Example C: dropdown table (`record_id` + `value`)
3) Provide short recipe-builder notes:
- Use component **Title** values
- Prefilled fields are read-only unless `"disabled": false` is included
- Max URL length: 8000 chars
