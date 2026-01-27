## SYSTEM
You are a world-class Workato custom connector engineer (Workato Ruby DSL). Produce **DSL-valid** connector code that runs in Workato’s runtime. Use only standard Ruby libs (`json`, `uri`, `csv`, `base64`). No monkey patches. No defining helper methods inside `execute`; put helpers in `methods:`. Use Workato conventions: `help:` as a lambda, `test:` returns a hash, selects use `pick_list:` + `pick_lists:` (not raw `options:`).

## TASK
Build a **Utility custom connector** for **Workflow Apps** that generates a **“New request” URL** with the `prefilled_values` parameter and (optionally) converts JSON to CSV.

## OBJECTIVE (must follow Workflow Apps prefill behavior)
- The URL must include query parameter **`prefilled_values`**.
- Parameter value is **URL-encoded JSON** (not base64).
- JSON keys must be **component Title** (builder-visible), not the Label.
- Each prefilled field value uses `{ "value": ..., "disabled": false? }`.
- Prefilled components are **read-only by default** unless `"disabled": false` is set.
- Enforce **max URL length ~ 8000 characters** (detect and fail/return error based on setting).

## DELIVERABLES
Return the **complete connector Ruby DSL** in one code block, including:
- `title`, `subtitle`, `description`, `help: -> { ... }`
- `connection` (no auth; no unused base_uri)
- `test: ->(_connection) { { success: true } }`
- `pick_lists:` (for select inputs)
- `actions:` and `methods:`
Also include:
- 3 concrete examples (checkbox/date/integer/text, manual dropdown, table-backed dropdown)
- Short recipe-builder notes (Title vs Label, read-only default, URL length limit)

---

# ACTION 1: build_prefilled_request_url

## Inputs
1) `request_url` (string, required)
   - The Workflow Apps “New request page” URL.
2) `prefilled_values_json` (string, required)
   - Raw JSON text representing the prefill payload.
   - This avoids Workato typing ambiguity; always parse JSON in code.
3) `default_editable` (boolean, optional, default: false)
   - If true: any component entry missing `disabled` should get `"disabled": false`.
   - If false: do not add `disabled` unless explicitly present (read-only default).
4) `on_url_limit` (string select, optional, default: `error`)
   - pick_list options: `error` | `return_error_object`

## Validation & normalization rules
- Reject blank `request_url` or URLs not starting with http/https.
- Parse `prefilled_values_json`:
  - If invalid JSON: raise a clear error.
  - Must parse to an **object/hash** (not array).
- Each key is treated as **component Title** (do not attempt label mapping).
- Each value must be an object/hash that includes `"value"` (string key) or `:value`:
  - If missing, raise: “Component '<Title>' must include 'value'.”
- If `default_editable=true`, add `"disabled": false` only when disabled is absent.

## URL building requirements (must handle edge cases)
- Use `URI.parse(request_url)` to safely modify the URL.
- Correctly handle URL fragments (`#...`):
  - Ensure the `prefilled_values` query parameter is added **before** the fragment.
- Preserve existing query parameters:
  - If `prefilled_values` already exists, **replace** it (do not duplicate).
- Encode:
  - `json_payload = JSON.generate(normalized_hash)`
  - `encoded = URI.encode_www_form_component(json_payload)`
- Rebuild final URL with updated query.

## URL length enforcement
- Compute final URL length.
- If > 8000:
  - If `on_url_limit == "error"`: raise a clear error.
  - Else: return `within_limit=false` and populate `error`.

## Outputs
Return:
- `prefilled_url` (string)
- `prefilled_values_json` (string) — normalized JSON actually used
- `prefilled_values_encoded` (string)
- `url_length` (integer)
- `within_limit` (boolean)
- `error` (string, optional)

---

# ACTION 2 (OPTIONAL): json_to_csv

## Inputs
- `json_string` (string, required): raw JSON text (object or array)

## Behavior
- Parse JSON; accept array of objects or a single object (treat as one-row array).
- Flatten:
  - Hashes → dotted keys: `a.b.c`
  - Arrays → indexed keys: `items[0].id`
- Stable header order: first-seen keys across rows.
- Any remaining Hash/Array cell values should be JSON-stringified.
- Output:
  - `csv_string` (string)
  - `csv_binary` (string base64 of csv_string using `Base64.strict_encode64`)
  - `csv_file` (object: `{ content, content_type, original_filename }`)

---

# IMPLEMENTATION REQUIREMENTS
- Put flattening and URL manipulation in `methods:` helpers.
- Use `pick_lists:` for `on_url_limit` select values (no inline `options:` arrays).
- Return Workato-friendly errors using `error("...")` where appropriate.
- Provide examples showing:
  - A: basic primitives + default read-only vs editable
  - B: manual dropdown value
  - C: table dropdown value object `{ record_id, value }`


{
  "contract_name": "workato.workflow_apps_prefill_utility_connector.v1",
  "purpose": "Assess a Workato custom connector (Ruby DSL) that generates Workflow Apps prefilled request URLs (prefilled_values) and optionally converts JSON to CSV.",
  "scope": {
    "primary_action": "build_prefilled_request_url",
    "optional_actions": ["json_to_csv"]
  },
  "connector_requirements": {
    "top_level_fields_required": ["title", "subtitle", "description", "help", "connection", "test", "actions"],
    "help_must_be_lambda": true,
    "connection": {
      "authorization_type": "none",
      "no_unused_base_uri": true
    },
    "test_contract": {
      "must_return_hash": true,
      "preferred_shape": { "success": true }
    },
    "allowed_standard_libs": ["json", "uri", "csv", "base64"],
    "dsl_conventions": {
      "select_inputs_use_pick_list": true,
      "pick_lists_defined_at_top_level": true,
      "no_def_inside_execute": true,
      "helpers_live_in_methods_block": true
    }
  },
  "pick_lists": {
    "required": [
      {
        "name": "on_url_limit_options",
        "items": [
          { "label": "Raise Error (Fail Job)", "value": "error" },
          { "label": "Return Error Object", "value": "return_error_object" }
        ]
      }
    ]
  },
  "actions": {
    "build_prefilled_request_url": {
      "required": true,
      "title_contains": ["prefill", "url"],
      "inputs": {
        "required_fields": [
          {
            "name": "request_url",
            "type": "string",
            "optional": false,
            "validation": {
              "non_blank": true,
              "must_match_regex": "^https?://"
            }
          },
          {
            "name": "prefilled_values_json",
            "type": "string",
            "optional": false,
            "validation": {
              "non_blank": true,
              "must_be_valid_json": true,
              "must_parse_to": "object"
            }
          }
        ],
        "optional_fields": [
          {
            "name": "default_editable",
            "type": "boolean",
            "default": false
          },
          {
            "name": "on_url_limit",
            "type": "string",
            "control_type": "select",
            "default": "error",
            "pick_list": "on_url_limit_options"
          }
        ]
      },
      "outputs": {
        "required_fields": [
          { "name": "prefilled_url", "type": "string" },
          { "name": "prefilled_values_json", "type": "string" },
          { "name": "prefilled_values_encoded", "type": "string" },
          { "name": "url_length", "type": "integer" },
          { "name": "within_limit", "type": "boolean" }
        ],
        "optional_fields": [{ "name": "error", "type": "string" }]
      },
      "behavior_rules": {
        "parameter_name": "prefilled_values",
        "encoding": {
          "json_serializer": "JSON.generate",
          "url_encoder": "URI.encode_www_form_component",
          "not_allowed": ["base64_encoding_of_payload"]
        },
        "payload_shape": {
          "top_level": "object",
          "keys_are_component_title": true,
          "entry_must_be_object": true,
          "entry_must_include_value": true,
          "disabled_default": "read_only_by_default_when_disabled_missing",
          "default_editable_true_means_inject_disabled_false_when_missing": true
        },
        "url_manipulation": {
          "must_preserve_existing_query_params": true,
          "must_replace_existing_prefilled_values_param": true,
          "must_handle_fragment_hash": true,
          "fragment_preserved": true,
          "query_must_appear_before_fragment": true
        },
        "url_length_limit": {
          "limit_chars": 8000,
          "on_exceed_error_mode": "raise_error",
          "on_exceed_return_error_object_mode": {
            "within_limit": false,
            "error_populated": true,
            "prefilled_url_returned": true
          }
        }
      },
      "required_helper_methods": [
        "parse_prefilled_values_json",
        "normalize_prefilled_values_hash",
        "upsert_query_param",
        "rebuild_url_preserving_fragment"
      ]
    },
    "json_to_csv": {
      "required": false,
      "inputs": {
        "required_fields": [
          {
            "name": "json_string",
            "type": "string",
            "optional": false,
            "validation": { "must_be_valid_json": true, "must_parse_to": ["object", "array"] }
          }
        ]
      },
      "outputs": {
        "required_fields": [
          { "name": "csv_string", "type": "string" },
          { "name": "csv_binary", "type": "string", "format": "base64" },
          {
            "name": "csv_file",
            "type": "object",
            "properties_required": [
              { "name": "content", "type": "string", "format": "base64" },
              { "name": "content_type", "type": "string", "const": "text/csv" },
              { "name": "original_filename", "type": "string" }
            ]
          }
        ]
      },
      "behavior_rules": {
        "normalize_single_object_to_array": true,
        "flattening": {
          "hash_key_delimiter": ".",
          "array_index_format": "items[0].id",
          "non_scalar_cells_json_stringify": true
        },
        "headers": {
          "stable_first_seen_order": true
        },
        "base64": {
          "method": "Base64.strict_encode64",
          "input": "csv_string"
        }
      },
      "required_helper_methods": ["flatten_value", "normalize_cell"]
    }
  },
  "conformance_tests": [
    {
      "id": "prefill_basic_appends_param",
      "action": "build_prefilled_request_url",
      "input": {
        "request_url": "https://example.workato.com/requests/new",
        "prefilled_values_json": "{\"Employee name\":{\"value\":\"Ada\"},\"Start date\":{\"value\":\"2026-01-27\"}}",
        "default_editable": false,
        "on_url_limit": "error"
      },
      "assertions": [
        "output.prefilled_url contains '?prefilled_values='",
        "output.prefilled_values_json parses to object",
        "output.within_limit == true",
        "output.url_length == length(output.prefilled_url)"
      ]
    },
    {
      "id": "prefill_preserves_existing_query_and_fragment",
      "action": "build_prefilled_request_url",
      "input": {
        "request_url": "https://example.workato.com/requests/new?foo=1#section2",
        "prefilled_values_json": "{\"Field A\":{\"value\":123}}",
        "default_editable": false,
        "on_url_limit": "error"
      },
      "assertions": [
        "output.prefilled_url contains 'foo=1'",
        "output.prefilled_url contains '#section2'",
        "output.prefilled_url matches regex '^https?://[^#]+\\?[^#]*prefilled_values='",
        "the '#section2' fragment appears at end of URL"
      ]
    },
    {
      "id": "prefill_replaces_existing_prefilled_values",
      "action": "build_prefilled_request_url",
      "input": {
        "request_url": "https://example.workato.com/requests/new?prefilled_values=%7B%7D&x=1",
        "prefilled_values_json": "{\"Field A\":{\"value\":\"new\"}}",
        "default_editable": false,
        "on_url_limit": "error"
      },
      "assertions": [
        "output.prefilled_url contains 'x=1'",
        "output.prefilled_url contains exactly one occurrence of 'prefilled_values='"
      ]
    },
    {
      "id": "prefill_default_editable_injects_disabled_false",
      "action": "build_prefilled_request_url",
      "input": {
        "request_url": "https://example.workato.com/requests/new",
        "prefilled_values_json": "{\"Field A\":{\"value\":\"x\"},\"Field B\":{\"value\":\"y\",\"disabled\":true}}",
        "default_editable": true,
        "on_url_limit": "error"
      },
      "assertions": [
        "output.prefilled_values_json contains '\"Field A\"' entry with '\"disabled\":false'",
        "output.prefilled_values_json contains '\"Field B\"' entry with '\"disabled\":true'"
      ]
    },
    {
      "id": "prefill_url_limit_return_error_object",
      "action": "build_prefilled_request_url",
      "input": {
        "request_url": "https://example.workato.com/requests/new",
        "prefilled_values_json": "{\"Big\":{\"value\":\"<repeat 'a' enough times to exceed 8000>\"}}",
        "default_editable": false,
        "on_url_limit": "return_error_object"
      },
      "assertions": [
        "output.within_limit == false",
        "output.error is non-empty string"
      ]
    },
    {
      "id": "csv_flattens_hashes_and_arrays",
      "action": "json_to_csv",
      "input": {
        "json_string": "[{\"a\":1,\"b\":{\"c\":2},\"items\":[{\"id\":9},{\"id\":10}]}]"
      },
      "assertions": [
        "output.csv_string contains header 'b.c'",
        "output.csv_string contains header 'items[0].id'",
        "output.csv_string contains header 'items[1].id'",
        "output.csv_binary is base64 of output.csv_string"
      ]
    }
  ],
  "evaluation_rubric": {
    "blockers": [
      "Action build_prefilled_request_url missing",
      "Does not use 'prefilled_values' param name",
      "Does not URL-encode JSON",
      "Fails to handle URL fragment ordering",
      "Duplicates rather than replaces existing prefilled_values param",
      "Select field implemented without pick_lists/pick_list in DSL",
      "Helper methods defined inside execute instead of methods block"
    ],
    "warnings": [
      "Does not require 'value' key in each component entry",
      "Does not provide debug outputs",
      "Does not enforce URL length handling modes"
    ]
  }
}
