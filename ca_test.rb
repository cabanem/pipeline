# frozen_string_literal: true
require 'json'
require 'uri'
require 'csv'
require 'base64'

{
  title: 'Workflow Apps Utility',
  subtitle: 'Prefilled URL Builder + JSON→CSV',
  description: 'Generate Workflow Apps "New request" URLs with prefilled_values and convert JSON to CSV.',
  help: -> {
    'Utilities for Workflow Apps. Build a "New request" URL with the prefilled_values parameter (URL-encoded JSON keyed by component Title), and optionally convert JSON to CSV.'
  },

  connection: {
    fields: [],
    authorization: { type: 'none' }
  },

  test: ->(_connection) { { success: true } },

  pick_lists: {
    on_url_limit_options: lambda do |_connection|
      [
        ['Raise Error (Fail Job)', 'error'],
        ['Return Error Object', 'return_error_object']
      ]
    end
  },

  actions: {
    build_prefilled_request_url: {
      title: 'Build prefilled request URL',
      subtitle: 'Workflow Apps prefilled_values URL',
      description: 'Builds a "New request" URL with the prefilled_values parameter (URL-encoded JSON).',
      help: -> {
        'Keys must match Workflow Apps component Title (builder-visible). Prefilled fields are read-only by default unless disabled:false is set.'
      },

      input_fields: lambda do
        [
          {
            name: 'request_url',
            type: :string,
            optional: false,
            label: 'Request URL',
            hint: 'The Workflow Apps "New request" page URL (must start with http:// or https://).'
          },
          {
            name: 'prefilled_values_json',
            type: :string,
            control_type: :text_area,
            optional: false,
            label: 'Prefilled values JSON',
            hint: 'Raw JSON object. Keys = Component Titles. Values must be objects containing "value". Example: {"Employee Name":{"value":"Ada"}}'
          },
          {
            name: 'default_editable',
            type: :boolean,
            optional: true,
            default: false,
            label: 'Default editable?',
            hint: 'If true, injects "disabled": false when missing. If false, leaves disabled absent (read-only by default).'
          },
          {
            name: 'on_url_limit',
            type: :string,
            control_type: :select,
            pick_list: 'on_url_limit_options',
            optional: true,
            default: 'error',
            label: 'On URL limit exceeded',
            hint: 'If the final URL exceeds ~8000 chars, either raise an error or return within_limit=false with error populated.'
          }
        ]
      end,

      execute: lambda do |_connection, input|
        url_string = input['request_url'].to_s.strip
        if url_string.empty? || !url_string.match?(/^https?:\/\//i)
          error('Request URL must be a valid http/https URL.')
        end

        raw_json = input['prefilled_values_json']
        payload = parse_prefilled_values_json(raw_json)

        default_editable = strict_bool(input['default_editable'])
        normalized_payload = normalize_wfa_payload(payload, default_editable)

        json_string = JSON.generate(normalized_payload)
        encoded_values = URI.encode_www_form_component(json_string)

        final_url = upsert_query_param(url_string, 'prefilled_values', json_string)

        limit = 8000
        url_len = final_url.length
        within_limit = (url_len <= limit)

        if !within_limit
          msg = "Generated URL length (#{url_len}) exceeds the safe limit of #{limit} characters."
          if input['on_url_limit'].to_s == 'error'
            error(msg)
          end

          # return_error_object mode: keep the URL for debugging (do not nil it out)
          return {
            'prefilled_url' => final_url,
            'prefilled_values_json' => json_string,
            'prefilled_values_encoded' => encoded_values,
            'url_length' => url_len,
            'within_limit' => false,
            'error' => msg
          }
        end

        {
          'prefilled_url' => final_url,
          'prefilled_values_json' => json_string,
          'prefilled_values_encoded' => encoded_values,
          'url_length' => url_len,
          'within_limit' => true,
          'error' => nil
        }
      end,

      output_fields: lambda do
        [
          { name: 'prefilled_url', type: :string, label: 'Full Prefilled URL' },
          { name: 'prefilled_values_json', type: :string, label: 'Normalized JSON (Raw)' },
          { name: 'prefilled_values_encoded', type: :string, label: 'Encoded JSON Parameter' },
          { name: 'url_length', type: :integer, label: 'URL length' },
          { name: 'within_limit', type: :boolean, label: 'Within URL limit?' },
          { name: 'error', type: :string, label: 'Error (if any)' }
        ]
      end
    },

    json_to_csv: {
      title: 'Convert JSON to CSV',
      subtitle: 'Flatten & format',
      description: 'Parses JSON (object or array), flattens nested structures, and returns CSV string + base64 + file object.',

      input_fields: lambda do
        [
          {
            name: 'json_string',
            type: :string,
            control_type: :text_area,
            optional: false,
            label: 'JSON string',
            hint: 'Raw JSON array or object.'
          }
        ]
      end,

      execute: lambda do |_connection, input|
        raw = input['json_string'].to_s
        if raw.strip.empty?
          return {
            'csv_string' => '',
            'csv_binary' => '',
            'csv_file' => { 'content' => '', 'content_type' => 'text/csv', 'original_filename' => 'export.csv' }
          }
        end

        begin
          data = JSON.parse(raw)
        rescue JSON::ParserError => e
          error("Invalid JSON in 'json_string': #{e.message}")
        end

        rows =
          if data.is_a?(Array)
            data
          elsif data.is_a?(Hash)
            [data]
          else
            error("JSON must parse to an Object or Array, got #{data.class}.")
          end

        # Flatten each row; non-hash rows become {_value: <row>}
        flat_rows = rows.map do |r|
          r = { '_value' => r } unless r.is_a?(Hash)
          flatten_value(r)
        end

        # Stable header order (first-seen)
        headers = []
        flat_rows.each { |row| headers |= row.keys }

        csv_string = CSV.generate do |csv|
          csv << headers
          flat_rows.each do |row|
            csv << headers.map { |h| normalize_cell(row[h]) }
          end
        end

        b64 = Base64.strict_encode64(csv_string)

        {
          'csv_string' => csv_string,
          'csv_binary' => b64,
          'csv_file' => {
            'content' => b64,
            'content_type' => 'text/csv',
            'original_filename' => 'export.csv'
          }
        }
      end,

      output_fields: lambda do
        [
          { name: 'csv_string', type: :string, label: 'CSV (text)' },
          { name: 'csv_binary', type: :string, label: 'CSV (base64)' },
          {
            name: 'csv_file',
            type: :object,
            label: 'CSV file',
            properties: [
              { name: 'content', type: :string },
              { name: 'content_type', type: :string },
              { name: 'original_filename', type: :string }
            ]
          }
        ]
      end
    }
  },

  methods: {
    # Strict-ish boolean: only true boolean or string "true" (case-insensitive) counts as true.
    strict_bool: lambda do |v|
      v == true || v.to_s.strip.downcase == 'true'
    end,

    # Parse and validate prefilled_values_json: must be valid JSON object/hash.
    parse_prefilled_values_json: lambda do |raw_json|
      s = raw_json.to_s
      if s.strip.empty?
        error("Missing required 'prefilled_values_json'.")
      end

      begin
        parsed = JSON.parse(s)
      rescue JSON::ParserError => e
        error("Invalid JSON in 'prefilled_values_json': #{e.message}")
      end

      unless parsed.is_a?(Hash)
        error("Prefilled values JSON must be an Object (Hash), got #{parsed.class}.")
      end

      parsed
    end,

    # Normalize WFA payload:
    # - Keys are treated as component Titles (stringified)
    # - Each entry must be an object containing 'value' (presence required, value may be nil/false/0)
    # - Preserve 'disabled' when present (including false)
    # - If missing and default_editable true -> set disabled:false; otherwise omit disabled
    normalize_wfa_payload: lambda do |payload, default_editable|
      out = {}

      payload.each do |title, entry|
        unless entry.is_a?(Hash)
          error("Component '#{title}' value must be an Object containing 'value', got #{entry.class}.")
        end

        has_value = entry.key?('value') || entry.key?(:value)
        unless has_value
          error("Component '#{title}' object is missing the required 'value' property.")
        end
        value = entry.key?('value') ? entry['value'] : entry[:value]

        normalized = { 'value' => value }

        has_disabled = entry.key?('disabled') || entry.key?(:disabled)
        if has_disabled
          disabled_val = entry.key?('disabled') ? entry['disabled'] : entry[:disabled]
          normalized['disabled'] = disabled_val
        elsif default_editable
          normalized['disabled'] = false
        end

        out[title.to_s] = normalized
      end

      out
    end,

    # Upsert a query param safely, preserving existing query params and fragments.
    # IMPORTANT: pass raw (unencoded) value_string; this method encodes all query params.
    # - Replaces existing key occurrences
    # - Ensures query appears before fragment
    upsert_query_param: lambda do |url_string, key, value_string|
      uri = URI.parse(url_string)

      pairs = URI.decode_www_form(uri.query || '')
      pairs.reject! { |(k, _v)| k == key }
      pairs << [key, value_string.to_s]

      new_query = pairs.map do |k, v|
        "#{URI.encode_www_form_component(k)}=#{URI.encode_www_form_component(v.to_s)}"
      end.join('&')

      uri.query = new_query
      uri.to_s
    end,

    # Flatten hashes + arrays into a flat hash with dotted keys and indexed array keys.
    flatten_value: lambda do |value, parent_key = nil, out = {}|
      case value
      when Hash
        value.each do |k, v|
          key = parent_key ? "#{parent_key}.#{k}" : k.to_s
          flatten_value(v, key, out)
        end
      when Array
        value.each_with_index do |v, i|
          key = parent_key ? "#{parent_key}[#{i}]" : "[#{i}]"
          flatten_value(v, key, out)
        end
      else
        out[parent_key.to_s] = value
      end
      out
    end,

    # Normalize cell for CSV output.
    normalize_cell: lambda do |v|
      case v
      when NilClass
        nil
      when Hash, Array
        JSON.generate(v)
      else
        v
      end
    end
  }
}

# ---------------------------------------------------------------------------
# Examples (for testing)
# ---------------------------------------------------------------------------
# 1) Prefill payload example (manual dropdown, editable by default):
# {
#   "Employee name": { "value": "Ada Lovelace" },
#   "Department":    { "value": "Engineering", "disabled": false }
# }
#
# 2) Prefill payload example (table dropdown):
# {
#   "Manager": { "value": { "record_id": "abcd-1234", "value": "Grace Hopper" } }
# }
#
# 3) JSON→CSV input example:
# [{"a":1,"b":{"c":2},"items":[{"id":9},{"id":10}]}]
