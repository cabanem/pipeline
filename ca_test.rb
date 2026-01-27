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

          # OPTION A: easiest for data pills (recommended)
          {
            name: 'prefilled_value_entries',
            type: :array,
            of: :object,
            optional: true,
            label: 'Prefill entries (recommended)',
            hint: 'Use this to build prefilled values with data pills. Each row: Title + Value + optional Disabled.',
            properties: [
              {
                name: 'title',
                type: :string,
                optional: false,
                label: 'Component Title',
                hint: 'Must match the Workflow Apps component Title exactly (builder-visible).'
              },
              {
                name: 'value',
                type: :object,
                optional: false,
                label: 'Value',
                hint: 'Any primitive or object. For table dropdowns, pass { "record_id": "...", "value": "..." }.'
              },
              {
                name: 'disabled',
                type: :boolean,
                optional: true,
                label: 'Disabled?',
                hint: 'If omitted, behavior is controlled by Default editable?.'
              }
            ]
          },

          # OPTION B: advanced users can pass the strict object shape directly (also supports pills)
          {
            name: 'prefilled_values',
            type: :object,
            optional: true,
            label: 'Prefilled values (object)',
            hint: 'Strict shape: { "Title": { "value": <...>, "disabled": <bool optional> } }. Supports data pills.'
          },

          # OPTION C: static JSON (no pills)
          {
            name: 'prefilled_values_json',
            type: :string,
            control_type: :text_area,
            optional: true,
            label: 'Prefilled values JSON (static)',
            hint: 'Raw JSON object. Keys = Titles. Values must be objects containing "value". Example: {"Employee Name":{"value":"Ada"}}'
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

        # Resolve payload source (entries > object > json string)
        payload = resolve_prefilled_values_input(
          input['prefilled_value_entries'],
          input['prefilled_values'],
          input['prefilled_values_json']
        )

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

          return {
            'prefilled_url' => final_url,              # keep for debugging
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

        flat_rows = rows.map do |r|
          r = { '_value' => r } unless r.is_a?(Hash)
          flatten_value(r)
        end

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
    strict_bool: lambda do |v|
      v == true || v.to_s.strip.downcase == 'true'
    end,

    # Selects a payload source in priority order:
    # 1) prefilled_value_entries (best for pills)
    # 2) prefilled_values object (strict shape, supports pills)
    # 3) prefilled_values_json (static)
    resolve_prefilled_values_input: lambda do |entries, obj, json|
      if entries.is_a?(Array) && !entries.empty?
        out = {}

        entries.each_with_index do |e, idx|
          unless e.is_a?(Hash)
            error("Prefill entries[#{idx}] must be an object with title/value/disabled.")
          end

          title = (e['title'] || e[:title]).to_s.strip
          if title.empty?
            error("Prefill entries[#{idx}] is missing required 'title' (component Title).")
          end

          has_value = e.key?('value') || e.key?(:value)
          unless has_value
            error("Prefill entries[#{idx}] (title '#{title}') is missing required 'value'.")
          end
          value = e.key?('value') ? e['value'] : e[:value]

          entry = { 'value' => value }

          has_disabled = e.key?('disabled') || e.key?(:disabled)
          if has_disabled
            disabled_val = e.key?('disabled') ? e['disabled'] : e[:disabled] # preserves false
            entry['disabled'] = disabled_val
          end

          out[title] = entry
        end

        return out
      end

      if obj.is_a?(Hash) && !obj.empty?
        return obj
      end

      s = json.to_s
      if !s.strip.empty?
        return parse_prefilled_values_json(s)
      end

      error("Provide one of: Prefill entries, Prefilled values (object), or Prefilled values JSON.")
    end,

    parse_prefilled_values_json: lambda do |raw_json|
      s = raw_json.to_s
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
          disabled_val = entry.key?('disabled') ? entry['disabled'] : entry[:disabled] # preserves false
          normalized['disabled'] = disabled_val
        elsif default_editable
          normalized['disabled'] = false
        end

        out[title.to_s] = normalized
      end

      out
    end,

    # Upsert query param safely (preserves existing params + fragments; replaces existing key).
    # Pass raw (unencoded) value; this encodes consistently.
    upsert_query_param: lambda do |url_string, key, value_string|
      uri = URI.parse(url_string)

      pairs = URI.decode_www_form(uri.query || '')
      pairs.reject! { |(k, _v)| k == key }
      pairs << [key, value_string.to_s]

      uri.query = pairs.map { |k, v|
        "#{URI.encode_www_form_component(k)}=#{URI.encode_www_form_component(v.to_s)}"
      }.join('&')

      uri.to_s
    end,

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
# Recipe-builder usage tip (data pills):
# - Use "Prefill entries (recommended)" and add rows like:
#   title: "Employee name"   value: (Employee name pill)
#   title: "Start date"      value: (date pill)
#   title: "Manager"         value: { "record_id": "...", "value": "Grace Hopper" }  (table dropdown)
# ---------------------------------------------------------------------------
