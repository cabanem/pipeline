# frozen_string_literal: true
require 'json'
require 'uri'
require 'csv'
require 'base64'

{
  title: 'Workflow Apps Utility',
  subtitle: 'Prefilled URL Builder + JSON→CSV',
  description: 'Generate Workflow Apps "New request" URLs with prefilled_values (pill-friendly inputs) and convert JSON to CSV.',
  help: -> {
    'Build Workflow Apps prefilled request URLs using friendly nested inputs (supports data pills), and optionally convert JSON to CSV.'
  },

  connection: {
    fields: [],
    authorization: { type: 'none' }
  },

  test: ->(_connection) { { success: true } },

  object_definitions: {
    # ---- Entry row for prefill list (this is the "easy" UX) ----
    prefill_entry_fields: {
      fields: ->(_connection, _config_fields) {
        [
          {
            name: 'title',
            type: :string,
            label: 'Component Title',
            control_type: 'text',
            optional: false,
            sticky: true,
            hint: 'Must match the Workflow Apps component Title exactly (builder-visible).'
          },
          {
            name: 'value_mode',
            type: :string,
            label: 'Value mode',
            control_type: 'select',
            pick_list: 'prefill_value_mode_options',
            optional: true,
            default: 'auto',
            sticky: true,
            hint: 'Auto uses JSON if provided, else table dropdown if record_id provided, else Value (text/pill).'
          },

          # Primitive/pill-friendly value
          {
            name: 'value',
            type: :string,
            label: 'Value (text / pill)',
            control_type: 'text',
            optional: true,
            sticky: true,
            hint: 'Map a pill here for most fields (text, numbers, dates as strings, etc.).'
          },

          # Table-backed dropdown support
          {
            name: 'table_record_id',
            type: :string,
            label: 'Table record_id',
            control_type: 'text',
            optional: true,
            sticky: true,
            hint: 'For table dropdowns: record_id of the selected row.'
          },
          {
            name: 'table_value',
            type: :string,
            label: 'Table display value',
            control_type: 'text',
            optional: true,
            sticky: true,
            hint: 'For table dropdowns: display value (optional but recommended).'
          },

          # Advanced JSON value
          {
            name: 'value_json',
            type: :string,
            label: 'Value (raw JSON)',
            control_type: 'text-area',
            optional: true,
            sticky: true,
            hint: 'Advanced: provide raw JSON for complex values (object/array/string/number/bool/null).'
          },

          {
            name: 'disabled',
            type: :boolean,
            label: 'Disabled?',
            control_type: 'checkbox',
            optional: true,
            sticky: true,
            hint: 'If omitted, Default editable? controls whether disabled:false is injected.'
          }
        ]
      }
    },

    # ---- Action input schema ----
    wfa_url_input: {
      fields: ->(object_definitions, _connection, _config_fields) {
        [
          {
            name: 'request_url',
            type: :string,
            label: 'Request URL',
            control_type: 'text',
            optional: false,
            sticky: true,
            hint: 'Workflow Apps "New request" URL (must start with http:// or https://).'
          },

          {
            name: 'input_mode',
            type: :string,
            label: 'Input mode',
            control_type: 'select',
            pick_list: 'prefill_input_mode_options',
            optional: true,
            default: 'entries',
            sticky: true,
            hint: 'Recommended: Entries (nested list). Advanced: Object or JSON.'
          },

          {
            control_type: 'nested_fields',
            type: 'array',
            of: 'object',
            name: 'prefill_entries',
            label: 'Prefill Entries (recommended)',
            optional: true,
            sticky: true,
            hint: 'Add rows: Title + Value (pill-friendly). Use Value mode for table dropdown or JSON.',
            properties: object_definitions['prefill_entry_fields']
          },

          # Advanced option: strict object (supports pills, but requires strict shape)
          {
            name: 'prefilled_values',
            type: :object,
            label: 'Prefilled values (object) — advanced',
            optional: true,
            sticky: true,
            hint: 'Strict shape: { "Title": { "value": <...>, "disabled": <bool optional> } }.'
          },

          # Advanced option: JSON string (static / copy-paste)
          {
            name: 'prefilled_values_json',
            type: :string,
            label: 'Prefilled values JSON — advanced',
            control_type: 'text-area',
            optional: true,
            sticky: true,
            hint: 'Raw JSON object: {"Title":{"value":"x","disabled":false}}. No pills here.'
          },

          {
            name: 'default_editable',
            type: :boolean,
            label: 'Default editable?',
            optional: true,
            default: false,
            sticky: true,
            hint: 'If true, injects disabled:false when missing. If false, omit disabled (read-only default).'
          },

          {
            name: 'value_coercion',
            type: :string,
            label: 'Coerce value types',
            control_type: 'select',
            pick_list: 'value_coercion_options',
            optional: true,
            default: 'preserve',
            sticky: true,
            hint: 'If your pills arrive as strings, you can infer int/float/bool/null from common patterns.'
          },

          {
            name: 'on_url_limit',
            type: :string,
            label: 'On URL limit exceeded',
            control_type: :select,
            pick_list: 'on_url_limit_options',
            optional: true,
            default: 'error',
            sticky: true,
            hint: 'If final URL exceeds ~8000 chars, either raise an error or return within_limit=false.'
          }
        ]
      }
    }
  },

  pick_lists: {
    on_url_limit_options: ->(_connection) {
      [
        ['Raise Error (Fail Job)', 'error'],
        ['Return Error Object', 'return_error_object']
      ]
    },

    prefill_input_mode_options: ->(_connection) {
      [
        ['Entries (recommended)', 'entries'],
        ['Object (advanced)', 'object'],
        ['JSON (advanced)', 'json']
      ]
    },

    prefill_value_mode_options: ->(_connection) {
      [
        ['Auto', 'auto'],
        ['Value (text / pill)', 'value'],
        ['Table dropdown (record_id + value)', 'table_dropdown'],
        ['Raw JSON', 'json']
      ]
    },

    value_coercion_options: ->(_connection) {
      [
        ['Preserve (no coercion)', 'preserve'],
        ['Infer common types (int/float/bool/null)', 'infer_common'],
        ['Force string', 'force_string']
      ]
    }
  },

  actions: {
    build_prefilled_request_url: {
      title: 'Build prefilled request URL',
      subtitle: 'Workflow Apps prefilled_values URL',
      description: 'Builds a "New request" URL with prefilled_values (URL-encoded JSON).',

      input_fields: ->(object_definitions) {
        object_definitions['wfa_url_input']
      },

      execute: lambda do |_connection, input|
        url_string = input['request_url'].to_s.strip
        if url_string.empty? || !url_string.match?(/^https?:\/\//i)
          error('Request URL must be a valid http/https URL.')
        end

        mode = input['input_mode'].to_s.strip
        mode = 'entries' if mode.empty?

        default_editable = call('strict_bool', input['default_editable'])
        coercion = input['value_coercion'].to_s
        coercion = 'preserve' if coercion.strip.empty?

        payload =
          case mode
          when 'entries'
            call('payload_from_entries', input['prefill_entries'])
          when 'object'
            call('payload_from_object', input['prefilled_values'])
          when 'json'
            call('payload_from_json_string', input['prefilled_values_json'])
          else
            error("Invalid input_mode: #{mode.inspect}. Use entries/object/json.")
          end

        normalized = call('normalize_wfa_payload', payload, default_editable, coercion)

        json_string = JSON.generate(normalized)
        encoded_values = URI.encode_www_form_component(json_string)

        final_url = call('upsert_query_param', url_string, 'prefilled_values', json_string)

        limit = 8000
        url_len = final_url.length
        within_limit = url_len <= limit

        if !within_limit
          msg = "Generated URL length (#{url_len}) exceeds the safe limit of #{limit} characters."
          if input['on_url_limit'].to_s == 'error'
            error(msg)
          end
          return {
            'prefilled_url' => final_url, # keep for debugging
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
          { name: 'prefilled_url', type: :string },
          { name: 'prefilled_values_json', type: :string },
          { name: 'prefilled_values_encoded', type: :string },
          { name: 'url_length', type: :integer },
          { name: 'within_limit', type: :boolean },
          { name: 'error', type: :string }
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
            control_type: 'text-area',
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
          call('flatten_value', r)
        end

        headers = []
        flat_rows.each { |row| headers |= row.keys }

        csv_string = CSV.generate do |csv|
          csv << headers
          flat_rows.each do |row|
            csv << headers.map { |h| call('normalize_cell', row[h]) }
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
          { name: 'csv_string', type: :string },
          { name: 'csv_binary', type: :string },
          {
            name: 'csv_file',
            type: :object,
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
    strict_bool: ->(v) {
      v == true || v.to_s.strip.downcase == 'true'
    },

    present?: ->(v) {
      !v.nil? && (v.respond_to?(:empty?) ? !v.empty? : true)
    },

    payload_from_entries: ->(entries) {
      entries = Array(entries)
      error("Provide at least one Prefill Entry (Title + Value).") if entries.empty?

      out = {}

      entries.each_with_index do |e, idx|
        unless e.is_a?(Hash)
          error("Prefill entries[#{idx}] must be an object.")
        end

        title = (e['title'] || e[:title]).to_s.strip
        error("Prefill entries[#{idx}] is missing required 'title'.") if title.empty?

        value = call('resolve_entry_value', e, idx)

        entry = { 'value' => value }

        # Preserve disabled if explicitly present (including false)
        has_disabled = e.key?('disabled') || e.key?(:disabled)
        if has_disabled
          entry['disabled'] = e.key?('disabled') ? e['disabled'] : e[:disabled]
        end

        out[title] = entry
      end

      out
    },

    payload_from_object: ->(obj) {
      unless obj.is_a?(Hash) && !obj.empty?
        error("Input mode is 'object' but 'prefilled_values' is missing/empty.")
      end
      obj
    },

    payload_from_json_string: ->(s) {
      str = s.to_s
      error("Input mode is 'json' but 'prefilled_values_json' is blank.") if str.strip.empty?

      begin
        parsed = JSON.parse(str)
      rescue JSON::ParserError => e
        error("Invalid JSON in 'prefilled_values_json': #{e.message}")
      end

      unless parsed.is_a?(Hash)
        error("prefilled_values_json must parse to an Object (Hash), got #{parsed.class}.")
      end

      parsed
    },

    resolve_entry_value: ->(entry, idx) {
      mode = (entry['value_mode'] || entry[:value_mode] || 'auto').to_s

      value_text = entry.key?('value') ? entry['value'] : entry[:value]
      value_json = entry.key?('value_json') ? entry['value_json'] : entry[:value_json]
      rec_id = entry.key?('table_record_id') ? entry['table_record_id'] : entry[:table_record_id]
      rec_val = entry.key?('table_value') ? entry['table_value'] : entry[:table_value]

      case mode
      when 'json'
        str = value_json.to_s
        error("Prefill entries[#{idx}] value_mode=json requires value_json.") if str.strip.empty?
        begin
          JSON.parse(str)
        rescue JSON::ParserError => e
          error("Prefill entries[#{idx}] value_json is not valid JSON: #{e.message}")
        end

      when 'table_dropdown'
        rid = rec_id.to_s.strip
        error("Prefill entries[#{idx}] value_mode=table_dropdown requires table_record_id.") if rid.empty?
        {
          'record_id' => rid,
          'value' => rec_val.to_s
        }

      when 'value'
        # allow empty string as a legitimate value; require that the key exists at least
        unless entry.key?('value') || entry.key?(:value)
          error("Prefill entries[#{idx}] value_mode=value requires the 'value' field.")
        end
        value_text

      when 'auto'
        # JSON wins if present, then table dropdown, else value
        if call('present?', value_json)
          begin
            JSON.parse(value_json.to_s)
          rescue JSON::ParserError => e
            error("Prefill entries[#{idx}] value_json is not valid JSON: #{e.message}")
          end
        elsif call('present?', rec_id)
          rid = rec_id.to_s.strip
          error("Prefill entries[#{idx}] table_record_id is blank.") if rid.empty?
          { 'record_id' => rid, 'value' => rec_val.to_s }
        else
          unless entry.key?('value') || entry.key?(:value)
            error("Prefill entries[#{idx}] must provide value, value_json, or table_record_id.")
          end
          value_text
        end

      else
        error("Prefill entries[#{idx}] has invalid value_mode: #{mode.inspect}")
      end
    },

    normalize_wfa_payload: ->(payload, default_editable, coercion) {
      out = {}

      payload.each do |title, entry|
        unless entry.is_a?(Hash)
          error("Component '#{title}' value must be an Object containing 'value', got #{entry.class}.")
        end

        has_value = entry.key?('value') || entry.key?(:value)
        error("Component '#{title}' is missing required 'value'.") unless has_value

        value = entry.key?('value') ? entry['value'] : entry[:value]
        value = call('apply_value_coercion', value, coercion)

        normalized = { 'value' => value }

        has_disabled = entry.key?('disabled') || entry.key?(:disabled)
        if has_disabled
          normalized['disabled'] = entry.key?('disabled') ? entry['disabled'] : entry[:disabled] # preserves false
        elsif default_editable
          normalized['disabled'] = false
        end

        out[title.to_s] = normalized
      end

      out
    },

    apply_value_coercion: ->(v, coercion) {
      mode = coercion.to_s
      return v if mode == 'preserve'

      if mode == 'force_string'
        return v.nil? ? nil : v.to_s
      end

      # infer_common: only coerce strings; leave objects/hashes/arrays alone
      return v unless v.is_a?(String)

      s = v.strip
      return '' if v == '' # preserve empty string exactly

      # null
      return nil if s.casecmp('null').zero?

      # bool
      return true  if s.casecmp('true').zero?
      return false if s.casecmp('false').zero?

      # int
      return s.to_i if s.match?(/\A-?\d+\z/)

      # float
      return s.to_f if s.match?(/\A-?\d+\.\d+\z/)

      s
    },

    # Upsert query param safely (preserves existing params and fragments; replaces existing key).
    # Pass raw (unencoded) value; this method encodes consistently.
    upsert_query_param: ->(url_string, key, value_string) {
      uri = URI.parse(url_string)

      pairs = URI.decode_www_form(uri.query || '')
      pairs.reject! { |(k, _v)| k == key }
      pairs << [key, value_string.to_s]

      uri.query = pairs.map { |k, v|
        "#{URI.encode_www_form_component(k)}=#{URI.encode_www_form_component(v.to_s)}"
      }.join('&')

      uri.to_s
    },

    flatten_value: ->(value, parent_key = nil, out = {}) {
      case value
      when Hash
        value.each do |k, v|
          key = parent_key ? "#{parent_key}.#{k}" : k.to_s
          call('flatten_value', v, key, out)
        end
      when Array
        value.each_with_index do |v, i|
          key = parent_key ? "#{parent_key}[#{i}]" : "[#{i}]"
          call('flatten_value', v, key, out)
        end
      else
        out[parent_key.to_s] = value
      end
      out
    },

    normalize_cell: ->(v) {
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
