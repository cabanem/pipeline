# frozen_string_literal: true
require 'uri'
require 'json'
require 'date'

{
  title: 'URL builder and entry serialization',
  subtitle: 'Generic data shaping + parameterized URL payloads',
  description: 'Utility connector for serializing repeated entry forms, deserializing JSON arrays, and generating parameterized URLs with encoded JSON payloads.',
  help: -> { 'Generic utilities: serialize/deserialize entries with a field spec, and build parameterized URLs with encoded JSON payloads.' },
  
  connection: {
    fields: [],
    authorization: { type: 'none' }
  },
  test: ->(_connection) { { success: true, message: 'Connected successfully.' } },

  pick_lists: {
    num_opts_thru_10: -> { (1..10).map { |i| [i, i] } },
    num_opts_thru_31: -> { (1..31).map { |i| [i, i] } },
    output_format_options: -> {
      [
        ['Nested Array', 'nested_array'],
        ['Flat Fields (entry_1_field, entry_2_field...)', 'flat_fields'],
        ['Lists (field_list[])', 'lists'],
        ['Both Nested + Lists + Flat', 'both']
      ]
    },
    field_type_options: -> {
      [
        ['String', 'string'],
        ['Number', 'number'],
        ['Integer', 'integer'],
        ['Date', 'date'],
        ['Boolean', 'boolean']
      ]
    },
    coerce_options: -> {
      [
        ['None', 'none'],
        ['String', 'string'],
        ['Float', 'float'],
        ['Integer', 'int'],
        ['Date', 'date'],
        ['Boolean', 'bool']
      ]
    },
    payload_encoding_options: -> {
      [
        ['URL-encoded JSON (pretty)', 'urlencoded_json_pretty'],
        ['URL-encoded JSON (compact)', 'urlencoded_json_compact']
      ]
    },
    payload_style_options: -> {
      [
        ['Value wrapper (key => {value, disabled})', 'value_wrapper'],
        ['Raw object (key => value)', 'raw']
      ]
    },
    ui_mode_options: -> {
      [
        ['Simple (prefill only)', 'simple'],
        ['Advanced (static + mappings + indexed entries)', 'advanced']
      ]
    }
  },

  actions: {
    serialize_entries: {
      title: 'Serialize Entries to JSON',
      subtitle: 'Transform repeated entry forms into entries + lists + flat fields, with coercion + validation',
      display_priority: 10,

      help: -> {
        'Define a field spec once, then serialize repeated entries (entry_1, entry_2, ...) into a JSON string plus optional nested array, list outputs, and flat fields.'
      },

      config_fields: [
        { name: 'num_entries', type: :integer, label: 'Number of Entries', control_type: 'select', pick_list: 'num_opts_thru_31',
          optional: false, default: 5, hint: 'Maximum number of repeated entry forms to show (entry_1..entry_N).' },
        { name: 'entry_prefix', type: :string, label: 'Entry Prefix', control_type: 'text', optional: false,
          default: 'entry_', hint: 'Prefix for repeated entry field groups (e.g., entry_1, entry_2, ...).' },
        { name: 'field_specs', type: :array, of: :object, label: 'Field Specs', optional: false,
          hint: 'Defines the shape of each entry. Keys must be unique.', initially_expanded: true,
          properties: [
            { name: 'key', type: :string, label: 'Key', control_type: 'text', optional: false, sticky: true, hint: 'Example: date, units, note' },
            { name: 'label', type: :string, label: 'Label', control_type: 'text', optional: true, sticky: true },
            { name: 'type', type: :string, label: 'Type', control_type: 'select', pick_list: 'field_type_options', optional: false, default: 'string' },
            { name: 'coerce', type: :string, label: 'Coerce', control_type: 'select', pick_list: 'coerce_options', optional: false, default: 'none', hint: 'Coercion for incoming values' },
            { name: 'optional', type: :boolean, label: 'Optional?', control_type: 'checkbox', optional: true, default: true },
            { name: 'include_in_lists', type: :boolean, label: 'Include in lists?', control_type: 'checkbox', optional: true, default: true },
            { name: 'include_in_flat', type: :boolean, label: 'Include in flat fields?', control_type: 'checkbox', optional: true, default: true }
          ] },
        { name: 'required_keys', type: :string, label: 'Required Keys', control_type: 'text', optional: true,
          hint: 'Comma-separated keys required for an entry to be considered valid. Example: date, units' },
        { name: 'strict', type: :boolean, label: 'Strict mode?', control_type: 'checkbox', optional: true, default: false,
          hint: 'If true, any invalid entry causes an error. If false, invalid entries can be dropped and errors returned.'},
        { name: 'drop_invalid_entries', type: :boolean, label: 'Drop invalid entries?', control_type: 'checkbox',
          optional: true, default: true, hint: 'If strict=false, invalid entries can be skipped.' },
        { name: 'output_format', label: 'Output Format', type: :string, control_type: 'select',
          pick_list: 'output_format_options', optional: false, default: 'both' },
        { name: 'max_flat_entries', label: 'Maximum Flat Entries', type: :integer, control_type: 'select', pick_list: 'num_opts_thru_31',
          optional: true, default: 10, hint: 'How many entries to expose as individual flat fields.' },
        { name: 'sum_field_key', label: 'Sum Field Key', type: :string, control_type: 'text', optional: true,
          hint: "If provided, computes a sum across entries for this field (numeric). Example: units" },
        { name: 'sum_output_name', label: 'Sum Output Field Name', type: :string, control_type: 'text',
          optional: true, default: 'total_sum', hint: 'Output field name for the computed sum.' }
      ],
      input_fields: lambda do |_object_definitions, _connection, config_fields|
        num_entries  = (config_fields['num_entries'] || 1).to_i
        entry_prefix = (config_fields['entry_prefix'] || 'entry_').to_s

        specs = config_fields['field_specs']
        specs = call(:normalize_field_specs, specs)
        properties = call(:build_properties_from_specs, specs)

        (1..num_entries).map do |i|
          {
            name: "#{entry_prefix}#{i}",
            label: "Entry #{i}",
            type: :object,
            control_type: 'form',
            optional: true,
            sticky: true,
            properties: properties
          }
        end
      end,
      output_fields: lambda do |_object_definitions, _connection, config_fields|
        format    = (config_fields['output_format'] || 'both').to_s
        max_flat  = (config_fields['max_flat_entries'] || 10).to_i
        sum_key   = config_fields['sum_field_key']&.to_s
        sum_name  = (config_fields['sum_output_name'] || 'total_sum').to_s

        specs = config_fields['field_specs']
        specs = call(:normalize_field_specs, specs)
        props = call(:build_properties_from_specs, specs)

        fields = [
          { name: 'serialized_json', type: :string, label: 'Serialized JSON', control_type: 'text' },
          { name: 'errors', type: :array, of: :string, label: 'Errors', optional: true }
        ]

        if format == 'nested_array' || format == 'both'
          fields << {
            name: 'entries',
            label: 'Entries (Nested)',
            type: :array,
            of: :object,
            control_type: 'nested_fields',
            properties: props
          }
        end

        if format == 'lists' || format == 'both'
          specs.each do |s|
            next unless s['include_in_lists']
            key = s['key']
            wtype = call(:workato_type_for, s['type'])
            fields << { name: "#{key}_list", label: "#{key} (List)", type: :array, of: wtype }
          end
        end

        if format == 'flat_fields' || format == 'both'
          (1..max_flat).each do |i|
            specs.each do |s|
              next unless s['include_in_flat']
              key = s['key']
              wtype = call(:workato_type_for, s['type'])
              fields << { name: "entry_#{i}_#{key}", label: "Entry #{i} #{key}", type: wtype, optional: true }
            end
          end
        end

        if call(:present?, sum_key)
          fields << { name: sum_name, label: "Sum of #{sum_key}", type: :number, optional: true }
        end

        fields
      end,
      execute: lambda do |_connection, input|
        num_entries  = (input['num_entries'] || 0).to_i
        entry_prefix = (input['entry_prefix'] || 'entry_').to_s
        strict       = !!input['strict']
        drop_invalid = input.key?('drop_invalid_entries') ? !!input['drop_invalid_entries'] : true
        format       = (input['output_format'] || 'both').to_s
        max_flat     = (input['max_flat_entries'] || 10).to_i

        raw_specs = input['field_specs']
        unless raw_specs.is_a?(Array) && raw_specs.any?
          raise 'field_specs must be a non-empty array'
        end
        specs = call(:normalize_field_specs, raw_specs)
        required = call(:parse_csv_keys, input['required_keys'])
        sum_key  = input['sum_field_key']&.to_s
        sum_name = (input['sum_output_name'] || 'total_sum').to_s

        entries = []
        errors  = []

        (1..num_entries).each do |i|
          raw = input["#{entry_prefix}#{i}"] || {}
          next unless call(:present_entry_any_field?, raw, specs)

          begin
            converted = call(:coerce_entry, raw, specs)
            call(:validate_required_keys!, converted, required, i) if required.any?
            entries << converted
          rescue StandardError => e
            msg = "Entry #{i}: #{e.message}"
            if strict
              raise msg
            else
              errors << msg
              next if drop_invalid
              # if not dropping, still include the best-effort converted data
              begin
                entries << call(:coerce_entry, raw, specs)
              rescue
                # ignore
              end
            end
          end
        end

        output = {
          'serialized_json' => entries.to_json,
          'errors' => errors.empty? ? nil : errors
        }

        if call(:present?, sum_key)
          sum = entries.sum do |e|
            v = e[sum_key]
            v.is_a?(Numeric) ? v : 0.0
          end
          output[sum_name] = sum
        end

        if format == 'nested_array' || format == 'both'
          output['entries'] = entries
        end

        if format == 'lists' || format == 'both'
          specs.each do |s|
            next unless s['include_in_lists']
            key = s['key']
            output["#{key}_list"] = entries.map { |e| e[key] }.compact
          end
        end

        if format == 'flat_fields' || format == 'both'
          entries.each_with_index do |entry, idx|
            break if idx >= max_flat
            i = idx + 1
            specs.each do |s|
              next unless s['include_in_flat']
              key = s['key']
              val = entry[key]
              output["entry_#{i}_#{key}"] = val if !val.nil?
            end
          end
        end

        output
      end,
      sample_output: ->(_connection, input) {
        format    = (input['output_format'] || 'both').to_s
        max_flat  = (input['max_flat_entries'] || 10).to_i
        sum_key   = input['sum_field_key']&.to_s
        sum_name  = (input['sum_output_name'] || 'total_sum').to_s

        specs = call(:normalize_field_specs, input['field_specs'])

        entries = (1..2).map do |i|
          specs.each_with_object({}) do |s, h|
            h[s['key']] = call(:sample_value_for_spec, s, i)
          end
        end

        out = {
          'serialized_json' => entries.to_json,
          'errors' => nil
        }

        if format == 'nested_array' || format == 'both'
          out['entries'] = entries
        end

        if format == 'lists' || format == 'both'
          specs.each do |s|
            next unless s['include_in_lists']
            k = s['key']
            out["#{k}_list"] = entries.map { |e| e[k] }
          end
        end

        if format == 'flat_fields' || format == 'both'
          entries.each_with_index do |entry, idx|
            break if idx >= max_flat
            i = idx + 1
            specs.each do |s|
              next unless s['include_in_flat']
              k = s['key']
              out["entry_#{i}_#{k}"] = entry[k]
            end
          end
        end

        if call(:present?, sum_key)
          sum = entries.sum { |e| e[sum_key].is_a?(Numeric) ? e[sum_key] : 0.0 }
          out[sum_name] = sum
        end

        out
      }
    },
    deserialize_entries_json: {
      title: 'Deserialize JSON to Entries',
      subtitle: 'Parse JSON into entries + lists + flat fields, with coercion + validation',
      display_priority: 9,

      config_fields: [
        { name: 'entries_path', type: :string, label: 'Entries Path (optional)', control_type: 'text',
          optional: true, hint: 'Dot path to the array inside JSON. Example: data.entries' },
        { name: 'field_specs', type: :array, of: :object, label: 'Field Specs', optional: false, initially_expanded: true,
          properties: [
            { name: 'key', type: :string, label: 'Key', control_type: 'text', optional: false, sticky: true },
            { name: 'label', type: :string, label: 'Label', control_type: 'text', optional: true, sticky: true },
            { name: 'type', type: :string, label: 'Type', control_type: 'select', pick_list: 'field_type_options', optional: false, default: 'string' },
            { name: 'coerce', type: :string, label: 'Coerce', control_type: 'select', pick_list: 'coerce_options', optional: false, default: 'none' },
            { name: 'optional', type: :boolean, label: 'Optional?', control_type: 'checkbox', optional: true, default: true },
            { name: 'include_in_lists', type: :boolean, label: 'Include in lists?', control_type: 'checkbox', optional: true, default: true },
            { name: 'include_in_flat', type: :boolean, label: 'Include in flat fields?', control_type: 'checkbox', optional: true, default: true }
          ] },
        { name: 'required_keys', type: :string, label: 'Required Keys', control_type: 'text',
          optional: true, hint: 'Comma-separated keys required for each entry. Example: date, units' },
        { name: 'strict', type: :boolean, label: 'Strict mode?', control_type: 'checkbox', optional: true, default: true,
          hint: 'If true, any invalid entry causes an error. If false, invalid entries are skipped and errors returned.'},
        { name: 'output_format', label: 'Output Format', type: :string,  control_type: 'select',  
          pick_list: 'output_format_options', optional: false,  default: 'both'  },
        { name: 'max_flat_entries', label: 'Maximum Flat Entries', type: :integer, control_type: 'select',
          pick_list: 'num_opts_thru_31', optional: true, default: 10 }
      ],
      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [ { name: 'serialized_json', label: 'Serialized JSON', type: :string, control_type: 'text', optional: false,
            hint: 'JSON string representing an array of entries (or an object containing the array if entries_path is set).'
          } ]
      end,
      output_fields: lambda do |_object_definitions, _connection, config_fields|
        format   = (config_fields['output_format'] || 'both').to_s
        max_flat = (config_fields['max_flat_entries'] || 10).to_i

        specs = call(:normalize_field_specs, config_fields['field_specs'])
        props = call(:build_properties_from_specs, specs)

        fields = [
          { name: 'errors', type: :array, of: :string, label: 'Errors', optional: true }
        ]

        if format == 'nested_array' || format == 'both'
          fields << {
            name: 'entries',
            label: 'Entries (Nested)',
            type: :array,
            of: :object,
            control_type: 'nested_fields',
            properties: props
          }
        end

        if format == 'lists' || format == 'both'
          specs.each do |s|
            next unless s['include_in_lists']
            key = s['key']
            wtype = call(:workato_type_for, s['type'])
            fields << { name: "#{key}_list", label: "#{key} (List)", type: :array, of: wtype }
          end
        end

        if format == 'flat_fields' || format == 'both'
          (1..max_flat).each do |i|
            specs.each do |s|
              next unless s['include_in_flat']
              key = s['key']
              wtype = call(:workato_type_for, s['type'])
              fields << { name: "entry_#{i}_#{key}", label: "Entry #{i} #{key}", type: wtype, optional: true }
            end
          end
        end

        fields
      end,
      execute: lambda do |_connection, input|
        strict   = input.key?('strict') ? !!input['strict'] : true
        format   = (input['output_format'] || 'both').to_s
        max_flat = (input['max_flat_entries'] || 10).to_i
        path     = input['entries_path']&.to_s

        raw_specs = input['field_specs']
        unless raw_specs.is_a?(Array) && raw_specs.any?
          raise 'field_specs must be a non-empty array'
        end
        specs = call(:normalize_field_specs, raw_specs)
        required = call(:parse_csv_keys, input['required_keys'])

        raw = input['serialized_json']
        obj = call(:parse_json_any, raw)

        arr =
          if call(:present?, path)
            call(:dig_by_path, obj, path)
          else
            obj
          end

        unless arr.is_a?(Array)
          raise "Expected an Array of entries after parsing#{call(:present?, path) ? " (entries_path=#{path})" : ''}, got #{arr.class}"
        end

        entries = []
        errors  = []

        arr.each_with_index do |e, idx|
          unless e.is_a?(Hash)
            msg = "Entry #{idx + 1}: expected object, got #{e.class}"
            if strict then raise msg else errors << msg; next end
          end

          begin
            converted = call(:coerce_entry, e, specs)
            call(:validate_required_keys!, converted, required, idx + 1) if required.any?
            entries << converted
          rescue StandardError => ex
            msg = "Entry #{idx + 1}: #{ex.message}"
            if strict
              raise msg
            else
              errors << msg
            end
          end
        end

        output = { 'errors' => errors.empty? ? nil : errors }

        if format == 'nested_array' || format == 'both'
          output['entries'] = entries
        end

        if format == 'lists' || format == 'both'
          specs.each do |s|
            next unless s['include_in_lists']
            key = s['key']
            output["#{key}_list"] = entries.map { |en| en[key] }.compact
          end
        end

        if format == 'flat_fields' || format == 'both'
          entries.each_with_index do |entry, idx|
            break if idx >= max_flat
            i = idx + 1
            specs.each do |s|
              next unless s['include_in_flat']
              key = s['key']
              val = entry[key]
              output["entry_#{i}_#{key}"] = val if !val.nil?
            end
          end
        end

        output
      end,
      sample_output: ->(_connection, input) {
        format   = (input['output_format'] || 'both').to_s
        max_flat = (input['max_flat_entries'] || 10).to_i

        specs = call(:normalize_field_specs, input['field_specs'])

        entries = (1..2).map do |i|
          specs.each_with_object({}) do |s, h|
            h[s['key']] = call(:sample_value_for_spec, s, i)
          end
        end

        out = { 'errors' => nil }

        if format == 'nested_array' || format == 'both'
          out['entries'] = entries
        end

        if format == 'lists' || format == 'both'
          specs.each do |s|
            next unless s['include_in_lists']
            k = s['key']
            out["#{k}_list"] = entries.map { |e| e[k] }
          end
        end

        if format == 'flat_fields' || format == 'both'
          entries.each_with_index do |entry, idx|
            break if idx >= max_flat
            i = idx + 1
            specs.each do |s|
              next unless s['include_in_flat']
              k = s['key']
              out["entry_#{i}_#{k}"] = entry[k]
            end
          end
        end

        out
      }
    },                                                    
    build_parameterized_url: {
      title: 'Build Parameterized URL',
      subtitle: 'Encode a JSON payload into a URL query parameter (progressive disclosure + key/value editor)',
      display_priority: 6,

      config_fields: [
        { name: 'ui_mode', type: :string, label: 'mode', control_type: 'select', pick_list: 'ui_mode_options', optional: false, default: 'simple',
          sticky: true, hint: 'Simple shows only Base URL + Prefill Fields. Advanced reveals mappings, static fields, and indexed entry expansion.' },
        # Always-visible basics
        { name: 'param_name', type: :string, label: 'Query Parameter Name', control_type: 'text', optional: false, default: 'prefilled_values',
          sticky: true, hint: 'The query parameter that will receive the encoded payload.' },
        { name: 'payload_encoding', type: :string, label: 'Payload Encoding', control_type: 'select', pick_list: 'payload_encoding_options', optional: false,
          default: 'urlencoded_json_compact', sticky: true },
        { name: 'payload_style', type: :string, label: 'Payload Style', control_type: 'select', pick_list: 'payload_style_options', optional: false,
          default: 'raw', sticky: true, hint: 'raw => {key: value}. value_wrapper => {key: {value, disabled}}.' },
        # Applies to prefill_fields when using value_wrapper (key_value UI only collects key/value)
        { name: 'prefill_disabled_default', type: :boolean, label: 'Mark all prefill fields as disabled?', control_type: 'checkbox', optional: true,
          default: false, ngIf: 'input.payload_style == "value_wrapper"', hint: 'Applies disabled=true to all prefill_fields entries.' },
        # Advanced toggles
        { name: 'include_blank_values', type: :boolean, label: 'Include blank values?', control_type: 'checkbox', optional: true, default: false,
          ngIf: 'input.ui_mode == "advanced"', hint: 'If false, blank/empty values are omitted from the payload.' },
        # Static fields (advanced)
        { name: 'use_static_fields', type: :boolean, label: 'Use static fields?', control_type: 'checkbox', optional: true, default: false,
          ngIf: 'input.ui_mode == "advanced"', hint: 'Adds fixed key/value pairs into the payload.' },
        { name: 'static_fields', type: :array, of: :object, label: 'Static Fields', optional: true,
          ngIf: 'input.ui_mode == "advanced" && input.use_static_fields == true',
          properties: [
            { name: 'key', type: :string, label: 'Key', control_type: 'text', optional: true, sticky: true },
            { name: 'value', type: :string, label: 'Value', control_type: 'text', optional: true, sticky: true },
            { name: 'disabled', type: :boolean, label: 'Disabled?', control_type: 'checkbox', optional: true, default: false }
          ] },
        # Mappings (advanced)
        { name: 'use_mappings', type: :boolean, label: 'Use mappings from source fields?', control_type: 'checkbox', optional: true, default: false,
          ngIf: 'input.ui_mode == "advanced"', hint: 'Turn on when you want to map/rename/cast fields from a key/value list.' },
        { name: 'mappings', type: :array, of: :object, label: 'Mappings', optional: true,
          ngIf: 'input.ui_mode == "advanced" && input.use_mappings == true',
          properties: [
            { name: 'input_key', type: :string, label: 'Source key', control_type: 'text', optional: true, sticky: true },
            { name: 'output_key', type: :string, label: 'Payload key', control_type: 'text', optional: true, sticky: true },
            { name: 'cast', type: :string, label: 'Cast', control_type: 'select', pick_list: 'coerce_options', optional: true, default: 'none' },
            { name: 'required', type: :boolean, label: 'Required?', control_type: 'checkbox', optional: true, default: false },
            { name: 'disabled', type: :boolean, label: 'Disabled?', control_type: 'checkbox', optional: true, default: false }
          ] },
        # Indexed entries (advanced)
        { name: 'use_indexed_entries', type: :boolean, label: 'Use indexed entry expansion (date1, date2, ...)?', control_type: 'checkbox', optional: true, default: false,
          ngIf: 'input.ui_mode == "advanced"', hint: 'Expands entries[] into keys using templates like date{{i}}, units{{i}}.' },
        { name: 'indexed_entry_mappings', type: :array, of: :object, label: 'Indexed Entry Mappings', optional: true,
          ngIf: 'input.ui_mode == "advanced" && input.use_indexed_entries == true',
          properties: [
            { name: 'field_key', type: :string, label: 'Entry field key', control_type: 'text', optional: true, sticky: true, hint: 'Key within each entries[] object' },
            { name: 'key_template', type: :string, label: 'Payload key template', control_type: 'text', optional: true, sticky: true, hint: 'Use {{i}} e.g. date{{i}}' },
            { name: 'cast', type: :string, label: 'Cast', control_type: 'select', pick_list: 'coerce_options', optional: true, default: 'none' },
            { name: 'disabled', type: :boolean, label: 'Disabled?', control_type: 'checkbox', optional: true, default: false }
          ] },
        { name: 'index_start', type: :integer, label: 'Index Start', control_type: 'number', optional: true, default: 1,
          ngIf: 'input.ui_mode == "advanced" && input.use_indexed_entries == true', hint: 'Starting index for {{i}} substitution (usually 1).' }
      ],
      input_fields: lambda do |_object_definitions, _connection, config_fields|
        cfg = config_fields.is_a?(Hash) ? config_fields : {}

        mode        = (cfg['ui_mode'] || 'simple').to_s
        use_mappings = cfg['use_mappings'] == true
        use_indexed  = cfg['use_indexed_entries'] == true

        fields = [
          {
            name: 'base_url',
            type: 'string',
            label: 'Base URL',
            control_type: 'url',
            optional: false,
            sticky: true,
            hint: 'Target URL (query param appended).'
          },
          {
            name: 'prefill_fields',
            control_type: 'key_value',
            label: 'Prefill fields',
            type: 'array',
            of: 'object',
            optional: true,
            sticky: true,

            # Use schema-supported empty message knobs (and avoid relying on older keys)
            item_label: 'Field',
            add_field_label: 'Add field',
            empty_schema_message: 'Add keys and values to include in payload.',

            # Keep properties canonical for key_value
            properties: [
              { name: 'key', type: 'string', optional: true },
              { name: 'value', type: 'string', optional: true }
            ]
          }
        ]

        if mode == 'advanced' && use_mappings
          fields << {
            name: 'source_fields',
            type: 'array',
            of: 'object',
            label: 'Source Fields',
            optional: true,
            hint: 'Key/value inputs used by mappings.',
            properties: [
              { name: 'key', type: 'string', optional: false },
              { name: 'value', type: 'string', optional: true }
            ]
          }
        end

        if mode == 'advanced' && use_indexed
          fields << {
            name: 'entries',
            type: 'array',
            of: 'object',
            label: 'Entries',
            optional: true,
            list_mode: 'dynamic',
            list_mode_toggle: true,
            hint: 'Array of objects used by indexed_entry_mappings (usually mapped from an upstream datapill).',
            properties: [
              { name: 'placeholder', type: 'string', label: '(map datapill)', optional: true }
            ]
          }
        end

        fields
      end,
      output_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'url', type: :string, label: 'Parameterized URL' },
          { name: 'payload_json', type: :string, label: 'Payload JSON' },
          { name: 'payload_encoded', type: :string, label: 'Encoded Payload' }
        ]
      end,
      execute: lambda do |_connection, input|
        base_url      = input['base_url'].to_s
        param_name    = (input['param_name'] || 'prefilled_values').to_s
        encoding      = (input['payload_encoding'] || 'urlencoded_json_pretty').to_s

        # Align fallback with config default (default: 'raw')
        style         = (input['payload_style'] || 'raw').to_s

        index_start   = (input['index_start'] || 1).to_i
        include_blank = !!input['include_blank_values']
        prefill_disabled_default = !!input['prefill_disabled_default']

        raise 'Missing base_url' unless call(:present?, base_url)
        raise 'param_name must be non-empty' unless call(:present?, param_name)

        static_fields = input['static_fields'].is_a?(Array) ? input['static_fields'] : []
        mappings      = input['mappings'].is_a?(Array) ? input['mappings'] : []
        idx_maps      = input['indexed_entry_mappings'].is_a?(Array) ? input['indexed_entry_mappings'] : []
        source_fields = input['source_fields'].is_a?(Array) ? input['source_fields'] : []

        # key_value sometimes comes through as Hash (key=>value) depending on UI/runtime.
        prefill_raw = input['prefill_fields']
        prefill_fields =
          if prefill_raw.is_a?(Array)
            prefill_raw
          elsif prefill_raw.is_a?(Hash)
            prefill_raw.map { |k, v| { 'key' => k, 'value' => v } }
          else
            []
          end

        entries = input['entries'].is_a?(Array) ? input['entries'] : []

        payload = {}

        # 1) Static fields
        static_fields.each do |f|
          # Only use key/value to determine "blank row" (disabled defaults to false and will otherwise trip you)
          next if call(:row_blank?, f, 'key', :key, 'value', :value)

          key = f['key'].to_s
          raise 'Static field missing key' unless call(:present?, key)

          val = f['value']
          next if !include_blank && !call(:present?, val)

          payload[key] = call(:wrap_payload_value, val, f['disabled'], style)
        end

        # 2) Mappings (from source_fields)
        mappings.each do |m|
          # Only input_key/output_key decide blankness (cast defaults to 'none')
          next if call(:row_blank?, m, 'input_key', :input_key, 'output_key', :output_key)

          in_key  = m['input_key'].to_s
          out_key = m['output_key'].to_s
          raise 'Mapping missing input_key' unless call(:present?, in_key)
          raise 'Mapping missing output_key' unless call(:present?, out_key)

          val = call(:lookup_kv, source_fields, in_key)
          raise "Missing required mapped field: #{in_key}" if !!m['required'] && !call(:present?, val)
          next if !include_blank && !call(:present?, val)

          cast    = (m['cast'] || 'none').to_s
          coerced = call(:coerce_value, val, cast)
          payload[out_key] = call(:wrap_payload_value, coerced, m['disabled'], style)
        end

        # 3) Prefill fields (from key_value UI)
        prefill_fields.each do |f|
          next if call(:row_blank?, f, 'key', :key, 'value', :value)

          key = f['key'].to_s
          raise 'Prefill field missing key' unless call(:present?, key)

          val = f['value']
          next if !include_blank && !call(:present?, val)

          disabled = f.key?('disabled') ? f['disabled'] : prefill_disabled_default
          payload[key] = call(:wrap_payload_value, val, disabled, style)
        end

        # 4) Indexed entry expansion
        idx_maps_norm = idx_maps.each_with_object([]) do |r, acc|
          # Only field_key/key_template decide blankness (cast defaults to 'none')
          next if call(:row_blank?, r, 'field_key', :field_key, 'key_template', :key_template)

          field_key = r['field_key'].to_s
          tpl       = r['key_template'].to_s
          raise 'indexed_entry_mappings.field_key missing' unless call(:present?, field_key)
          raise 'indexed_entry_mappings.key_template missing' unless call(:present?, tpl)

          acc << r
        end

        if idx_maps_norm.any? && entries.any?
          entries.each_with_index do |entry, idx|
            next unless entry.is_a?(Hash)

            i = index_start + idx
            idx_maps_norm.each do |r|
              field_key = r['field_key'].to_s
              tpl       = r['key_template'].to_s

              k = tpl.gsub('{{i}}', i.to_s)
              next unless call(:present?, k)

              raw_val = entry[field_key] || entry[field_key.to_sym]
              next if !include_blank && !call(:present?, raw_val)

              cast    = (r['cast'] || 'none').to_s
              coerced = call(:coerce_value, raw_val, cast)
              payload[k] = call(:wrap_payload_value, coerced, r['disabled'], style)
            end
          end
        end

        json      = call(:payload_json, payload, style, encoding)
        encoded   = URI.encode_www_form_component(json)
        delimiter = base_url.include?('?') ? '&' : '?'
        url       = "#{base_url}#{delimiter}#{param_name}=#{encoded}"

        { url: url, payload_json: json, payload_encoded: encoded }
      end,
      sample_output: ->(_connection, input) {
        param_name = (input['param_name'] || 'prefilled_values').to_s
        encoding   = (input['payload_encoding'] || 'urlencoded_json_pretty').to_s
        style      = (input['payload_style'] || 'value_wrapper').to_s

        payload = {
          'project_id'  => call(:wrap_payload_value, 'P-123', true, style),
          'date'        => call(:wrap_payload_value, '2026-01-26', true, style),
          'supplier_id' => call(:wrap_payload_value, 'S-456', true, style)
        }

        json = call(:payload_json, payload, style, encoding)
        encoded = URI.encode_www_form_component(json)
        base_url = 'https://example.com/form'
        url = "#{base_url}?#{param_name}=#{encoded}"

        { url: url, payload_json: json, payload_encoded: encoded }
      }
    }
  },

  methods: {
    present?: ->(value) {
      !value.nil? && (value.respond_to?(:empty?) ? !value.empty? : true)
    },
    row_blank?: ->(h, *keys) {
      return true unless h.is_a?(Hash)
      keys.all? do |k|
        v = h[k] || h[k.to_s] || h[k.to_sym]
        !call(:present?, v)
      end
    },
    default_field_specs: -> {
      [
        {
          'key' => 'value',
          'label' => 'Value',
          'type' => 'string',
          'coerce' => 'none',
          'optional' => true,
          'include_in_lists' => true,
          'include_in_flat' => true
        }
      ]
    },
    parse_csv_keys: ->(csv) {
      return [] unless call(:present?, csv)
      csv.to_s.split(',').map(&:strip).reject(&:empty?)
    },
    normalize_field_specs: ->(specs) {
      arr = specs.is_a?(Array) ? specs : []
      # IMPORTANT: do not raise here. Workato calls input_fields/output_fields while config is incomplete.
      # Provide a placeholder spec so the UI can render.
      return call(:default_field_specs) if arr.empty?

      normalized = arr.map do |s|
        raise 'field_spec must be an object' unless s.is_a?(Hash)
        key = s['key']&.to_s
        raise 'field_spec.key is required' unless call(:present?, key)

        {
          'key' => key,
          'label' => (s['label'] || key).to_s,
          'type' => (s['type'] || 'string').to_s,
          'coerce' => (s['coerce'] || 'none').to_s,
          'optional' => s.key?('optional') ? !!s['optional'] : true,
          'include_in_lists' => s.key?('include_in_lists') ? !!s['include_in_lists'] : true,
          'include_in_flat' => s.key?('include_in_flat') ? !!s['include_in_flat'] : true
        }
      end

      keys = normalized.map { |x| x['key'] }
      dupes = keys.select { |k| keys.count(k) > 1 }.uniq
      raise "Duplicate field_spec keys: #{dupes.join(', ')}" if dupes.any?

      normalized
    },
    workato_type_for: ->(t) {
      case t.to_s
      when 'string' then :string
      when 'number' then :number
      when 'integer' then :integer
      when 'date' then :date
      when 'boolean' then :boolean
      else :string
      end
    },
    build_properties_from_specs: ->(specs) {
      specs.map do |s|
        {
          name: s['key'],
          label: s['label'],
          type: call(:workato_type_for, s['type']),
          control_type: (s['type'].to_s == 'boolean' ? 'checkbox' : 'text'),
          optional: !!s['optional'],
          sticky: true
        }
      end
    },
    present_entry_any_field?: ->(raw, specs) {
      return false unless raw.is_a?(Hash)
      specs.any? do |s|
        v = raw[s['key']] || raw[s['key'].to_sym]
        call(:present?, v)
      end
    },
    coerce_value: ->(value, cast) {
      return nil if value.nil?
      c = cast.to_s

      case c
      when 'none'
        value
      when 'string'
        value.to_s
      when 'float'
        begin
          Float(value)
        rescue
          raise "Invalid float: #{value.inspect}"
        end
      when 'int'
        begin
          Integer(value)
        rescue
          begin
            Integer(Float(value))
          rescue
            raise "Invalid integer: #{value.inspect}"
          end
        end
      when 'date'
        begin
          value.is_a?(Date) ? value : Date.parse(value.to_s)
        rescue
          raise "Invalid date: #{value.inspect}"
        end
      when 'bool'
        v = value
        return v if v == true || v == false
        s = value.to_s.strip.downcase
        if %w[true t yes y 1].include?(s)
          true
        elsif %w[false f no n 0].include?(s)
          false
        else
          raise "Invalid boolean: #{value.inspect}"
        end
      else
        value
      end
    },
    coerce_entry: ->(raw, specs) {
      raise 'Entry must be an object' unless raw.is_a?(Hash)

      out = {}
      specs.each do |s|
        key = s['key']
        val = raw[key] || raw[key.to_sym]
        next if val.nil?

        cast = s['coerce'].to_s
        coerced = call(:coerce_value, val, cast)
        out[key] = coerced
      end
      out
    },
    validate_required_keys!: ->(entry, required_keys, idx) {
      missing = required_keys.reject { |k| call(:present?, entry[k]) }
      raise "Missing required keys: #{missing.join(', ')}" if missing.any?
      true
    },
    parse_json_any: ->(raw) {
      return raw if raw.is_a?(Hash) || raw.is_a?(Array)

      unless raw.is_a?(String)
        raise "Expected JSON string/array/object, got #{raw.class}"
      end

      begin
        JSON.parse(raw)
      rescue JSON::ParserError => e
        raise "Invalid JSON: #{e.message}"
      end
    },
    dig_by_path: ->(obj, path) {
      parts = path.to_s.split('.').map(&:strip).reject(&:empty?)
      cur = obj
      parts.each do |p|
        if cur.is_a?(Array) && p.match?(/^\d+$/)
          cur = cur[p.to_i]
        elsif cur.is_a?(Hash)
          cur = cur[p] || cur[p.to_sym]
        else
          return nil
        end
      end
      cur
    },
    lookup_kv: ->(kvs, key) {
      return nil unless kvs.is_a?(Array) && call(:present?, key)
      found = kvs.find { |h| h.is_a?(Hash) && (h['key'].to_s == key.to_s) }
      found ? found['value'] : nil
    },
    wrap_payload_value: ->(value, disabled, style) {
      if style.to_s == 'raw'
        value
      else
        out = { 'value' => value }
        out['disabled'] = true if disabled
        out
      end
    },
    payload_json: ->(payload, style, encoding) {
      # payload is already correctly shaped for either style. Only control pretty vs compact.
      if encoding.to_s == 'urlencoded_json_compact'
        JSON.generate(payload)
      else
        JSON.pretty_generate(payload)
      end
    },
    sample_value_for_spec: ->(spec, idx = 1) {
      key  = spec['key'].to_s
      type = spec['type'].to_s

      case type
      when 'string'
        "#{key}_#{idx}"
      when 'number'
        (idx * 1.25)
      when 'integer'
        idx
      when 'date'
        Date.today - (idx - 1)
      when 'boolean'
        idx.odd?
      else
        "#{key}_#{idx}"
      end
    }

  }
}
