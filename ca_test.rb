{
  title: 'URL Builder',
  
  description: 'Build parameterized URLs with prefilled form values for Workflow Apps',

  # ═══════════════════════════════════════════════════════════════════════════
  # HELPER METHODS
  # ═══════════════════════════════════════════════════════════════════════════
  methods: {
    # Safe presence check - handles nil, empty string, empty array/hash
    present?: lambda do |value|
      return false if value.nil?
      return false if value.is_a?(String) && value.strip.empty?
      return false if value.respond_to?(:empty?) && value.empty?
      true
    end,

    # Normalize field specs from various input formats to consistent array of hashes
    # UI-safe: returns empty array if input is nil/invalid (prevents build-time crashes)
    normalize_field_specs: lambda do |specs|
      return [] unless call(:present?, specs)
      
      normalized = if specs.is_a?(Array)
        specs.map do |spec|
          if spec.is_a?(Hash)
            spec.transform_keys(&:to_s)
          elsif spec.is_a?(String)
            { 'name' => spec }
          else
            nil
          end
        end.compact
      elsif specs.is_a?(Hash)
        specs.map { |k, v| { 'name' => k.to_s, 'value' => v } }
      else
        []
      end
      
      normalized.select { |s| call(:present?, s['name']) }
    end,

    # Coerce a value to the specified type
    coerce_value: lambda do |value, type|
      return nil if value.nil?
      
      case type.to_s.downcase
      when 'string', 'text'
        value.to_s
      when 'integer', 'int'
        value.to_s.strip.empty? ? nil : value.to_i
      when 'float', 'decimal', 'number'
        value.to_s.strip.empty? ? nil : value.to_f
      when 'boolean', 'bool'
        val_str = value.to_s.downcase.strip
        %w[true 1 yes on].include?(val_str)
      when 'date'
        value.to_s.strip.empty? ? nil : value.to_date&.strftime('%Y-%m-%d') rescue value.to_s
      when 'datetime'
        value.to_s.strip.empty? ? nil : value.to_time&.iso8601 rescue value.to_s
      when 'json'
        value.is_a?(String) ? (JSON.parse(value) rescue value) : value
      else
        value
      end
    end,

    # Coerce an entry hash applying type conversion and validation
    coerce_entry: lambda do |entry, mappings_config|
      return entry unless call(:present?, mappings_config)
      
      mappings = call(:normalize_field_specs, mappings_config)
      return entry if mappings.empty?
      
      coerced = entry.dup
      
      mappings.each do |mapping|
        field_name = mapping['name'].to_s
        next unless coerced.key?(field_name)
        
        if mapping['type'].present?
          coerced[field_name] = call(:coerce_value, coerced[field_name], mapping['type'])
        end
      end
      
      coerced
    end,

    # Convert payload hash to JSON string
    payload_json: lambda do |payload|
      return '{}' unless call(:present?, payload)
      payload.to_json
    end,

    # Wrap a value according to payload_style
    # raw: value as-is
    # value_wrapper: { value: x, disabled: bool }
    wrap_payload_value: lambda do |value, style, disabled|
      case style.to_s.downcase
      when 'value_wrapper'
        wrapper = { 'value' => value }
        wrapper['disabled'] = true if disabled
        wrapper
      else # 'raw' or default
        value
      end
    end,

    # Lookup a value from key_value array by key
    lookup_kv: lambda do |kv_array, key|
      return nil unless call(:present?, kv_array) && call(:present?, key)
      
      found = kv_array.find { |item| item['key'].to_s == key.to_s }
      found&.dig('value')
    end,

    # Safely dig into a hash/object by dot-separated path
    dig_by_path: lambda do |obj, path|
      return nil unless call(:present?, obj) && call(:present?, path)
      
      keys = path.to_s.split('.')
      current = obj
      
      keys.each do |key|
        return nil unless current.respond_to?(:dig) || current.respond_to?(:[])
        current = current.is_a?(Array) ? current[key.to_i] : current[key] || current[key.to_sym]
        return nil if current.nil?
      end
      
      current
    end,

    # Build the final payload by layering: static -> mappings -> prefill -> indexed
    build_layered_payload: lambda do |input|
      payload = {}
      
      style = input['payload_style'] || 'raw'
      include_blanks = input['include_blank_values'] == 'true' || input['include_blank_values'] == true
      prefill_disabled = input['prefill_disabled_default'] == 'true' || input['prefill_disabled_default'] == true
      
      # Layer 1: Static fields
      if input['use_static_fields'] == 'true' && call(:present?, input['static_fields'])
        static = call(:normalize_field_specs, input['static_fields'])
        static.each do |field|
          name = field['name']
          value = field['value']
          disabled = field['disabled'] == 'true' || field['disabled'] == true
          
          next if !include_blanks && !call(:present?, value)
          payload[name] = call(:wrap_payload_value, value, style, disabled)
        end
      end
      
      # Layer 2: Mappings from source_fields
      if input['use_mappings'] == 'true' && call(:present?, input['mappings'])
        mappings = call(:normalize_field_specs, input['mappings'])
        source = input['source_fields'] || {}
        
        mappings.each do |mapping|
          source_path = mapping['source'] || mapping['name']
          target_name = mapping['target'] || mapping['name']
          
          raw_value = call(:dig_by_path, source, source_path)
          
          # Apply type coercion if specified
          value = if call(:present?, mapping['type'])
            call(:coerce_value, raw_value, mapping['type'])
          else
            raw_value
          end
          
          # Validate required fields
          if mapping['required'] == 'true' || mapping['required'] == true
            unless call(:present?, value)
              error("Required field '#{target_name}' is missing or empty")
            end
          end
          
          next if !include_blanks && !call(:present?, value)
          
          disabled = mapping['disabled'] == 'true' || mapping['disabled'] == true
          payload[target_name] = call(:wrap_payload_value, value, style, disabled)
        end
      end
      
      # Layer 3: Prefill fields (key_value input) - these override previous layers
      if call(:present?, input['prefill_fields'])
        prefill = input['prefill_fields']
        prefill = [prefill] unless prefill.is_a?(Array)
        
        prefill.each do |item|
          next unless item.is_a?(Hash)
          key = item['key'].to_s
          value = item['value']
          
          next if key.empty?
          next if !include_blanks && !call(:present?, value)
          
          payload[key] = call(:wrap_payload_value, value, style, prefill_disabled)
        end
      end
      
      # Layer 4: Indexed entry expansion
      if input['use_indexed_entries'] == 'true' && call(:present?, input['entries'])
        entries = input['entries']
        entries = [entries] unless entries.is_a?(Array)
        
        key_template = input['key_template'] || 'field_{{i}}'
        index_start = (input['index_start'] || 0).to_i
        
        entries.each_with_index do |entry, idx|
          next unless entry.is_a?(Hash)
          
          current_index = index_start + idx
          
          entry.each do |field_key, field_value|
            # Replace {{i}} with current index in key template
            expanded_key = key_template.gsub('{{i}}', current_index.to_s)
                                       .gsub('{{field}}', field_key.to_s)
            
            next if !include_blanks && !call(:present?, field_value)
            payload[expanded_key] = call(:wrap_payload_value, field_value, style, false)
          end
        end
      end
      
      payload
    end,

    # URL-encode a string (RFC 3986 compliant)
    url_encode: lambda do |str|
      return '' if str.nil?
      ERB::Util.url_encode(str.to_s)
    end,

    # Build the final URL with encoded payload parameter
    build_url: lambda do |base_url, param_name, payload_json, encoding|
      return { error: 'base_url is required' } unless call(:present?, base_url)
      return { error: 'param_name is required' } unless call(:present?, param_name)
      
      encoded_payload = case encoding.to_s.downcase
      when 'base64'
        Base64.strict_encode64(payload_json)
      when 'base64url'
        Base64.urlsafe_encode64(payload_json, padding: false)
      else # 'url' encoding (default)
        call(:url_encode, payload_json)
      end
      
      separator = base_url.include?('?') ? '&' : '?'
      url = "#{base_url}#{separator}#{call(:url_encode, param_name)}=#{encoded_payload}"
      
      {
        url: url,
        payload_encoded: encoded_payload
      }
    end
  },

  # ═══════════════════════════════════════════════════════════════════════════
  # CONNECTION (No auth required)
  # ═══════════════════════════════════════════════════════════════════════════
  connection: {
    fields: [],
    
    authorization: {
      type: 'custom_auth',
      
      apply: lambda do |_connection|
        # No authentication required
      end
    },
    
    base_uri: lambda do |_connection|
      # No base URI - this connector builds URLs, doesn't call them
      ''
    end
  },

  test: lambda do |_connection|
    # Always succeeds - no actual connection to test
    { success: true, message: 'URL Builder connector ready' }
  end,

  # ═══════════════════════════════════════════════════════════════════════════
  # PICK LISTS
  # ═══════════════════════════════════════════════════════════════════════════
  pick_lists: {
    ui_mode_options: lambda do |_connection|
      [
        ['Simple', 'simple'],
        ['Advanced', 'advanced']
      ]
    end,

    payload_encoding_options: lambda do |_connection|
      [
        ['URL Encoding (default)', 'url'],
        ['Base64', 'base64'],
        ['Base64 URL-safe', 'base64url']
      ]
    end,

    payload_style_options: lambda do |_connection|
      [
        ['Raw values (key: value)', 'raw'],
        ['Value wrapper (key: {value: x, disabled: bool})', 'value_wrapper']
      ]
    end,

    field_type_options: lambda do |_connection|
      [
        ['String (default)', 'string'],
        ['Integer', 'integer'],
        ['Float/Decimal', 'float'],
        ['Boolean', 'boolean'],
        ['Date (YYYY-MM-DD)', 'date'],
        ['DateTime (ISO 8601)', 'datetime'],
        ['JSON (parse string)', 'json']
      ]
    end,

    coerce_options: lambda do |_connection|
      [
        ['No coercion', ''],
        ['To String', 'string'],
        ['To Integer', 'integer'],
        ['To Float', 'float'],
        ['To Boolean', 'boolean']
      ]
    end,

    boolean_options: lambda do |_connection|
      [
        ['No', 'false'],
        ['Yes', 'true']
      ]
    end
  },

  # ═══════════════════════════════════════════════════════════════════════════
  # OBJECT DEFINITIONS
  # ═══════════════════════════════════════════════════════════════════════════
  object_definitions: {
    # Static field definition for advanced mode
    static_field_spec: {
      fields: lambda do |_connection, _config|
        [
          {
            name: 'name',
            label: 'Field Name',
            type: 'string',
            optional: false,
            hint: 'The key name in the payload'
          },
          {
            name: 'value',
            label: 'Value',
            type: 'string',
            optional: true,
            hint: 'Static value for this field'
          },
          {
            name: 'disabled',
            label: 'Disabled',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Mark field as disabled (value_wrapper style only)'
          }
        ]
      end
    },

    # Mapping definition for advanced mode
    mapping_spec: {
      fields: lambda do |_connection, _config|
        [
          {
            name: 'name',
            label: 'Field Name',
            type: 'string',
            optional: false,
            hint: 'Identifier for this mapping'
          },
          {
            name: 'source',
            label: 'Source Path',
            type: 'string',
            optional: true,
            hint: 'Dot-notation path in source_fields (defaults to field name)'
          },
          {
            name: 'target',
            label: 'Target Key',
            type: 'string',
            optional: true,
            hint: 'Key name in output payload (defaults to field name)'
          },
          {
            name: 'type',
            label: 'Type Coercion',
            type: 'string',
            control_type: 'select',
            pick_list: 'field_type_options',
            optional: true,
            hint: 'Convert value to this type'
          },
          {
            name: 'required',
            label: 'Required',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Fail if value is missing or empty'
          },
          {
            name: 'disabled',
            label: 'Disabled',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Mark field as disabled (value_wrapper style only)'
          }
        ]
      end
    },

    # Indexed entry definition
    indexed_entry_spec: {
      fields: lambda do |_connection, _config|
        [
          {
            name: 'value',
            label: 'Entry Value',
            type: 'string',
            optional: true,
            hint: 'Value for this indexed entry'
          }
        ]
      end
    }
  },

  # ═══════════════════════════════════════════════════════════════════════════
  # ACTIONS
  # ═══════════════════════════════════════════════════════════════════════════
  actions: {
    build_parameterized_url: {
      title: 'Build Parameterized URL',
      subtitle: 'Create URL with prefilled form parameters',
      description: "Build a <span class='provider'>Workflow Apps</span> request-page URL with " \
                   "prefilled form values encoded as a query parameter",
      
      help: {
        body: <<~HELP
          Creates a URL with form field values encoded as a JSON payload in a query parameter.
          
          **Simple Mode**: Use the key/value editor to define prefill fields directly.
          
          **Advanced Mode**: Layer multiple data sources:
          1. Static fields (hardcoded values)
          2. Mappings (transform source data with type coercion)
          3. Prefill fields (manual overrides)
          4. Indexed entries (generate numbered field keys)
          
          **Payload Styles**:
          - *Raw*: `{ fieldName: "value" }`
          - *Value Wrapper*: `{ fieldName: { value: "value", disabled: false } }`
        HELP
      },

      # ─────────────────────────────────────────────────────────────────────────
      # CONFIG FIELDS (control UI mode)
      # ─────────────────────────────────────────────────────────────────────────
      config_fields: [
        {
          name: 'ui_mode',
          label: 'Configuration Mode',
          type: 'string',
          control_type: 'select',
          pick_list: 'ui_mode_options',
          optional: true,
          default: 'simple',
          hint: 'Simple: key/value editor only. Advanced: full layering control.'
        },
        {
          name: 'payload_style',
          label: 'Payload Style',
          type: 'string',
          control_type: 'select',
          pick_list: 'payload_style_options',
          optional: true,
          default: 'raw',
          hint: 'How values are structured in the JSON payload'
        }
      ],

      # ─────────────────────────────────────────────────────────────────────────
      # INPUT FIELDS
      # ─────────────────────────────────────────────────────────────────────────
      input_fields: lambda do |object_definitions, _connection, config_fields|
        # CRITICAL: config_fields may be nil or incomplete at recipe build time.
        # All config-dependent logic MUST have safe defaults.
        config = config_fields || {}
        ui_mode = config['ui_mode'] || 'simple'
        payload_style = config['payload_style'] || 'raw'
        is_advanced = ui_mode == 'advanced'
        is_value_wrapper = payload_style == 'value_wrapper'

        fields = []

        # ═══════════════════════════════════════════════════════════════════════
        # ALWAYS VISIBLE: Core URL fields
        # ═══════════════════════════════════════════════════════════════════════
        fields << {
          name: 'base_url',
          label: 'Base URL',
          type: 'string',
          control_type: 'url',
          optional: false,
          hint: 'The Workflow Apps request page URL (e.g., https://app.workato.com/request_pages/...)'
        }

        fields << {
          name: 'param_name',
          label: 'Parameter Name',
          type: 'string',
          optional: true,
          default: 'prefill',
          hint: "Query parameter name for the payload (default: 'prefill')"
        }

        fields << {
          name: 'payload_encoding',
          label: 'Payload Encoding',
          type: 'string',
          control_type: 'select',
          pick_list: 'payload_encoding_options',
          optional: true,
          default: 'url',
          hint: 'How to encode the JSON payload in the URL'
        }

        # ═══════════════════════════════════════════════════════════════════════
        # ALWAYS VISIBLE: Primary prefill fields (key_value control)
        # ═══════════════════════════════════════════════════════════════════════
        fields << {
          name: 'prefill_fields',
          label: 'Prefill Fields',
          type: 'array',
          of: 'object',
          control_type: 'key_value',
          optional: true,
          hint: 'Key/value pairs to prefill in the form. These override all other layers.',
          properties: [
            { name: 'key', type: 'string' },
            { name: 'value', type: 'string' }
          ]
        }

        # ═══════════════════════════════════════════════════════════════════════
        # VALUE_WRAPPER MODE: Disabled default toggle
        # ═══════════════════════════════════════════════════════════════════════
        if is_value_wrapper
          fields << {
            name: 'prefill_disabled_default',
            label: 'Prefill Fields Disabled by Default',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'When using value_wrapper style, set disabled=true for all prefill fields'
          }
        end

        # ═══════════════════════════════════════════════════════════════════════
        # ADVANCED MODE: Additional options
        # ═══════════════════════════════════════════════════════════════════════
        if is_advanced
          fields << {
            name: 'include_blank_values',
            label: 'Include Blank Values',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Include fields with empty/nil values in the payload'
          }

          # ─────────────────────────────────────────────────────────────────────
          # STATIC FIELDS (Layer 1)
          # ─────────────────────────────────────────────────────────────────────
          fields << {
            name: 'use_static_fields',
            label: 'Use Static Fields',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Enable static field definitions (Layer 1 - lowest priority)'
          }

          fields << {
            name: 'static_fields',
            label: 'Static Fields',
            type: 'array',
            of: 'object',
            list_mode: 'static',
            optional: true,
            ngIf: 'input.use_static_fields == "true"',
            hint: 'Hardcoded field values (can be overridden by mappings and prefill)',
            properties: object_definitions['static_field_spec']&.dig('fields') || []
          }

          # ─────────────────────────────────────────────────────────────────────
          # MAPPINGS FROM SOURCE (Layer 2)
          # ─────────────────────────────────────────────────────────────────────
          fields << {
            name: 'use_mappings',
            label: 'Use Field Mappings',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Enable field mappings from source data (Layer 2)'
          }

          fields << {
            name: 'source_fields',
            label: 'Source Data',
            type: 'object',
            optional: true,
            ngIf: 'input.use_mappings == "true"',
            hint: 'Source object containing data to map into the payload',
            properties: []
          }

          fields << {
            name: 'mappings',
            label: 'Field Mappings',
            type: 'array',
            of: 'object',
            list_mode: 'static',
            optional: true,
            ngIf: 'input.use_mappings == "true"',
            hint: 'Define how source fields map to payload keys with optional type coercion',
            properties: object_definitions['mapping_spec']&.dig('fields') || []
          }

          # ─────────────────────────────────────────────────────────────────────
          # INDEXED ENTRIES (Layer 4)
          # ─────────────────────────────────────────────────────────────────────
          fields << {
            name: 'use_indexed_entries',
            label: 'Use Indexed Entries',
            type: 'string',
            control_type: 'select',
            pick_list: 'boolean_options',
            optional: true,
            default: 'false',
            hint: 'Enable indexed entry expansion (Layer 4 - generates numbered keys)'
          }

          fields << {
            name: 'key_template',
            label: 'Key Template',
            type: 'string',
            optional: true,
            default: 'field_{{i}}',
            ngIf: 'input.use_indexed_entries == "true"',
            hint: 'Template for generated keys. Use {{i}} for index, {{field}} for field name.'
          }

          fields << {
            name: 'index_start',
            label: 'Starting Index',
            type: 'integer',
            optional: true,
            default: 0,
            ngIf: 'input.use_indexed_entries == "true"',
            hint: 'Starting number for {{i}} placeholder (default: 0)'
          }

          fields << {
            name: 'entries',
            label: 'Entries',
            type: 'array',
            of: 'object',
            list_mode: 'dynamic',
            optional: true,
            ngIf: 'input.use_indexed_entries == "true"',
            hint: 'Array of entries to expand with indexed keys',
            properties: [
              {
                name: 'value',
                label: 'Value',
                type: 'string',
                optional: true
              }
            ]
          }
        end

        fields
      end,

      # ─────────────────────────────────────────────────────────────────────────
      # EXECUTE
      # ─────────────────────────────────────────────────────────────────────────
      execute: lambda do |_connection, input|
        # Validate required fields
        base_url = input['base_url']&.strip
        unless call(:present?, base_url)
          error('Base URL is required')
        end

        param_name = input['param_name']&.strip
        param_name = 'prefill' unless call(:present?, param_name)

        # Validate URL format
        unless base_url.start_with?('http://') || base_url.start_with?('https://')
          error("Base URL must start with http:// or https:// (got: #{base_url})")
        end

        # Build the layered payload
        payload = call(:build_layered_payload, input)

        # Convert to JSON
        json_payload = call(:payload_json, payload)

        # Build the final URL
        encoding = input['payload_encoding'] || 'url'
        result = call(:build_url, base_url, param_name, json_payload, encoding)

        if result[:error]
          error(result[:error])
        end

        # Return complete output
        {
          url: result[:url],
          payload_json: json_payload,
          payload_encoded: result[:payload_encoded],
          field_count: payload.keys.size,
          base_url: base_url,
          param_name: param_name,
          encoding_used: encoding
        }
      end,

      # ─────────────────────────────────────────────────────────────────────────
      # OUTPUT FIELDS
      # ─────────────────────────────────────────────────────────────────────────
      output_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          {
            name: 'url',
            label: 'Complete URL',
            type: 'string',
            hint: 'The full URL with encoded payload parameter'
          },
          {
            name: 'payload_json',
            label: 'Payload (JSON)',
            type: 'string',
            hint: 'The JSON payload before encoding'
          },
          {
            name: 'payload_encoded',
            label: 'Payload (Encoded)',
            type: 'string',
            hint: 'The encoded payload as it appears in the URL'
          },
          {
            name: 'field_count',
            label: 'Field Count',
            type: 'integer',
            hint: 'Number of fields in the payload'
          },
          {
            name: 'base_url',
            label: 'Base URL',
            type: 'string',
            hint: 'The original base URL provided'
          },
          {
            name: 'param_name',
            label: 'Parameter Name',
            type: 'string',
            hint: 'The query parameter name used'
          },
          {
            name: 'encoding_used',
            label: 'Encoding Used',
            type: 'string',
            hint: 'The encoding method applied to the payload'
          }
        ]
      end,

      # ─────────────────────────────────────────────────────────────────────────
      # SAMPLE OUTPUT (critical for datapill rendering)
      # ─────────────────────────────────────────────────────────────────────────
      sample_output: lambda do |_connection, _input|
        {
          url: 'https://app.workato.com/request_pages/abc123?prefill=%7B%22firstName%22%3A%22John%22%2C%22lastName%22%3A%22Doe%22%7D',
          payload_json: '{"firstName":"John","lastName":"Doe"}',
          payload_encoded: '%7B%22firstName%22%3A%22John%22%2C%22lastName%22%3A%22Doe%22%7D',
          field_count: 2,
          base_url: 'https://app.workato.com/request_pages/abc123',
          param_name: 'prefill',
          encoding_used: 'url'
        }
      end
    }
  }
}
