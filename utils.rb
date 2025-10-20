# frozen_string_literal: true

require 'time'
require 'securerandom'
require 'digest'
require 'json'

{
  title: 'RAG Utilities',
  version: '0.5.0',
  description: 'Utility adapter for retrieval augmented generation systems',

  # --------- CONNECTION ---------------------------------------------------
  connection: { 
    # Workato's runtime and DSL require the fields and authorization block,
    # even if they're empty and there isn't a connection. 
    fields: [],
    authorization: { type: 'none' }
  },

  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |_connection|
    # Workato's runtime requires a connection test with a primary output "true"
    { success: true, message: 'Connected successfully.' }
  end,

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {
    prep_result: {
      fields: lambda do |object_definitions = {}|
        [
          { name: 'doc_id' },
          { name: 'file_path' },
          { name: 'checksum' },
          { name: 'chunk_count', type: 'integer' },
          { name: 'max_chunk_chars', type: 'integer' },
          { name: 'overlap_chars', type: 'integer' },
          { name: 'created_at' },
          { name: 'duration_ms', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object',
            properties: (object_definitions['chunk'] || []) },
          { name: 'trace_id', optional: true },
          { name: 'notes', optional: true }
        ]
      end
    },
    prep_batch: {
      fields: lambda do |object_definitions = {}|
        [
          { name: 'results', type: 'array', of: 'object',
            properties: (object_definitions['prep_result'] || []) },
          { name: 'count', type: 'integer' },
          { name: 'batch_trace_id', optional: true }
        ]
      end
    },
    table_row: {
      fields: lambda do |_connection|
        [
          { name: 'id' },
          { name: 'doc_id' },
          { name: 'file_path' },
          { name: 'checksum' },
          { name: 'tokens', type: 'integer' },
          { name: 'span_start', type: 'integer' },
          { name: 'span_end',   type: 'integer' },
          { name: 'created_at' },
          { name: 'text' } # optional at runtime, but declare so the pill exists
        ]
      end
    },
    envelope_fields: {
      fields: lambda do |_connection|
        [
          { name: 'error', type: 'object', optional: true, properties: [
              { name: 'code', type: 'integer' },
              { name: 'status' },
              { name: 'reason' },
              { name: 'domain' },
              { name: 'location' },
              { name: 'message' },
              { name: 'service', hint: 'utility' },
              { name: 'operation' }
            ] },
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
              { name: 'http_status', type: 'integer' },
              { name: 'message' },
              { name: 'duration_ms', type: 'integer' },
              { name: 'correlation_id' }
            ] },
          { name: 'upstream', type: 'object', optional: true, properties: [
              { name: 'code', type: 'integer' },
              { name: 'status' },
              { name: 'reason' },
              { name: 'domain' },
              { name: 'location' },
              { name: 'message' }
            ] }
        ]
      end
    },
    span: {
      fields: lambda do |_connection|
        [
          { name: 'start', type: 'integer' },
          { name: 'end',   type: 'integer' }
        ]
      end
    },
    source: {
      fields: lambda do |_connection|
        [
          { name: 'file_path' },
          { name: 'checksum' }
        ]
      end
    },
    chunk: {
      fields: lambda do |object_definitions = {}|
        [
          { name: 'doc_id' },
          { name: 'chunk_id' },
          { name: 'index', type: 'integer' },
          { name: 'text' },
          { name: 'tokens', type: 'integer' },
          { name: 'span_start', type: 'integer' },
          { name: 'span_end',   type: 'integer' },
          { name: 'source', type: 'object', properties: object_definitions['source'] },
          { name: 'metadata', type: 'object' },
          { name: 'embedding', label: 'Embedding vector', type: 'array', of: 'number', optional: true },
          { name: 'created_at' }
        ]
      end
    },
    citation: {
      fields: lambda do |object_definitions|
        [
          { name: 'label' },
          { name: 'chunk_id' },
          { name: 'doc_id' },
          { name: 'file_path' },
          { name: 'span', type: 'object', properties: object_definitions['span'] },
          { name: 'metadata', type: 'object' }
        ]
      end
    },
    upsert_record: {
      fields: lambda do |_connection|
        [
          { name: 'id' },
          { name: 'vector', type: 'array', of: 'number' },
          { name: 'namespace' },
          { name: 'metadata', type: 'object' }
        ]
      end
    },
    ingest_item: {
      fields: lambda do |_connection|
        [
          { name: 'file_path', optional: true },
          { name: 'content',   control_type: 'text-area' },
          { name: 'max_chunk_chars', type: 'integer', optional: true },
          { name: 'overlap_chars',   type: 'integer', optional: true },
          { name: 'metadata', type: 'object', optional: true }
        ]
      end
    },
    vertex_prediction: {
      fields: lambda do |_|
        [
          # Common Vertex shapes we want to expose for mapping:
          { name: 'embeddings', type: 'object', properties: [
              { name: 'values', type: 'array', of: 'number',
                hint: 'Primary: predictions[*].embeddings.values (float array)' }
            ]
          },
          # Some SDKs return an array of embeddings objects
          { name: 'embeddings_list', label: 'embeddings[]', type: 'array', of: 'object', properties: [
              { name: 'values', type: 'array', of: 'number',
                hint: 'Alternate: predictions[*].embeddings[0].values' }
            ],
            hint: 'Use when embeddings is an array; map here instead of embeddings'
          },
          # Other alternates seen in Vertex responses
          { name: 'values',   type: 'array', of: 'number', optional: true,
            hint: 'Alternate: predictions[*].values (float array)' },
          { name: 'embedding', type: 'array', of: 'number', optional: true,
            hint: 'Alternate: predictions[*].embedding (float array)' }
        ]
      end
    },
    embedding_pair: {
      fields: lambda do |_|
        [
          { name: 'id' },
          { name: 'embedding', type: 'array', of: 'number' }
        ]
      end
    },
    embedding_bundle: {
      fields: lambda do |object_definitions|
        [
          { name: 'embeddings', type: 'array', of: 'object',
            properties: object_definitions['embedding_pair'],
            hint: 'Output of “Map Vertex embeddings to ids”' },
          { name: 'count', type: 'integer', optional: true },
          { name: 'vector_dim', type: 'integer', optional: true },
          { name: 'trace_id', optional: true }
        ]
      end
    },

    vector_item: {
      fields: lambda do |_|
        [
          # Workato can’t map into bare arrays; wrap each vector as an object.
          { name: 'values', type: 'array', of: 'number',
            hint: 'One embedding vector (float array)' }
        ]
      end
    },
  },

  # --------- METHODS ------------------------------------------------------
  methods: {
    detect_pairs_from_bundle: lambda do |bundle|
      return [] unless bundle.is_a?(Hash)
      list = bundle['embeddings']
      return [] unless list.is_a?(Array)
      list.map do |e|
        next nil unless e.is_a?(Hash)
        {
          'id' => (e['id'] || e['chunk_id'] || e[:id] || e[:chunk_id]).to_s,
          'embedding' => e['embedding'] || e[:embedding]
        }
      end.compact
    end,
    guid: lambda { SecureRandom.uuid },
    now_iso: lambda { Time.now.utc.iso8601 },
    generate_document_id: lambda do |file_path, checksum|
      Digest::SHA256.hexdigest("#{file_path.to_s.strip}|#{checksum.to_s.strip}")
    end,
    est_tokens: lambda do |s|
      s.to_s.strip.split(/\s+/).size
    end,
    clamp_int: lambda do |val, min_v, max_v|
      v = val.to_i
      v = min_v if v < min_v
      v = max_v if v > max_v
      v
    end,
    normalize_newlines: lambda do |s|
      s.to_s.gsub(/\r\n?/, "\n")
    end,
    sanitize_metadata: lambda do |obj|
      h = obj.is_a?(Hash) ? obj : {}
      # Force string keys; drop non-JSON-safe values
      Hash[h.map { |k, v| [k.to_s, v.is_a?(Hash) || v.is_a?(Array) || v.is_a?(String) || v.is_a?(Numeric) || v == true || v == false || v.nil? ? v : v.to_s] }]
    end,
    safe_bool: lambda do |v|
      v == true || v.to_s.strip.downcase == 'true'
    end,
    safe_hash: lambda do |obj|
      obj.is_a?(Hash) ? obj : {}
    end,
    require_array_of_objects: lambda do |arr, name|
      a = arr.is_a?(Array) ? arr : nil
      error("#{name} must be an array") unless a
      a.each_with_index do |e, i|
        error("#{name}[#{i}] must be an object") unless e.is_a?(Hash)
      end
      a
    end,
    resolve_chunk_id: lambda do |c|
      id = (c['chunk_id'] || c['id'] || c['index']).to_s
      error('Missing id for chunk') if id.empty?
      id
    end,
    strip_control_chars: lambda do |s|
      s.to_s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '')
          .gsub(/[[:cntrl:]]/) { |c| c == "\n" ? "\n" : ' ' }.gsub(/[ \t]+/, ' ').strip
    end,
    chunk_by_chars: lambda do |text, max_chars, overlap|
      t = text.to_s
      return [] if t.empty? || max_chars.to_i <= 0
      maxc = [[max_chars.to_i, 8000].min, 200].max
      ov   = [[overlap.to_i, maxc - 1].min, 0].max
      out  = []
      i    = 0
      idx  = 0
      n    = t.length
      while i < n
        hard_end = [i + maxc, n].min
        # Try to break on whitespace near the end of the window to avoid chopping words
        window = t[i...hard_end] || ''
        break_at = window.rindex(/\s/) || (hard_end - i - 1)
        break_at = 0 if break_at.negative?
        slice_end = i + break_at + 1
        slice_end = hard_end if slice_end <= i # fallback to hard cut
        slice = t[i...slice_end] || ''
        out << { 'index' => idx, 'text' => slice, 'span_start' => i, 'span_end' => slice_end }
        idx += 1
        i = slice_end - ov
      end
      out
    end,
    ensure_array: lambda do |v|
      v.is_a?(Array) ? v : (v.nil? ? [] : [v])
    end,
    flatten_chunks_input: lambda do |input|
      # Accept either:
      #  - { chunks: [...] }
      #  - { documents: [ {chunks:[...]}, ... ] }
      all = []
      if input['chunks'].is_a?(Array)
        input['chunks'].each { |c| all << (c.is_a?(Hash) ? c : {}) }
      end
      if input['documents'].is_a?(Array)
        input['documents'].each do |d|
          next unless d.is_a?(Hash)
          (d['chunks'] || []).each { |c| all << (c.is_a?(Hash) ? c : {}) }
        end
      end
      all
    end,
    telemetry_envelope: lambda do |started_at, correlation_id, ok, code, message, upstream=nil|
      dur = ((Time.now - started_at) * 1000.0).to_i
      base = {
        'ok' => !!ok,
        'telemetry' => {
          'http_status'    => code.to_i,
          'message'        => (message || (ok ? 'OK' : 'ERROR')).to_s,
          'duration_ms'    => dur,
          'correlation_id' => correlation_id
        }
      }
      upstream.is_a?(Hash) ? base.merge('upstream' => upstream) : base
    end,
    normalize_error_for_pills: lambda do |details, operation|
      d = details.is_a?(Hash) ? details.dup : {}
      {
        'code'      => (d['code'] || d[:code]),
        'status'    => (d['status'] || d[:status]).to_s,
        'reason'    => (d['reason'] || d[:reason]).to_s,
        'domain'    => (d['domain'] || d[:domain]).to_s,
        'location'  => (d['location'] || d[:location]).to_s,
        'message'   => (d['message'] || d[:message] || d['error'] || d[:error]).to_s,
        'service'   => 'utility',
        'operation' => operation.to_s
      }.compact
    end,
    schema_builder_config_fields: lambda do
      [
        {
          name: 'override_output_schema',
          type: 'boolean',
          control_type: 'checkbox',
          label: 'Design custom output schema',
          hint: 'Use Schema Builder to define this action’s datapills.'
        },
        {
          name: 'custom_output_schema',
          extends_schema: true,
          control_type: 'schema-designer',
          schema_neutral: false,
          sticky: true,
          optional: true,
          label: 'Output columns',
          hint: 'Define the output fields (datapills) for this action.',
          sample_data_type: 'csv'
        }
      ]
    end,
    resolve_output_schema: lambda do |default_fields, cfg, object_definitions|
      custom = cfg.is_a?(Hash) &&
               cfg['override_output_schema'] &&
               cfg['custom_output_schema'].is_a?(Array) &&
               !cfg['custom_output_schema'].empty? ?
                 cfg['custom_output_schema'] : nil

      base = custom || default_fields
      base + Array(object_definitions['envelope_fields'])
    end,
    schema_builder_ingest_items_config_fields: lambda do
      [
        { name: 'design_item_schema', type: 'boolean', control_type: 'checkbox', label: 'Design item schema',
          sticky: true, hint: 'Check to use Schema Builder to define the shape of each list element.' },
        {
          name: 'item_schema',
          extends_schema: true,
          control_type: 'schema-designer',
          schema_neutral: false,
          sticky: true,
          optional: true,
          label: 'Item fields',
          hint: 'Define the fields present in each item of the list.'
        },
        # Which fields in the item correspond to our semantics?
        {
          name: 'item_content_field',
          control_type: 'select',
          label: 'Item field: content',
          optional: true,
          pick_list: 'item_schema_field_names'
        },
        {
          name: 'item_file_path_field',
          control_type: 'select',
          label: 'Item field: file_path',
          optional: true,
          pick_list: 'item_schema_field_names'
        },
        {
          name: 'item_metadata_field',
          control_type: 'select',
          label: 'Item field: metadata (object)',
          optional: true,
          pick_list: 'item_schema_field_names'
        },
        {
          name: 'item_max_chunk_chars_field',
          control_type: 'select',
          label: 'Item field: max_chunk_chars',
          optional: true,
          pick_list: 'item_schema_field_names'
        },
        {
          name: 'item_overlap_chars_field',
          control_type: 'select',
          label: 'Item field: overlap_chars',
          optional: true,
          pick_list: 'item_schema_field_names'
        }
      ]
    end,
    get_item_field: lambda do |item, cfg, declared_key, fallback_key|
      key = (cfg[declared_key] || fallback_key).to_s
      v = item[key]
      v.nil? ? item[fallback_key] : v
    end,
    cfg_or_input: lambda do |cfg, input, key|
      v = nil
      v = cfg[key] if cfg.is_a?(Hash) && cfg.key?(key)
      v = input[key] if v.nil? && input.is_a?(Hash)
      v
    end,
    coerce_int_or_nil: lambda do |v|
      return nil if v.nil? || v.to_s.strip.empty?
      Integer(v) rescue nil
    end,
    chunk_preset_bounds: lambda do |preset|
      case preset.to_s.downcase
      when 'tiny'   then { 'max' => 600,  'overlap' => 60  }
      when 'small'  then { 'max' => 1200, 'overlap' => 120 }
      when 'medium' then { 'max' => 2000, 'overlap' => 200 }
      when 'large'  then { 'max' => 3500, 'overlap' => 240 }
      when 'max'    then { 'max' => 8000, 'overlap' => 400 }
      else               { 'max' => 2000, 'overlap' => 200 }
      end
    end,
    resolve_chunks_array: lambda do |input|
      # Flatten documents[*].chunks or chunks[*]
      if input['chunks'].is_a?(Array)
        input['chunks']
      elsif input['documents'].is_a?(Array)
        input['documents'].flat_map { |d| (d || {})['chunks'] || [] }
      else
        []
      end
    end,
    require_nonempty_string: lambda do |val, name|
      s = val.to_s
      error("#{name} is required") if s.strip.empty?
      s
    end,
    json_bytesize: lambda do |v|
      # Conservative byte estimate for JSON payloads (not perfect; good enough for caps)
      v.to_json.to_s.bytesize rescue v.to_s.bytesize
    end,
    truncate_string: lambda do |s, max_bytes|
      str = s.to_s
      return str if max_bytes.to_i <= 0
      b = str.encode('UTF-8', invalid: :replace, undef: :replace).bytes
      return str if b.length <= max_bytes
      # keep as many bytes as fit, add ellipsis
      kept = b[0, [max_bytes - 3, 0].max]
      kept.pack('C*').force_encoding('UTF-8').scrub + '…'
    end,
    flatten_object_shallow: lambda do |obj, prefix|
      return {} unless obj.is_a?(Hash)
      out = {}
      pfx = prefix.to_s
      obj.each do |k,v|
        key = (pfx.empty? ? k.to_s : "#{pfx}#{k}")
        out[key] = v
      end
      out
    end,
    # Metadata coercion:
    # - mode 'none' => {}
    # - mode 'pass' => JSON-safe (stringify keys, drop unserializable), no flattening
    # - mode 'flat' => flatten 1 level (object only), wrap primitives/arrays under a key
    # Caps: max_keys (default 50), max_bytes (default 4096), truncate long strings
    coerce_metadata: lambda do |val, cfg|
      mode       = (cfg['metadata_mode'] || 'flat').to_s
      prefix     = (cfg['metadata_prefix'] || '').to_s
      max_keys   = (cfg['metadata_max_keys'] || 50).to_i
      max_bytes  = (cfg['metadata_max_bytes'] || 4096).to_i

      return {} if mode == 'none' || val.nil?

      base =
        if mode == 'pass'
          h = val.is_a?(Hash) ? val : { 'value' => val }
          Hash[h.map { |k, v| [k.to_s, v] }]
        else # flat (default)
          case val
          when Hash
            call(:flatten_object_shallow, val, prefix)
          when Array
            { (prefix + 'list') => val }
          when String, Numeric, TrueClass, FalseClass
            { (prefix + 'value') => val }
          else
            { (prefix + 'value') => val.to_s }
          end
        end

      # Enforce key cap
      if max_keys > 0 && base.keys.length > max_keys
        base = base.first(max_keys).to_h.merge('__meta_truncated__' => true)
      end

      # Truncate long strings and enforce byte cap
      base.each do |k,v|
        if v.is_a?(String)
          base[k] = call(:truncate_string, v, [max_bytes / 8, 0].max) # per-field soft cap
        end
      end
      if max_bytes > 0 && call(:json_bytesize, base) > max_bytes
        # Drop lowest-priority keys until under cap (naive heuristic: drop by sorted key)
        base.keys.sort.reverse.each do |k|
          next if k == '__meta_truncated__'
          base.delete(k)
          if call(:json_bytesize, base) <= max_bytes
            base['__meta_truncated__'] = true
            break
          end
        end
      end
      base
    end,
    parse_file_path_meta: lambda do |file_path|
      fp = file_path.to_s
      return {} if fp.empty?
      # drive://folder/file.txt or gcs://bucket/dir/file.pdf
      m = fp.match(/\A([a-z0-9+.-]+):\/\/(.+)\z/i)
      scheme = m ? m[1] : nil
      rest   = m ? m[2] : fp
      parts  = rest.split('/')
      fname  = parts.last.to_s
      ext    = fname.include?('.') ? fname.split('.').last : ''
      {
        'uri_scheme' => scheme,
        'path'       => rest,
        'filename'   => fname,
        'extension'  => ext,
        'folder'     => parts[0..-2].join('/')
      }.delete_if { |_,v| v.to_s.empty? }
    end,
    build_metadata_from_ui: lambda do |cfg, input|
      mode = (cfg['metadata_source'] || 'none').to_s
      case mode
      when 'none'
        {}
      when 'kv'
        pairs = input['metadata_pairs'].is_a?(Array) ? input['metadata_pairs'] : []
        out = {}
        pairs.each do |p|
          next unless p.is_a?(Hash)
          k = p['key'].to_s.strip
          next if k.empty?
          out[k] = p['value']
        end
        out
      when 'tags_csv'
        raw = input['metadata_tags_csv'].to_s
        tags = raw.split(',').map { |t| t.strip }.reject(&:empty?)
        tags.empty? ? {} : { 'tags' => tags }
      when 'auto_from_path'
        call(:parse_file_path_meta, input['file_path'])
      when 'advanced'
        # For advanced users only; still JSON-safe
        call(:coerce_metadata, input['metadata'], cfg || {})
      else
        {}
      end
    end,
    # Size heuristics chosen to “just work”:
    # - short docs keep chunks small to avoid over-splitting
    # - long docs increase chunk to reduce record explosion
    auto_chunk_bounds_for: lambda do |text_len|
      n = text_len.to_i
      case n
      when 0..2_000     then { 'max' => 800,  'overlap' => 80  }   # ~1–2 pages
      when 2_001..8_000 then { 'max' => 1_600,'overlap' => 160 }   # ~3–8 pages
      when 8_001..25_000 then { 'max' => 2_400,'overlap' => 200 }  # ~10–30 pages
      else                  { 'max' => 3_600,'overlap' => 240 }    # larger corpora
      end
    end,
    choose_metadata_simple: lambda do |input|
      # Priority: KV > Tags > Use path > nothing
      pairs = input['metadata_kv'].is_a?(Array) ? input['metadata_kv'] : []
      unless pairs.empty?
        out = {}
        pairs.each do |p|
          next unless p.is_a?(Hash)
          k = p['key'].to_s.strip
          next if k.empty?
          out[k] = p['value']
        end
        return out
      end
      tags_csv = input['metadata_tags_csv'].to_s
      tags = tags_csv.split(',').map { |t| t.strip }.reject(&:empty?)
      return({ 'tags' => tags }) unless tags.empty?
      return call(:parse_file_path_meta, input['file_path']) if input['metadata_use_path'] == true
      {}
    end,
    scalarize: lambda do |v|
      case v
      when String, Numeric, TrueClass, FalseClass, NilClass then v
      else v.to_s
      end
    end,
    flatten_metadata_for_table: lambda do |metadata, prefix, caps = {}|
      meta = call(:sanitize_metadata, metadata || {})
      flat = call(:flatten_object_shallow, meta, prefix.to_s)
      # enforce caps similar to coerce_metadata
      max_keys  = (caps['metadata_max_keys']  || 50).to_i
      max_bytes = (caps['metadata_max_bytes'] || 4096).to_i
      flat = flat.first(max_keys).to_h.merge('__meta_truncated__' => true) if max_keys > 0 && flat.length > max_keys
      # string truncation and byte cap
      flat.each { |k, v| flat[k] = v.is_a?(String) ? call(:truncate_string, v, [max_bytes / 8, 0].max) : v }
      if max_bytes > 0 && call(:json_bytesize, flat) > max_bytes
        flat.keys.sort.reverse.each do |k|
          next if k == '__meta_truncated__'
          flat.delete(k)
          break if call(:json_bytesize, flat) <= max_bytes
        end
        flat['__meta_truncated__'] = true
      end
      # force primitives only
      Hash[flat.map { |k, v| [k, call(:scalarize, v)] }]
    end,
    build_table_row: lambda do |chunk, profile, include_text, meta_prefix, caps|
      base = {
        'id'         => call(:resolve_chunk_id, chunk),
        'doc_id'     => chunk['doc_id'],
        'file_path'  => chunk.dig('source','file_path'),
        'checksum'   => chunk.dig('source','checksum'),
        'tokens'     => chunk['tokens'],
        'span_start' => chunk['span_start'],
        'span_end'   => chunk['span_end'],
        'created_at' => chunk['created_at']
      }.delete_if { |_, v| v.nil? }

      case profile
      when 'slim'
        row = {
          'id'        => base['id'],
          'doc_id'    => base['doc_id'],
          'file_path' => base['file_path']
        }
        row['text'] = chunk['text'].to_s if include_text
        row
      when 'wide'
        row = base.dup
        row['text'] = chunk['text'].to_s if include_text
        meta = call(:flatten_metadata_for_table, chunk['metadata'], (meta_prefix || 'meta_'), caps || {})
        row.merge(meta)
      else # 'standard' (default)
        row = base.dup
        row['text'] = chunk['text'].to_s if include_text
        row
      end
    end

  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    
    # --- INGESTION (SEEDING) ----------------------------------------------

    # ---- 1.  Prep for indexing -------------------------------------------
    prep_for_indexing: {
      title: 'Ingestion - Prepare document for indexing',
      subtitle: 'Cleans, chunks, and emits chunk records with IDs + metadata',
      display_priority: 10,
      help: lambda do |_|
        { body: 'Paste text, optionally include a Source URI, pick a Chunk size (Auto is best). Add metadata via rows/tags — no JSON needed. Use “Show advanced options” for custom sizes or JSON metadata.' }
      end,

      config_fields: [
        { name: 'preset', control_type: 'select', pick_list: 'chunking_presets', sticky: true,
          optional: true, hint: 'Auto picks sensible sizes from content length.' },
        { name: 'show_advanced', type: 'boolean', control_type: 'checkbox', sticky: true,
          label: 'Show advanced options', hint: 'Reveal custom sizes, JSON metadata, and caps.' }
      ],

      input_fields: lambda do |_object_definitions, _connection, cfg|
        cfg ||= {}
        advanced = !!cfg['show_advanced']
        preset   = (cfg['preset'] || 'auto').to_s
        fields   = [
          { name: 'file_path', label: 'Source URI (recommended)',
            hint: 'Stable URI like gcs://bucket/path or drive://folder/file; used to derive deterministic doc_id.',
            optional: true },
          { name: 'content', label: 'Plain text content (required)', hint: 'UTF-8 text only. Convert PDFs/DOCX before calling.',
            optional: false, control_type: 'text-area' },
        ]

        # Small file handling
        fields << { name: 'no_chunk_under_chars', type: 'integer', optional: true,
                    hint: 'If total characters <= this, emit 1 chunk (overlap=0).' }
        fields << { name: 'no_chunk_under_tokens', type: 'integer', optional: true,
                    hint: 'If total tokens <= this, emit 1 chunk (overlap=0).' }


        # Simple metadata block (no JSON in normal path)
        fields << {
          name: 'metadata_kv', type: 'array', of: 'object', optional: true,
          label: 'Metadata (key–value pairs)', hint: 'Add optional attributes as rows.',
          properties: [{ name: 'key' }, { name: 'value' }]
        }
        fields << { name: 'metadata_tags_csv', optional: true, label: 'Tags (comma separated)',
                    hint: 'e.g., HR,policy,2025' }
        fields << { name: 'metadata_use_path', type: 'boolean', control_type: 'checkbox', optional: true,
                    label: 'Use details from Source URI', hint: 'Derives filename/extension/folder.' }

        # Custom sizes only if preset == custom OR advanced
        if preset == 'custom' || advanced
          fields << { name: 'max_chunk_chars', label: 'Max characters per chunk',
                      type: 'integer', optional: true, sticky: true, hint: 'Allowed: 200–8000. Example: 2000.' }
          fields << { name: 'overlap_chars',   label: 'Overlap between chunks (chars)',
                      type: 'integer', optional: true, sticky: true, hint: 'Must be < Max. Example: 200.' }
        end

        # Advanced JSON and caps (hidden unless Advanced)
        if advanced
          fields << { name: 'metadata_json', type: 'object', optional: true,
                      label: 'Advanced metadata (JSON object)', hint: 'For power users only.' }
          fields << { name: 'metadata_max_keys', type: 'integer', optional: true, sticky: true,
                      hint: 'Default 50. 0 = unlimited (not recommended).' }
          fields << { name: 'metadata_max_bytes', type: 'integer', optional: true, sticky: true,
                      hint: 'Default 4096 total bytes after coercion.' }
        end

        if advanced
          fields << { name: 'debug', label: 'Include debug notes', type: 'boolean',
                      control_type: 'checkbox', optional: true }
        end
        fields
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'doc_id' },
          { name: 'file_path' },
          { name: 'checksum' },
          { name: 'chunk_count', type: 'integer' },
          { name: 'max_chunk_chars', type: 'integer' },
          { name: 'overlap_chars', type: 'integer' },
          { name: 'created_at' },
          { name: 'duration_ms', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk'] },
          { name: 'trace_id', optional: true },
          { name: 'notes', optional: true }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input|
        t0   = Time.now
        corr = call(:guid)
        begin
          started_at  = Time.now
          trace_id    = call(:guid)
          # ---- VALIDATION ----
          raw_in      = input['content']
          file_path   = input['file_path'].to_s
          if raw_in.nil? || raw_in.to_s.strip.empty?
            # Helpful hint: user likely mapped a binary (e.g., PDF) without text extraction.
            hint = ''
            if file_path =~ /\.pdf\z/i || file_path =~ %r{\Ags://}i || file_path =~ %r{\Adrive://}i
              hint = ' (if this is a PDF or other binary, convert/extract text first; map text_content from Drive/GCS or your extractor)'
            end
            error("content is required and must be non-empty#{hint}.")
          end
          raw         = raw_in.to_s
          meta_simple = call(:choose_metadata_simple, input)
          meta_adv    = input['metadata_json'].is_a?(Hash) ? input['metadata_json'] : {}
          # Apply optional caps only if provided (advanced UI)
          caps = {
            'metadata_max_keys'  => input['metadata_max_keys'],
            'metadata_max_bytes' => input['metadata_max_bytes']
          }.compact
          user_meta = if caps.empty?
            call(:sanitize_metadata, meta_simple.merge(meta_adv))
          else
            call(:coerce_metadata, meta_simple.merge(meta_adv), {
              'metadata_mode'      => 'flat',
              'metadata_prefix'    => '',
              'metadata_max_keys'  => caps['metadata_max_keys']  || 50,
              'metadata_max_bytes' => caps['metadata_max_bytes'] || 4096
            })
          end
          debug       = call(:safe_bool, input['debug'])

          # 1) Normalize/Clean
          normalized  = call(:normalize_newlines, raw)
          cleaned     = call(:strip_control_chars, normalized)
          total_chars = cleaned.length
          total_toks  = call(:est_tokens, cleaned)

          # 2) Infer "custom" when caller provided explicit bounds; else auto/balanced/small/large by input hint (optional)
          provided_max = input['max_chunk_chars']
          provided_ovl = input['overlap_chars']
          preset = 'auto'
          if provided_max || provided_ovl
            preset = 'custom'
          elsif %w[small large balanced].include?(input['preset'].to_s)
            preset = input['preset'].to_s
          end
          if preset == 'custom'
            max_in  = input['max_chunk_chars']
            ov_in   = input['overlap_chars']
            max_chars = call(:clamp_int, (max_in || 2000), 200, 8000)
            overlap   = call(:clamp_int, (ov_in  || 200),    0, 4000)
          else
            case preset
            when 'auto'
              bounds    = call(:auto_chunk_bounds_for, cleaned.length)
              max_chars = bounds['max']
              overlap   = bounds['overlap']
            when 'small'    then max_chars, overlap = 1000, 100
            when 'large'    then max_chars, overlap = 4000, 200
            when 'balanced' then max_chars, overlap = 2000, 200
            else                 max_chars, overlap = 2000, 200
            end
          end

          # Tiny-doc override: force single chunk if thresholds say so
          clamp_note = nil
          nch = call(:coerce_int_or_nil, input['no_chunk_under_chars'])
          ntk = call(:coerce_int_or_nil, input['no_chunk_under_tokens'])
          tiny_by_chars  = (nch && total_chars <= nch)
          tiny_by_tokens = (ntk && total_toks  <= ntk)
          if tiny_by_chars || tiny_by_tokens
            max_chars = [total_chars, 1].max
            overlap   = 0
            clamp_note = "Tiny-doc override: total_chars=#{total_chars}, total_tokens=#{total_toks} → 1 chunk."
          end

          # Harmonize with batch behavior: clamp, but surface a debug note.
          if overlap >= max_chars
            clamp_note = "Requested overlap=#{overlap} >= max=#{max_chars}. Clamped to #{[max_chars - 1, 0].max}."
            overlap = [max_chars - 1, 0].max
          end

          # 3) IDs
          checksum   = Digest::SHA256.hexdigest(cleaned)
          doc_id     = call(:generate_document_id, file_path, checksum)

          # 4) Chunk
          spans = call(:chunk_by_chars, cleaned, max_chars, overlap)

          # 5) Emit records
          created_at = call(:now_iso)
          records = spans.map do |span|
            chunk_id = "#{doc_id}:#{span['index']}"
            text     = span['text']
            {
              'doc_id'      => doc_id,
              'chunk_id'    => chunk_id,
              'index'       => span['index'],
              'text'        => text,
              'tokens'      => call(:est_tokens, text),
              'span_start'  => span['span_start'],
              'span_end'    => span['span_end'],
              'source'      => { 'file_path' => file_path, 'checksum' => checksum },
              'metadata'    => user_meta,
              'created_at'  => created_at
            }
          end

          base = {
            'doc_id'          => doc_id,
            'file_path'       => file_path,
            'checksum'        => checksum,
            'chunk_count'     => records.length,
            'max_chunk_chars' => max_chars,
            'overlap_chars'   => overlap,
            'created_at'      => created_at,
            'chunks'          => records
          }
          base['trace_id']  = corr if debug
          base['notes']     = [
            "prep_for_indexing completed (preset=#{preset}, max=#{max_chars}, overlap=#{overlap})",
            (clamp_note if clamp_note)
          ].compact.join(' | ') if debug
          base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          details = { 'message' => e.to_s, 'status' => e.class.to_s }
          # Keep output schema stable: return empty doc with envelope + error.
          {}.merge(
            'doc_id'      => nil,
            'file_path'   => input['file_path'].to_s,
            'checksum'    => nil,
            'chunk_count' => 0,
            'chunks'      => []
          ).merge(
            'error' => call(:normalize_error_for_pills, details, 'prep_for_indexing')
          ).merge(
            call(:telemetry_envelope, t0, corr, false, 400, e.to_s, details)
          )
        end
      end,

      sample_output: lambda do
        {
          'doc_id' => 'd9f1…',
          'file_path' => 'drive://Reports/2025/summary.txt',
          'checksum' => '3a2b…',
          'chunk_count' => 2,
          'max_chunk_chars' => 2000,
          'overlap_chars' => 200,
          'created_at' => '2025-10-15T12:00:00Z',
          'chunks' => [
            {
              'doc_id' => 'd9f1…',
              'chunk_id' => 'd9f1…:0',
              'index' => 0,
              'text' => 'First slice…',
              'tokens' => 42,
              'span_start' => 0,
              'span_end' => 1800,
              'source' => { 'file_path' => 'drive://Reports/2025/summary.txt', 'checksum' => '3a2b…' },
              'metadata' => { 'department' => 'HR' },
              'created_at' => '2025-10-15T12:00:00Z' }],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample' }
        }
      end
    },
    prep_for_indexing_batch: {
      title: 'Ingestion - Prepare multiple documents for indexing',
      subtitle: 'Cleans + chunks N documents; returns an array of per-doc results',
      batch: true,
      display_priority: 10,

      config_fields: [
        {
          name: 'design_item_schema',
          type: 'boolean',
          control_type: 'checkbox',
          label: 'Design item schema',
          hint: 'Use Schema Builder to define the shape of each list element.'
        },
        {
          name: 'item_schema',
          extends_schema: true,
          control_type: 'schema-designer',
          schema_neutral: false,
          sticky: true,
          optional: true,
          label: 'Item fields',
          hint: 'Define the fields present in each item of the list.'
        },
        { name: 'item_content_field',      control_type: 'select', label: 'Item field: content',        optional: true, pick_list: 'item_schema_field_names' },
        { name: 'item_file_path_field',    control_type: 'select', label: 'Item field: file_path',      optional: true, pick_list: 'item_schema_field_names' },
        { name: 'item_metadata_field',     control_type: 'select', label: 'Item field: metadata (obj)', optional: true, pick_list: 'item_schema_field_names' },
        { name: 'item_max_chunk_chars_field', control_type: 'select', label: 'Item field: max_chunk_chars', optional: true, pick_list: 'item_schema_field_names' },
        { name: 'item_overlap_chars_field',   control_type: 'select', label: 'Item field: overlap_chars',   optional: true, pick_list: 'item_schema_field_names' }
      ],

      input_fields: lambda do |object_definitions, _connection, config_fields|
        # If a custom item schema is designed in the step config, use it here;
        # otherwise fall back to the static object_definitions['ingest_item'].
        item_props =
          if config_fields['design_item_schema'] &&
              config_fields['item_schema'].is_a?(Array) &&
              !config_fields['item_schema'].empty?
            config_fields['item_schema']
          else
            object_definitions['ingest_item']
          end
        [
          { name: 'items', type: 'array', of: 'object', properties: item_props, optional: false,
            hint: 'Map your list here. Use “Design item schema” if your list shape is custom.' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
        # Batch-level tiny-doc defaults (items can still pass explicit per-item fields if you add them)
        .push({ name: 'no_chunk_under_chars', type: 'integer', optional: true })
        .push({ name: 'no_chunk_under_tokens', type: 'integer', optional: true })
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          {
            name: 'results', type: 'array', of: 'object', properties: [
              { name: 'doc_id' },
              { name: 'file_path' },
              { name: 'checksum' },
              { name: 'chunk_count', type: 'integer' },
              { name: 'max_chunk_chars', type: 'integer' },
              { name: 'overlap_chars', type: 'integer' },
              { name: 'created_at' },
              { name: 'duration_ms', type: 'integer' },
              { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk'] },
              { name: 'trace_id', optional: true },
              { name: 'notes', optional: true }
            ]
          },
          { name: 'count', type: 'integer' },
          { name: 'batch_trace_id', optional: true }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input, _schema, _input_schema_name, _connection_schema, _config_fields|
        started_batch = Time.now
        batch_trace   = call(:guid)
        debug         = call(:safe_bool, input['debug'])
        items         = call(:ensure_array, input['items'])

        results = items.map do |it|
          t0          = Time.now
          # Resolve fields using config mapping (falls back to canonical keys)
          raw         = call(:get_item_field, it, _config_fields, 'item_content_field', 'content').to_s
          file_path   = call(:get_item_field, it, _config_fields, 'item_file_path_field', 'file_path').to_s
          user_meta   = call(:sanitize_metadata, call(:get_item_field, it, _config_fields, 'item_metadata_field', 'metadata'))
          # Per-item overrides for chunking bounds (optional)
          max_in      = call(:get_item_field, it, _config_fields, 'item_max_chunk_chars_field', 'max_chunk_chars')
          ov_in       = call(:get_item_field, it, _config_fields, 'item_overlap_chars_field',   'overlap_chars')

          normalized  = call(:normalize_newlines, raw)
          cleaned     = call(:strip_control_chars, normalized)
          total_chars = cleaned.length
          total_toks  = call(:est_tokens, cleaned)
          max_chars   = call(:clamp_int, (max_in || 2000), 200, 8000)
          overlap     = call(:clamp_int, (ov_in  || 200),   0,   4000)
          overlap     = [overlap, max_chars - 1].min
          # Tiny-doc override (batch level)
          nch = call(:coerce_int_or_nil, input['no_chunk_under_chars'])
          ntk = call(:coerce_int_or_nil, input['no_chunk_under_tokens'])
          if (nch && total_chars <= nch) || (ntk && total_toks <= ntk)
            max_chars = [total_chars, 1].max
            overlap   = 0
          end
          checksum    = Digest::SHA256.hexdigest(cleaned)
          doc_id      = call(:generate_document_id, file_path, checksum)
          spans       = call(:chunk_by_chars, cleaned, max_chars, overlap)
          created_at  = call(:now_iso)
          records     = spans.map do |span|
            chunk_id = "#{doc_id}:#{span['index']}"
            text     = span['text']
            {
              'doc_id'      => doc_id,
              'chunk_id'    => chunk_id,
              'index'       => span['index'],
              'text'        => text,
              'tokens'      => call(:est_tokens, text),
              'span_start'  => span['span_start'],
              'span_end'    => span['span_end'],
              'source'      => { 'file_path' => file_path, 'checksum' => checksum },
              'metadata'    => user_meta,
              'created_at'  => created_at
            }
          end
          per = {
            'doc_id'          => doc_id,
            'file_path'       => file_path,
            'checksum'        => checksum,
            'chunk_count'     => records.length,
            'max_chunk_chars' => max_chars,
            'overlap_chars'   => overlap,
            'created_at'      => created_at,
            'duration_ms'     => ((Time.now - t0) * 1000).round,
            'chunks'          => records
          }
          if debug
            per['trace_id'] = call(:guid)
            per['notes']    = 'prep_for_indexing(item) completed'
          end
          per
        end

        base = { 'results' => results, 'count' => results.length }
        base['batch_trace_id'] = batch_trace if debug
        base.merge(call(:telemetry_envelope, started_batch, batch_trace, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'results' => [
            {
              'doc_id' => 'doc-abc123',
              'file_path' => 'drive://Reports/2025/summary.txt',
              'checksum' => '3a2b9c…',
              'chunk_count' => 2,
              'max_chunk_chars' => 2000,
              'overlap_chars' => 200,
              'created_at' => '2025-10-15T12:00:00Z',
              'duration_ms' => 11,
              'chunks' => [
                {
                  'doc_id' => 'doc-abc123',
                  'chunk_id' => 'doc-abc123:0',
                  'index' => 0,
                  'text' => 'First slice…',
                  'tokens' => 42,
                  'span_start' => 0,
                  'span_end' => 1799,
                  'source' => { 'file_path' => 'drive://Reports/2025/summary.txt', 'checksum' => '3a2b9c…' },
                  'metadata' => { 'department' => 'HR' },
                  'created_at' => '2025-10-15T12:00:00Z'
                }
              ],
              'trace_id' => 'trace-1',
              'notes' => 'prep_for_indexing(item) completed'
            }
          ],
          'count' => 1,
          'batch_trace_id' => 'batch-trace-1234'
        }
      end
    },

    # ---- 2.  Build index upserts -----------------------------------------
    build_index_upserts: {
      title: 'Ingestion - Build index upserts',
      subtitle: 'Provider-agnostic: [{id, vector, namespace, metadata}]',
      display_priority: 9,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do |object_definitions, _connection, _config_fields|
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false, properties: object_definitions['chunk'],
            hint: 'Map the Chunks list from “Prepare document for indexing”.', sticky: true },
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'count', type: 'integer' },
          { name: 'records', type: 'array', of: 'object', properties: object_definitions['upsert_record'] }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0      = Time.now
        corr    = call(:guid)
        ns      = nil
        prv     = nil
        chunks  = call(:require_array_of_objects, input['chunks'], 'chunks')
        upserts = chunks.select { |c| c['embedding'].is_a?(Array) }.map do |c|
          {
            'id'        => call(:resolve_chunk_id, c),
            'vector'    => c['embedding'],
            'namespace' => ns,
            'metadata'  => {
              'doc_id'   => c['doc_id'],
              'file_path'=> c.dig('source','file_path'),
              'span'     => { 'start' => c['span_start'], 'end' => c['span_end'] },
              'tokens'   => c['tokens'],
              'extra'    => call(:sanitize_metadata, c['metadata'])
            }.compact
          }.compact
        end
        { 'records' => upserts, 'count' => upserts.length }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'provider' => 'vertex',
          'namespace' => 'hr-knowledge-v1',
          'count' => 2,
          'records' => [
            {
              'id' => 'doc-abc123:0',
              'vector' => [0.01, 0.02, 0.03],
              'namespace' => 'hr-knowledge-v1',
              'metadata' => {
                'doc_id' => 'doc-abc123',
                'file_path' => 'drive://Reports/2025/summary.txt',
                'span' => { 'start' => 0, 'end' => 1799 },
                'tokens' => 42,
                'extra' => { 'department' => 'HR' }
              }
            }
          ]
        }
      end
    },
    build_index_upserts_batch: {
      title: 'Ingestion - Build index upserts',
      subtitle: 'Accepts documents[*].chunks or chunks[*]; emits provider-agnostic upserts',
      batch: true,
      display_priority: 9,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox', label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true, label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'documents', type: 'array', of: 'object', optional: true },
          { name: 'chunks', type: 'array', of: 'object', optional: true }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'count', type: 'integer' },
          { name: 'records', type: 'array', of: 'object', properties: object_definitions['upsert_record'] }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0  = Time.now
        corr = call(:guid)
        ns   = nil
        prv  = nil
        chunks = call(:flatten_chunks_input, input)
        upserts = chunks.select { |c| c['embedding'].is_a?(Array) }.map do |c|
          {
            'id'        => call(:resolve_chunk_id, c),
            'vector'    => c['embedding'],
            'namespace' => ns,
            'metadata'  => {
              'doc_id'    => c['doc_id'],
              'file_path' => c.dig('source','file_path'),
              'span'      => { 'start' => c['span_start'], 'end' => c['span_end'] },
              'tokens'    => c['tokens'],
              'extra'     => call(:sanitize_metadata, c['metadata'])
            }.compact
          }.compact
        end
        {
          'records' => upserts, 'count' => upserts.length
        }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,
  
      sample_output: lambda do
        {
          'provider' => 'vertex',
          'namespace' => 'hr-knowledge-v1',
          'count' => 2,
          'records' => [
            {
              'id' => 'doc-abc123:0',
              'vector' => [0.01, 0.02, 0.03],
              'namespace' => 'hr-knowledge-v1',
              'metadata' => {
                'doc_id' => 'doc-abc123',
                'file_path' => 'drive://Reports/2025/summary.txt',
                'span' => { 'start' => 0, 'end' => 1799 },
                'tokens' => 42,
                'extra' => { 'department' => 'HR' }
              }
            }
          ]
        }
      end
    },
    build_vertex_datapoints: {
      title: 'Ingestion - Build Vertex datapoints',
      subtitle: 'Chunks with embeddings → [{datapointId, featureVector, restricts, labels?, metadata?}]',
      display_priority: 7,

      input_fields: lambda do |object_definitions, _connection, _cfg|
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false,
            properties: object_definitions['chunk'],
            hint: 'Use output of attach_embeddings (chunks now include embedding)' },
          { name: 'tenant', optional: true, hint: 'Applied as restricts: {namespace: "tenant"}' },
          { name: 'source_token', optional: true, hint: 'Applied as restricts: {namespace: "source"} (e.g., handbook)' },
          { name: 'doc_namespace', optional: true, hint: 'Namespace name for document ids (default "doc")' },
          { name: 'labels', type: 'object', optional: true, hint: 'Optional labels object added per datapoint' }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'datapoints', type: 'array', of: 'object', properties: [
              { name: 'datapointId' },
              { name: 'featureVector', type: 'array', of: 'number' },
              { name: 'restricts', type: 'array', of: 'object', properties: [
                  { name: 'namespace' },
                  { name: 'allowTokens', type: 'array', of: 'string' },
                  { name: 'denyTokens',  type: 'array', of: 'string' }
                ]
              },
              { name: 'crowdingTag' },
              { name: 'labels', type: 'object' },
              { name: 'metadata', type: 'object' }
            ]
          }
        ]
      end,

      execute: lambda do |_connection, input|
        chunks = input['chunks'].is_a?(Array) ? input['chunks'] : []
        error('chunks must be a non-empty array') if chunks.empty?
        doc_ns = (input['doc_namespace'] || 'doc').to_s
        tenant = input['tenant'].to_s
        source = input['source_token'].to_s
        labels = input['labels'].is_a?(Hash) ? input['labels'] : nil

        dps = chunks.map.with_index do |c, i|
          id   = (c['chunk_id'] || c['id'] || c['index']).to_s
          error("chunk[#{i}] missing id/chunk_id") if id.empty?
          vec  = c['embedding']
          error("chunk[#{i}] missing embedding") if !vec.is_a?(Array) || vec.empty?
          # Build restricts
          rest = []
          rest << { 'namespace' => doc_ns, 'allowTokens' => [c['doc_id'].to_s] } if c['doc_id'].to_s != ''
          rest << { 'namespace' => 'tenant', 'allowTokens' => [tenant] } if !tenant.empty?
          rest << { 'namespace' => 'source', 'allowTokens' => [source] } if !source.empty?
          # Metadata passthrough (safe)
          md = {
            'doc_id'    => c['doc_id'],
            'source'    => c['source'],
            'span'      => { 'start' => c['span_start'], 'end' => c['span_end'] },
            'tokens'    => c['tokens'],
            'extra'     => c['metadata']
          }.delete_if { |_k,v| v.nil? }
          dp = {
            'datapointId'   => id,
            'featureVector' => vec.map { |x| Float(x) rescue nil }.compact,
            'restricts'     => rest,
            'metadata'      => md
          }
          dp['labels'] = labels if labels
          dp
        end
        { 'count' => dps.length, 'datapoints' => dps }
      end,

      sample_output: lambda do
        {
          'count' => 1,
          'datapoints' => [
            {
              'datapointId' => 'doc-abc123:0',
              'featureVector' => [0.01, 0.02],
              'restricts' => [
                { 'namespace' => 'doc', 'allowTokens' => ['doc-abc123'] },
                { 'namespace' => 'tenant', 'allowTokens' => ['acme'] },
                { 'namespace' => 'source', 'allowTokens' => ['handbook'] }
              ],
              'metadata' => {
                'doc_id' => 'doc-abc123',
                'source' => { 'file_path' => 'drive://Policies/PTO.md' },
                'span'   => { 'start' => 0, 'end' => 1799 },
                'tokens' => 42,
                'extra'  => { 'department' => 'HR' }
              }
            }
          ]
        }
      end
    },
    map_vertex_embeddings: {
      title: 'Ingestion - Map Vertex embeddings to ids',
      subtitle: 'Align requests[*].id with embed_text.predictions[*].embeddings.values',
      display_priority: 7,

      input_fields: lambda do |object_definitions, _connection, _cfg|
        [
          { name: 'requests',  type: 'array', of: 'object', optional: false,
            hint: 'From build_embedding_requests: [{id,text,metadata}]',
            properties: [
              { name: 'id' }, { name: 'text' },
              { name: 'metadata', type: 'object' }
            ]
          },
          # Option A: map the whole Vertex output object here (which contains predictions: [])
          { name: 'predictions_root', type: 'object', optional: true, properties: [
              { name: 'predictions', type: 'array', of: 'object',
                properties: object_definitions['vertex_prediction'] }
            ],
            hint: 'If your Vertex step outputs {predictions:[...]}, map it here.'
          },
          # Option B: map just the predictions array directly
          { name: 'predictions', type: 'array', of: 'object', optional: true,
            properties: object_definitions['vertex_prediction'],
            hint: 'Or map predictions[*] directly if you already have the array.'
          },
          { name: 'check_dimension', type: 'integer', optional: true,
            hint: 'Optional: expected vector length (index dimension)' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'embeddings', type: 'array', of: 'object',
            properties: object_definitions['embedding_pair'] },
          { name: 'vector_dim', type: 'integer' },
          # Add a wrapper object so users can map a single pill → embeddings_bundle
          { name: 'bundle', type: 'object', properties: object_definitions['embedding_bundle'] }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input|
        t0   = Time.now
        corr = call(:guid)
        reqs  = input['requests'].is_a?(Array) ? input['requests'] : []
        preds = []
        if input['predictions'].is_a?(Array)
          preds = input['predictions']
        elsif input['predictions_root'].is_a?(Hash) && input['predictions_root']['predictions'].is_a?(Array)
          preds = input['predictions_root']['predictions']
        end
        error('requests and predictions length mismatch') if reqs.length != preds.length

        out = []
        dim = nil
        reqs.each_with_index do |r, i|
          id = (r['id'] || r[:id]).to_s
          error("requests[#{i}].id is required") if id.empty?
          # Try common Vertex shapes in priority order
          vec = preds[i].dig('embeddings','values') ||
                (preds[i]['embeddings'].is_a?(Array) && preds[i].dig('embeddings',0,'values')) ||
                # support vertex_prediction.embeddings_list[]
                (preds[i]['embeddings_list'].is_a?(Array) && preds[i].dig('embeddings_list',0,'values')) ||
                preds[i]['values'] ||
                preds[i]['embedding']
          error("predictions[#{i}] missing embeddings.values") if !vec.is_a?(Array) || vec.empty?
          vec = vec.map { |x| Float(x) rescue nil }.compact
          error("predictions[#{i}] contains non-numeric values") if vec.empty?
          dim ||= vec.length
          error("predictions[#{i}] dimension mismatch (#{vec.length} != #{dim})") if vec.length != dim
          if input['check_dimension'].to_i > 0
            exp = input['check_dimension'].to_i
            error("vector dimension #{vec.length} != expected #{exp}") if vec.length != exp
          end
          out << { 'id' => id, 'embedding' => vec }
        end
        bundle = { 'embeddings' => out, 'count' => out.length, 'vector_dim' => (dim || 0), 'trace_id' => corr }
        {
          'count'      => out.length,
          'embeddings' => out,
          'vector_dim' => (dim || 0),
          'bundle'     => bundle
        }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'count' => 2,
          'embeddings' => [
            { 'id' => 'doc-abc123:0', 'embedding' => [0.01, 0.02, 0.03] },
            { 'id' => 'doc-abc123:1', 'embedding' => [0.04, 0.05, 0.06] }
          ],
          'vector_dim' => 3,
          'bundle' => {
            'embeddings' => [
              { 'id' => 'doc-abc123:0', 'embedding' => [0.01, 0.02, 0.03] },
              { 'id' => 'doc-abc123:1', 'embedding' => [0.04, 0.05, 0.06] }
            ],
            'count' => 2,
            'vector_dim' => 3,
            'trace_id' => 'sample'
          },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },
    enrich_chunks_with_metadata: {
      title: 'Ingestion - Enrich chunks with document metadata',
      description: 'Merges document-level metadata (source, uri, mime, author, version) into each chunk.metadata.',

      input_fields: lambda do |object_definitions, _connection, _cfg|
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false,
            properties: object_definitions['chunk']},
          { name: 'document_metadata', type: 'object', optional: false,
            properties: [
              { name: 'document_id' }, { name: 'file_name' }, { name: 'uri' },
              { name: 'mime_type' }, { name: 'source_system' }, { name: 'version' },
              { name: 'ingested_at' }, { name: 'labels', type: 'array', of: 'string' }
            ] }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'chunks', type: 'array', of: 'object',
            properties: object_definitions['chunk'] }
        ].concat(object_definitions['envelope_fields'] )
      end,

      sample_output: lambda do |_|
        {
          'chunks' => [
            { 'chunk_id' => 'doc1#0001', 'text' => '...', 'metadata' => {
              'document_id' => 'doc1', 'uri' => 'gs://bucket/a.pdf', 'mime_type' => 'application/pdf'
            }}
          ],
          'ok' => true, 'telemetry' => { 'http_status' => 200, 'message' => 'OK' }
        }
      end,
      execute: lambda do |_connection, input|
        t0   = Time.now
        corr = call(:guid)
        meta = input['document_metadata'] || {}
        chunks = Array(input['chunks']).map do |c|
          m = (c['metadata'] || {}).merge(meta) { |_k, old_v, new_v| old_v.nil? ? new_v : old_v }
          c.merge('metadata' => m)
        end
        { 'chunks' => chunks }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end
    },

    # ---- 3.  Embedding  --------------------------------------------------
    build_embedding_requests: {
      title: 'Ingestion - Build embedding requests from chunks',
      subtitle: '[{id, text, metadata}] for your embedding step',
      display_priority: 8,
      # chunks[*] -> [{id, text, metadata}] for embedding

      input_fields: lambda do |object_definitions, _connection, _config_fields|
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false, properties: object_definitions['chunk'],
            hint: 'Map the Chunks list from “Prepare document for indexing”.' },
          { name: 'debug',  type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'requests', type: 'array', of: 'object', properties: [
            { name: 'id' }, { name: 'text' }, { name: 'metadata', type: 'object' }
          ] }
        ]
      end,

      execute: lambda do |_connection, input|
        chunks = call(:require_array_of_objects, input['chunks'], 'chunks')
        reqs = chunks.map do |c|
          {
            'id'       => call(:resolve_chunk_id, c),
            'text'     => c['text'].to_s,
            'metadata' => call(:sanitize_metadata, c['metadata'])
          }
        end
        out = { 'requests' => reqs, 'count' => reqs.length }
        out['trace_id'] = call(:guid) if call(:safe_bool, input['debug'])
        out
      end,

      sample_output: lambda do
        {
          'count' => 2,
          'requests' => [
            { 'id' => 'docA:0', 'text' => '…', 'metadata' => { 'department' => 'HR' } },
            { 'id' => 'docA:1', 'text' => '…', 'metadata' => { } }
          ]
        }
      end
    },
    build_embedding_requests_batch: {
      title: 'Ingestion - Build embedding requests',
      subtitle: 'Accepts documents[*].chunks or chunks[*]; flattens to [{id,text,metadata}]',
      batch: true,
      display_priority: 8,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'documents', type: 'array', of: 'object', optional: true,
            hint: 'Each doc should include chunks:[...]' },
          { name: 'chunks', type: 'array', of: 'object', optional: true },
          { name: 'debug',  type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'requests', type: 'array', of: 'object', properties: [
            { name: 'id' }, { name: 'text' }, { name: 'metadata', type: 'object' }
          ] },
          { name: 'trace_id', optional: true }
        ]
      end,

      execute: lambda do |_connection, input|
        chunks = call(:flatten_chunks_input, input)
        reqs = chunks.map do |c|
          {
            'id'       => call(:resolve_chunk_id, c),
            'text'     => c['text'].to_s,
            'metadata' => call(:sanitize_metadata, c['metadata'])
          }
        end
        out = { 'requests' => reqs, 'count' => reqs.length }
        out['trace_id'] = call(:guid) if call(:safe_bool, input['debug'])
        out
      end,
    
      sample_output: lambda do
        {
          'count' => 2,
          'requests' => [
            { 'id' => 'doc-abc123:0', 'text' => 'First slice…', 'metadata' => { 'department' => 'HR' } },
            { 'id' => 'doc-abc123:1', 'text' => 'Second slice…', 'metadata' => {} }
          ],
          'trace_id' => 'trace-emb-batch-1'
        }
      end
    },
    attach_embeddings: {
      title: 'Ingestion - Attach embeddings to chunks',
      help: lambda do |_|
        { body: 'Fast path: map “Embeddings bundle” from previous step; Advanced allows custom keys/modes' }
      end,
      display_priority: 7,

      input_fields: lambda do |object_definitions, _connection, cfg|
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false, properties: object_definitions['chunk'],
            hint: 'Map the Chunks list from “Prepare document for indexing” or later.' },
          # --- Simple / Recommended path ---
          { name: 'embeddings_bundle', type: 'object', optional: true, properties: object_definitions['embedding_bundle'],
            hint: 'Map the whole output of “Map Vertex embeddings to ids”.' },
          # --- Alternate inputs (only one is needed) ---
          { name: 'embeddings', label: 'Embeddings [{id,embedding}]', type: 'array', of: 'object', optional: true,
            properties: object_definitions['embedding_pair'], hint: 'Use when you already have the list of {id, embedding} objects.' },
          { name: 'vectors', label: 'Vectors (parallel to chunks)', type: 'array', of: 'object', optional: true,
            properties: object_definitions['vector_item'],  hint: 'Use only if you have no ids; requires Alignment = by_index.' },
          { name: 'alignment', control_type: 'select', optional: true,
            pick_list: 'attach_alignment_modes', hint: 'Default: by_id (safer)' },
          { name: 'show_advanced', type: 'boolean', control_type: 'checkbox', optional: true, sticky: true,
            label: 'Show advanced options' },
          # --- Advanced ---
          (cfg && cfg['show_advanced'] ? { name: 'id_key', optional: true, hint: 'Default id' } : nil),
          (cfg && cfg['show_advanced'] ? { name: 'embedding_key', optional: true, hint: 'Default embedding' } : nil),
          (cfg && cfg['show_advanced'] ? { name: 'strict', type: 'boolean', control_type: 'checkbox', optional: true,
            hint: 'When true, raise on missing/duplicate ids instead of skipping.' } : nil),
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ].compact
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk'] }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input|
      t0   = Time.now
      corr = call(:guid)
        align = (input['alignment'] || 'by_id').to_s
        id_key  = (input['id_key'] || 'id').to_s
        emb_key = (input['embedding_key'] || 'embedding').to_s
        strict  = !!input['strict']

        chunks = call(:require_array_of_objects, input['chunks'], 'chunks')

        # 1) Normalize embedding pairs
        pairs = []
        if input['embeddings_bundle'].is_a?(Hash)
          pairs = call(:detect_pairs_from_bundle, input['embeddings_bundle'])
        elsif input['embeddings'].is_a?(Array)
          pairs = input['embeddings'].map do |e|
            next nil unless e.is_a?(Hash)
            { 'id' => e[id_key].to_s, 'embedding' => e[emb_key] }
          end.compact
      elsif input['vectors'].is_a?(Array)
          error('Alignment must be by_index when using vectors') unless align == 'by_index'
          vecs = input['vectors'].map { |v| v.is_a?(Hash) ? v['values'] : v }
          error('vectors length must equal chunks length') unless vecs.length == chunks.length
          # keep vecs in scope for by_index branch
        end

        # 2) Merge
        out_chunks =
        if align == 'by_index'
          if input['vectors'].is_a?(Array)
            chunks.each_with_index.map do |c, i|
              vec = vecs[i]  # use normalized per-index array
              next c unless vec.is_a?(Array) && !vec.empty?
              c.merge('embedding' => vec.map { |x| Float(x) rescue nil }.compact)
            end
          else
            error('by_index alignment requires vectors[*].values (or raw float arrays)')
          end
          else # by_id (default)
            idx = {}
            dupes = []
            pairs.each do |p|
              pid = p['id'].to_s
              if pid.empty?
                error('Found embedding without id') if strict
                next
              end
              dupes << pid if idx.key?(pid) && strict
              idx[pid] = p['embedding']
            end
            error("Duplicate ids in embeddings: #{dupes.uniq.join(', ')}") if strict && !dupes.empty?
            chunks.map do |c|
              cid = call(:resolve_chunk_id, c)
              vec = idx[cid]
              next c unless vec.is_a?(Array) && !vec.empty?
              c.merge('embedding' => vec.map { |x| Float(x) rescue nil }.compact)
            end
          end

      out = { 'count' => out_chunks.length, 'chunks' => out_chunks }
      out['trace_id'] = call(:guid) if call(:safe_bool, input['debug'])
      out.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
      {
        'count' => 2,
        'chunks' => [{ 'chunk_id' => 'docA:0', 'embedding' => [0.1, 0.2] }],
        'ok' => true,
        'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
      }
      end

    },
    attach_embeddings_batch: {
      title: 'Ingestion - Attach embeddings to chunks',
      subtitle: 'Supports [{chunks,embeddings}] or top-level chunks/embeddings',
      batch: true,
      display_priority: 7,

      input_fields: lambda do |object_definitions, _connection, cfg|
        [
          # Either process one big set...
          { name: 'chunks', type: 'array', of: 'object', optional: true,
            properties: object_definitions['chunk'], hint: 'Flat list (optional if using pairs[])' },
          { name: 'embeddings_bundle', type: 'object', optional: true, properties: object_definitions['embedding_bundle'] },
          { name: 'embeddings', type: 'array', of: 'object', optional: true,
            properties: object_definitions['embedding_pair'] },
          { name: 'vectors', type: 'array', of: 'object', optional: true, properties: object_definitions['vector_item'] },
          { name: 'alignment', control_type: 'select', optional: true, pick_list: 'attach_alignment_modes' },
          # ...or many sets via pairs[]
          { name: 'pairs', type: 'array', of: 'object', optional: true,
            hint: 'Each: {chunks, embeddings_bundle?, embeddings?, vectors?, alignment?, id_key?, embedding_key?}',
            properties: [
              { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk'] },
              { name: 'embeddings_bundle', type: 'object', properties: object_definitions['embedding_bundle'] },
              { name: 'embeddings', type: 'array', of: 'object', properties: object_definitions['embedding_pair'] },
              { name: 'vectors', type: 'array', of: 'object', properties: object_definitions['vector_item'] },
              { name: 'alignment', control_type: 'select', pick_list: 'attach_alignment_modes' },
              { name: 'id_key' }, { name: 'embedding_key' }
            ]
          },
          { name: 'show_advanced', type: 'boolean', control_type: 'checkbox', optional: true, sticky: true,
            label: 'Show advanced options' },
          (cfg && cfg['show_advanced'] ? { name: 'id_key', optional: true, hint: 'Default id' } : nil),
          (cfg && cfg['show_advanced'] ? { name: 'embedding_key', optional: true, hint: 'Default embedding' } : nil),
          (cfg && cfg['show_advanced'] ? { name: 'strict', type: 'boolean', control_type: 'checkbox', optional: true } : nil),
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ].compact
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk'] },
          { name: 'trace_id', optional: true }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input|
        t0   = Time.now
        corr = call(:guid)
        id_key  = (input['id_key'] || 'id').to_s
        emb_key = (input['embedding_key'] || 'embedding').to_s
        strict  = !!input['strict']
        merged = []

        attach_once = lambda do |cList, opts|
          align   = (opts['alignment'] || input['alignment'] || 'by_id').to_s
          idk     = (opts['id_key'] || id_key).to_s
          embk    = (opts['embedding_key'] || emb_key).to_s
          # Build pairs from whichever embedding input is present
          pairs =
            if opts['embeddings_bundle'].is_a?(Hash)
              call(:detect_pairs_from_bundle, opts['embeddings_bundle'])
            elsif opts['embeddings'].is_a?(Array)
              opts['embeddings'].map { |e| { 'id' => e[idk].to_s, 'embedding' => e[embk] } }.compact
            else
              []
            end

          if align == 'by_index'
            vecs = call(:ensure_array, opts['vectors']).map { |v| v.is_a?(Hash) ? v['values'] : v }
            error('vectors length must equal chunks length') unless vecs.length == cList.length
            return cList.each_with_index.map { |c,i|
              v = vecs[i]; v.is_a?(Array) && !v.empty? ? c.merge('embedding' => v.map { |x| Float(x) rescue nil }.compact) : c
            }
          else
            # by_id
            idx = {}
            dups = []
            pairs.each do |p|
              pid = p['id'].to_s
              if pid.empty?
                error('Found embedding without id') if strict
                next
              end
              dups << pid if idx.key?(pid) && strict
              idx[pid] = p['embedding']
            end
            error("Duplicate ids in embeddings: #{dups.uniq.join(', ')}") if strict && !dups.empty?
            return cList.map { |c|
              vec = idx[call(:resolve_chunk_id, c)]
              vec.is_a?(Array) && !vec.empty? ? c.merge('embedding' => vec.map { |x| Float(x) rescue nil }.compact) : c
            }
          end
        end

        if input['pairs'].is_a?(Array) && !input['pairs'].empty?
          input['pairs'].each do |p|
            next unless p.is_a?(Hash)
            cList = call(:require_array_of_objects, p['chunks'], 'chunks')
            merged.concat(attach_once.call(cList, p))
          end
        elsif input['chunks'].is_a?(Array)
          cList = call(:require_array_of_objects, input['chunks'], 'chunks')
          merged.concat(attach_once.call(cList, input))
        else
          error('Supply either top-level chunks[...] (+ embeddings*) or pairs[*].')
        end

        out = { 'count' => merged.length, 'chunks' => merged }
        out['trace_id'] = call(:guid) if call(:safe_bool, input['debug'])
        out.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'count' => 2,
          'chunks' => [
            {
              'chunk_id' => 'doc-abc123:0',
              'doc_id' => 'doc-abc123',
              'text' => 'First slice…',
              'embedding' => [0.11, 0.12, 0.13],
              'span_start' => 0,
              'span_end' => 1799,
              'source' => { 'file_path' => 'drive://Reports/2025/summary.txt', 'checksum' => '3a2b9c…' },
              'metadata' => { 'department' => 'HR' },
              'created_at' => '2025-10-15T12:00:00Z'
            }
          ],
          'trace_id' => 'trace-attach-batch-1',
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # ---- 3.  Emit --------------------------------------------------------
    extract_chunks: {
      title: 'Ingestion - Extract chunks',
      subtitle: 'Accepts {chunks:[...]} or {results:[{chunks:[...]}]} and emits {chunks:[...]}',
      display_priority: 6,

      input_fields: lambda do |object_definitions, _connection, _config_fields|
        [
          { name: 'document', type: 'object', optional: true,
            properties: (object_definitions['prep_result'] || []),
            hint: 'Output of Prepare document for indexing (single)' },
          { name: 'batch', type: 'object', optional: true,
            properties: (object_definitions['prep_batch'] || []),
            hint: 'Output of Prepare multiple documents for indexing (batch)' },
          { name: 'chunks', type: 'array', of: 'object', optional: true,
            properties: object_definitions['chunk'],
            hint: 'Alternative: map a flat chunks list directly' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk'] }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input|
        t0    = Time.now
        corr  = call(:guid)
        doc   = input['document'].is_a?(Hash) ? input['document'] : {}
        batch = input['batch'].is_a?(Hash)    ? input['batch']    : {}

        chunks =
          if input['chunks'].is_a?(Array)
            input['chunks']
          elsif doc['chunks'].is_a?(Array)
            doc['chunks']
          elsif batch['results'].is_a?(Array)
            batch['results'].flat_map { |r| (r || {})['chunks'] || [] }
          else
            []
          end
          error('No chunks found: map one of document.chunks, batch.results[].chunks, or chunks[].') if chunks.empty?


        { 'chunks' => chunks }
          .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,
      sample_output: lambda do
        {
          'chunks' => [
            {
              'doc_id' => 'doc-abc123',
              'chunk_id' => 'doc-abc123:0',
              'index' => 0,
              'text' => 'First slice…',
              'tokens' => 42,
              'span_start' => 0,
              'span_end' => 1799,
              'source' => { 'file_path' => 'drive://Reports/2025/summary.txt', 'checksum' => '3a2b9c…' },
              'metadata' => { 'department' => 'HR' },
              'created_at' => '2025-10-15T12:00:00Z'
            }
          ],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },
    to_data_table_rows: {
      title: 'Ingestion - To Data Table rows',
      subtitle: 'Slim corpus rows from chunks for persistence',
      display_priority: 6,

      config_fields: [
        { name: 'row_profile', control_type: 'select', pick_list: 'table_row_profiles',
          sticky: true, optional: true, hint: 'Standard is safe default.' },
        { name: 'show_advanced', type: 'boolean', control_type: 'checkbox', sticky: true,
          label: 'Show advanced options', hint: 'Reveal metadata caps.' }
      ],

      input_fields: lambda do |object_definitions, _connection, cfg|
        advanced = !!cfg['show_advanced']
        fields = [
          { name: 'document', type: 'object', optional: true, properties: (object_definitions['prep_result'] || []),
            hint: 'Directly map the whole output of “Prepare document for indexing”' },
          { name: 'batch', type: 'object', optional: true, properties: (object_definitions['prep_batch'] || []),
            hint: 'Directly map the whole output of “Prepare multiple documents for indexing”' },
          { name: 'table_name', optional: true, hint: 'Optional label for your downstream step; result is unaffected when absent.' },
          { name: 'include_text', type: 'boolean', control_type: 'checkbox', optional: true,
            hint: 'Default true' }
        ]
        if advanced
          fields << { name: 'metadata_prefix', optional: true, hint: 'Prefix for flattened metadata (Wide). Default meta_' }
          fields << { name: 'metadata_max_keys', type: 'integer', optional: true, hint: 'Cap flattened keys. Default 50' }
          fields << { name: 'metadata_max_bytes', type: 'integer', optional: true, hint: 'Total bytes cap for flattened metadata. Default 4096' }
        end
        fields
      end,

      output_fields: lambda do |object_definitions, _cfg|
        [
          { name: 'table' },
          { name: 'profile' },
          { name: 'count', type: 'integer' },
          { name: 'rows', type: 'array', of: 'object', properties: object_definitions['table_row'] }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input, _schema = nil, _input_schema_name = nil, _connection_schema = nil, cfg = {}|
        t0    = Time.now
        corr  = call(:guid)
        prof  = (cfg['row_profile'] || 'standard').to_s
        include_text = input['include_text'].nil? ? true : !!input['include_text']
        chunks =
          if input['document'].is_a?(Hash) && input['document']['chunks'].is_a?(Array)
            input['document']['chunks']
          elsif input['batch'].is_a?(Hash) && input['batch']['results'].is_a?(Array)
            input['batch']['results'].flat_map { |r| (r || {})['chunks'] || [] }
          else
            call(:require_array_of_objects, input['chunks'], 'chunks')
          end
        caps = {
          'metadata_max_keys'  => input['metadata_max_keys'],
          'metadata_max_bytes' => input['metadata_max_bytes']
        }.compact
        meta_prefix = (input['metadata_prefix'] || 'meta_').to_s

        rows = chunks.map do |c|
          call(:build_table_row, c, prof, include_text, meta_prefix, caps)
        end

        {
          'table'   => input['table_name'].to_s,
          'profile' => prof,
          'count'   => rows.length,
          'rows'    => rows
        }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'table' => 'kb_chunks',
          'profile' => 'standard',
          'count' => 2,
          'rows'  => [
            {
              'id' => 'doc-abc123:0',
              'doc_id' => 'doc-abc123',
              'file_path' => 'drive://Reports/2025/summary.txt',
              'checksum' => '3a2b9c…',
              'tokens' => 42,
              'span_start' => 0,
              'span_end' => 1799,
              'created_at' => '2025-10-15T12:00:00Z',
              'text' => 'First slice…'
            }
          ],
          'ok'    => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end

    },
    to_data_table_rows_batch: {
      title: 'Ingestion - To Data Table rows',
      subtitle: 'Flattens multiple documents into slim corpus rows',
      batch: true,
      display_priority: 6,

      config_fields: [
        { name: 'row_profile', control_type: 'select', pick_list: 'table_row_profiles',
          sticky: true, optional: true, hint: 'Standard is safe default.' },
        { name: 'show_advanced', type: 'boolean', control_type: 'checkbox', sticky: true,
          label: 'Show advanced options', hint: 'Reveal schema designer and metadata caps.' },
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do |_object_definitions, _connection, cfg|
        advanced = !!cfg['show_advanced']
        fields = [
          { name: 'documents', type: 'array', of: 'object', optional: true,
            hint: 'Each doc contains chunks:[...]' },
          { name: 'chunks', type: 'array', of: 'object', optional: true,
            hint: 'You can also pass a flat chunks list.' },
          { name: 'table_name', optional: true, hint: 'Optional label for your downstream step' },
          { name: 'include_text', type: 'boolean', control_type: 'checkbox', optional: true,
            hint: 'Default true' }
        ]
        if advanced
          fields << { name: 'metadata_prefix', optional: true, hint: 'Prefix for flattened metadata (Wide). Default meta_' }
          fields << { name: 'metadata_max_keys', type: 'integer', optional: true, hint: 'Cap flattened keys. Default 50' }
          fields << { name: 'metadata_max_bytes', type: 'integer', optional: true, hint: 'Total bytes cap for flattened metadata. Default 4096' }
        end
        fields
      end,

      output_fields: lambda do |object_definitions, cfg|
        default_fields = [
          { name: 'table' },
          { name: 'profile' },
          { name: 'count', type: 'integer' },
          { name: 'rows', type: 'array', of: 'object', properties: object_definitions['table_row'] }
        ]
        call(:resolve_output_schema, default_fields, cfg, object_definitions)
      end,

      execute: lambda do |_connection, input, _schema = nil, _input_schema_name = nil, _connection_schema = nil, cfg = {}|
        t0    = Time.now
        corr  = call(:guid)
        prof  = (cfg['row_profile'] || 'standard').to_s
        include_text = input['include_text'].nil? ? true : !!input['include_text']
        chunks = call(:flatten_chunks_input, input)
        caps = {
          'metadata_max_keys'  => input['metadata_max_keys'],
          'metadata_max_bytes' => input['metadata_max_bytes']
        }.compact
        meta_prefix = (input['metadata_prefix'] || 'meta_').to_s

        rows = chunks.map do |c|
          call(:build_table_row, c, prof, include_text, meta_prefix, caps)
        end

        {
          'table'   => input['table_name'].to_s,
          'profile' => prof,
          'count'   => rows.length,
          'rows'    => rows
        }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,
    
      sample_output: lambda do
        {
          'table' => 'kb_chunks',
          'profile' => 'wide',
          'count' => 2,
          'rows' => [
            {
              'id' => 'doc-abc123:0',
              'doc_id' => 'doc-abc123',
              'file_path' => 'drive://Reports/2025/summary.txt',
              'checksum' => '3a2b9c…',
              'tokens' => 42,
              'span_start' => 0,
              'span_end' => 1799,
              'created_at' => '2025-10-15T12:00:00Z',
              'text' => 'First slice…',
              'meta_department' => 'HR',
              'meta_owner' => 'PeopleOps'
            }
          ],
          'ok'    => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    make_gcs_manifest: {
      title: 'Ingestion - Make GCS manifest',
      subtitle: 'Build {object_name, content_type, body} for corpus snapshot',
      display_priority: 5,

      input_fields: lambda do |object_definitions, _connection, _cfg|
        [
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'doc_id', optional: true },
          { name: 'chunks', type: 'array', of: 'object', optional: false, properties: object_definitions['chunk'],
            hint: 'Map the Chunks list.' },
          { name: 'prefix', optional: true, hint: 'e.g., manifests/' },
          { name: 'format', optional: true, hint: 'json|ndjson (default json)' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'object_name' },
          { name: 'content_type' },
          { name: 'bytes', type: 'integer' },
          { name: 'body' }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |_connection, input|
        t0 = Time.now
        corr = call(:guid)
        ns   = (input['namespace'] || 'default').to_s
        did  = (input['doc_id'] || 'multi').to_s
        pre  = (input['prefix'] || '').to_s
        fmt  = (input['format'] || 'json').to_s.downcase
        ts   = Time.now.utc.strftime('%Y%m%dT%H%M%SZ')
        oname = "#{pre}#{ns}/#{did}/manifest-#{ts}.#{fmt == 'ndjson' ? 'ndjson' : 'json'}"

        records = call(:require_array_of_objects, input['chunks'], 'chunks').map do |c|
          {
            'id'        => call(:resolve_chunk_id, c),
            'doc_id'    => c['doc_id'],
            'text'      => c['text'].to_s,
            'tokens'    => c['tokens'],
            'span'      => { 'start' => c['span_start'], 'end' => c['span_end'] },
            'file_path' => c.dig('source','file_path'),
            'checksum'  => c.dig('source','checksum'),
            'metadata'  => call(:sanitize_metadata, c['metadata'])
          }
        end

        if fmt == 'ndjson'
          body = records.map { |r| r.to_json }.join("\n")
          ctype = 'application/x-ndjson'
        else
          body = { 'namespace' => ns, 'doc_id' => did, 'generated_at' => Time.now.utc.iso8601, 'records' => records }.to_json
          ctype = 'application/json'
        end

        {
          'object_name'  => oname,
          'content_type' => ctype,
          'bytes'        => body.bytesize,
          'body'         => body
        }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,
    
      sample_output: lambda do
        {
          'object_name'  => 'manifests/ns/doc/manifest-20250101T000000Z.json',
          'content_type' => 'application/json',
          'bytes'        => 123,
          'body'         => '{…}',
          'ok'           => true,
          'telemetry'    => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },
    make_gcs_manifest_batch: {
      title: 'Ingestion - Make GCS manifests',
      subtitle: 'One manifest per document; supports json or ndjson',
      batch: true,
      display_priority: 6,

      input_fields: lambda do |_object_definitions, _connection, _cfg|
        [
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'documents', type: 'array', of: 'object', optional: false,
            hint: 'Each doc requires {doc_id?, chunks:[...]} (doc_id inferred if missing)' },
          { name: 'prefix', optional: true, hint: 'e.g., manifests/' },
          { name: 'format', optional: true, hint: 'json|ndjson (default json)' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        [
          { name: 'count', type: 'integer' },
          { name: 'manifests', type: 'array', of: 'object', properties: [
            { name: 'object_name' },
            { name: 'content_type' },
            { name: 'bytes', type: 'integer' },
            { name: 'body' }
          ] }
        ]
      end,

      execute: lambda do |_connection, input|
        ns  = (input['namespace'] || 'default').to_s
        pre = (input['prefix'] || '').to_s
        fmt = (input['format'] || 'json').to_s.downcase
        ts  = Time.now.utc.strftime('%Y%m%dT%H%M%SZ')

        manifests = []
        call(:ensure_array, input['documents']).each do |doc|
          chunks = call(:ensure_array, doc['chunks'])
          # Derive doc_id if not present (use first chunk’s doc_id or fallback uuid)
          did = (doc['doc_id'] || chunks.dig(0, 'doc_id') || call(:guid)).to_s
          oname = "#{pre}#{ns}/#{did}/manifest-#{ts}.#{fmt == 'ndjson' ? 'ndjson' : 'json'}"

          records = chunks.map do |c|
            {
              'id'        => call(:resolve_chunk_id, c),
              'doc_id'    => c['doc_id'],
              'text'      => c['text'].to_s,
              'tokens'    => c['tokens'],
              'span'      => { 'start' => c['span_start'], 'end' => c['span_end'] },
              'file_path' => c.dig('source','file_path'),
              'checksum'  => c.dig('source','checksum'),
              'metadata'  => call(:sanitize_metadata, c['metadata'])
            }
          end

          if fmt == 'ndjson'
            body  = records.map { |r| r.to_json }.join("\n")
            ctype = 'application/x-ndjson'
          else
            body  = { 'namespace' => ns, 'doc_id' => did, 'generated_at' => Time.now.utc.iso8601, 'records' => records }.to_json
            ctype = 'application/json'
          end

          manifests << {
            'object_name'  => oname,
            'content_type' => ctype,
            'bytes'        => body.bytesize,
            'body'         => body
          }
        end

        { 'count' => manifests.length, 'manifests' => manifests }
      end,

      sample_output: lambda do
        {
          'count' => 2,
          'manifests' => [
            {
              'object_name' => 'manifests/hr-knowledge-v1/doc-abc123/manifest-20251015T120000Z.json',
              'content_type' => 'application/json',
              'bytes' => 2048,
              'body' => '{"namespace":"hr-knowledge-v1","doc_id":"doc-abc123","generated_at":"2025-10-15T12:00:00Z","records":[…]}'
            }
          ]
        }
      end
    },

    # --- SERVE (QUERY)  ---------------------------------------------------
    # 7.  Build vector query
    build_vector_query: {
      title: 'Serve - Build vector query',
      subtitle: 'Normalize text + optional embedding into a search request object',
      display_priority: 5,

      input_fields: lambda do |_object_definitions, _connection, _cfg|
        [
          { name: 'query_text', optional: true, control_type: 'text-area' },
          { name: 'query_embedding', type: 'array', of: 'number', optional: true },
          { name: 'namespace', optional: true },
          { name: 'top_k', type: 'integer', optional: true, hint: 'Default 20' }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [{ name: 'query', type: 'object' }]
      end,

      execute: lambda do |_connection, input|
        obj = {
          'namespace' => (input['namespace'] || '').to_s,
          'top_k' => (input['top_k'] || 20).to_i
        }
        if input['query_embedding'].is_a?(Array)
          obj['mode'] = 'vector'
          obj['embedding'] = input['query_embedding']
        else
          obj['mode'] = 'text'
          obj['query_text'] = input['query_text'].to_s
        end
        obj['top_k'] = 1 if obj['top_k'] < 1
        { 'query' => obj }
      end,

      sample_output: lambda do
        {
          'query' => {
            'namespace' => 'hr-knowledge-v1',
            'top_k' => 20,
            'mode' => 'text',
            'query_text' => 'What is our PTO policy?'
          }
        }
      end
    },

    # 8.  Merge search results
    merge_search_results: {
      title: 'Serve - Merge search results',
      subtitle: 'Normalize, dedupe by chunk_id, keep best score',
      display_priority: 5,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do |_object_definitions, _connection, _cfg|
        [
          { name: 'results', type: 'array', of: 'object', optional: false }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'count', type: 'integer' },
          { name: 'results', type: 'array', of: 'object' }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0 = Time.now
        corr = call(:guid)
        results = call(:require_array_of_objects, input['results'], 'results')
        out = {}
        results.each do |r|
          id = (r['chunk_id'] || r['id']).to_s
          next if id.empty?
          score = r['score'].to_f
          if !out.key?(id) || score > out[id]['score'].to_f
            out[id] = {
              'chunk_id' => id,
              'doc_id'   => r['doc_id'],
              'text'     => r['text'].to_s,
              'score'    => score,
              'metadata' => call(:sanitize_metadata, r['metadata']),
              'source'   => r['source']   || {}
            }
          end
        end
        merged = out.values.sort_by { |x| -x['score'].to_f }
        { 'count' => merged.length, 'results' => merged }
          .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'count' => 2,
          'results' => [
            {
              'chunk_id' => 'doc-abc123:0',
              'doc_id' => 'doc-abc123',
              'text' => 'First slice…',
              'score' => 0.92,
              'metadata' => { 'tokens' => 42 },
              'source' => { 'file_path' => 'drive://Reports/2025/summary.txt' }
            },
            {
              'chunk_id' => 'doc-xyz789:3',
              'doc_id' => 'doc-xyz789',
              'text' => 'Another match…',
              'score' => 0.87,
              'metadata' => {},
              'source' => { 'file_path' => 'drive://Policies/PTO.md' }
            }
          ]
        }
      end
    },

    # 9.  Select top-k by score
    # 10. Select context by token budget
    select_context_by_token_budget: {
      title: 'Serve - Select context by token budget',
      subtitle: 'Greedy pack by tokens with optional per-doc cap',
      display_priority: 4,

      input_fields: lambda do |_object_definitions, _connection, _cfg|
        [
          { name: 'results', type: 'array', of: 'object', optional: false },
          { name: 'token_budget', type: 'integer', optional: false },
          { name: 'max_per_doc', type: 'integer', optional: true, hint: 'Default 3' }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [
          { name: 'total_tokens', type: 'integer' },
          { name: 'count', type: 'integer' },
          { name: 'context', type: 'array', of: 'object' }
        ]
      end,

      execute: lambda do |_connection, input|
        budget = (input['token_budget'] || 1200).to_i
        perdoc = (input['max_per_doc'] || 3).to_i
        used = 0
        per = Hash.new(0)
        picked = []
        results = call(:require_array_of_objects, input['results'], 'results')
        results.each do |r|
          doc = r['doc_id'].to_s
          t = (r['tokens'] || r.dig('metadata','tokens') || r['text'].to_s.split(/\s+/).size).to_i
          next if per[doc] >= perdoc
          break if used + t > budget
          picked << r.merge('tokens' => t)
          used += t
          per[doc] += 1
        end
        { 'total_tokens' => used, 'count' => picked.length, 'context' => picked }
      end,

      sample_output: lambda do
        {
          'total_tokens' => 120,
          'count' => 2,
          'context' => [
            {
              'chunk_id' => 'doc-abc123:0',
              'doc_id' => 'doc-abc123',
              'text' => 'First slice…',
              'tokens' => 60,
              'metadata' => { 'section' => 'Benefits' }
            },
            {
              'chunk_id' => 'doc-xyz789:1',
              'doc_id' => 'doc-xyz789',
              'text' => 'Second slice…',
              'tokens' => 60,
              'metadata' => {}
            }
          ]
        }
      end

    },
    # 11. Build citation map
    # 12. Build messages for Gemini
    build_messages_gemini: {
      title: 'Serve - Build Gemini messages',
      subtitle: 'Construct system/user messages with injected context',
      display_priority: 3,

      input_fields: lambda do |_object_definitions, _connection, _cfg|
        [
          { name: 'user_query', optional: false, control_type: 'text-area' },
          { name: 'context', type: 'array', of: 'object', optional: true },
          { name: 'system_preamble', optional: true, control_type: 'text-area',
            hint: 'High-level instructions and guardrails' }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [{ name: 'messages', type: 'array', of: 'object' }]
      end,

      execute: lambda do |_connection, input|
        sys = (input['system_preamble'] || 'Answer using only the provided context. Cite with [^n] labels.').to_s
        ctx_lines = (input['context'] || []).map.with_index do |c, i|
          label = i + 1
          "[^#{label}] #{c['text']}"
        end
        user = <<~U
          Question:
          #{input['user_query']}

          Context:
          #{ctx_lines.join("\n\n")}
        U
        { 'messages' => [
            { 'role' => 'system', 'content' => sys },
            { 'role' => 'user',   'content' => user.strip }
          ]
        }
      end,

      sample_output: lambda do
        { 'messages' => [
            { 'role' => 'system', 'content' => 'Answer using only the provided context. Cite with [^n] labels.' },
            { 'role' => 'user', 'content' => "Question:\n...\n\nContext:\n[^1] ..." }
        ] }
      end

    },
    # 13. Postprocess answer
    postprocess_answer: {
      title: 'Serve - Postprocess LLM answer',
      subtitle: 'Extract [^n] citations and attach structured metadata',
      display_priority: 3,

      input_fields: lambda do |_object_definitions, _connection, _cfg|
        [
          { name: 'llm_output', label: 'LLM output text', control_type: 'text-area', optional: false },
          { name: 'context', type: 'array', of: 'object', optional: true,
            hint: 'Items used to build the prompt; index order matches [^n] labels' }
        ]
      end,

      output_fields: lambda do |_object_definitions, _config_fields|
        [
          { name: 'answer' },
          { name: 'citation_count', type: 'integer' },
          { name: 'citations', type: 'array', of: 'object', properties: [
            { name: 'label' }, { name: 'chunk_id' }, { name: 'doc_id' }, { name: 'file_path' },
            { name: 'span', type: 'object', properties: [{ name: 'start', type: 'integer' }, { name: 'end', type: 'integer' }] },
            { name: 'metadata', type: 'object' }
          ] }
        ]
      end,

      execute: lambda do |_connection, input|
        text = (input['llm_output'] || '').to_s
        ctx  = (input['context'] || [])

        # Find labels like [^1], [^2], etc.
        labels = text.scan(/\[\^(\d+)\]/).flatten.map(&:to_i).uniq.sort

        citations = labels.map do |n|
          i = n - 1
          src = ctx[i] || {}
          {
            'label'     => "[^#{n}]",
            'chunk_id'  => (src['chunk_id'] || src['id']),
            'doc_id'    => src['doc_id'],
            'file_path' => src.dig('source','file_path'),
            'span'      => { 'start' => src['span_start'], 'end' => src['span_end'] },
            'metadata'  => call(:sanitize_metadata, src['metadata'])
          }
        end

        # Strip duplicate whitespace and trim edges; leave labels intact
        clean = text.gsub(/[ \t]+/, ' ').gsub(/\n{3,}/, "\n\n").strip

        { 'answer' => clean, 'citations' => citations, 'citation_count' => citations.length }
      end,
  
      sample_output: lambda do
        {
          'answer' => "Our PTO policy grants 15 days annually. See [^1] and [^2] for details.",
          'citation_count' => 2,
          'citations' => [
            {
              'label' => '[^1]',
              'chunk_id' => 'doc-abc123:0',
              'doc_id' => 'doc-abc123',
              'file_path' => 'drive://Policies/PTO.md',
              'span' => { 'start' => 0, 'end' => 500 },
              'metadata' => { 'section' => 'Overview' }
            },
            {
              'label' => '[^2]',
              'chunk_id' => 'doc-xyz789:2',
              'doc_id' => 'doc-xyz789',
              'file_path' => 'drive://Policies/Benefits.md',
              'span' => { 'start' => 1200, 'end' => 1500 },
              'metadata' => { 'section' => 'Entitlements' }
            }
          ]
        }
      end

    }
  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {

    attach_alignment_modes: lambda do |_connection=nil, _cfg={}|
      [
        ['Match by id (recommended)', 'by_id'],
        ['Match by index (vectors[i] -> chunks[i])', 'by_index']
      ]
    end,

    item_schema_field_names: lambda do |_connection, _config_fields = {}|
      fields = _config_fields['item_schema'].is_a?(Array) ? _config_fields['item_schema'] : []
      names  = fields.map { |f| f['name'].to_s }.reject(&:empty?).uniq
      names.map { |n| [n, n] }
    end,

    input_source_modes: lambda do |_connection = nil, _cfg = {}|
      [
        ['Chunks list (chunks[*])', 'chunks'],
        ['Documents list (documents[*].chunks[*])', 'documents']
      ]
    end,

    chunking_presets: lambda do |_connection = nil, _cfg = {}|
      [
        ['Auto (recommended)', 'auto'],
        ['Balanced (2k max, 200 overlap)', 'balanced'],
        ['Small (1k max, 100 overlap)', 'small'],
        ['Large (4k max, 200 overlap)', 'large'],
        ['Custom (enter values below)', 'custom']
      ]
    end,

    metadata_modes: lambda do |_connection = nil, _cfg = {}|
      [
        ['Ignore (no metadata included)', 'none'],
        ['Flat & safe (recommended)',     'flat'],
        ['Pass-through (no transformation)', 'pass']
      ]
    end,

    metadata_sources: lambda do |_connection = nil, _cfg = {}|
      [
        ['None', 'none'],
        ['Key–value list', 'kv'],
        ['Tags (comma-separated)', 'tags_csv'],
        ['Auto from file_path', 'auto_from_path'],
        ['Advanced (enter JSON object)', 'advanced']
      ]
    end,

    table_row_profiles: lambda do |_connection = nil, _cfg = {}|
      [
        ['Slim (id, doc_id, file_path, +text?)', 'slim'],
        ['Standard (adds checksum, tokens, span, created_at)', 'standard'],
        ['Wide (Standard + flattened metadata)', 'wide']
      ]
    end,
  },

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {}

}
