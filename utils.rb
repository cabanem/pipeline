# frozen_string_literal: true

require 'time'
require 'securerandom'
require 'digest'
require 'json'

{
  title: 'RAG Utilities',
  version: '0.4.0',
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
    envelope_fields: {
      fields: lambda do |_|
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
      fields: lambda do
        [
          { name: 'start', type: 'integer' },
          { name: 'end',   type: 'integer' }
        ]
      end
    },
    source: {
      fields: lambda do
        [
          { name: 'file_path' },
          { name: 'checksum' }
        ]
      end
    },
    chunk: {
      fields: lambda do |object_definitions|
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
      fields: lambda do
        [
          { name: 'id' },
          { name: 'vector', type: 'array', of: 'number' },
          { name: 'namespace' },
          { name: 'metadata', type: 'object' }
        ]
      end
    },
    ingest_item: {
      fields: lambda do |_object_definitions, _config_fields = {}|
        # If user designed a schema, use it; else default to your canonical item.
        if _config_fields['design_item_schema'] &&
           _config_fields['item_schema'].is_a?(Array) &&
           !_config_fields['item_schema'].empty?
          _config_fields['item_schema']
        else
          [
            { name: 'file_path', optional: true },
            { name: 'content',   control_type: 'text-area' },
            { name: 'max_chunk_chars', type: 'integer', optional: true },
            { name: 'overlap_chars',   type: 'integer', optional: true },
            { name: 'metadata', type: 'object', optional: true }
          ]
        end
      end
    }
  },

  # --------- METHODS ------------------------------------------------------
  methods: {
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
    # Safe getter using declared field name or fallback name
    get_item_field: lambda do |item, cfg, declared_key, fallback_key|
      key = (cfg[declared_key] || fallback_key).to_s
      v = item[key]
      v.nil? ? item[fallback_key] : v
    end,
  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    
    # --- INGESTION (SEEDING) ----------------------------------------------

    # ---- 1.  Prep for indexing -------------------------------------------
    prep_for_indexing: {
      title: 'Ingestion: Prepare document for indexing',
      subtitle: 'Cleans, chunks, and emits chunk records with IDs + metadata',
      display_priority: 10,
      help: lambda do |_|
        { body: 'Provide raw text and (optionally) a file path + metadata. Returns normalized chunks ready for embedding/indexing.' }
      end,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema',
          hint: 'Check to use the schema builder to define this action’s datapills.' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv',
          hint: 'Use the Schema Builder to define the output fields (datapills).' }
      ],

      input_fields: lambda do
        [
          { name: 'file_path', label: 'Source URI (recommended)',
            hint: 'Stable URI like gcs://bucket/path or drive://folder/file; used to derive deterministic doc_id.',
            optional: true },
          { name: 'content', label: 'Plain text content (required)',
            hint: 'UTF-8 text only. Convert PDFs/DOCX before calling.',
            optional: false, control_type: 'text-area' },
          { name: 'max_chunk_chars', label: 'Max characters per chunk',
            type: 'integer', optional: true,
            hint: 'Default 2000. Allowed range: 200–8000.' },
          { name: 'overlap_chars', label: 'Overlap between chunks (chars)',
            type: 'integer', optional: true,
            hint: 'Default 200. Must be less than Max characters per chunk.' },
          { name: 'metadata', type: 'object', optional: true,
            hint: 'Small JSON-safe facts (strings/numbers/bools/flat objects). Avoid large blobs/PII.' },
          { name: 'debug', label: 'Include debug notes',
            type: 'boolean', control_type: 'checkbox', optional: true,
            hint: 'Adds trace_id and normalization notes to the output.' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
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
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0   = Time.now
        corr = call(:guid)
        started_at  = Time.now
        trace_id    = call(:guid)
        raw         = input['content'].to_s
        file_path   = input['file_path'].to_s
        user_meta   = call(:sanitize_metadata, input['metadata'])
        debug     = call(:safe_bool, input['debug'])

        # 1) Normalize/Clean
        normalized = call(:normalize_newlines, raw)
        cleaned    = call(:strip_control_chars, normalized)

        # 2) Bounds and defaults
        max_in     = input['max_chunk_chars']
        ov_in      = input['overlap_chars']
        max_chars  = call(:clamp_int, (max_in || 2000), 200, 8000)
        overlap    = call(:clamp_int, (ov_in  || 200),  0,   4000)
        # Hard rule: overlap must be < max_chars (don’t silently fix without telling the user)
        if overlap >= max_chars
          error("overlap_chars (#{overlap}) must be less than max_chunk_chars (#{max_chars}). Try overlap_chars=#{[max_chars/10,1].max}.")
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
        base['trace_id'] = corr if debug
        base['notes']    = 'prep_for_indexing completed' if debug
        base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
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
      title: 'Ingestion: Prepare multiple documents for indexing',
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

      input_fields: lambda do |object_definitions, _config_fields ={}|
        [
          {
            name: 'items',
            type: 'array',
            of: 'object',
            properties: object_definitions['ingest_item'],
            optional: false,
            hint: 'Map your list here. Use “Design item schema” in the step’s config if your list shape is custom.'
          },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do |object_definitions|
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
          max_chars   = call(:clamp_int, (max_in || 2000), 200, 8000)
          overlap     = call(:clamp_int, (ov_in  || 200),   0,   4000)
          overlap     = [overlap, max_chars - 1].min
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
      title: 'Ingestion: Build index upserts',
      subtitle: 'Provider-agnostic: [{id, vector, namespace, metadata}]',
      display_priority: 9,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'provider', optional: true, hint: 'e.g., vertex' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'provider' },
          { name: 'namespace' },
          { name: 'count', type: 'integer' },
          { name: 'records', type: 'array', of: 'object', properties: object_definitions['upsert_record'] }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0  = Time.now
        corr = call(:guid)
        ns   = input['namespace'].to_s
        prv  = input['provider'].to_s
        chunks = call(:require_array_of_objects, input['chunks'], 'chunks')
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
        {
          'provider' => prv, 'namespace' => ns,
          'records'  => upserts, 'count' => upserts.length
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
    build_index_upserts_batch: {
      title: 'Ingestion: Build index upserts',
      subtitle: 'Accepts documents[*].chunks or chunks[*]; emits provider-agnostic upserts',
      batch: true,
      display_priority: 9,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox', label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true, label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do
        [
          { name: 'documents', type: 'array', of: 'object', optional: true },
          { name: 'chunks', type: 'array', of: 'object', optional: true },
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'provider',  optional: true, hint: 'e.g., vertex' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'provider' },
          { name: 'namespace' },
          { name: 'count', type: 'integer' },
          { name: 'records', type: 'array', of: 'object', properties: object_definitions['upsert_record'] }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0  = Time.now
        corr = call(:guid)
        ns   = input['namespace'].to_s
        prv  = input['provider'].to_s
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
          'provider' => prv, 'namespace' => ns, 'records' => upserts, 'count' => upserts.length
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

    # ---- 3.  Embedding  --------------------------------------------------
    build_embedding_requests: {
      title: 'Ingestion: Build embedding requests from chunks',
      subtitle: '[{id, text, metadata}] for your embedding step',
      display_priority: 8,
      # chunks[*] -> [{id, text, metadata}] for embedding

      input_fields: lambda do
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'debug',  type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do
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
      title: 'Ingestion: Build embedding requests',
      subtitle: 'Accepts documents[*].chunks or chunks[*]; flattens to [{id,text,metadata}]',
      batch: true,
      display_priority: 8,

      input_fields: lambda do
        [
          { name: 'documents', type: 'array', of: 'object', optional: true,
            hint: 'Each doc should include chunks:[...]' },
          { name: 'chunks', type: 'array', of: 'object', optional: true },
          { name: 'debug',  type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do
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
      title: 'Ingestion: Attach embeddings to chunks',
      subtitle: 'Merges [{id, embedding}] onto chunks by id',
      display_priority: 7,

      input_fields: lambda do
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'embeddings', label: 'Embeddings [{id, embedding}]', type: 'array', of: 'object', optional: false },
          { name: 'embedding_key', optional: true, hint: 'Default: embedding' },
          { name: 'id_key', optional: true, hint: 'Default: id' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do
        [
          { name: 'count', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object' }
        ]
      end,

      execute: lambda do |_connection, input|
        id_key = (input['id_key'] || 'id').to_s
        emb_key = (input['embedding_key'] || 'embedding').to_s
        embeddings = call(:require_array_of_objects, input['embeddings'], 'embeddings')
        chunks     = call(:require_array_of_objects, input['chunks'], 'chunks')
        idx = {}
        embeddings.each do |e|
          idx[e[id_key].to_s] = e[emb_key]
        end
        out_chunks = chunks.map do |c|
          cid = call(:resolve_chunk_id, c)
          vec = idx[cid]
          next c unless vec
          c.merge('embedding' => vec)
        end
        out = { 'count' => out_chunks.length, 'chunks' => out_chunks }
        out['trace_id'] = call(:guid) if call(:safe_bool, input['debug'])
        out
      end,

      sample_output: lambda do
        { 'count' => 2, 'chunks' => [{ 'chunk_id' => 'docA:0', 'embedding' => [0.1, 0.2] }] }
      end

    },
    attach_embeddings_batch: {
      title: 'Ingestion: Attach embeddings to chunks',
      subtitle: 'Supports [{chunks,embeddings}] or top-level chunks/embeddings',
      batch: true,
      display_priority: 7,

      input_fields: lambda do
        [
          { name: 'pairs', type: 'array', of: 'object', optional: true,
            hint: 'Each: {chunks:[...], embeddings:[{id,embedding}], id_key?, embedding_key?}' },
          { name: 'chunks', type: 'array', of: 'object', optional: true },
          { name: 'embeddings', type: 'array', of: 'object', optional: true },
          { name: 'id_key', optional: true, hint: 'Default id' },
          { name: 'embedding_key', optional: true, hint: 'Default embedding' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,

      output_fields: lambda do
        [
          { name: 'count', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object' },
          { name: 'trace_id', optional: true }
        ]
      end,

      execute: lambda do |_connection, input|
        id_key = (input['id_key'] || 'id').to_s
        emb_key = (input['embedding_key'] || 'embedding').to_s
        merged = []

        if input['pairs'].is_a?(Array) && !input['pairs'].empty?
          input['pairs'].each do |p|
            next unless p.is_a?(Hash)
            eList = call(:require_array_of_objects, p['embeddings'], 'embeddings')
            cList = call(:require_array_of_objects, p['chunks'], 'chunks')
            idx = {}
            eList.each { |e| idx[e[id_key].to_s] = e[emb_key] }
            cList.each do |c|
              cid = call(:resolve_chunk_id, c)
              vec = idx[cid]
              merged << (vec ? c.merge('embedding' => vec) : c)
            end
          end
        else
          eList = call(:require_array_of_objects, input['embeddings'] || [], 'embeddings')
          cList = call(:require_array_of_objects, input['chunks']     || [], 'chunks')
          idx = {}
          eList.each { |e| idx[e[id_key].to_s] = e[emb_key] }
          cList.each do |c|
            cid = call(:resolve_chunk_id, c)
            vec = idx[cid]
            merged << (vec ? c.merge('embedding' => vec) : c)
          end
        end

        out = { 'count' => merged.length, 'chunks' => merged }
        out['trace_id'] = call(:guid) if call(:safe_bool, input['debug'])
        out
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
          'trace_id' => 'trace-attach-batch-1'
        }
      end
    },

    # ---- 3.  Emit --------------------------------------------------------
    to_data_table_rows: {
      title: 'Ingestion: To Data Table rows',
      subtitle: 'Slim corpus rows from chunks for persistence',
      display_priority: 6,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema',
          hint: 'Use Schema Builder to define this action’s datapills.' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', hint: 'Define output fields (datapills).',
          sample_data_type: 'csv' }
      ],

      input_fields: lambda do
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'table_name', optional: true, hint: 'For your own bookkeeping' },
          { name: 'include_text', type: 'boolean', control_type: 'checkbox', optional: true, hint: 'Default true' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'table' },
          { name: 'count', type: 'integer' },
          { name: 'rows', type: 'array', of: 'object' }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        t0 = Time.now
        corr = call(:guid)
        include_text = input['include_text'].nil? ? true : !!input['include_text']
        chunks = call(:require_array_of_objects, input['chunks'], 'chunks')
        rows = chunks.map do |c|
          base = {
            'id'         => call(:resolve_chunk_id, c),
            'doc_id'     => c['doc_id'],
            'file_path'  => c.dig('source','file_path'),
            'checksum'   => c.dig('source','checksum'),
            'tokens'     => c['tokens'],
            'span_start' => c['span_start'],
            'span_end'   => c['span_end'],
            'metadata'   => call(:sanitize_metadata, c['metadata']),
            'created_at' => c['created_at']
          }
          include_text ? base.merge('text' => c['text'].to_s) : base
        end
        {
          'table' => input['table_name'].to_s,
          'count' => rows.length,
          'rows'  => rows
        }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      end,

      sample_output: lambda do
        {
          'table' => 'kb_chunks',
          'count' => 2,
          'rows'  => [{ 'id' => 'docA:0', 'doc_id' => 'docA' }],
          'ok'    => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }

      end
    },
    to_data_table_rows_batch: {
      title: 'Ingestion: To Data Table rows',
      subtitle: 'Flattens multiple documents into slim corpus rows',
      batch: true,
      display_priority: 6,

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do
        [
          { name: 'documents', type: 'array', of: 'object', optional: true },
          { name: 'chunks', type: 'array', of: 'object', optional: true },
          { name: 'table_name', optional: true },
          { name: 'include_text', type: 'boolean', control_type: 'checkbox', optional: true, hint: 'Default true' }
        ]
      end,

      output_fields: lambda do |object_definitions, _config_fields|
        default_fields = [
          { name: 'table' },
          { name: 'count', type: 'integer' },
          { name: 'rows', type: 'array', of: 'object' }
        ]
        call(:resolve_output_schema, default_fields, _config_fields, object_definitions)
      end,

      execute: lambda do |_connection, input|
        include_text = input['include_text'].nil? ? true : !!input['include_text']
        chunks = call(:flatten_chunks_input, input)
        rows = chunks.map do |c|
          base = {
            'id'         => call(:resolve_chunk_id, c),
            'doc_id'     => c['doc_id'],
            'file_path'  => c.dig('source','file_path'),
            'checksum'   => c.dig('source','checksum'),
            'tokens'     => c['tokens'],
            'span_start' => c['span_start'],
            'span_end'   => c['span_end'],
            'metadata'   => call(:sanitize_metadata, c['metadata']),
            'created_at' => c['created_at']
          }
          include_text ? base.merge('text' => c['text'].to_s) : base
        end
        { 'table' => input['table_name'].to_s, 'count' => rows.length, 'rows' => rows }
      end,
  
      sample_output: lambda do
        {
          'table' => 'kb_chunks',
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
              'metadata' => { 'department' => 'HR' },
              'created_at' => '2025-10-15T12:00:00Z',
              'text' => 'First slice…'
            }
          ]
        }
      end
    },

    make_gcs_manifest: {
      title: 'Ingestion: Make GCS manifest',
      subtitle: 'Build {object_name, content_type, body} for corpus snapshot',
      display_priority: 5,

      input_fields: lambda do
        [
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'doc_id', optional: true },
          { name: 'chunks', type: 'array', of: 'object', optional: false },
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
      title: 'Ingestion: Make GCS manifests',
      subtitle: 'One manifest per document; supports json or ndjson',
      batch: true,
      display_priority: 6,

      input_fields: lambda do
        [
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'documents', type: 'array', of: 'object', optional: false,
            hint: 'Each doc requires {doc_id?, chunks:[...]} (doc_id inferred if missing)' },
          { name: 'prefix', optional: true, hint: 'e.g., manifests/' },
          { name: 'format', optional: true, hint: 'json|ndjson (default json)' }
        ]
      end,

      output_fields: lambda do
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
      title: 'Serve: Build vector query',
      subtitle: 'Normalize text + optional embedding into a search request object',

      input_fields: lambda do
        [
          { name: 'query_text', optional: true, control_type: 'text-area' },
          { name: 'query_embedding', type: 'array', of: 'number', optional: true },
          { name: 'namespace', optional: true },
          { name: 'top_k', type: 'integer', optional: true, hint: 'Default 20' }
        ]
      end,

      output_fields: lambda do
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
      title: 'Serve: Merge search results',
      subtitle: 'Normalize, dedupe by chunk_id, keep best score',

      config_fields: [
        { name: 'override_output_schema', type: 'boolean', control_type: 'checkbox',
          label: 'Design custom output schema' },
        { name: 'custom_output_schema', extends_schema: true, control_type: 'schema-designer',
          schema_neutral: false, sticky: true, optional: true,
          label: 'Output columns', sample_data_type: 'csv' }
      ],

      input_fields: lambda do
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
      title: 'Serve: Select context by token budget',
      subtitle: 'Greedy pack by tokens with optional per-doc cap',

      input_fields: lambda do
        [
          { name: 'results', type: 'array', of: 'object', optional: false },
          { name: 'token_budget', type: 'integer', optional: false },
          { name: 'max_per_doc', type: 'integer', optional: true, hint: 'Default 3' }
        ]
      end,

      output_fields: lambda do
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
      title: 'Serve: Build Gemini messages',
      subtitle: 'Construct system/user messages with injected context',

      input_fields: lambda do
        [
          { name: 'user_query', optional: false, control_type: 'text-area' },
          { name: 'context', type: 'array', of: 'object', optional: true },
          { name: 'system_preamble', optional: true, control_type: 'text-area',
            hint: 'High-level instructions and guardrails' }
        ]
      end,

      output_fields: lambda do
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
      title: 'Serve: Postprocess LLM answer',
      subtitle: 'Extract [^n] citations and attach structured metadata',

      input_fields: lambda do
        [
          { name: 'llm_output', label: 'LLM output text', control_type: 'text-area', optional: false },
          { name: 'context', type: 'array', of: 'object', optional: true,
            hint: 'Items used to build the prompt; index order matches [^n] labels' }
        ]
      end,

      output_fields: lambda do
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

    item_schema_field_names: lambda do |_connection, _config_fields = {}|
      fields = _config_fields['item_schema'].is_a?(Array) ? _config_fields['item_schema'] : []
      names  = fields.map { |f| f['name'].to_s }.reject(&:empty?).uniq
      names.map { |n| [n, n] }
    end

  },

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {}

}
