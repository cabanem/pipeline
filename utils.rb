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
      fields: lambda do
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
      fields: lambda do
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
          { name: 'vector', type: 'array' },
          { name: 'namespace' },
          { name: 'metadata', type: 'object' }
        ]
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
    end
  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    
    # --- INGESTION (SEEDING) ----------------------------------------------
    # 1.  Prep for indexing
    prep_for_indexing: {
      title: 'Ingestion: Prepare document for indexing',
      subtitle: 'Cleans, chunks, and emits chunk records with IDs + metadata',
      help: lambda do |_|
        { body: 'Provide raw text and (optionally) a file path + metadata. Returns normalized chunks ready for embedding/indexing.' }
      end,

      input_fields: lambda do
        [
          { name: 'file_path', label: 'File path / source key', optional: true },
          { name: 'content', label: 'Raw text content', optional: false, control_type: 'text-area' },
          { name: 'max_chunk_chars', type: 'integer', optional: true, hint: 'Default 2000; hard clamp [200..8000]' },
          { name: 'overlap_chars', type: 'integer', optional: true, hint: 'Default 200; must be < max_chunk_chars' },
          { name: 'metadata', type: 'object', optional: true, hint: 'Arbitrary key/value pairs to carry forward' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true, hint: 'Include trace and timings in output' }
        ]
      end,

      output_fields: lambda do
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
          { name: 'trace_id' , optional: true },
          { name: 'notes',    optional: true }
        ]
      end,

      execute: lambda do |_connection, input|
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
        max_chars  = call(:clamp_int, (input['max_chunk_chars'] || 2000), 200, 8000)
        overlap    = call(:clamp_int, (input['overlap_chars']  || 200),   0,   4000)
        overlap    = [overlap, max_chars - 1].min

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

        out = {
          'doc_id'       => doc_id,
          'file_path'    => file_path,
          'checksum'     => checksum,
          'chunk_count'  => records.length,
          'max_chunk_chars' => max_chars,
          'overlap_chars'   => overlap,
          'created_at'   => created_at,
          'duration_ms'  => ((Time.now - started_at) * 1000).round,
          'chunks'       => records
        }
        if debug
          out['trace_id'] = trace_id
          out['notes']    = 'prep_for_indexing completed'
        end
        out
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
          'duration_ms' => 12,
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
              'created_at' => '2025-10-15T12:00:00Z'
            }
          ]
        }
      end
    },

    # 2.  Build embedding requests
    build_embedding_requests: {
      title: 'Ingestion: Build embedding requests from chunks',
      subtitle: '[{id, text, metadata}] for your embedding step',
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

    # 3.  Attach embeddings
    attach_embeddings: {
      title: 'Ingestion: Attach embeddings to chunks',
      subtitle: 'Merges [{id, embedding}] onto chunks by id',

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

    # 4.  Build index upserts
    build_index_upserts: {
      title: 'Ingestion: Build index upserts',
      subtitle: 'Provider-agnostic: [{id, vector, namespace, metadata}]',

      input_fields: lambda do
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'provider', optional: true, hint: 'e.g., vertex' }
        ]
      end,

      output_fields: lambda do
        [
          { name: 'provider' }, { name: 'namespace' }, { name: 'count', type: 'integer' },
          { name: 'records', type: 'array', of: 'object', properties: object_definitions['upsert_record'] },
        ]
      end,

      execute: lambda do |_connection, input|
        ns  = input['namespace'].to_s
        prv = input['provider'].to_s
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
        { 'provider' => prv, 'namespace' => ns, 'records' => upserts, 'count' => upserts.length }
      end
    },

    # 5.  Emit data to table rows
    to_data_table_rows: {
      title: 'Ingestion: To Data Table rows',
      subtitle: 'Slim corpus rows from chunks for persistence',

      input_fields: lambda do
        [
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'table_name', optional: true, hint: 'For your own bookkeeping' },
          { name: 'include_text', type: 'boolean', control_type: 'checkbox', optional: true, hint: 'Default true' }
        ]
      end,

      output_fields: lambda do
        [
          { name: 'table' },
          { name: 'count', type: 'integer' },
          { name: 'rows', type: 'array', of: 'object' }
        ]
      end,

      execute: lambda do |_connection, input|
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
        { 'table' => input['table_name'].to_s, 'count' => rows.length, 'rows' => rows }
      end,

      sample_output: lambda do
        { 'table' => 'kb_chunks', 'count' => 2, 'rows' => [{ 'id' => 'docA:0', 'doc_id' => 'docA' }] }
      end
    },

    # 6.  Construct GCS manifest
    make_gcs_manifest: {
      title: 'Ingestion: Make GCS manifest',
      subtitle: 'Build {object_name, content_type, body} for corpus snapshot',

      input_fields: lambda do
        [
          { name: 'namespace', optional: true, hint: 'e.g., hr-knowledge-v1' },
          { name: 'doc_id', optional: true },
          { name: 'chunks', type: 'array', of: 'object', optional: false },
          { name: 'prefix', optional: true, hint: 'e.g., manifests/' },
          { name: 'format', optional: true, hint: 'json|ndjson (default json)' }
        ]
      end,

      output_fields: lambda do
        [
          { name: 'object_name' },
          { name: 'content_type' },
          { name: 'bytes', type: 'integer' },
          { name: 'body' }
        ]
      end,

      execute: lambda do |_connection, input|
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
        }
      end,
    
      sample_output: lambda do
        { 'object_name' => 'manifests/ns/doc/manifest-20250101T000000Z.json',
          'content_type' => 'application/json', 'bytes' => 123, 'body' => '{…}' }
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
      end
    },

    # 8.  Merge search results
    merge_search_results: {
      title: 'Serve: Merge search results',
      subtitle: 'Normalize, dedupe by chunk_id, keep best score',

      input_fields: lambda do
        [
          { name: 'results', type: 'array', of: 'object', optional: false }
        ]
      end,

      output_fields: lambda do
        [
          { name: 'count', type: 'integer' },
          { name: 'results', type: 'array', of: 'object' }
        ]
      end,     

      execute: lambda do |_connection, input|
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
      end
    }
  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {},

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {}

}
