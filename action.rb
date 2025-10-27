fetch_contexts: {
  # - 1.  Required metadata --------------------------------------------
  title: 'Fetch contexts (Vertex RAG)',
  subtitle: 'Auto-selects v1 (location) or v1beta1 (corpus) for retrieveContexts',
  display_priority: 10,

  # - 2.  Workato ------------------------------------------------------
  retry_on_request:  ['GET', 'HEAD', 'PUT', 'DELETE'],
  retry_on_response: [408, 429, 500, 502, 503, 504],
  max_retries: 3,

  after_response: lambda { |code, body, headers, message|
    parsed = body.is_a?(Hash) ? body : (body.present? ? JSON.parse(body) rescue { 'raw' => body.to_s } : {})

    meta = parsed['_meta'] ||= {}
    meta['http_status']    = code
    meta['request_id']     = headers['X-Request-Id'] || headers['x-request-id']
    meta['retry_after']    = headers['Retry-After']
    meta['rate_limit_rem'] = headers['X-RateLimit-Remaining']
    meta['etag']           = headers['ETag']
    meta['last_modified']  = headers['Last-Modified']
    meta['model_version']  = headers['x-goog-model-id'] || headers['X-Model-Version']
    parsed['_meta']['next_page_token'] ||= parsed['nextPageToken'] || parsed['next_page_token']

    if parsed['error'].is_a?(Hash)
      e = parsed['error']
      raise error({
        'code'     => e['code'] || code,
        'status'   => e['status'],
        'message'  => e['message'] || message || 'Request failed',
        'details'  => e['details']
      })
    end
    parsed
  },

  after_error_response: lambda { |code, body, headers, message|
    normalized = { 'code' => code }
    begin
      json = body.is_a?(Hash) ? body : (body.present? ? JSON.parse(body) : {})
    rescue
      json = { 'raw' => body.to_s }
    end

    if json['error'].is_a?(Hash)
      e = json['error']
      normalized['status']   = e['status']
      normalized['message']  = e['message'] || message || 'Request failed'
      normalized['details']  = e['details']
    else
      normalized['message']  = message || json['message'] || 'Request failed'
      normalized['raw']      = json unless json.empty?
    end

    normalized['retryable']     = [408, 429, 500, 502, 503, 504].include?(code)
    normalized['retry_after']   = headers['Retry-After']
    normalized['request_id']    = headers['X-Request-Id'] || headers['x-request-id']
    normalized['model_version'] = headers['x-goog-model-id'] || headers['X-Model-Version']
    error(normalized)
  },

  # - 3. Input fields ---------------------------------------------------
  input_fields: lambda do |_defs, _conn, _cfg|
    [
      # Keep mode, but default to AUTO (no user intervention needed).
      {
        name: 'mode',
        label: 'Endpoint mode',
        control_type: 'select',
        pick_list: [
          ['Auto (recommended)', 'auto'],
          ['Location (v1)', 'at_location_v1'],
          ['Corpus (v1beta1)', 'at_corpus_v1beta1']
        ],
        optional: true,
        default: 'auto',
        hint: 'Auto uses v1 for location calls; switches to v1beta1 for corpus-scoped calls.'
      },
      { name: 'text', label: 'Query text', optional: false },

      # Signals for corpus mode
      { name: 'corpus', label: 'Corpus (ID or full resource name)', optional: true },

      {
        name: 'rag_resources',
        label: 'RAG resources',
        type: 'array', of: 'object', optional: true,
        hint: 'Each item becomes dataSource.vertexRagStore.ragResources[*]',
        properties: [
          { name: 'rag_corpus', label: 'Corpus (ID or full resource name)', optional: false },
          { name: 'rag_file_ids', label: 'File IDs', type: 'array', of: 'string', optional: true }
        ]
      },

      { name: 'debug', type: 'boolean', control_type: 'checkbox', label: 'Return request/response in _debug', optional: true }
    ]
  end,

  # - 4. Output fields --------------------------------------------------
  output_fields: lambda do |_object_definitions, _connection|
    [
      { name: 'contexts', type: 'array', of: 'object', properties: [
        { name: 'chunkId' }, { name: 'text' }, { name: 'score', type: 'number' },
        { name: 'sourceUri' }, { name: 'sourceDisplayName' }, { name: 'chunk', type: 'object' },
        { name: 'metadata', type: 'object' }
      ]},
      { name: '_meta', type: 'object', properties: [
        { name: 'http_status', type: 'integer' },
        { name: 'request_id' }, { name: 'retry_after' }, { name: 'rate_limit_rem' },
        { name: 'etag' }, { name: 'last_modified' }, { name: 'model_version' },
        { name: 'next_page_token' }, { name: 'duration_ms', type: 'integer' },
        { name: 'correlation_id' }, { name: 'endpoint_mode' }, { name: 'api_version' }, { name: 'url' }
      ]},
      { name: '_debug', type: 'object', properties: [
        { name: 'request', type: 'object' }, { name: 'response', type: 'object' }
      ]}
    ]
  end,

  # - 5. Execute --------------------------------------------------------
  execute: lambda do |connection, input|
    t0   = Time.now
    corr = SecureRandom.uuid

    proj = connection['project_id']
    loc  = connection['location']
    raise 'Connection is missing project_id' if proj.blank?
    raise 'Connection is missing location'   if loc.blank?

    # Server base: favor regional endpoint; version added later.
    base_host = (connection['base_url'].presence || "https://#{loc}-aiplatform.googleapis.com").gsub(%r{/+$}, '')

    headers = {
      'Accept'           => 'application/json',
      'Content-Type'     => 'application/json',
      'X-Correlation-Id' => corr
    }
    headers['x-goog-user-project'] = connection['billing_project'] if connection['billing_project'].present?

    text = input['text'].to_s
    raise 'text is required' if text.blank?

    # ---------- Normalize corpora ----------
    norm_corpus_fqn = lambda do |val|
      v = val.to_s
      return v if v.start_with?('projects/')
      raise 'corpus is required' if v.blank?
      "projects/#{proj}/locations/#{loc}/ragCorpora/#{v}"
    end

    # Build normalized rag_resources array (FQNs only)
    in_resources = Array(input['rag_resources']).select { |r| r.is_a?(Hash) && r['rag_corpus'].present? }
    normalized_resources = in_resources.map do |r|
      rc = norm_corpus_fqn.call(r['rag_corpus'])
      obj = { 'ragCorpus' => rc }
      ids = r['rag_file_ids']
      obj['ragFileIds'] = ids if ids.present?
      obj
    end

    # If top-level corpus is provided, prefer it
    top_corpus = input['corpus'].present? ? norm_corpus_fqn.call(input['corpus']) : nil

    # Decide endpoint automatically unless overridden
    mode = (input['mode'].presence || 'auto').to_s

    # Heuristics for auto:
    # - If a top-level corpus is provided => use corpus-scoped (v1beta1)
    # - Else if exactly one resource corpus is present => corpus-scoped (v1beta1)
    # - Else => location-scoped (v1)
    wants_corpus = case mode
                   when 'at_corpus_v1beta1' then true
                   when 'at_location_v1'    then false
                   else
                     top_corpus.present? || normalized_resources.map { |r| r['ragCorpus'] }.uniq.size == 1
                   end

    api_version = wants_corpus ? 'v1beta1' : 'v1'
    path        = nil

    if wants_corpus
      parent = top_corpus || normalized_resources.first&.dig('ragCorpus')
      raise 'A corpus is required to call the corpus-scoped endpoint' if parent.blank?

      path = "/#{api_version}/#{parent}:retrieveContexts"
      headers['x-goog-request-params'] = "parent=#{parent}"

      # Ensure body has a single ragResource aligned to parent
      resource = { 'ragCorpus' => parent }
      # If caller provided file IDs on first item, thread them through
      first_ids = normalized_resources.first&.dig('ragFileIds')
      resource['ragFileIds'] = first_ids if first_ids.present?
      rag_resources = [resource]

    else
      path = "/#{api_version}/projects/#{proj}/locations/#{loc}:retrieveContexts"
      headers['x-goog-request-params'] = "parent=projects/#{proj}/locations/#{loc}"

      # If user only provided top_corpus, synthesize one resource from it
      if normalized_resources.empty? && top_corpus.present?
        rag_resources = [{ 'ragCorpus' => top_corpus }]
      else
        rag_resources = normalized_resources
      end

      raise 'At least one rag_resource or corpus is required for location-scoped call' if rag_resources.empty?
    end

    url  = "#{base_host}#{path}"
    body = {
      'query'      => { 'text' => text },
      'dataSource' => { 'vertexRagStore' => { 'ragResources' => rag_resources } }
    }

    response = post(url).headers(headers).payload(body).request_format_json

    out = response.is_a?(Hash) ? response : { 'raw' => response.to_s }
    out['_meta'] ||= {}
    out['_meta']['duration_ms']    = ((Time.now - t0) * 1000).to_i
    out['_meta']['correlation_id'] = corr
    out['_meta']['endpoint_mode']  = wants_corpus ? 'corpus' : 'location'
    out['_meta']['api_version']    = api_version
    out['_meta']['url']            = url if input['debug']

    if input['debug']
      out['_debug'] = {
        'request'  => { 'method' => 'POST', 'url' => url, 'headers' => headers, 'body' => body },
        'response' => response
      }
    end

    out
  rescue StandardError => e
    e.is_a?(Hash) ? error(e) : error({ 'message' => e.message })
  end,

  # - 6. Sample output --------------------------------------------------
  sample_output: lambda do
    {
      'contexts' => [
        {
          'chunkId' => 'c-001',
          'text'    => 'Parental leave is 12 weeks paid for all FTEs.',
          'score'   => 0.8642,
          'sourceUri' => 'gs://corp-bucket/policies/benefits.pdf#page=2',
          'sourceDisplayName' => 'benefits.pdf',
          'metadata' => { 'page' => 2 }
        }
      ],
      '_meta' => {
        'http_status'   => 200,
        'request_id'    => 'req-123',
        'duration_ms'   => 18,
        'correlation_id'=> 'uuid-abc',
        'endpoint_mode' => 'corpus',
        'api_version'   => 'v1beta1'
      }
    }
  end
}
