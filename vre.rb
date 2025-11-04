rag_retrieve_contexts_enhanced: {
  title: 'RAG: Retrieve contexts (Vertex v1)',
  subtitle: 'projects.locations:retrieveContexts',
  display_priority: 120,
  retry_on_response: [408, 429, 500, 502, 503, 504],
  max_retries: 3,
  help: lambda do |_|
    { body: 'Retrieve relevant contexts from a Vertex RAG Store corpus. Pass request per the v1 data contract.' }
  end,

  # Config fields must be a literal array (no lambdas/methods)
  config_fields: [
    { name: 'show_advanced', label: 'Show advanced options',
      type: 'boolean', control_type: 'checkbox',
      default: false, sticky: true, extends_schema: true }
  ],

  input_fields: lambda do |_od, connection, cfg|
    adv = (cfg['show_advanced'] == true)

    [
      # Path
      { name: 'parent', label: 'Parent (projects/{project}/locations/{location})',
        hint: 'If blank, uses connection project/location.',
        sticky: true },

      # Request body (strictly from the contract)
      { name: 'query', type: 'object', optional: false, properties: [
          { name: 'text', label: 'Query text' },
          { name: 'ragRetrievalConfig', type: 'object', optional: true }
        ],
        hint: 'At minimum, provide text. ragRetrievalConfig is passthrough.' },

      { name: 'data_source', type: 'object', optional: false, properties: [
          { name: 'vertexRagStore', type: 'object', properties: [
              { name: 'ragResources', type: 'array', of: 'object', properties: [
                  { name: 'ragCorpus', hint: 'projects/{project}/locations/{location}/ragCorpora/{corpus}' },
                  { name: 'ragFileIds', type: 'array', of: 'string' }
                ] },
              { name: 'vectorDistanceThreshold', type: 'number',
                hint: 'Deprecated by Google; keep empty unless you know you need it.' }
            ] }
        ] },

      # Optional headers / diagnostics
      { name: 'x_goog_user_project', label: 'x-goog-user-project (billing)', sticky: true, optional: true },
      { name: 'cid', label: 'Correlation ID (x-correlation-id)', sticky: true, optional: true, hint: 'For tracing/logs.' },

      # Toggle to expose raw passthrough (entire request JSON)
      (adv ? { name: 'raw_request_json', label: 'Raw request override (JSON)',
               control_type: 'text-area', sticky: true, optional: true,
               hint: 'If provided, this exact JSON is sent as the request body (query/data_source fields above are ignored).' } : nil)
    ].compact
  end,

  execute: lambda do |connection, input|
    # Build parent from connection if not provided
    parent = input['parent'].presence
    if parent.blank?
      proj = connection['project_id'].presence || connection['project'].presence
      loc  = (connection['location'].presence || 'us-central1')
      error('Connection missing project_id/project') if proj.blank?
      parent = "projects/#{proj}/locations/#{loc}"
    end

    url = "https://aiplatform.googleapis.com/v1/#{parent}:retrieveContexts"

    # Headers (auth handled by connector framework)
    headers = { 'Content-Type' => 'application/json' }
    headers['x-goog-user-project'] = input['x_goog_user_project'] if input['x_goog_user_project'].present?
    headers['x-correlation-id']    = input['cid'] if input['cid'].present?

    # Body: either raw override or structured per contract
    body =
      if input['raw_request_json'].present?
        call(:parse_json, input['raw_request_json'])
      else
        req = {}
        if input['query'].present?
          # keep only known keys; pass ragRetrievalConfig through
          q = {}
          q['text'] = input['query']['text'] if input['query']['text'].present?
          if input['query']['ragRetrievalConfig'].present?
            q['ragRetrievalConfig'] = input['query']['ragRetrievalConfig']
          end
          req['query'] = q
        end
        if input['data_source'].present?
          req['data_source'] = input['data_source']
        end
        req
      end

    # Guardrails
    error('Missing query.text') unless body.dig('query', 'text').present?
    error('Missing data_source.vertexRagStore') unless body.dig('data_source', 'vertexRagStore').present?

    rsp = post(url, body, headers)

    # Normalize response shapes:
    # A) { "contexts": { "contexts": [ ... ] } }
    # B) { "contexts": [ ... ] }
    # C) { "contexts": null }  → []
    contexts =
      if rsp['contexts'].is_a?(Hash) && rsp['contexts']['contexts'].is_a?(Array)
        rsp['contexts']['contexts']
      elsif rsp['contexts'].is_a?(Array)
        rsp['contexts']
      else
        []
      end

    # Emit a flat array; also pass through the raw for debugging
    {
      contexts: contexts,
      _raw_response: rsp
    }
  end,

  output_fields: lambda do |_od, _connection|
    [
      { name: 'contexts', type: 'array', of: 'object', properties: [
          { name: 'sourceUri' },
          { name: 'sourceDisplayName' },
          { name: 'text' },
          { name: 'chunk', type: 'object', properties: [
              { name: 'text' },
              { name: 'pageSpan', type: 'object' }
            ] },
          { name: 'score', type: 'number' }
        ] },
      # handy for debugging / recipes
      { name: '_raw_response', type: 'object' }
    ]
  end,

  sample_output: lambda do
    {
      'contexts' => [
        {
          'sourceUri' => 'gs://bucket/file.pdf',
          'sourceDisplayName' => 'file.pdf',
          'text' => 'Example chunk text…',
          'chunk' => { 'text' => 'Example chunk text…', 'pageSpan' => { 'start' => 2, 'end' => 2 } },
          'score' => 0.27
        }
      ],
      '_raw_response' => { 'contexts' => { 'contexts' => [] } }
    }
  end
}
