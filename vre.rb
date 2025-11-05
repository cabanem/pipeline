rag_retrieve_contexts_enhanced: {
  title: 'RAG: Retrieve contexts (enhanced)',
  subtitle: 'projects.locations:retrieveContexts (Vertex RAG Engine, v1)',
  display_priority: 120,
  retry_on_response: [408, 429, 500, 502, 503, 504],
  max_retries: 3,
  
  config_fields: [
    { name: 'show_advanced', label: 'Show advanced options',
      type: 'boolean', control_type: 'checkbox',
      default: false, sticky: true, extends_schema: true,
      hint: 'Toggle to reveal threshold/ranker controls.' }
  ],
  
  input_fields: lambda do |od, connection, cfg|
    show_adv = (cfg['show_advanced'] == true)
    
    base = [
      { name: 'query_text', label: 'Query text', optional: false },
      { name: 'rag_corpus', optional: true,
        hint: 'Full or short ID. Example: my-corpus. Will auto-expand using connection project/location.' },
      { name: 'rag_file_ids', type: 'array', of: 'string', optional: true,
        hint: 'Optional: limit to these file IDs (must belong to the same corpus).' },
      { name: 'top_k', type: 'integer', optional: true, default: 20, hint: 'Max contexts to return.' },
      { name: 'correlation_id', optional: true, hint: 'For tracking related requests.' }
    ]
    
    adv = [
      { name: 'vector_distance_threshold', type: 'number', optional: true,
        hint: 'Return contexts with distance ≤ threshold. For COSINE, lower is better.' },
      { name: 'vector_similarity_threshold', type: 'number', optional: true,
        hint: 'Return contexts with similarity ≥ threshold. Do NOT set both thresholds.' },
      { name: 'rank_service_model', optional: true,
        hint: 'Vertex Ranking model, e.g. semantic-ranker-512@latest.' },
      { name: 'llm_ranker_model', optional: true,
        hint: 'LLM ranker, e.g. gemini-2.5-flash.' }
    ]
    
    show_adv ? (base + adv) : base
  end,
  
  output_fields: lambda do |object_definitions, connection|
    [
      { name: 'question' },
      { name: 'contexts', type: 'array', of: 'object', properties: [
          { name: 'id' },
          { name: 'text' },
          { name: 'score', type: 'number' },
          { name: 'source' },
          { name: 'uri' },
          { name: 'metadata', type: 'object' },
          { name: 'metadata_kv', type: 'array', of: 'object' },
          { name: 'metadata_json' }
        ]
      },
      { name: 'ok', type: 'boolean' },
      { name: 'telemetry', type: 'object', properties: [
          { name: 'http_status', type: 'integer' },
          { name: 'message' },
          { name: 'duration_ms', type: 'integer' },
          { name: 'correlation_id' }
        ]}
    ]
  end,
  
  execute: lambda do |connection, input|
    started_at = Time.now
    correlation_id = input['correlation_id'] || SecureRandom.uuid
    
    begin
      # Extract project/location from connection
      sa_key = JSON.parse(connection['service_account_key_json'] || '{}') rescue {}
      project = connection['project_id'] || sa_key['project_id']
      location = connection['location'] || 'us-central1'
      
      error('Project is required') if project.nil? || project.empty?
      error('Location is required') if location.nil? || location.empty?
      error('Location cannot be global for RAG retrieval') if location.downcase == 'global'
      
      # Build corpus path
      corpus = input['rag_corpus'].to_s.strip
      if corpus.present? && !corpus.start_with?('projects/')
        corpus = "projects/#{project}/locations/#{location}/ragCorpora/#{corpus}"
      end
      
      # Validate threshold parameters
      if input['vector_distance_threshold'].present? && input['vector_similarity_threshold'].present?
        error('Set ONLY one of: vector_distance_threshold OR vector_similarity_threshold.')
      end
      if input['rank_service_model'].present? && input['llm_ranker_model'].present?
        error('Choose ONE ranker: rank_service_model OR llm_ranker_model.')
      end
      
      # Build ragResources
      rag_resources = {}
      rag_resources['ragCorpus'] = corpus if corpus.present?
      file_ids = Array(input['rag_file_ids']).map(&:to_s).map(&:strip).reject(&:empty?)
      rag_resources['ragFileIds'] = file_ids if file_ids.any?
      
      error('Provide rag_corpus and/or rag_file_ids') if rag_resources.empty?
      
      # Build retrieval config
      retrieval_cfg = { 'topK' => (input['top_k'] || 20).to_i }
      
      # Add filter if specified
      if input['vector_distance_threshold'].present?
        retrieval_cfg['filter'] = { 'vectorDistanceThreshold' => input['vector_distance_threshold'].to_f }
      elsif input['vector_similarity_threshold'].present?
        retrieval_cfg['filter'] = { 'vectorSimilarityThreshold' => input['vector_similarity_threshold'].to_f }
      end
      
      # Add ranking if specified
      if input['rank_service_model'].present?
        retrieval_cfg['ranking'] = { 'rankService' => { 'modelName' => input['rank_service_model'] } }
      elsif input['llm_ranker_model'].present?
        retrieval_cfg['ranking'] = { 'llmRanker' => { 'modelName' => input['llm_ranker_model'] } }
      end
      
      # Build request
      url = "https://#{location}-aiplatform.googleapis.com/v1/projects/#{project}/locations/#{location}:retrieveContexts"
      
      body = {
        'query' => {
          'text' => input['query_text'],
          'ragRetrievalConfig' => retrieval_cfg
        },
        'vertexRagStore' => {
          'ragResources' => [rag_resources]
        }
      }
      
      headers = {
        'Authorization' => "Bearer #{connection['access_token']}",
        'Content-Type' => 'application/json',
        'X-Correlation-Id' => correlation_id,
        'x-goog-request-params' => "parent=projects/#{project}/locations/#{location}"
      }
      
      # Make request
      t_req = Time.now
      response = post(url).headers(headers).payload(body)
      req_ms = ((Time.now - t_req) * 1000).round
      
      # Extract and map contexts
      contexts = []
      if response && response['contexts'] && response['contexts']['contexts']
        raw_contexts = response['contexts']['contexts']
        
        raw_contexts.each_with_index do |ctx, idx|
          # Extract metadata
          md = ctx['metadata'] || {}
          
          # Clean text - remove problematic Unicode but keep it readable
          text = (ctx['text'] || ctx.dig('chunk', 'text') || '').to_s
          text = text.encode('UTF-8', invalid: :replace, undef: :replace, replace: ' ')
                    .gsub(/\s+/, ' ')
                    .strip
          
          contexts << {
            'id' => ctx['chunkId'] || ctx['id'] || "ctx-#{idx + 1}",
            'text' => text,
            'score' => (ctx['score'] || ctx['relevanceScore'] || 0.0).to_f,
            'source' => ctx['sourceDisplayName'] || ctx['sourceUri']&.split('/')&.last,
            'uri' => ctx['sourceUri'],
            'metadata' => md,
            'metadata_kv' => md.map { |k, v| { 'key' => k.to_s, 'value' => v } },
            'metadata_json' => md.empty? ? nil : md.to_json
          }
        end
      end
      
      # Build output
      {
        'question' => input['query_text'],
        'contexts' => contexts,
        'ok' => true,
        'telemetry' => {
          'http_status' => 200,
          'message' => "Retrieved #{contexts.length} contexts",
          'duration_ms' => ((Time.now - started_at) * 1000).round,
          'correlation_id' => correlation_id
        }
      }
      
    rescue => e
      {
        'question' => input['query_text'],
        'contexts' => [],
        'ok' => false,
        'telemetry' => {
          'http_status' => 500,
          'message' => e.message,
          'duration_ms' => ((Time.now - started_at) * 1000).round,
          'correlation_id' => correlation_id
        }
      }
    end
  end
}
