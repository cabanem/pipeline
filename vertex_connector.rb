# frozen_string_literal: true

{
  title: 'Vertex AI Adapter',
  version: '0.8.0',
  description: '',


  # ====== CONNECTION ==================================================  
  connection: {
    fields: [
      { name: 'project_id', optional: false, hint: 'GCP project ID' },
      { name: 'location', optional: false, hint: 'e.g., global, us-east4' },

      { name: 'client_email', label: 'Service account client_email', optional: false },
      { name: 'private_key', label: 'Service account private_key', optional: false, control_type: 'password', multiline: true,
        hint: 'Include BEGIN/END PRIVATE KEY lines.' },

      { name: 'scope', optional: true, hint: 'OAuth scope(s)', default: 'https://www.googleapis.com/auth/cloud-platform',
        control_type: 'select', options: [['Cloud Platform (all)', 'https://www.googleapis.com/auth/cloud-platform']] }
    ],

    authorization: {
      type: 'custom_auth',

      acquire: ->(connection) {
        # Parse service account inputs
        iss = nil
        key = nil

        iss = connection['client_email']
        key = connection['private_key']

        error('Missing client_email for service account') if iss.blank?
        error('Missing private_key for service account') if key.blank?

        # Normalize newlines in pasted keys
        key = key.to_s.gsub(/\\n/, "\n")

        # Guard for clock skew
        iat = Time.now.to_i - 60
        exp = iat + 3600 # 1 hour validity per Google’s service-account flow

        jwt_body = {
          iat: iat,
          exp: exp,
          aud: 'https://oauth2.googleapis.com/token',
          iss: iss,
          scope: (connection['scope'].presence || 'https://www.googleapis.com/auth/cloud-platform')
        }

        assertion = workato.jwt_encode(jwt_body, key, 'RS256')

        # Return the token payload; Workato merges it into `connection`
        post('https://oauth2.googleapis.com/token')
          .payload(grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: assertion)
          .request_format_www_form_urlencoded
      },

      apply: ->(connection) {
        bearer = connection['access_token']
        headers('Authorization': "Bearer #{bearer}")
      },

      refresh_on: [401],
      detect_on: [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i]
    },

    base_uri: ->(connection) {
      'https://aiplatform.googleapis.com'
    }
  },

  test: ->(connection) {
    project_id  = connection['project_id']
    location    = connection['location']
    get('/v1/publishers/google/models/gemini-1.5-pro')
      # Global list of publisher models (beta surface)== 'get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')'
      # for custom models API (UCAIP) == 'get("/v1/projects/#{project_id}/locations/#{location}/models")''
      # alt == 'get("/v1/projects/#{project_id}/locations/#{location}/endpoints")'
      .params(pageSize: 1)
  },

  # ====== OBJECT DEFINITIONS ==========================================
  object_definitions: {
    content_part: {
      fields: ->() {
        [
          { name: 'text' },
          { name: 'inlineData', type: 'object', properties: [
              { name: 'mimeType' }, { name: 'data', hint: 'Base64' }
            ]
          },
          { name: 'fileData', type: 'object', properties: [
              { name: 'mimeType' }, { name: 'fileUri', hint: 'gs://, https://, etc.' }
            ]
          },
          # Tool interaction fields (optional)
          { name: 'functionCall', type: 'object' },
          { name: 'functionResponse', type: 'object' },
          { name: 'executableCode', type: 'object' },
          { name: 'codeExecutionResult', type: 'object' }
        ]
      }
    },

    content: {
      fields: ->(object_definitions) {
        [
          { name: 'role', control_type: 'select', pick_list: 'roles', optional: false },
          { name: 'parts', type: 'array', of: 'object', properties: object_definitions['content_part'], optional: false }
        ]
      }
    },

    generation_config: {
      fields: ->() {
        [
          { name: 'temperature', type: 'number' },
          { name: 'topP', type: 'number' },
          { name: 'topK', type: 'integer' },
          { name: 'maxOutputTokens', type: 'integer' },
          { name: 'candidateCount', type: 'integer' },
          { name: 'stopSequences', type: 'array', of: 'string' },
          { name: 'responseMimeType' },
          { name: 'responseSchema', type: 'object' } # for structured output
        ]
      }
    },

    safety_setting: {
      fields: ->() { [
        { name: 'category' },   # e.g., 'HARM_CATEGORY_UNSPECIFIED'
        { name: 'threshold' }   # e.g., 'BLOCK_LOW_AND_ABOVE'
      ] }
    },

    # Tools for grounding
    tool_google_search: {
      fields: ->() { [ { name: 'googleSearch', type: 'object' } ] }
    },
    tool_vertex_ai_search: {
      fields: ->() {
        [
          {
            name: 'retrieval', type: 'object', properties: [
              {
                name: 'vertexAiSearch', type: 'object', properties: [
                  { name: 'datastore', hint: 'projects/.../locations/.../collections/default_collection/dataStores/...' },
                  { name: 'servingConfig', hint: 'Optional ServingConfig path' }
                ]
              }
            ]
          }
        ]
      }
    },

    generate_content_output: {
      fields: ->() {
        [
          { name: 'responseId' },
          { name: 'modelVersion' },
          { name: 'usageMetadata', type: 'object', properties: [
              { name: 'promptTokenCount', type: 'integer' },
              { name: 'candidatesTokenCount', type: 'integer' },
              { name: 'totalTokenCount', type: 'integer' }
            ]
          },
          { name: 'candidates', type: 'array', of: 'object', properties: [
              { name: 'finishReason' },
              { name: 'safetyRatings', type: 'array', of: 'object' },
              { name: 'groundingMetadata', type: 'object' },
              { name: 'content', type: 'object', properties: [
                  { name: 'role' },
                  { name: 'parts', type: 'array', of: 'object' }
                ]
              }
            ]
          }
        ]
      }
    },

    embed_output: {
      fields: ->() {
        [
          { name: 'predictions', type: 'array', of: 'object',
            properties: [
              { name: 'embeddings', type: 'array', of: 'object',
                properties: [ { name: 'values', type: 'array', of: 'number' } ]
              }]
          }]
      }
    },

    predict_output: {
      fields: ->() {
        [
          { name: 'predictions', type: 'array', of: 'object' },
          { name: 'deployedModelId' }
        ]
      }
    },

    batch_job: {
      fields: ->() {
        [
          { name: 'name' },
          { name: 'displayName' },
          { name: 'state' },
          { name: 'model' },
          { name: 'modelVersionId' },
          { name: 'error', type: 'object' },
          { name: 'outputInfo', type: 'object' },
          { name: 'resourcesConsumed', type: 'object' },
          { name: 'partialFailures', type: 'array', of: 'object' }
        ]
      }
    }
  },

  # ====== ACTIONS =====================================================
  actions: {

    # Generative
    gen_categorize_email: {
      title: 'Generative:  Categorize email',
      description: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.',
      input_fields: ->() {
        [
          { name: 'mode', control_type: 'select', pick_list: 'modes_classification', optional: false, default: 'embedding',
            hint: 'embedding (deterministic), generative (LLM-only), or hybrid (embeddings + LLM referee).' },

          { name: 'subject', optional: true },
          { name: 'body', optional: true },

          { name: 'categories', optional: false, type: 'array', of: 'object', properties: [
              { name: 'name', optional: false },
              { name: 'description' },
              { name: 'examples', type: 'array', of: 'string' }
            ],
            hint: 'At least 2. You can also pass simple strings (names only).' },

          { name: 'embedding_model', label: 'Embedding model',
            control_type: 'select', pick_list: 'models_embedding', optional: true, default: 'text-embedding-004',
            hint: 'Used in embedding or hybrid modes.' },

          { name: 'generative_model', label: 'Generative model',
            control_type: 'select', pick_list: 'models_generative', optional: true,
            hint: 'Required for generative mode. Optional in hybrid (for explanation).' },

          { name: 'min_confidence', type: 'number', optional: true, default: 0.25,
            hint: '0–1. If top score falls below this, fallback is used.' },

          { name: 'fallback_category', optional: true, default: 'Other' },

          { name: 'top_k', type: 'integer', optional: true, default: 3,
            hint: 'In hybrid mode, pass top-K candidates to the LLM referee.' },

          { name: 'return_explanation', type: 'boolean', optional: true, default: false,
            hint: 'If true and a generative model is provided, returns a short reasoning + distribution.' }
        ]
      },

      execute: ->(connection, input) {
        subj = (input['subject'] || '').to_s.strip
        body = (input['body'] || '').to_s.strip
        email_text = call(:build_email_text, subj, body)
        error('Provide subject and/or body') if email_text.blank?

        # Normalize categories (allow strings or objects)
        raw_cats = input['categories'] || []
        cats = raw_cats.map { |c|
          if c.is_a?(String)
            { 'name' => c, 'description' => nil, 'examples' => [] }
          else
            { 'name' => c['name'] || c[:name],
              'description' => c['description'] || c[:description],
              'examples' => c['examples'] || c[:examples] || [] }
          end
        }.select { |c| c['name'].present? }

        error('At least 2 categories are required') if cats.length < 2

        mode = (input['mode'] || 'embedding').to_s.downcase
        min_conf = (input['min_confidence'].presence || 0.25).to_f

        if %w[embedding hybrid].include?(mode)
          emb_model = (input['embedding_model'].presence || 'text-embedding-004')
          emb_model_path = call(:build_model_path_with_global_preview, connection, emb_model)

          # Build instances: first = email, rest = category texts
          email_inst = { 'content' => email_text, 'task' => 'RETRIEVAL_QUERY' }
          cat_insts = cats.map do |c|
            txt = [c['name'], c['description'], *(c['examples'] || [])].compact.join("\n")
            { 'content' => txt, 'task' => 'RETRIEVAL_DOCUMENT' }
          end

          emb_resp = post("/v1/#{emb_model_path}:predict").payload({ 'instances' => [email_inst] + cat_insts })
          preds = (emb_resp['predictions'] || [])
          error('Embedding model returned no predictions') if preds.empty?

          email_vec = call(:extract_embedding_vector, preds.first)
          cat_vecs = preds.drop(1).map { |p| call(:extract_embedding_vector, p) }

          sims = cat_vecs.each_with_index.map { |v, i| [i, call(:vector_cosine_similarity, email_vec, v)] }
          sims.sort_by! { |(_i, s)| -s }

          # Score in [0,1] from cosine [-1,1]
          scores = sims.map do |(i, s)|
            { 'category' => cats[i]['name'], 'score' => ((s + 1.0) / 2.0), 'cosine' => s }
          end

          top = scores.first
          chosen = top['category']
          confidence = top['score']

          # Threshold / fallback
          if confidence < min_conf && input['fallback_category'].present?
            chosen = input['fallback_category']
          end

          result = {
            'mode' => mode,
            'chosen' => chosen,
            'confidence' => confidence.round(4),
            'scores' => scores
          }

          # Optional LLM referee (hybrid or just explanation flag)
          if (mode == 'hybrid' || input['return_explanation']) && input['generative_model'].present?
            top_k = [[(input['top_k'] || 3).to_i, 1].max, cats.length].min
            shortlist = scores.first(top_k).map { |h| h['category'] }
            referee = call(:llm_referee, connection, input['generative_model'], email_text, shortlist, cats)
            result['referee'] = referee

            # Prefer the referee’s pick if it’s in the shortlist
            if referee['category'].present? && shortlist.include?(referee['category'])
              result['chosen'] = referee['category']
              result['confidence'] = [result['confidence'], referee['confidence']].compact.max
            end
          end

          result

        elsif mode == 'generative'
          error('generative_model is required when mode=generative') if input['generative_model'].blank?
          referee = call(:llm_referee, connection, input['generative_model'], email_text, cats.map { |c| c['name'] }, cats)
          chosen =
            if referee['confidence'].to_f < min_conf && input['fallback_category'].present?
              input['fallback_category']
            else
              referee['category']
            end

          {
            'mode' => mode,
            'chosen' => chosen,
            'confidence' => referee['confidence'],
            'referee' => referee
          }

        else
          error("Unknown mode: #{mode}")
        end
      },

      output_fields: ->() {
        [
          { name: 'mode' },
          { name: 'chosen' },
          { name: 'confidence', type: 'number' },
          { name: 'scores', type: 'array', of: 'object', properties: [
              { name: 'category' }, { name: 'score', type: 'number' }, { name: 'cosine', type: 'number' }
            ]
          },
          { name: 'referee', type: 'object', properties: [
              { name: 'category' }, { name: 'confidence', type: 'number' }, { name: 'reasoning' },
              { name: 'distribution', type: 'array', of: 'object', properties: [
                  { name: 'category' }, { name: 'prob', type: 'number' }
                ]
              }
            ]
          }
        ]
      },

      sample_output: ->() {
        {
          'mode' => 'embedding',
          'chosen' => 'Billing',
          'confidence' => 0.91,
          'scores' => [
            { 'category' => 'Billing', 'score' => 0.91, 'cosine' => 0.82 },
            { 'category' => 'Tech Support', 'score' => 0.47, 'cosine' => -0.06 },
            { 'category' => 'Sales', 'score' => 0.41, 'cosine' => -0.18 }
          ],
          'referee' => {
            'category' => 'Billing',
            'confidence' => 0.86,
            'reasoning' => 'Mentions invoice #4411, past‑due payment, and refund request.',
            'distribution' => [
              { 'category' => 'Billing', 'prob' => 0.86 },
              { 'category' => 'Tech Support', 'prob' => 0.10 },
              { 'category' => 'Sales', 'prob' => 0.04 }
            ]
          }
        }
      }
    },

    gen_generate_content: {
      title: 'Generative:  Generate content (Gemini)',
      description: 'POST :generateContent on a publisher model',
      input_fields: ->(object_definitions) {
        [
          { name: 'model', label: 'Model', optional: false,
            control_type: 'select', pick_list: 'models_generative',
            hint: 'Pick from list or paste a model ID/path.' },
          { name: 'contents', type: 'array', of: 'object',
            properties: object_definitions['content'], optional: false },
          { name: 'systemInstruction', type: 'object',
            properties: object_definitions['content'] },
          { name: 'tools', type: 'array', of: 'object', properties: [
              { name: 'googleSearch', type: 'object' },
              { name: 'retrieval', type: 'object' },
              { name: 'codeExecution', type: 'object' },
              { name: 'functionDeclarations', type: 'array', of: 'object' }
            ]
          },
          { name: 'toolConfig', type: 'object' },
          { name: 'safetySettings', type: 'array', of: 'object',
            properties: object_definitions['safety_setting'] },
          { name: 'generationConfig', type: 'object',
            properties: object_definitions['generation_config'] }
        ]
      },
      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        contents = call(:sanitize_contents_roles, input['contents'])
        sys_inst = call(:normalize_system_instruction, input['systemInstruction'])

        payload = {
          'contents' => contents,
          'systemInstruction' => sys_inst,
          'tools' => input['tools'],
          'toolConfig' => input['toolConfig'],
          'safetySettings' => input['safetySettings'],
          'generationConfig' => input['generationConfig']
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

        post("/v1/#{model_path}:generateContent").payload(payload)
      },
      output_fields: ->(object_definitions) { object_definitions['generate_content_output'] },
      sample_output: ->() {
        {
          'responseId' => 'resp-123',
          'modelVersion' => 'gemini-2.5-pro',
          'usageMetadata' => { 'promptTokenCount' => 42, 'candidatesTokenCount' => 128, 'totalTokenCount' => 170 },
          'candidates' => [
            { 'finishReason' => 'STOP',
              'content' => { 'role' => 'model', 'parts' => [ { 'text' => 'Hello, world.' } ] },
              'groundingMetadata' => { 'citationSources' => [ { 'uri' => 'https://example.com' } ] } }
          ]
        }
      }
    },

    gen_generate_grounded: {
      title: 'Generative:  Generate (grounded)',
      description: 'Generate with grounding via Google Search or Vertex AI Search',
      input_fields: ->(object_definitions) {
        [
          { name: 'model', label: 'Model', optional: false,
            control_type: 'select', pick_list: 'models_generative' },
          { name: 'grounding', control_type: 'select', pick_list: 'modes_grounding', optional: false },
          { name: 'vertex_ai_search_datastore',
            hint: 'Required when grounding=vertex_ai_search: projects/.../locations/.../collections/default_collection/dataStores/...' },
          { name: 'contents', type: 'array', of: 'object',
            properties: object_definitions['content'], optional: false },
          { name: 'generationConfig', type: 'object', properties: object_definitions['generation_config'] },
          { name: 'safetySettings', type: 'array', of: 'object', properties: object_definitions['safety_setting'] }
        ]
      },
      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        contents = call(:sanitize_contents_roles, input['contents'])
        sys_inst = call(:normalize_system_instruction, input['systemInstruction'])

        tools =
          if input['grounding'] == 'google_search'
            [ { 'googleSearch' => {} } ]
          else
            ds = input['vertex_ai_search_datastore']
            error('vertex_ai_search_datastore is required for vertex_ai_search grounding') if ds.blank?
            [ { 'retrieval' => { 'vertexAiSearch' => { 'datastore' => ds } } } ]
          end

          payload = {
            'contents' => contents,
            'systemInstruction' => sys_inst,
            'tools' => tools,
            'toolConfig' => input['toolConfig'],
            'safetySettings' => input['safetySettings'],
            'generationConfig' => input['generationConfig']
          }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

          post("/v1/#{model_path}:generateContent").payload(payload)
      },
      output_fields: ->(object_definitions) { object_definitions['generate_content_output'] },
      sample_output: ->() {
        {
          'responseId' => 'resp-456',
          'modelVersion' => 'gemini-2.5-pro',
          'candidates' => [
            { 'content' => { 'role' => 'model', 'parts' => [ { 'text' => 'Grounded answer...' } ] },
              'groundingMetadata' => { 'citationSources' => [ { 'uri' => 'https://en.wikipedia.org/wiki/...' } ] } }
          ]
        }
      }
    },

    # Embedding
    embed_text: {
      title: 'Embed text',
      description: 'POST :predict on a publisher embedding model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,
      input_fields: ->() {
        [
          { name: 'model', label: 'Embedding model', optional: false,
            control_type: 'select', pick_list: 'models_embedding',
            hint: 'Pick from list or paste a model ID/path (e.g., text-embedding-004).' },
          { name: 'texts', type: 'array', of: 'string', optional: false },
          { name: 'task', hint: 'Optional: RETRIEVAL_QUERY or RETRIEVAL_DOCUMENT' },
          { name: 'autoTruncate', type: 'boolean', hint: 'Truncate long inputs automatically' }
        ]
      },
      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])
        instances = (input['texts'] || []).map { |t| { 'content' => t, 'task' => input['task'] }.delete_if { |_k, v| v.nil? } }
        params = {}
        params['autoTruncate'] = input['autoTruncate'] unless input['autoTruncate'].nil?

        post("/v1/#{model_path}:predict")
          .payload({ 'instances' => instances, 'parameters' => params }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) })
      },
      output_fields: ->(object_definitions) { object_definitions['embed_output'] },
      sample_output: ->() {
        { 'predictions' => [
            { 'embeddings' => [ { 'values' => [0.012, -0.034, 0.056] } ] },
            { 'embeddings' => [ { 'values' => [0.023, -0.045, 0.067] } ] }
          ] }
      }
    },

    # Utility
    count_tokens: {
      title: 'Utility:  Count tokens',
      description: 'POST :countTokens on a publisher model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,
      input_fields: ->(object_definitions) {
        [
          { name: 'model', label: 'Model', optional: false,
            control_type: 'select', pick_list: 'models_generative' },
          { name: 'contents', type: 'array', of: 'object', properties: object_definitions['content'], optional: false },
          { name: 'systemInstruction', type: 'object', properties: object_definitions['content'] }
        ]
      },
      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])
        contents = call(:sanitize_contents_roles, input['contents'])
        sys_inst = call(:normalize_system_instruction, input['systemInstruction'])

        payload = {
          'contents' => contents,
          'systemInstruction' => sys_inst,
          'tools' => input['tools'],
          'toolConfig' => input['toolConfig'],
          'safetySettings' => input['safetySettings'],
          'generationConfig' => input['generationConfig']
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

        post("/v1/#{model_path}:countTokens").payload({
          'contents' => contents,
          'systemInstruction' => sys_inst
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) })

      },
      output_fields: ->() {
        [
          { name: 'totalTokens', type: 'integer' },
          { name: 'totalBillableCharacters', type: 'integer' },
          { name: 'promptTokensDetails', type: 'array', of: 'object' }
        ]
      },
      sample_output: ->() {
        { 'totalTokens' => 31, 'totalBillableCharacters' => 96,
          'promptTokensDetails' => [ { 'modality' => 'TEXT', 'tokenCount' => 31 } ] }
      }
    },

    upload_to_gcs: {
      title: 'Utility:  Upload to Cloud Storage (simple upload)',
      description: 'Simple media upload to GCS (uploadType=media)',
      input_fields: ->() {
        [
          { name: 'bucket', optional: false },
          { name: 'object_name', optional: false, label: 'Object path/name' },
          { name: 'content_type', optional: false },
          { name: 'file', type: 'file', optional: false }
        ]
      },
      execute: ->(_connection, input) {
        post("https://storage.googleapis.com/upload/storage/v1/b/#{CGI.escape(input['bucket'])}/o")
          .params(uploadType: 'media', name: input['object_name'])
          .headers('Content-Type': input['content_type'])
          .request_body(input['file'])
      },
      output_fields: ->() {
        [ { name: 'bucket' }, { name: 'name' }, { name: 'generation' },
          { name: 'size' }, { name: 'contentType' }, { name: 'mediaLink' } ]
      },
      sample_output: ->() {
        { 'bucket' => 'my-bucket', 'name' => 'docs/foo.pdf', 'generation' => '1728533890000',
          'size' => '123456', 'contentType' => 'application/pdf',
          'mediaLink' => 'https://storage.googleapis.com/download/storage/v1/b/my-bucket/o/docs%2Ffoo.pdf?gen=...' }
      }
    },

    # Generic
    endpoint_predict: {
      title: 'Endpoint predict (custom model)',
      description: 'POST :predict to a Vertex AI Endpoint',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,
      input_fields: ->() {
        [
          { name: 'endpoint', optional: false, hint: 'Endpoint ID or full resource path' },
          { name: 'instances', type: 'array', of: 'object', optional: false },
          { name: 'parameters', type: 'object' }
        ]
      },
      execute: ->(connection, input) {
        # Confirm regional endpoint (UCAIP)
        call(:ensure_regional_location!) # will throw if connection does not contain required element
        # Build endpoint path
        endpoint_path = call(:build_endpoint_path, connection, input['endpoint'])
        # Post
        post("/v1/#{endpoint_path}:predict")
          .payload({ 'instances' => input['instances'], 'parameters' => input['parameters'] }.delete_if { |_k, v| v.nil? })
      },
      output_fields: ->(object_definitions) { object_definitions['predict_output'] },
      sample_output: ->() {
        { 'predictions' => [ { 'score' => 0.92, 'label' => 'positive' } ],
          'deployedModelId' => '1234567890' }
      }
    },

    # Batch
    batch_prediction_create: {
      title: 'Batch: Create prediction job',
      description: 'Create projects.locations.batchPredictionJobs',
      batch: true,
      input_fields: ->() {
        [
          { name: 'displayName', optional: false },
          { name: 'model', optional: false, hint: 'Full model resource or publisher model' },
          { name: 'gcsInputUris', type: 'array', of: 'string', optional: false },
          { name: 'instancesFormat', optional: false, hint: 'jsonl,csv,bigquery,tf-record,file-list' },
          { name: 'predictionsFormat', optional: false, hint: 'jsonl,csv,bigquery' },
          { name: 'gcsOutputUriPrefix', optional: false, hint: 'gs://bucket/path/' },
          { name: 'modelParameters', type: 'object' }
        ]
      },
      execute: ->(connection, input) {
        # Confirm regional endpoint (UCAIP)
        call(:ensure_regional_location!) # will throw if connection does not contain required element
        # Build Path
        path = "/v1/projects/#{connection['project_id']}/locations/#{connection['location']}/batchPredictionJobs"
        # Build payload
        payload = {
          'displayName' => input['displayName'],
          'model' => input['model'],
          'inputConfig' => {
            'instancesFormat' => input['instancesFormat'],
            'gcsSource' => { 'uris' => input['gcsInputUris'] }
          },
          'outputConfig' => {
            'predictionsFormat' => input['predictionsFormat'],
            'gcsDestination' => { 'outputUriPrefix' => input['gcsOutputUriPrefix'] }
          },
          'modelParameters' => input['modelParameters']
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
        # Post
        post(path).payload(payload)
      },
      output_fields: ->(object_definitions) { object_definitions['batch_job'] },
      sample_output: ->() {
        { 'name' => 'projects/p/locations/us-central1/batchPredictionJobs/123',
          'displayName' => 'batch-2025-10-06',
          'state' => 'JOB_STATE_PENDING',
          'model' => 'projects/p/locations/us-central1/models/456' }
      }
    },

    batch_prediction_get: {
      title: 'Batch: Fetch prediction job (get)',
      description: 'Get a batch prediction job by ID',
      batch: true,
      input_fields: ->() { [ { name: 'job_id', optional: false } ] },
      execute: ->(connection, input) {
        # Confirm regional endpoint (UCAIP)
        call(:ensure_regional_location!) # will throw if connection does not contain required element
        # Find the job
        name = input['job_id'].to_s.start_with?('projects/') ?
          input['job_id'] :
          "projects/#{connection['project_id']}/locations/#{connection['location']}/batchPredictionJobs/#{input['job_id']}"
        # Get
        get("/v1/#{name}")
      },
      output_fields: ->(object_definitions) { object_definitions['batch_job'] },
      sample_output: ->() {
        { 'name' => 'projects/p/locations/us-central1/batchPredictionJobs/123',
          'displayName' => 'batch-2025-10-06',
          'state' => 'JOB_STATE_SUCCEEDED',
          'outputInfo' => { 'gcsOutputDirectory' => 'gs://my-bucket/prediction-...' } }
      }
    }
  },

  # ====== PICK LISTS ==================================================
  pick_lists: {
    # --- Modes
    modes_classification: ->() {
      [%w[Embedding embedding], %w[Generative generative], %w[Hybrid hybrid]]
    },
    modes_grounding: ->() { [%w[Google\ Search google_search], %w[Vertex\ AI\ Search vertex_ai_search]] },

    # --- Models
    models_embedding: ->(connection) {
      # Models for embeddings get the complete global resource path
      resp = get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
              .params(pageSize: 2000, listAllVersions: true)
      ids = (resp['publisherModels'] || [])
              .map { |m| m['name'].to_s.split('/').last }
              .select { |id| id.include?('embedding') || id.start_with?('text-embedding') }
              .uniq
              .sort
      ids.map { |id|
        [id, "projects/#{connection['project_id']}/locations/global/publishers/google/models/#{id}"]
      }
    },

    # All Gemini-like text/VLM models, with preview 2.5s emitting a GLOBAL-anchored value
    models_generative: ->(connection) {
      resp = get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
              .params(pageSize: 2000, listAllVersions: true)
      items = (resp['publisherModels'] || [])
                .map { |m| m['name'].to_s.split('/').last }
                .select { |id| id.start_with?('gemini-') }  # exclude imagen
                .uniq
                .sort
      items.map { |id|
        [id, "projects/#{connection['project_id']}/locations/global/publishers/google/models/#{id}"]
      }
    },

    # --- Roles
    roles: ->() { [['user','user'], ['model','model'], ['system','system']] }

  },

  # ====== METHODS =====================================================
  methods: {
    # Normalizes a "model" input into a full resource path.
    build_model_path_with_global_preview: ->(connection, model) {
      m = (model || '').strip
      return m if m.start_with?('projects/')  # full path from picklist => use as-is

      # Allow shorthand "google/models/..." -> "publishers/google/models/..."
      m = "publishers/#{m}" if m.start_with?('google/models/')

      # For publisher models (Gemini/embeddings), default to global; it’s the safest superset.
      loc = 'global'

      if m.start_with?('publishers/')
        "projects/#{connection['project_id']}/locations/#{loc}/#{m}"
      else
        "projects/#{connection['project_id']}/locations/#{loc}/publishers/google/models/#{m}"
      end
    },

    build_endpoint_path: ->(connection, endpoint) {
      ep = (endpoint || '').strip
      ep.start_with?('projects/') ? ep : "projects/#{connection['project_id']}/locations/#{connection['location']}/endpoints/#{ep}"
    },

    # Build a single text for embedding/classification
    build_email_text: ->(subject, body) {
      s = subject.to_s.strip
      b = body.to_s.strip
      parts = []
      parts << "Subject: #{s}" if s.present?
      parts << "Body:\n#{b}" if b.present?
      parts.join("\n\n")
    },

    ensure_regional_location!: ->(connection) {
      loc = (connection['location'] || '').downcase
      error("This action requires a regional location (e.g., us-central1). Current location is '#{loc}'.") if loc.blank? || loc == 'global'
    },
    # Extracts a float vector from Vertex embedding prediction shapes
    extract_embedding_vector: ->(pred) {
      vec = pred.dig('embeddings', 0, 'values') ||
            pred.dig('embeddings', 'values') ||
            pred['values']
      error('Embedding prediction missing values') if vec.blank?
      vec.map(&:to_f)
    },

    vector_cosine_similarity: ->(a, b) {
      return 0.0 if a.blank? || b.blank?
      # dot(a,b) / (||a|| * ||b||)
      dot = 0.0
      sum_a = 0.0
      sum_b = 0.0
      len = [a.length, b.length].min
      i = 0
      while i < len
        ai = a[i].to_f
        bi = b[i].to_f
        dot += ai * bi
        sum_a += ai * ai
        sum_b += bi * bi
        i += 1
      end
      denom = Math.sqrt(sum_a) * Math.sqrt(sum_b)
      denom.zero? ? 0.0 : (dot / denom)
    },

    # Calls Gemini to choose among categories and explain (structured output)
    llm_referee: ->(connection, model, email_text, shortlist_names, all_cats) {
      model_path = call(:build_model_path_with_global_preview, connection, model)

      # Build a normalized list (name + optional details) and restrict to shortlist if provided
      cats_norm = all_cats.map { |c|
        c.is_a?(Hash) ? c : { 'name' => c.to_s }
      }
      allowed = shortlist_names.present? ? shortlist_names : cats_norm.map { |c| c['name'] }

      system_text = <<~SYS
        You are a strict email classifier. Choose exactly one category from the allowed list.
        Output MUST be valid JSON only (no prose).
        Confidence is a calibrated estimate in [0,1]. Keep reasoning crisp (<= 2 sentences).
      SYS

      user_text = <<~USR
        Email:
        #{email_text}

        Allowed categories:
        #{allowed.join(", ")}

        Category descriptions (if any):
        #{cats_norm.map { |c|
            desc = c['description']
            exs = (c['examples'] || [])
            line = "- #{c['name']}"
            line += ": #{desc}" if desc.present?
            line += " | examples: #{exs.join(' ; ')}" if exs.present?
            line
          }.join("\n")}
      USR

      payload = {
        'systemInstruction' => { 'role' => 'SYSTEM', 'parts' => [ { 'text' => system_text } ] },
        'contents' => [
          { 'role' => 'USER', 'parts' => [ { 'text' => user_text } ] }
        ],
        'generationConfig' => {
          'temperature' => 0,
          'maxOutputTokens' => 256,
          'responseMimeType' => 'application/json',
          'responseSchema' => {
            'type' => 'object',
            'additionalProperties' => false,
            'properties' => {
              'category' => { 'type' => 'string' },
              'confidence' => { 'type' => 'number' },
              'reasoning' => { 'type' => 'string' },
              'distribution' => {
                'type' => 'array',
                'items' => {
                  'type' => 'object',
                  'additionalProperties' => false,
                  'properties' => {
                    'category' => { 'type' => 'string' },
                    'prob' => { 'type' => 'number' }
                  },
                  'required' => %w[category prob]
                }
              }
            },
            'required' => %w[category]
          }
        }
      }.delete_if { |_k, v| v.nil? }

      resp = post("/v1/#{model_path}:generateContent").payload(payload)
      text = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s.strip
      parsed = JSON.parse(text) rescue { 'category' => nil, 'confidence' => nil, 'reasoning' => nil, 'distribution' => [] }

      # Ensure category stays within allowed list
      unless parsed['category'].present? && allowed.include?(parsed['category'])
        parsed['category'] = allowed.first
      end

      parsed
    },

    sanitize_contents_roles: ->(contents) {
      (contents || []).map do |c|
        dup = c.dup
        r = dup['role'] || dup[:role]
        dup['role'] = r.to_s.downcase if r
        dup
      end
    },

    normalize_system_instruction: ->(si) {
      return nil if si.blank?
      dup = si.dup
      dup['role'] = 'system'
      dup
    }
  },

  # ====== TRIGGERS ====================================================
  triggers: {},

  # ====== CUSTOM ACTION SUPPORT  ======================================
  custom_action: true,
  custom_action_help: {
    body: 'Create custom Vertex AI operations using the established connection'
  }
}
