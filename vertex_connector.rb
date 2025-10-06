# frozen_string_literal: true

{
  title: 'Vertex AI Adapter',
  version: '0.9.0-draft',
  description: 'Vertex AI (Gemini + Text Embeddings + Endpoints) via service account JWT',

  # ====== CONNECTION ==================================================
  connection: {
    fields: [
      { name: 'project_id', optional: false, hint: 'GCP project ID' },
      { name: 'location', optional: false, hint: 'e.g., global, us-central1, us-east4' },
      { name: 'quota_project_id', label: 'Quota/billing project (optional)', optional: true,
        hint: 'Sets x-goog-user-project for billing/quota. Service account must have roles/serviceusage.serviceUsageConsumer on this project.' },

      { name: 'client_email', label: 'Service account client_email', optional: false },
      { name: 'private_key',  label: 'Service account private_key',  optional: false,
        control_type: 'password', multiline: true,
        hint: 'Include BEGIN/END PRIVATE KEY lines.' },

      { name: 'scope', optional: true, hint: 'OAuth scope(s)',
        default: 'https://www.googleapis.com/auth/cloud-platform',
        control_type: 'select',
        options: [['Cloud Platform (all)', 'https://www.googleapis.com/auth/cloud-platform']] }
    ],

    authorization: {
      type: 'custom_auth',

      acquire: ->(connection) {
        iss = connection['client_email'].to_s.strip
        key = connection['private_key'].to_s

        error('Missing client_email for service account') if iss.blank?
        error('Missing private_key for service account') if key.blank?

        # Normalize pasted keys with literal "\n"
        key = key.gsub(/\\n/, "\n")

        # Guard for clock skew
        iat = Time.now.to_i - 60
        exp = iat + 3600 # 1 hour

        jwt_body = {
          iat: iat,
          exp: exp,
          aud: 'https://oauth2.googleapis.com/token',
          iss: iss,
          scope: (connection['scope'].presence || 'https://www.googleapis.com/auth/cloud-platform')
        }

        assertion = workato.jwt_encode(jwt_body, key, 'RS256')

        post('https://oauth2.googleapis.com/token')
          .payload(grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: assertion)
          .request_format_www_form_urlencoded
      },

      apply: ->(connection) {
        hdrs = { 'Authorization': "Bearer #{connection['access_token']}" }
        # Only send X-Goog-User-Project if caller explicitly configured it and has the right IAM.
        if connection['quota_project_id'].present?
          hdrs['X-Goog-User-Project'] = connection['quota_project_id'].to_s
        end
        headers(hdrs)

      },

      # Let Workato trigger re-acquire on auth errors
      refresh_on: [401, 403],
      detect_on:  [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    },

    base_uri: ->(_connection) { 'https://aiplatform.googleapis.com' }
  },

  test: ->(connection) {
    # Use a regional location for endpoints listing even if caller chose global
    region = (connection['location'].presence || 'us-central1').to_s.downcase
    region = 'us-central1' if region == 'global'
    get("https://aiplatform.googleapis.com/v1/projects/#{connection['project_id']}/locations/#{region}/endpoints")
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
          # tool/function scaffolding (pass-through)
          { name: 'functionCall',        type: 'object' },
          { name: 'functionResponse',    type: 'object' },
          { name: 'executableCode',      type: 'object' },
          { name: 'codeExecutionResult', type: 'object' }
        ]
      }
    },

    # Per contract: role ∈ {user, model}
    content: {
      fields: ->(object_definitions) {
        [
          { name: 'role', control_type: 'select', pick_list: 'roles', optional: false },
          { name: 'parts', type: 'array', of: 'object',
            properties: object_definitions['content_part'], optional: false }
        ]
      }
    },

    generation_config: {
      fields: ->() {
        [
          { name: 'temperature',       type: 'number'  },
          { name: 'topP',              type: 'number'  },
          { name: 'topK',              type: 'integer' },
          { name: 'maxOutputTokens',   type: 'integer' },
          { name: 'candidateCount',    type: 'integer' },
          { name: 'stopSequences',     type: 'array', of: 'string' },
          { name: 'responseMimeType' },
          { name: 'responseSchema',    type: 'object' } # structured output
        ]
      }
    },

    safety_setting: {
      fields: ->() {
        [
          { name: 'category'  },   # e.g., HARM_CATEGORY_*
          { name: 'threshold' }    # e.g., BLOCK_LOW_AND_ABOVE
        ]
      }
    },

    generate_content_output: {
      fields: ->() {
        [
          { name: 'responseId' },
          { name: 'modelVersion' },
          { name: 'usageMetadata', type: 'object', properties: [
              { name: 'promptTokenCount',     type: 'integer' },
              { name: 'candidatesTokenCount', type: 'integer' },
              { name: 'totalTokenCount',      type: 'integer' }
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

    # Align to contract: embeddings object, not array
    embed_output: {
      fields: ->() {
        [
          { name: 'predictions', type: 'array', of: 'object', properties: [
              { name: 'embeddings', type: 'object', properties: [
                  { name: 'values', type: 'array', of: 'number' },
                  { name: 'statistics', type: 'object', properties: [
                      { name: 'truncated',   type: 'boolean' },
                      { name: 'token_count', type: 'integer' }
                    ]
                  }
                ]
              }
            ]
          },
          # NEW: surface billing metadata from the REST response
          { name: 'metadata', type: 'object', properties: [
              { name: 'billableCharacterCount', type: 'integer' }
            ]
          }
        ]
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

    # -------------------- Email categorization ------------------------
    gen_categorize_email: {
      title: 'Generative - Categorize email',
      description: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->() {
        [
          { name: 'mode', control_type: 'select', pick_list: 'modes_classification', optional: false, default: 'embedding',
            hint: 'embedding (deterministic), generative (LLM-only), or hybrid (embeddings + LLM referee).' },

          { name: 'subject', optional: true },
          { name: 'body',    optional: true },

          { name: 'categories', optional: false, type: 'array', of: 'object', properties: [
              { name: 'name',      optional: false },
              { name: 'description' },
              { name: 'examples',  type: 'array', of: 'string' }
            ],
            hint: 'At least 2. You can also pass simple strings (names only).' },

          { name: 'embedding_model', label: 'Embedding model', control_type: 'select', pick_list: 'models_embedding', optional: true,
            default: 'text-embedding-005', hint: 'Used in embedding or hybrid modes.' },

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
        body = (input['body']    || '').to_s.strip
        email_text = call(:build_email_text, subj, body)
        error('Provide subject and/or body') if email_text.blank?

        # Normalize categories
        raw_cats = input['categories']
        cats = call(:safe_array, raw_cats).map { |c|
          if c.is_a?(String)
            { 'name' => c, 'description' => nil, 'examples' => [] }
          else
            { 'name' => c['name'] || c[:name],
              'description' => c['description'] || c[:description],
              'examples' => call(:safe_array, (c['examples'] || c[:examples])) }
          end
        }.select { |c| c['name'].present? }
        error('At least 2 categories are required') if cats.length < 2

        mode      = (input['mode'] || 'embedding').to_s.downcase
        min_conf  = (input['min_confidence'].presence || 0.25).to_f

        # Embedding/hybrid
        if %w[embedding hybrid].include?(mode)
          emb_model      = (input['embedding_model'].presence || 'text-embedding-005')
          emb_model_path = call(:build_embedding_model_path, connection, emb_model)

          email_inst = { 'content' => email_text, 'task_type' => 'RETRIEVAL_QUERY' }
          cat_insts  = cats.map do |c|
            txt = [c['name'], c['description'], *(c['examples'] || [])].compact.join("\n")
            { 'content' => txt, 'task_type' => 'RETRIEVAL_DOCUMENT' }
          end

          emb_resp = call(:predict_embeddings, emb_model_path, [email_inst] + cat_insts)
          preds    = call(:safe_array, emb_resp && emb_resp['predictions'])
          error('Embedding model returned no predictions') if preds.empty?

          email_vec = call(:extract_embedding_vector, preds.first)
          cat_vecs  = preds.drop(1).map { |p| call(:extract_embedding_vector, p) }

          sims = cat_vecs.each_with_index.map { |v, i| [i, call(:vector_cosine_similarity, email_vec, v)] }
          sims.sort_by! { |(_i, s)| -s }

          scores = sims.map { |(i, s)| { 'category' => cats[i]['name'], 'score' => (((s + 1.0) / 2.0).round(6)), 'cosine' => s.round(6) } }
          top = scores.first
          chosen     = top['category']
          confidence = top['score']

          chosen = input['fallback_category'] if confidence < min_conf && input['fallback_category'].present?

          result = {
            'mode'       => mode,
            'chosen'     => chosen,
            'confidence' => confidence.round(4),
            'scores'     => scores
          }

          if (mode == 'hybrid' || input['return_explanation']) && input['generative_model'].present?
            top_k     = [[(input['top_k'] || 3).to_i, 1].max, cats.length].min
            shortlist = scores.first(top_k).map { |h| h['category'] }
            referee   = call(:llm_referee, connection, input['generative_model'], email_text, shortlist, cats, input['fallback_category'])
            result['referee'] = referee

            if referee['category'].present? && shortlist.include?(referee['category'])
              result['chosen']     = referee['category']
              result['confidence'] = [result['confidence'], referee['confidence']].compact.max
            end
          end

          result

        elsif mode == 'generative'
          error('generative_model is required when mode=generative') if input['generative_model'].blank?
          referee = call(:llm_referee, connection, input['generative_model'], email_text, cats.map { |c| c['name'] }, cats, input['fallback_category'])
          chosen =
            if referee['confidence'].to_f < min_conf && input['fallback_category'].present?
              input['fallback_category']
            else
              referee['category']
            end

          { 'mode' => mode, 'chosen' => chosen, 'confidence' => referee['confidence'], 'referee' => referee }

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

    # -------------------- Generate content (Gemini) -------------------
    gen_generate_content: {
      title: 'Generative - Generate content (Gemini)',
      description: 'POST :generateContent on a publisher model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->(object_definitions) {
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative', hint: 'Select or use a custom value.',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'contents', type: 'array', of: 'object',
            properties: object_definitions['content'], optional: false },

          # Contract-friendly: accept plain text; connector will inject a proper systemInstruction
          { name: 'system_preamble', label: 'System preamble (text)', optional: true, hint: 'Optional system guidance.' },

          { name: 'tools', type: 'array', of: 'object', properties: [
              { name: 'googleSearch', type: 'object' },
              { name: 'retrieval',    type: 'object' },
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
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst = call(:system_instruction_from_text, input['system_preamble'])

        gen_cfg = call(:sanitize_generation_config, input['generationConfig'])

        payload = {
          'contents'          => contents,
          'systemInstruction' => sys_inst,
          'tools'             => input['tools'],
          'toolConfig'        => input['toolConfig'],
          'safetySettings'    => input['safetySettings'],
          'generationConfig'  => gen_cfg
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

    # -------------------- Grounded generation -------------------------
    gen_generate_grounded: {
      title: 'Generative - Generate (grounded)',
      description: 'Generate with grounding via Google Search or Vertex AI Search',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->(object_definitions) {
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'grounding', control_type: 'select', pick_list: 'modes_grounding', optional: false },

          { name: 'vertex_ai_search_datastore',
            hint: 'Required when grounding=vertex_ai_search: projects/.../locations/.../collections/default_collection/dataStores/...' },

          { name: 'contents', type: 'array', of: 'object',
            properties: object_definitions['content'], optional: false },

          { name: 'system_preamble', label: 'System preamble (text)', optional: true },

          { name: 'toolConfig', type: 'object' },

          { name: 'generationConfig', type: 'object', properties: object_definitions['generation_config'] },

          { name: 'safetySettings',  type: 'array', of: 'object', properties: object_definitions['safety_setting'] }
        ]
      },

      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        tools =
          if input['grounding'] == 'google_search'
            [ { 'googleSearch' => {} } ]
          else
            ds = input['vertex_ai_search_datastore']
            error('vertex_ai_search_datastore is required for vertex_ai_search grounding') if ds.blank?
            [ { 'retrieval' => { 'vertexAiSearch' => { 'datastore' => ds } } } ]
          end

        gen_cfg = call(:sanitize_generation_config, input['generationConfig'])

        payload = {
          'contents'          => contents,
          'systemInstruction' => sys_inst,
          'tools'             => tools,
          'toolConfig'        => input['toolConfig'],
          'safetySettings'    => input['safetySettings'],
          'generationConfig'  => gen_cfg
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

    # -------------------- New: Query with context chunks --------------
    gen_answer_with_context: {
      title: 'Generative - Answer with provided context chunks',
      description: 'Answer a question using caller-supplied context chunks (RAG-lite). Returns structured JSON with citations.',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->() {
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'question', optional: false },

          { name: 'context_chunks', type: 'array', of: 'object', optional: false, properties: [
              { name: 'id' },
              { name: 'text', optional: false },
              { name: 'source' },              # e.g., dataset, email, kb
              { name: 'uri', label: 'URI' },  # link to doc if any
              { name: 'score', type: 'number' },
              { name: 'metadata', type: 'object' }
            ],
            hint: 'Pass the top-N chunks from your retriever / process.'
          },

          { name: 'max_chunks', type: 'integer', optional: true, default: 20,
            hint: 'Hard cap to avoid overlong prompts.' },

          { name: 'system_preamble', optional: true,
            hint: 'Optional guardrails (e.g., “only answer from context; say I don’t know otherwise”).' },

          { name: 'temperature', type: 'number', optional: true, hint: 'Override temperature (default 0).' }
        ]
      },

      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        max_chunks = call(:clamp_int, (input['max_chunks'] || 20), 1, 100)
        chunks     = call(:safe_array, input['context_chunks']).first(max_chunks)


        error('context_chunks must be a non-empty array') if chunks.blank?

        # Build a deterministic, schema-ed JSON response
        gen_cfg = {
          'temperature'      => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
          'maxOutputTokens'   => 1024,
          'responseMimeType'  => 'application/json',
          'responseSchema'    => {
            'type'  => 'object',
            'additionalProperties' => false,
            'properties' => {
              'answer'     => { 'type' => 'string' },
              'citations'  => {
                'type'  => 'array',
                'items' => {
                  'type' => 'object',
                  'additionalProperties' => false,
                  'properties' => {
                    'chunk_id' => { 'type' => 'string' },
                    'source'   => { 'type' => 'string' },
                    'uri'      => { 'type' => 'string' },
                    'score'    => { 'type' => 'number' }
                  }
                }
              }
            },
            'required' => ['answer']
          }
        }

        sys_inst = call(:system_instruction_from_text,
          input['system_preamble'].presence ||
            'Answer using ONLY the provided context chunks. ' \
            'If the context is insufficient, reply with “I don’t know.” Keep answers concise and cite chunk IDs.')

        # Format a single USER message containing the question + all chunks
        context_blob = call(:format_context_chunks, chunks)
        contents = [
          { 'role' => 'user', 'parts' => [
              { 'text' => "Question:\n#{input['question']}\n\nContext:\n#{context_blob}" }
            ]
          }
        ]

        payload = {
          'contents'          => contents,
          'systemInstruction' => sys_inst,
          'generationConfig'  => gen_cfg
        }

        resp = post("/v1/#{model_path}:generateContent").payload(payload)

        text = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
        parsed = call(:safe_parse_json, text)
        {
          'answer'     => parsed['answer'] || text,
          'citations'  => parsed['citations'] || [],
          'responseId' => resp['responseId'],
          'usage'      => resp['usageMetadata']
        }
      },

      output_fields: ->() {
        [
          { name: 'answer' },
          { name: 'citations', type: 'array', of: 'object', properties: [
              { name: 'chunk_id' }, { name: 'source' }, { name: 'uri' }, { name: 'score', type: 'number' }
            ]
          },
          { name: 'responseId' },
          { name: 'usage', type: 'object', properties: [
              { name: 'promptTokenCount', type: 'integer' },
              { name: 'candidatesTokenCount', type: 'integer' },
              { name: 'totalTokenCount', type: 'integer' }
            ]
          }
        ]
      },

      sample_output: ->() {
        {
          'answer' => 'The outage began at 09:12 UTC due to a misconfigured firewall rule.',
          'citations' => [
            { 'chunk_id' => 'doc-42#p3', 'source' => 'postmortem', 'uri' => 'https://kb/acme/pm-42#p3', 'score' => 0.89 }
          ],
          'responseId' => 'resp-789',
          'usage' => { 'promptTokenCount' => 311, 'candidatesTokenCount' => 187, 'totalTokenCount' => 498 }
        }
      }
    },

    # -------------------- Embeddings ---------------------------------
    embed_text: {
      title: 'Embedding - Embed text',
      description: 'POST :predict on a publisher embedding model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->() {
        [
          { name: 'model', label: 'Embedding model', optional: false, control_type: 'select', pick_list: 'models_embedding', default: 'text-embedding-005',
            hint: 'Select or use a custom value.', toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Embedding model', type: 'string', control_type: 'text' } },
          { name: 'texts', type: 'array', of: 'string', optional: false },

          { name: 'task', hint: 'Optional: RETRIEVAL_QUERY or RETRIEVAL_DOCUMENT' },

          { name: 'autoTruncate', type: 'boolean', hint: 'Truncate long inputs automatically' },

          { name: 'outputDimensionality', type: 'integer', optional: true, convert_input: 'integer_conversion',
            hint: 'Optional dimensionality reduction (see model docs).' }
        ]
      },

      execute: ->(connection, input) {
        model_path = call(:build_embedding_model_path, connection, input['model'])

        instances = call(:safe_array, input['texts']).map { |t|
          { 'content' => t, 'task_type' => input['task'] }.delete_if { |_k, v| v.nil? }
        }

        # Coerce/validate embedding parameters to correct JSON types
        params = call(:sanitize_embedding_params, {
          'autoTruncate'         => input['autoTruncate'],
          'outputDimensionality' => input['outputDimensionality']
        })

        call(:predict_embeddings, model_path, instances, params)
      },

      output_fields: ->(object_definitions) { object_definitions['embed_output'] },

      sample_output: ->() {
        {
          'predictions' => [
            { 'embeddings' => { 'values' => [0.012, -0.034, 0.056],
              'statistics' => { 'truncated' => false, 'token_count' => 21 } } },
            { 'embeddings' => { 'values' => [0.023, -0.045, 0.067],
              'statistics' => { 'truncated' => false, 'token_count' => 18 } } }
          ],
          'metadata' => { 'billableCharacterCount' => 230 }
        }
      }

    },

    # -------------------- Utility: count tokens -----------------------
    count_tokens: {
      title: 'Utility: Count tokens',
      description: 'POST :countTokens on a publisher model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->(object_definitions) {
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'contents', type: 'array', of: 'object', properties: object_definitions['content'], optional: false },

          { name: 'system_preamble', label: 'System preamble (text)', optional: true }
        ]
      },

      execute: ->(connection, input) {
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        post("/v1/#{model_path}:countTokens").payload({
          'contents'          => contents,
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

    # -------------------- GCS simple upload ---------------------------
    upload_to_gcs: {
      title: 'Utility: Upload to Cloud Storage (simple upload)',
      description: 'Simple media upload to GCS (uploadType=media)',

      input_fields: ->() {
        [
          { name: 'bucket',      optional: false },
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

    # -------------------- Generic endpoint predict --------------------
    endpoint_predict: {
      title: 'Endpoint predict (custom model)',
      description: 'POST :predict to a Vertex AI Endpoint',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: ->() {
        [
          { name: 'endpoint',   optional: false, hint: 'Endpoint ID or full resource path' },
          { name: 'instances',  type: 'array', of: 'object', optional: false },
          { name: 'parameters', type: 'object' }
        ]
      },

      execute: ->(connection, input) {
        call(:ensure_regional_location!, connection) # require non-global
        endpoint_path = call(:build_endpoint_path, connection, input['endpoint'])

        post("/v1/#{endpoint_path}:predict")
          .payload({ 'instances' => input['instances'], 'parameters' => input['parameters'] }.delete_if { |_k, v| v.nil? })
      },

      output_fields: ->(object_definitions) { object_definitions['predict_output'] },

      sample_output: ->() {
        { 'predictions' => [ { 'score' => 0.92, 'label' => 'positive' } ],
          'deployedModelId' => '1234567890' }
      }
    },

    # -------------------- Batch --------------------------------------
    batch_prediction_create: {
      title: 'Batch: Create prediction job',
      description: 'Create projects.locations.batchPredictionJobs',
      batch: true,

      input_fields: ->() {
        [
          { name: 'displayName', optional: false },
          { name: 'model',       optional: false, hint: 'Full model resource or publisher model' },
          { name: 'gcsInputUris', type: 'array', of: 'string', optional: false },
          { name: 'instancesFormat',   optional: false, hint: 'jsonl,csv,bigquery,tf-record,file-list' },
          { name: 'predictionsFormat', optional: false, hint: 'jsonl,csv,bigquery' },
          { name: 'gcsOutputUriPrefix', optional: false, hint: 'gs://bucket/path/' },
          { name: 'modelParameters', type: 'object' }
        ]
      },

      execute: ->(connection, input) {
        call(:ensure_regional_location!, connection)
        path = "/v1/projects/#{connection['project_id']}/locations/#{connection['location']}/batchPredictionJobs"

        payload = {
          'displayName'  => input['displayName'],
          'model'        => input['model'],
          'inputConfig'  => {
            'instancesFormat' => input['instancesFormat'],
            'gcsSource'       => { 'uris' => input['gcsInputUris'] }
          },
          'outputConfig' => {
            'predictionsFormat' => input['predictionsFormat'],
            'gcsDestination'    => { 'outputUriPrefix' => input['gcsOutputUriPrefix'] }
          },
          'modelParameters' => input['modelParameters']
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

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
        call(:ensure_regional_location!, connection)
        name = input['job_id'].to_s.start_with?('projects/') ?
          input['job_id'] :
          "projects/#{connection['project_id']}/locations/#{connection['location']}/batchPredictionJobs/#{input['job_id']}"
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
    modes_classification: ->() {
      [%w[Embedding embedding], %w[Generative generative], %w[Hybrid hybrid]]
    },

    modes_grounding: ->() {
      [%w[Google\ Search google_search], %w[Vertex\ AI\ Search vertex_ai_search]]
    },

    models_embedding: ->(connection) {
      begin
        loc = (connection['location'].presence || 'global').to_s.downcase
        proj = connection['project_id']
        resp = get("https://aiplatform.googleapis.com/v1/projects/#{proj}/locations/#{loc}/publishers/google/models")
                .params(pageSize: 200, view: 'BASIC')
        ids = call(:safe_array, resp && resp['publisherModels'])
                .map { |m| m['name'].to_s.split('/').last }
                .select { |id| id.start_with?('text-embedding') || id.start_with?('multimodal-embedding') || id.start_with?('gemini-embedding') }
                .uniq.sort
      rescue => _e
        ids = %w[gemini-embedding-001 text-embedding-005 multimodal-embedding-001]
      end
      ids.map { |id| [id, id] }
    },

    models_generative: ->(connection) {
      begin
        loc = (connection['location'].presence || 'global').to_s.downcase
        proj = connection['project_id']
        resp = get("https://aiplatform.googleapis.com/v1/projects/#{proj}/locations/#{loc}/publishers/google/models")
                .params(pageSize: 200, view: 'BASIC')
        items = call(:safe_array, resp && resp['publisherModels'])
                  .map { |m| m['name'].to_s.split('/').last }
                  .select { |id| id.start_with?('gemini-') }
                  .uniq.sort
      rescue => _e
        items = %w[gemini-2.5-pro gemini-2.5-flash gemini-2.5-flash-lite]
      end
      items.map { |id| [id, id] }
    },

    # Contract-conformant roles (system handled via system_preamble)
    roles: ->() { [['user','user'], ['model','model']] }
  },

  # ====== METHODS =====================================================
  methods: {
    build_model_path_with_global_preview: ->(connection, model) {
      m = call(:normalize_model_identifier, model)
      error('Model is required') if m.blank?
      return m if m.start_with?('projects/')

      # Accept common short forms
      if m.start_with?('models/')
        m = m.split('/', 2).last # drop leading "models/"
      end
      if m.start_with?('google/models/')
        m = "publishers/#{m}"
      end

      # Respect caller's location; default to global, which is widely supported for Gemini
      loc = (connection['location'].presence || 'global').to_s.downcase

      if m.start_with?('publishers/')
        "projects/#{connection['project_id']}/locations/#{loc}/#{m}"
      else
        "projects/#{connection['project_id']}/locations/#{loc}/publishers/google/models/#{m}"
      end
    },

    build_endpoint_path: ->(connection, endpoint) {
      ep = call(:normalize_endpoint_identifier, endpoint)
      ep.start_with?('projects/') ? ep :
        "projects/#{connection['project_id']}/locations/#{connection['location']}/endpoints/#{ep}"
    },

    build_embedding_model_path: ->(connection, model) {
      m = call(:normalize_model_identifier, model)
      error('Embedding model is required') if m.blank?
      return m if m.start_with?('projects/')

      # Accept common short forms
      if m.start_with?('models/')
        m = m.split('/', 2).last
      end
      if m.start_with?('google/models/')
        m = "publishers/#{m}"
      end

      loc = call(:embedding_region, connection)
      if m.start_with?('publishers/')
        "projects/#{connection['project_id']}/locations/#{loc}/#{m}"
      else
        "projects/#{connection['project_id']}/locations/#{loc}/publishers/google/models/#{m}"
      end
    },

    # Build a single email text body for classification
    build_email_text: ->(subject, body) {
      s = subject.to_s.strip
      b = body.to_s.strip
      parts = []
      parts << "Subject: #{s}" if s.present?
      parts << "Body:\n#{b}"    if b.present?
      parts.join("\n\n")
    },

    ensure_regional_location!: ->(connection) {
      loc = (connection['location'] || '').downcase
      error("This action requires a regional location (e.g., us-central1). Current location is '#{loc}'.") if loc.blank? || loc == 'global'
    },

    # Extracts float vector from embedding prediction (both shapes supported)
    extract_embedding_vector: ->(pred) {
      vec = pred.dig('embeddings', 'values') ||
            pred.dig('embeddings', 0, 'values') ||
            pred['values']
      error('Embedding prediction missing values') if vec.blank?
      vec.map(&:to_f)
    },

    vector_cosine_similarity: ->(a, b) {
      return 0.0 if a.blank? || b.blank?
      error("Embedding dimensions differ: #{a.length} vs #{b.length}") if a.length != b.length
      dot = 0.0; sum_a = 0.0; sum_b = 0.0
      a.each_index do |i|
        ai = a[i].to_f; bi = b[i].to_f
        dot += ai * bi; sum_a += ai * ai; sum_b += bi * bi
      end
      denom = Math.sqrt(sum_a) * Math.sqrt(sum_b)
      denom.zero? ? 0.0 : (dot / denom)
    },

    embedding_region: ->(connection) {
      loc = (connection['location'] || '').to_s.downcase
      # Embeddings support global and multi-regional endpoints. Respect caller choice; default to global.
      loc.present? ? loc : 'global'
    },

    # Conservative instance limits by model family
    embedding_max_instances: ->(model_path_or_id) {
      id = model_path_or_id.to_s.split('/').last
      if id.include?('gemini-embedding-001')
        1
      else
        250
      end
    },

    predict_embeddings: ->(model_path, instances, params={}) {
      max  = call(:embedding_max_instances, model_path)
      preds = []
      billable = 0
      (instances || []).each_slice(max) do |slice|
        resp = post("/v1/#{model_path}:predict")
                .payload({
                  'instances'  => slice,
                  'parameters' => (params.presence || {})
                }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) })
        preds.concat(resp['predictions'] || [])
        billable += resp.dig('metadata', 'billableCharacterCount').to_i
      end
      out = { 'predictions' => preds }
      out['metadata'] = { 'billableCharacterCount' => billable } if billable > 0
      out
    },

    # Minimal, schema-constrained JSON referee using Gemini
    llm_referee: ->(connection, model, email_text, shortlist_names, all_cats, fallback_category = nil) {
      model_path = call(:build_model_path_with_global_preview, connection, model)

      cats_norm = call(:safe_array, all_cats).map { |c| c.is_a?(Hash) ? c : { 'name' => c.to_s } }
      allowed   = if shortlist_names.present?
                     call(:safe_array, shortlist_names).map { |x| x.is_a?(Hash) ? (x['name'] || x[:name]).to_s : x.to_s }
                   else
                     cats_norm.map { |c| c['name'] }
                   end

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
            exs  = call(:safe_array, (c['examples'] || c[:examples]))
            line = "- #{c['name']}"
            line += ": #{desc}" if desc.present?
            line += " | examples: #{exs.join(' ; ')}" if exs.present?
            line
          }.join("\n")}
      USR

      payload = {
        'systemInstruction' => { 'role' => 'system', 'parts' => [ { 'text' => system_text } ] },
        'contents' => [
          { 'role' => 'user', 'parts' => [ { 'text' => user_text } ] }
        ],
        'generationConfig' => {
          'temperature'       => 0,
          'maxOutputTokens'   => 256,
          'responseMimeType'  => 'application/json',
          'responseSchema'    => {
            'type' => 'object',
            'additionalProperties' => false,
            'properties' => {
              'category'     => { 'type' => 'string' },
              'confidence'   => { 'type' => 'number' },
              'reasoning'    => { 'type' => 'string' },
              'distribution' => {
                'type'  => 'array',
                'items' => {
                  'type' => 'object',
                  'additionalProperties' => false,
                  'properties' => {
                    'category' => { 'type' => 'string' },
                    'prob'     => { 'type' => 'number' }
                  },
                  'required' => %w[category prob]
                }
              }
            },
            'required' => %w[category]
          }
        }
      }

      resp   = post("/v1/#{model_path}:generateContent").payload(payload)
      text   = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s.strip
      parsed = JSON.parse(text) rescue { 'category' => nil, 'confidence' => nil, 'reasoning' => nil, 'distribution' => [] }

      # Validate/repair category
      if parsed['category'].present? && !allowed.include?(parsed['category'])
        parsed['category'] = nil
      end
      if parsed['category'].blank? && fallback_category.present?
        parsed['category'] = fallback_category
      end
      error('Referee returned no valid category and no fallback is configured') if parsed['category'].blank?

      parsed
    },

    sanitize_contents_roles: ->(contents) {
      call(:safe_array, contents).each_with_object([]) do |c, acc|
        h = c.is_a?(Hash) ? c.transform_keys { |k| k.to_s } : {}
        role = (h['role'] || '').to_s.downcase
        next if role == 'system' # system handled via systemInstruction
        h['role'] = role if role.present?
        acc << h
      end
    },

    # Accept plain text and produce a proper systemInstruction
    system_instruction_from_text: ->(text) {
      return nil if text.blank?
      { 'role' => 'system', 'parts' => [ { 'text' => text.to_s } ] }
    },

    clamp_int: ->(n, min, max) {
      [[n.to_i, min].max, max].min
    },

    format_context_chunks: ->(chunks) {
      # Stable, parseable layout the model can learn
      call(:safe_array, chunks).each_with_index.map { |c, i|
        cid  = c['id'] || "chunk-#{i+1}"
        src  = c['source']
        uri  = c['uri']
        sc   = c['score']
        meta = c['metadata']

        header = ["[#{cid}]",
                  (src.present? ? "source=#{src}" : nil),
                  (uri.present? ? "uri=#{uri}"     : nil),
                  (sc  ? "score=#{sc}"             : nil)].compact.join(' ')

        body = c['text'].to_s
        meta_str = meta.present? ? "\n(meta: #{meta.to_json})" : ''
        "#{header}\n#{body}#{meta_str}"
      }.join("\n\n---\n\n")
    },

    safe_parse_json: ->(s) {
      JSON.parse(s) rescue { 'answer' => s }
    },
    
    # Normalize a user-provided "model" into a String.
    # Accepts String or Hash (from datapills/pick lists). Prefers common keys.
    normalize_model_identifier: ->(raw) {
      return '' if raw.nil? || raw == true || raw == false
      if raw.is_a?(Hash)
        v = raw['value'] || raw[:value] ||
            raw['id']    || raw[:id]    ||
            raw['path']  || raw[:path]  ||
            raw['name']  || raw[:name]  ||
            raw.to_s
        return v.to_s.strip
      end
      raw.to_s.strip
    },

    # Normalize a user-provided "endpoint" into a String as well.
    normalize_endpoint_identifier: ->(raw) {
      return '' if raw.nil? || raw == true || raw == false
      if raw.is_a?(Hash)
        v = raw['value'] || raw[:value] ||
            raw['id']    || raw[:id]    ||
            raw['path']  || raw[:path]  ||
            raw['name']  || raw[:name]  ||
            raw.to_s
        return v.to_s.strip
      end
      raw.to_s.strip
    },

    # Coerce anything into a safe array for mapping.
    safe_array: ->(v) {
      return [] if v.nil? || v == false
      return v  if v.is_a?(Array)
      # Avoid surprising Hash#to_a (which returns [[:k, v], ...]).
      # For Hash, we usually want a single element collection containing it.
      [v]
    },

    # Like Array#map but safe against nil/false/non-arrays.
    safe_map: ->(v) { call(:safe_array, v).map { |x| yield(x) } },
    
    safe_integer: ->(v) {
      return nil if v.nil?
      begin
        if v.is_a?(String)
          s = v.strip
          return nil if s.empty?
          Integer(s)
        else
          Integer(v)
        end
      rescue
        # Last resort: to_i (tolerates "768 " etc.)
        v.to_i
      end
    },

    safe_float: ->(v) {
      return nil if v.nil?
      begin
        if v.is_a?(String)
          s = v.strip
          return nil if s.empty?
          Float(s)
        else
          Float(v)
        end
      rescue
        v.to_f
      end
    },

    sanitize_embedding_params: ->(raw) {
      h = {}
      # Only include autoTruncate when explicitly true (keeps payload minimal)
      h['autoTruncate'] = true if raw['autoTruncate'] == true

      if raw['outputDimensionality'].present?
        od = call(:safe_integer, raw['outputDimensionality'])
        error('outputDimensionality must be a positive integer') if od && od < 1
        h['outputDimensionality'] = od if od
      end
      h
    },
  
    sanitize_generation_config: ->(cfg) {
      return nil if cfg.nil? || (cfg.respond_to?(:empty?) && cfg.empty?)
      g = cfg.dup

      # Floats
      if g.key?('temperature')     then g['temperature']     = call(:safe_float,  g['temperature'])     end
      if g.key?('topP')            then g['topP']            = call(:safe_float,  g['topP'])            end

      # Integers
      if g.key?('topK')            then g['topK']            = call(:safe_integer, g['topK'])           end
      if g.key?('maxOutputTokens') then g['maxOutputTokens'] = call(:safe_integer, g['maxOutputTokens']) end
      if g.key?('candidateCount')  then g['candidateCount']  = call(:safe_integer, g['candidateCount'])  end

      # Arrays
      if g.key?('stopSequences')
        g['stopSequences'] = call(:safe_array, g['stopSequences']).map(&:to_s)
      end

      g
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
