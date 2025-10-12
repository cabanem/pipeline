# frozen_string_literal: true

{
  title: 'Vertex AI Adapter',
  version: '0.9.0',
  description: 'Vertex AI (Gemini + Text Embeddings + Endpoints) via service account JWT',

  # --------- CONNECTION ---------------------------------------------------

  connection: {
    fields: [
      { name: 'service_account_key_json', control_type: 'text-area',  optional: false, hint: 'Paste full JSON key' },
      { name: 'location',                                             optional: false, hint: 'e.g., global, us-central1, us-east4' },
      { name: 'project_id', optional: false, hint: 'GCP project ID' },
      { name: 'quota_project_id', label: 'Quota/billing project', optional: true, extends_schema: true,
        hint: 'Sets x-goog-user-project for billing/quota. Service account must have roles/serviceusage.serviceUsageConsumer on this project.' },
      { name: 'show_legacy_sa_fields', label: 'Show legacy SA fields',optional: true,
        extends_schema: true, default: false, control_type: 'checkbox', type: 'boolean' },
        
      { name: 'client_email', label: 'Service account client_email (deprecated)', optional: true, extends_schema: true, 
        ngIf: 'input.show_legacy_sa_fields == "true"' },
      { name: 'private_key',  label: 'Service account private_key (deprecated)',  optional: true, control_type: 'password', multiline: true, 
        extends_schema: true, ngIf: 'input.show_legacy_sa_fields == "true"', hint: 'Include BEGIN/END PRIVATE KEY lines.' },
    ],

    authorization: {
      # Custom JWT-bearer --> OAuth access token exchange
      type: 'custom',

      acquire: lambda do |connection|
        # Build token via cache-aware helper
        scopes   = call(:const_default_scopes) # ['https://www.googleapis.com/auth/cloud-platform']
        token    = call(:auth_build_access_token!, connection, scopes: scopes)
        scope_key = scopes.join(' ')

        # Pull metadata from the cache written by auth_build_access_token!
        cached   = (connection['__token_cache'] ||= {})[scope_key]

        # Build payload
        {
          access_token: token,
          token_type:   'Bearer',
          expires_in:   (cached && cached['expires_in']) || 3600,
          expires_at:   (cached && cached['expires_at']) || (Time.now + 3600 - 60).utc.iso8601
        }
      end,

      apply: lambda do |connection|
        # Keep headers minimal; envelope/correlation lives in actions.
        headers(
          'Authorization': "Bearer #{connection['access_token']}",
          'x-goog-user-project': connection['quota_project_id'].to_s.strip.presence
        )
      end,

      token_url: 'https://oauth2.googleapis.com/token',

      # Let Workato trigger re-acquire on auth errors
      refresh_on: [401],
      detect_on:  [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    },

    base_uri: lambda do |_connection|
      'https://aiplatform.googleapis.com'
    end
  },

  # --------- CONNECTION TEST ----------------------------------------------

  test: ->(_connection) {
    get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
      .params(pageSize: 1, view: 'BASIC')
  },

  # --------- OBJECT DEFINITIONS -------------------------------------------

  object_definitions: {
    content_part: {
      fields: ->() {
        [
          { name: 'text' },
          { name: 'inlineData', type: 'object', properties: [
              { name: 'mimeType' }, { name: 'data', hint: 'Base64' }
          ]},
          { name: 'fileData', type: 'object', properties: [
              { name: 'mimeType' }, { name: 'fileUri' }
          ]},
          { name: 'functionCall', type: 'object', properties: [
              { name: 'name' }, { name: 'args', type: 'object' }
          ]},
          { name: 'functionResponse', type: 'object', properties: [
              { name: 'name' }, { name: 'response', type: 'object' }
          ]},
          { name: 'executableCode', type: 'object', properties: [
              { name: 'language' }, { name: 'code' }
          ]},
          { name: 'codeExecutionResult', type: 'object', properties: [
              { name: 'outcome' }, { name: 'stdout' }, { name: 'stderr' }
          ]}
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
            { name: 'safetyRatings', type: 'array', of: 'object', properties: [
                { name: 'category' }, { name: 'probability' }, { name: 'blocked', type: 'boolean' }
            ]},
            { name: 'groundingMetadata', type: 'object', properties: [
                { name: 'citationSources', type: 'array', of: 'object', properties: [
                    { name: 'uri' }, { name: 'license' }, { name: 'title' }, { name: 'publicationDate' }
                ]}
            ]},
            { name: 'content', type: 'object', properties: [
                { name: 'role' },
                { name: 'parts', type: 'array', of: 'object', properties: [
                    { name: 'text' }, { name: 'inlineData', type: 'object' }, { name: 'fileData', type: 'object' }
                ]}
            ]}
          ]}
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
                      { name: 'token_count', type: 'number' } # sometimes returned as decimal place, e.g., 7.0
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
          { name: 'partialFailures', type: 'array', of: 'object' },
          { name: 'labels', type: 'object' }
        ]
      }
    },

    envelope_fields: {
      fields: ->(_) {
        [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message', type: 'string' },
            { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id', type: 'string' }
          ] }
        ]
      }
    }

  },

  # --------- ACTIONS ------------------------------------------------------

  actions: {

    # -------------------- Email categorization ------------------------
    gen_categorize_email: {
      title: 'Generative - Categorize email',
      description: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |_|
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
      end,

      output_fields: lambda do |object_definitions|
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
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        t0   = Time.now
        corr = call(:build_correlation_id)
        begin
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

            emb_resp = call(:predict_embeddings, connection, emb_model_path, [email_inst] + cat_insts)
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
              if result['confidence'].to_f < min_conf && input['fallback_category'].present?
                result['chosen'] = input['fallback_category']
              end

            end

            result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

          elsif mode == 'generative'
            error('generative_model is required when mode=generative') if input['generative_model'].blank?
            referee = call(:llm_referee, connection, input['generative_model'], email_text, cats.map { |c| c['name'] }, cats, input['fallback_category'])
            chosen =
              if referee['confidence'].to_f < min_conf && input['fallback_category'].present?
                input['fallback_category']
              else
                referee['category']
              end

            result = { 
              'mode' => mode,
              'chosen' => chosen,
              'confidence' => referee['confidence'],
              'referee' => referee 
            }

            result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

          else
            error("Unknown mode: #{mode}")
          end
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
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
      end
    },

    # -------------------- Generate content (Gemini) -------------------
    # --- Gemini
    gen_generate_content: {
      title: 'Generative - Generate content (Gemini)',
      description: 'POST :generateContent on a publisher model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions|
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
      end,

      output_fields: lambda do |object_definitions|
         object_definitions['generate_content_output']
      end,

      execute: lambda do |connection, input|
        # Compute model path
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        # Build payload
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

        # Call endpoint
        loc = (connection['location'].presence || 'global').to_s.downcase
        post(call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")).payload(payload)

      end,

      sample_output: lambda do
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
      end
    },

    # --- Grounded
    gen_generate_grounded: {
      title: 'Generative - Generate (grounded)',
      description: 'Generate with grounding via Google Search or Vertex AI Search',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'grounding', control_type: 'select', pick_list: 'modes_grounding', optional: false },

          { name: 'vertex_ai_search_datastore', optional: true,
            hint: 'projects/.../locations/.../collections/default_collection/dataStores/...' },
          { name: 'vertex_ai_search_engine', optional: true, hint: 'projects/.../locations/.../collections/.../engines/...' },
          { name: 'contents', type: 'array', of: 'object',
            properties: object_definitions['content'], optional: false },

          { name: 'system_preamble', label: 'System preamble (text)', optional: true },

          { name: 'toolConfig', type: 'object' },

          { name: 'generationConfig', type: 'object', properties: object_definitions['generation_config'] },

          { name: 'safetySettings',  type: 'array', of: 'object', properties: object_definitions['safety_setting'] }
        ]
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['generate_content_output']
      end,

      execute: lambda do |connection, input|
        # Compute model path
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        # Build payload
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        tools =
          if input['grounding'] == 'google_search'
            tools = [ { 'googleSearch' => {} } ]
          else
            ds  = input['vertex_ai_search_datastore'].to_s
            eng = input['vertex_ai_search_engine'].to_s
            # Enforce XOR (exactly one)
            error('Provide exactly one of vertex_ai_search_datastore OR vertex_ai_search_engine') if (ds.blank? && eng.blank?) || (ds.present? && eng.present?)
            vas = {}
            vas['datastore'] = ds unless ds.blank?
            vas['engine']    = eng unless eng.blank?
            tools = [ { 'retrieval' => { 'vertexAiSearch' => vas } } ]
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

        # Call endpoint
        loc = (connection['location'].presence || 'global').to_s.downcase
        post(call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")).payload(payload)
      end,

      sample_output: lambda do
        {
          'responseId' => 'resp-456',
          'modelVersion' => 'gemini-2.5-pro',
          'candidates' => [
            { 'content' => { 'role' => 'model', 'parts' => [ { 'text' => 'Grounded answer...' } ] },
              'groundingMetadata' => { 'citationSources' => [ { 'uri' => 'https://en.wikipedia.org/wiki/...' } ] } }
          ]
        }
      end
    },

    # --- Query with context chunks
    gen_answer_with_context: {
      title: 'Generative - Answer with provided context chunks',
      description: 'Answer a question using caller-supplied context chunks (RAG-lite). Returns structured JSON with citations.',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |_|
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
      end,
      
      output_fields: lambda do |object_definitions|
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
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs/analytics
        t0 = Time.now
        corr = call(:build_correlation_id)

        begin
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

          loc  = (connection['location'].presence || 'global').to_s.downcase
          resp = post(call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")).payload(payload)

          text = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
          parsed = call(:safe_parse_json, text)
          {
            'answer'     => parsed['answer'] || text,
            'citations'  => parsed['citations'] || [],
            'responseId' => resp['responseId'],
            'usage'      => resp['usageMetadata']
          }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        {
          'answer' => 'The outage began at 09:12 UTC due to a misconfigured firewall rule.',
          'citations' => [
            { 'chunk_id' => 'doc-42#p3', 'source' => 'postmortem', 'uri' => 'https://kb/acme/pm-42#p3', 'score' => 0.89 }
          ],
          'responseId' => 'resp-789',
          'usage' => { 'promptTokenCount' => 311, 'candidatesTokenCount' => 187, 'totalTokenCount' => 498 }
        }
      end
    },

    # -------------------- Embeddings ---------------------------------
    embed_text: {
      title: 'Embedding - Embed text',
      description: 'POST :predict on a publisher embedding model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', label: 'Embedding model', optional: false, control_type: 'select', pick_list: 'models_embedding', default: 'text-embedding-005',
            hint: 'Select or use a custom value.', toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Embedding model', type: 'string', control_type: 'text' } },
          { name: 'texts', type: 'array', of: 'string', optional: false },

          { name: 'task', hint: 'Optional: RETRIEVAL_QUERY or RETRIEVAL_DOCUMENT' },

          { name: 'autoTruncate', type: 'boolean', hint: 'Truncate long inputs automatically' },

          { name: 'outputDimensionality', type: 'integer', optional: true, convert_input: 'integer_conversion',
            hint: 'Optional dimensionality reduction (see model docs).' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        Array(object_definitions['embed_output']) + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = SecureRandom.uuid
        
        begin
          model_path = call(:build_embedding_model_path, connection, input['model'])

          # Guard: texts length cannot exceed model batch limit (friendly message)
          max_per_call = call(:embedding_max_instances, model_path)
          texts = call(:safe_array, input['texts'])
          error("Too many texts (#{texts.length}). Max per request for this model is #{max_per_call}. Chunk upstream.") if texts.length > 0 && texts.length > max_per_call

          instances = call(:safe_array, input['texts']).map { |t|
            { 'content' => t, 'task_type' => input['task'] }.delete_if { |_k, v| v.nil? }
          }

          # Coerce/validate embedding parameters to correct JSON types
          params = call(:sanitize_embedding_params, {
            'autoTruncate'         => input['autoTruncate'],
            'outputDimensionality' => input['outputDimensionality']
          })

          call(:predict_embeddings, connection, model_path, instances, params)
            .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        {
          'predictions' => [
            { 'embeddings' => { 'values' => [0.012, -0.034, 0.056],
              'statistics' => { 'truncated' => false, 'token_count' => 21 } } },
            { 'embeddings' => { 'values' => [0.023, -0.045, 0.067],
              'statistics' => { 'truncated' => false, 'token_count' => 18 } } }
          ],
          'metadata' => { 'billableCharacterCount' => 230 }
        }
      end

    },
 
    # -------------------- Utility ------------------------------------
    # --- Count tokens
    count_tokens: {
      title: 'Utility: Count tokens',
      description: 'POST :countTokens on a publisher model',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'contents', type: 'array', of: 'object', properties: object_definitions['content'], optional: false },
          { name: 'system_preamble', label: 'System preamble (text)', optional: true }
        ]
      end,

      output_fields: lambda do |_|
        [
          { name: 'totalTokens', type: 'integer' },
          { name: 'totalBillableCharacters', type: 'integer' },
          { name: 'promptTokensDetails', type: 'array', of: 'object' }
        ]
      end,

      execute:  lambda do |connection, input|
        # Compute model path
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        # Build payload
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        loc = (connection['location'].presence || 'global').to_s.downcase
        post(call(:aipl_v1_url, connection, loc, "#{model_path}:countTokens")).payload({
          'contents'          => contents,
          'systemInstruction' => sys_inst
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) })
      end,

      sample_output: lambda do
        { 'totalTokens' => 31, 'totalBillableCharacters' => 96,
          'promptTokensDetails' => [ { 'modality' => 'TEXT', 'tokenCount' => 31 } ] }
      end
    },

    # --- GCS simple upload
    upload_to_gcs: {
      title: 'Utility: Upload to Cloud Storage (simple upload)',
      description: 'Simple media upload to GCS (uploadType=media)',

      input_fields: lambda do
        [
          { name: 'bucket',      optional: false },
          { name: 'object_name', optional: false, label: 'Object path/name' },
          { name: 'content_type', optional: false },
          { name: 'file', type: 'file', optional: false }
        ]
      end,

      output_fields: lambda do
        [ { name: 'bucket' }, { name: 'name' }, { name: 'generation' },
          { name: 'size' }, { name: 'contentType' }, { name: 'mediaLink' } ]
      end,

      execute: lambda do |_connection, input|
        post("https://storage.googleapis.com/upload/storage/v1/b/#{CGI.escape(input['bucket'])}/o")
          .params(uploadType: 'media', name: input['object_name'])
          .headers('Content-Type': input['content_type'])
          .request_body(input['file'])
      end,

      sample_output: lambda do
        { 'bucket' => 'my-bucket', 'name' => 'docs/foo.pdf', 'generation' => '1728533890000',
          'size' => '123456', 'contentType' => 'application/pdf',
          'mediaLink' => 'https://storage.googleapis.com/download/storage/v1/b/my-bucket/o/docs%2Ffoo.pdf?gen=...' }
      end
    },

    # -------------------- Predict ------------------------------------
    # --- Generic
    endpoint_predict: {
      title: 'Endpoint predict (custom model)',
      description: 'POST :predict to a Vertex AI Endpoint',
      retry_on_request: ['GET', 'HEAD', 'POST'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do
        [
          { name: 'endpoint',   optional: false, hint: 'Endpoint ID or full resource path' },
          { name: 'instances',  type: 'array', of: 'object', optional: false },
          { name: 'parameters', type: 'object' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['predict_output']
      end,

      execute: lambda do |connection, input|
        call(:ensure_regional_location!, connection) # require non-global

        url = call(:endpoint_predict_url, connection, input['endpoint'])
        post(url)
          .payload({ 'instances' => input['instances'], 'parameters' => input['parameters'] }.delete_if { |_k, v| v.nil? })

      end,

      sample_output: lambda do
        { 'predictions' => [ { 'score' => 0.92, 'label' => 'positive' } ],
          'deployedModelId' => '1234567890' }
      end
    },

    # --- Batch (create job)
    batch_prediction_create: {
      title: 'Batch: Create prediction job',
      description: 'Create projects.locations.batchPredictionJobs',
      batch: true,

      input_fields: lambda do
        [
          { name: 'displayName', optional: false },
          { name: 'model',       optional: false, hint: 'Full model resource or publisher model' },
          { name: 'gcsInputUris', type: 'array', of: 'string', optional: false },
          { name: 'instancesFormat',   optional: false, hint: 'jsonl,csv,bigquery,tf-record,file-list' },
          { name: 'predictionsFormat', optional: false, hint: 'jsonl,csv,bigquery' },
          { name: 'gcsOutputUriPrefix', optional: false, hint: 'gs://bucket/path/' },
          { name: 'modelParameters', type: 'object' },
          { name: 'labels', type: 'object', optional: true, hint: 'Key/Value labels for traceability' }

        ]
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['batch_job']
      end,

      execute: lambda do |connection, input|
        call(:ensure_regional_location!, connection)

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
          'modelParameters' => input['modelParameters'],
          'labels'          => input['labels']
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

        loc  = connection['location']
        path = "projects/#{connection['project_id']}/locations/#{loc}/batchPredictionJobs"

        post(call(:aipl_v1_url, connection, loc, path)).payload(payload)
      end,

      sample_output: ->() {
        { 'name' => 'projects/p/locations/us-central1/batchPredictionJobs/123',
          'displayName' => 'batch-2025-10-06',
          'state' => 'JOB_STATE_PENDING',
          'model' => 'projects/p/locations/us-central1/models/456' }
      }
    },

    # --- Batch (get job result)
    batch_prediction_get: {
      title: 'Batch: Fetch prediction job (get)',
      description: 'Get a batch prediction job by ID',
      batch: true,

      input_fields: lambda do
        [ { name: 'job_id', optional: false } ]
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['batch_job']
      end,

      execute: lambda do |connection, input|
        call(:ensure_regional_location!, connection)
        name = input['job_id'].to_s.start_with?('projects/') ?
          input['job_id'] :
          "projects/#{connection['project_id']}/locations/#{connection['location']}/batchPredictionJobs/#{input['job_id']}"
        loc = connection['location']
        get(call(:aipl_v1_url, connection, loc, name.sub(%r{^/v1/}, '')))
      end,

      sample_output: lambda do
        { 'name' => 'projects/p/locations/us-central1/batchPredictionJobs/123',
          'displayName' => 'batch-2025-10-06',
          'state' => 'JOB_STATE_SUCCEEDED',
          'outputInfo' => { 'gcsOutputDirectory' => 'gs://my-bucket/prediction-...' } }
      end
    }
  },

  # --------- PICK LISTS ---------------------------------------------------

  pick_lists: {
    modes_classification: ->() {
      [%w[Embedding embedding], %w[Generative generative], %w[Hybrid hybrid]]
    },

    modes_grounding: ->() {
      [%w[Google\ Search google_search], %w[Vertex\ AI\ Search vertex_ai_search]]
    },

    models_embedding: ->(_connection) {
      begin
        # v1beta1 publishers.models.list — global; no project/location.
        resp = get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
                .params(pageSize: 200, view: 'BASIC')

        ids = call(:safe_array, resp['publisherModels'])
                .map { |m| m['name'].to_s.split('/').last } # publishers/google/models/<id>
                .select { |id|
                  id.start_with?('gemini-embedding') ||
                  id.start_with?('text-embedding')   ||
                  id.start_with?('multimodal-embedding')
                }
                .uniq.sort
      rescue => _e
        # Don’t inject opinionated fallbacks; let users switch to “Use custom value”.
        ids = []
      end
      ids.map { |id| [id, id] }
    },

    models_generative: ->(connection) {
      begin
        resp = get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
                .params(pageSize: 200, view: 'BASIC')
        items = call(:safe_array, resp['publisherModels'])
                  .map { |m| m['name'].to_s.split('/').last }
                  .select { |id| id.start_with?('gemini-') }
                  .uniq.sort
      rescue => _e
        items = []
      end
      items.map { |id| [id, id] }
    },

    # Contract-conformant roles (system handled via system_preamble)
    roles: ->() { [['user','user'], ['model','model']] }
  },

  # --------- METHODS ------------------------------------------------------
  methods: {
    telemetry_envelope: ->(started_at, correlation_id, ok, code, message) {
      dur = ((Time.now - started_at) * 1000.0).to_i
      {
        'ok' => !!ok,
        'telemetry' => {
          'http_status'    => code.to_i,
          'message'        => (message || (ok ? 'OK' : 'ERROR')).to_s,
          'duration_ms'    => dur,
          'correlation_id' => correlation_id
        }
      }
    },

    telemetry_parse_error_code: ->(err) {
      m = err.to_s.match(/\b(\d{3})\b/)
      m ? m[1].to_i : 0
    },

    build_correlation_id: ->() { SecureRandom.uuid },

    clamp_int: ->(n, min, max) { [[n.to_i, min].max, max].min },

    b64url: ->(bytes) { Base64.urlsafe_encode64(bytes).gsub(/=+$/, '') },

    jwt_sign_rs256: ->(claims, private_key_pem) {
      header = { alg: 'RS256', typ: 'JWT' }
      enc_h  = call(:b64url, header.to_json)
      enc_p  = call(:b64url, claims.to_json)
      input  = "#{enc_h}.#{enc_p}"
      rsa = OpenSSL::PKey::RSA.new(private_key_pem.to_s)
      sig = rsa.sign(OpenSSL::Digest::SHA256.new, input)
      "#{input}.#{call(:b64url, sig)}"
    },

    auth_normalize_scopes: ->(scopes) {
      arr = case scopes
            when nil    then ['https://www.googleapis.com/auth/cloud-platform']
            when String then scopes.split(/\s+/)
            when Array  then scopes
            else              ['https://www.googleapis.com/auth/cloud-platform']
            end
      arr.map(&:to_s).reject(&:empty?).uniq
    },

    auth_token_cache_get: ->(connection, scope_key) {
      cache = (connection['__token_cache'] ||= {})
      tok   = cache[scope_key]
      return nil unless tok.is_a?(Hash) && tok['access_token'].present? && tok['expires_at'].present?
      exp = Time.parse(tok['expires_at']) rescue nil
      return nil unless exp && Time.now < (exp - 60)
      tok
    },
    auth_token_cache_put: ->(connection, scope_key, token_hash) {
      cache = (connection['__token_cache'] ||= {})
      cache[scope_key] = token_hash
      token_hash
    },

    auth_issue_token!: ->(connection, scopes) {
      key = JSON.parse(connection['service_account_key_json'].to_s)
      token_url = (key['token_uri'].presence || 'https://oauth2.googleapis.com/token')
      now = Time.now.to_i
      scope_str = scopes.join(' ')
      payload = {
        iss:   key['client_email'],
        scope: scope_str,
        aud:   token_url,
        iat:   now,
        exp:   now + 3600
      }
      assertion = call(:jwt_sign_rs256, payload, key['private_key'])
      res = post(token_url)
              .payload(grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: assertion)
              .request_format_www_form_urlencoded
      {
        'access_token' => res['access_token'],
        'token_type'   => res['token_type'],
        'expires_in'   => res['expires_in'],
        'expires_at'   => (Time.now + res['expires_in'].to_i).utc.iso8601,
        'scope_key'    => scope_str
      }
    },

    auth_build_access_token!: ->(connection, scopes: nil) {
      set = call(:auth_normalize_scopes, scopes)
      scope_key = set.join(' ')
      if (cached = call(:auth_token_cache_get, connection, scope_key))
        return cached['access_token']
      end
      fresh = call(:auth_issue_token!, connection, set)
      call(:auth_token_cache_put, connection, scope_key, fresh)['access_token']
    },

    safe_array: ->(v) {
      return [] if v.nil? || v == false
      return v  if v.is_a?(Array)
      [v]
    },
    safe_integer: ->(v) { return nil if v.nil?; Integer(v) rescue v.to_i },
    safe_float:   ->(v) { return nil if v.nil?; Float(v)   rescue v.to_f },
    # -------------------- API endpoint reconciliation -----------------
    # --- Host and URL
    # - Regional
    aipl_service_host: ->(connection, loc=nil) {
      l = (loc || connection['location']).to_s.downcase
      (l.blank? || l == 'global') ? 'aiplatform.googleapis.com' : "#{l}-aiplatform.googleapis.com"
    },

    aipl_v1_url: ->(connection, loc, path) {
      "https://#{call(:aipl_service_host, connection, loc)}/v1/#{path}"
    },

    endpoint_predict_url: ->(connection, endpoint) {
      ep = call(:normalize_endpoint_identifier, endpoint).to_s
      # Allow fully-qualified dedicated endpoint URLs.
      return (ep.include?(':predict') ? ep : "#{ep}:predict") if ep.start_with?('http')

      # Prefer region from the resource name; fallback to connection.
      m   = ep.match(%r{^projects/[^/]+/locations/([^/]+)/endpoints/})
      loc = (m && m[1]) || (connection['location'] || '').to_s.downcase
      error("This action requires a regional location. Current location is '#{loc}'.") if loc.blank? || loc == 'global'

      host = call(:aipl_service_host, connection, loc)
      "https://#{host}/v1/#{call(:build_endpoint_path, connection, ep)}:predict"
    },

    build_endpoint_path: ->(connection, endpoint) {
      ep = call(:normalize_endpoint_identifier, endpoint)
      ep.start_with?('projects/') ? ep :
        "projects/#{connection['project_id']}/locations/#{connection['location']}/endpoints/#{ep}"
    },

    # --- Model
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

    # --- Location or region
    ensure_regional_location!: ->(connection) {
      loc = (connection['location'] || '').downcase
      error("This action requires a regional location (e.g., us-central1). Current location is '#{loc}'.") if loc.blank? || loc == 'global'
    },

    embedding_region: ->(connection) {
      loc = (connection['location'] || '').to_s.downcase
      # Embeddings support global and multi-regional endpoints. Respect caller choice; default to global.
      loc.present? ? loc : 'global'
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

    # Build a single email text body for classification
    build_email_text: ->(subject, body) {
      s = subject.to_s.strip
      b = body.to_s.strip
      parts = []
      parts << "Subject: #{s}" if s.present?
      parts << "Body:\n#{b}"    if b.present?
      parts.join("\n\n")
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

    # Conservative instance limits by model family
    embedding_max_instances: ->(model_path_or_id) {
      id = model_path_or_id.to_s.split('/').last
      if id.include?('gemini-embedding-001')
        1
      else
        250
      end
    },

    predict_embeddings: ->(connection, model_path, instances, params={}) {
      max  = call(:embedding_max_instances, model_path)
      preds = []
      billable = 0
      # Derive location from the model path (projects/.../locations/{loc}/...)
      loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase

      (instances || []).each_slice(max) do |slice|
        url  = call(:aipl_v1_url, connection, loc, "#{model_path}:predict")
        resp = post(url).payload({
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

      loc  = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
      url  = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
      resp = post(url).payload(payload)

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
      call(:safe_array, contents).map do |c|
        h = c.is_a?(Hash) ? c.transform_keys(&:to_s) : {}
        role = (h['role'] || 'user').to_s.downcase
        error("Invalid role: #{role}") unless %w[user model].include?(role)
        h['role'] = role
        h
      end
    },

    # Accept plain text and produce a proper systemInstruction
    system_instruction_from_text: ->(text) {
      return nil if text.blank?
      { 'role' => 'system', 'parts' => [ { 'text' => text.to_s } ] }
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

    # Like Array#map but safe against nil/false/non-arrays.
    safe_map: ->(v) { call(:safe_array, v).map { |x| yield(x) } },

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
      g['temperature']     = call(:safe_float,  g['temperature'])     if g.key?('temperature')
      g['topP']            = call(:safe_float,  g['topP'])            if g.key?('topP')
      # Integers
      g['topK']            = call(:safe_integer,g['topK'])            if g.key?('topK')
      g['maxOutputTokens'] = call(:safe_integer,g['maxOutputTokens']) if g.key?('maxOutputTokens')
      g['candidateCount']  = call(:safe_integer,g['candidateCount'])  if g.key?('candidateCount')
      # Arrays
      g['stopSequences']   = call(:safe_array, g['stopSequences']).map(&:to_s) if g.key?('stopSequences')
      # Strip
      g.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
      g
    }

  },

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {},

  # --------- CUSTOM ACTION SUPPORT ----------------------------------------
  custom_action: true,
  custom_action_help: {
    body: 'Create custom Vertex AI operations using the established connection'
  }
}
