# frozen_string_literal: true
require 'openssl'
require 'base64'
require 'json'
require 'time'
require 'securerandom'


{
  title: 'Vertex RAG Engine',
  subtitle: 'RAG Engine',
  version: '0.9.9',
  description: 'RAG engine via service account (JWT)',
  help: lambda do |input, picklist_label|
    {
      body: 'The Vertex AI RAG Engine is a component of the Vertex AI platform, which facilitates Retrieval-Augmented-Generation (RAG).' \
            'RAG Engine enables Large Language Models (LLMs) to access and incorporate data from external knowledge sources, such as '  \
            'documents and databases. By using RAG, LLMs can generate more accurate and informative LLM responses.',
      learn_more_url: "https://aiplatform.googleapis.com/$discovery/rest?version=v1",
      learn_more_text: "Check out the Vertex AI Discovery Document"
      # Documentation:        cloud.google.com/vertex-ai/generative-ai/docs/rag-engine/rag-overview
    }
  end,

  # --------- CONNECTION ---------------------------------------------------
  connection: {
    fields: [
      # Prod/Dev toggle
      { name: 'prod_mode',                  optional: true,   control_type: 'checkbox', label: 'Production mode',
        type: 'boolean',  default: true, extends_schema: true, hint: 'When enabled, suppresses debug echoes and enforces strict idempotency/retry rules.' },
      # Service account details
      { name: 'service_account_key_json',   optional: false,  control_type: 'text-area', 
        hint: 'Paste full JSON key' },
      { name: 'location',                   optional: false,  control_type: 'text', hint: 'e.g., global, us-central1, us-east4' },
      { name: 'project_id',                 optional: false,   control_type: 'text',
        hint: 'GCP project ID (inferred from key if blank)' },
      { name: 'user_project',               optional: true,   control_type: 'text',      label: 'User project for quota/billing',
        extends_schema: true, hint: 'Sets x-goog-user-project for billing/quota. Service account must have roles/serviceusage.serviceUsageConsumer on this project.' },
      { name: 'discovery_api_version', label: 'Discovery API version', control_type: 'select', optional: true, default: 'v1alpha',
        pick_list: 'discovery_versions', hint: 'v1alpha for AI Applications; switch to v1beta/v1 if/when you migrate.' },
      # Facets logging feature-flag (on by default)
      { name: 'enable_facets_logging', label: 'Enable facets in tail logs',
        type: 'boolean', control_type: 'checkbox', optional: true, default: true,
        hint: 'Adds a compact jsonPayload.facets block (retrieval/ranking/generation metrics). No effect on action outputs.' }
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
        # Auth-only. All Google routing headers must be set per request.
        headers('Authorization' => "Bearer #{connection['access_token']}")
      end,

      token_url: 'https://oauth2.googleapis.com/token',

      # Let Workato trigger re-acquire on auth errors
      refresh_on: [401],
      detect_on:  [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    },
  
    # No base_uri. Every action constructs an absolute URL to prevent host bleed.
    # base_uri intentionally omitted

  },

  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |connection|
    proj = connection['project_id']
    loc  = (connection['location'].presence || 'global').to_s.downcase
    host = call(:aipl_service_host, connection, loc)
    get("https://#{host}/v1/projects/#{proj}/locations/#{loc}")
  end,

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {

    content_part: {
      fields: lambda do |connection, config_fields|
        [
          { name: 'text' },
          { name: 'inlineData', type: 'object', properties: [ { name: 'mimeType' }, { name: 'data', hint: 'Base64' } ]},
          { name: 'fileData', type: 'object', properties: [  { name: 'mimeType' }, { name: 'fileUri' } ]},
          { name: 'functionCall', type: 'object', properties: [ { name: 'name' }, { name: 'args', type: 'object' } ]},
          { name: 'functionResponse', type: 'object', properties: [ { name: 'name' }, { name: 'response', type: 'object' } ]},
          { name: 'executableCode', type: 'object', properties: [ { name: 'language' }, { name: 'code' }  ]},
          { name: 'codeExecutionResult', type: 'object', properties: [ { name: 'outcome' }, { name: 'stdout' }, { name: 'stderr' } ]}
        ]
      end
    },
    content: {
      # Per contract: role ∈ {user, model}
      fields: lambda do |_connection, _config_fields, object_definitions|
        [
          { name: 'role', control_type: 'select', pick_list: 'roles', optional: false },
          { name: 'parts', type: 'array', of: 'object', properties: object_definitions['content_part'], optional: false }
        ]
      end
    },
    gen_generate_input: {
      fields: lambda do |connection, config_fields, object_definitions|
        [
          { name: 'mode', control_type: 'select', pick_list: 'gen_generate_modes', optional: false, default: 'plain' },
          { name: 'model', optional: false, control_type: 'text' },

          # Show 'contents' for plain/grounded modes; hide for rag_with_context
          { name: 'contents',
            type: 'array', of: 'object', properties: object_definitions['content'], optional: true,
            ngIf: 'input.mode != "rag_with_context"' },

          { name: 'system_preamble', optional: true },
          { name: 'generation_config', type: 'object', properties: object_definitions['generation_config'] },
          { name: 'safetySettings', type: 'array', of: 'object', properties: object_definitions['safety_setting'] },
          { name: 'toolConfig', type: 'object' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true },

          # ---------- Grounding (only when grounded_* modes) ----------
          # Google Search has no extra inputs; just reveal hint line if desired
          { name: 'grounding_info', label: 'Grounding via Google Search',
            hint: 'Uses the built-in googleSearch tool.',
            ngIf: 'input.mode == "grounded_google"', optional: true },

          # Vertex AI Search XOR parameters (only show in grounded_vertex)
          { name: 'vertex_ai_search_datastore',
            hint: 'projects/.../locations/.../collections/default_collection/dataStores/...',
            ngIf: 'input.mode == "grounded_vertex"', optional: true },
          { name: 'vertex_ai_search_serving_config',
            hint: 'projects/.../locations/.../collections/.../engines/.../servingConfigs/default_config',
            ngIf: 'input.mode == "grounded_vertex"', optional: true },

          # ---------- RAG-lite (only when rag_with_context) ----------
          { name: 'question', optional: true, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'context_chunks', type: 'array', of: 'object', optional: true,
            properties: [
              { name: 'id' }, { name: 'text', optional: false }, { name: 'source' }, { name: 'uri' },
              { name: 'score', type: 'number' }, { name: 'metadata', type: 'object' }
            ],
            ngIf: 'input.mode == "rag_with_context"'
          },
          { name: 'max_chunks', type: 'integer', optional: true, default: 20, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'salience_text', optional: true, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'salience_id', optional: true, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'salience_score', type: 'number', optional: true, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'max_prompt_tokens', type: 'integer', optional: true, default: 3000, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'reserve_output_tokens', type: 'integer', optional: true, default: 512, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'count_tokens_model', optional: true, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'trim_strategy', control_type: 'select', pick_list: 'trim_strategies', optional: true, default: 'drop_low_score', ngIf: 'input.mode == "rag_with_context"' },
          { name: 'temperature', type: 'number', optional: true, ngIf: 'input.mode == "rag_with_context"' },
          { name: 'rag_corpus', optional: true, hint: 'projects/{project}/locations/{region}/ragCorpora/{corpus}',
            ngIf: 'input.mode == "grounded_rag_store"' },
          { name: 'rag_retrieval_config', label: 'Retrieval config', type: 'object',
            properties: object_definitions['rag_retrieval_config'],
            ngIf: 'input.mode == "grounded_rag_store"', optional: true },
          # Back-compat single fields (optional)
          { name: 'similarity_top_k', type: 'integer', optional: true, ngIf: 'input.mode == "grounded_rag_store"',
            hint: 'Pre-ranking candidate cap (1–200).' },
          { name: 'vector_distance_threshold', type: 'number', optional: true, ngIf: 'input.mode == "grounded_rag_store"' },
          { name: 'vector_similarity_threshold', type: 'number', optional: true, ngIf: 'input.mode == "grounded_rag_store"' },
          { name: 'rank_service_model', optional: true, ngIf: 'input.mode == "grounded_rag_store"' },
          { name: 'llm_ranker_model', optional: true, ngIf: 'input.mode == "grounded_rag_store"' }
        ]
      end
    },
    gen_generate_content_input: {
      fields: lambda do |_connection, config_fields, object_definitions|
        show_adv = (config_fields['show_advanced'] == true)

        base = [
          # UX toggle
          { name: 'show_advanced', label: 'Show advanced options',
            type: 'boolean', control_type: 'checkbox', optional: true, default: false },
          # Model (free-text only)
          { name: 'model', label: 'Model', optional: false, control_type: 'text' },

          # Contract-friendly content
          { name: 'contents', type: 'array', of: 'object',
            properties: object_definitions['content'], optional: false },
          # Simple system text that we convert to systemInstruction object
          { name: 'system_preamble', label: 'System preamble (text)', optional: true,
            hint: 'Optional; becomes systemInstruction.parts[0].text' }
        ]
        adv = [
          { name: 'tools', type: 'array', of: 'object', properties: [
              { name: 'googleSearch',    type: 'object' },
              { name: 'retrieval',       type: 'object' },
              { name: 'codeExecution',   type: 'object' },
              { name: 'functionDeclarations', type: 'array', of: 'object' }
            ]
          },
          { name: 'toolConfig',      type: 'object' },
          { name: 'safetySettings',  type: 'array', of: 'object',
            properties: object_definitions['safety_setting'] },
          { name: 'generationConfig', type: 'object',
            properties: object_definitions['generation_config'] }
        ]

        show_adv ? (base + adv) : base
      end
    },
    generation_config: {
      fields: lambda do |_connection, _config_fields|
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
      end
    },
    generate_content_output: {
      fields: lambda do |_connection, _config_fields|
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
      end
    },
    embed_output: {
      # Align to contract: embeddings object, not array
      fields: lambda do |_connection, _config_fields|
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
      end
    },
    predict_output: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'predictions', type: 'array', of: 'object' },
          { name: 'deployedModelId' }
        ]
      end
    },
    batch_job: {
      fields: lambda do |_connection, _config_fields|
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
      end
    },
    envelope_fields: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message', type: 'string' },
            { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id', type: 'string' }
          ] }
        ]
      end
    },
    safety_setting: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'category'  },   # e.g., HARM_CATEGORY_*
          { name: 'threshold' }    # e.g., BLOCK_LOW_AND_ABOVE
        ]
      end
    },
    kv_pair: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'key' },
          { name: 'value' }
        ]
      end
    },
    rag_retrieval_filter: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'vector_distance_threshold',   type: 'number',
            hint: 'Use ONLY ONE of distance or similarity. COSINE: distance≈1−similarity.' },
          { name: 'vector_similarity_threshold', type: 'number',
            hint: 'Use ONLY ONE of similarity or distance.' }
        ]
      end
    },
    rag_ranking: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'rank_service_model', hint: 'Semantic ranker (Discovery). Example: semantic-ranker-512@latest' },
          { name: 'llm_ranker_model',   hint: 'Gemini re-ranker (e.g., gemini-2.0-flash)' }
        ]
      end
    },
    rag_retrieval_config: {
      fields: lambda do |_connection, _config_fields, object_definitions|
        [
          { name: 'top_k', type: 'integer', hint: 'Candidate cap before ranking. Rule of thumb: 20–50.' },
          { name: 'filter',  type: 'object', properties: object_definitions['rag_retrieval_filter'] },
          { name: 'ranking', type: 'object', properties: object_definitions['rag_ranking'] }
        ]
      end
    }

  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {

    # 1) Email categorization
    gen_categorize_email: {
      title: 'Email: Categorize email',
      subtitle: 'Classify an email into a category',
      help: lambda do |input, picklist_label|
        {
          body: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.',
          learn_more_url: 'https://ai.google.dev/gemini-api/docs/models',
          learn_more_text: 'Find a current list of available Gemini models'
        }
      end,
      display_priority: 100,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, config_fields|
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
          { name: 'embedding_model', label: 'Embedding model', control_type: 'text', optional: true,
            default: 'text-embedding-005', },
          { name: 'generative_model', label: 'Generative model', control_type: 'text', optional: true },
          { name: 'min_confidence', type: 'number', optional: true, default: 0.25,
            hint: '0–1. If top score falls below this, fallback is used.' },
          { name: 'fallback_category', optional: true, default: 'Other' },
          { name: 'top_k', type: 'integer', optional: true, default: 3,
            hint: 'In hybrid mode, pass top-K candidates to the LLM referee.' },
          { name: 'return_explanation', type: 'boolean', optional: true, default: false,
            hint: 'If true and a generative model is provided, returns a short reasoning + distribution.' },
          # Salience fields
          { name: 'use_salience', type: 'boolean', control_type: 'checkbox', optional: true, default: true,
            hint: 'If enabled, classify based on a short salient span (better signal on long threads).' },
          { name: 'salience_model', control_type: 'text', optional: true, default: 'gemini-2.0-flash',
            hint: 'Model used to extract the salient span.' },
          { name: 'salience_max_span_chars', type: 'integer', optional: true, default: 500 },
          { name: 'salience_temperature', type: 'number', optional: true, hint: 'Default 0' },
          { name: 'confidence_blend', type: 'number', optional: true, default: 0.15,
            hint: 'How much to blend salience importance into the final confidence (0–0.5 typical).' },
        ]
      end,
      output_fields: lambda do |object_definitions, connection|
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
                  { name: 'category' }, { name: 'prob', type: 'number' } ] }
            ]},
          { name: 'preproc', type: 'object', properties: [
              { name: 'focus_preview' },
              { name: 'salient_span' },
              { name: 'reason' },
              { name: 'importance', type: 'number' }]},
        ] + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        # 1. Invariants
        started_at = Time.now.utc.iso8601 # for logging
        t0   = Time.now
        corr = call(:build_correlation_id)
        url = nil; req_body = nil
        begin
          # 2. Build the request
          subj = (input['subject'] || '').to_s.strip
          body = (input['body']    || '').to_s.strip
          error('Provide subject and/or body') if subj.empty? && body.empty?

          use_sal = call(:normalize_boolean, input['use_salience'])
          preproc = nil
          if use_sal
            preproc = call(:extract_salient_span!, connection, subj, body,
                          (input['salience_model'].presence || 'gemini-2.0-flash'),
                          (input['salience_max_span_chars'].presence || 500).to_i,
                          (input['salience_temperature'].presence || 0))
          end

          email_text =
            if use_sal && preproc && preproc['salient_span'].to_s.strip.length > 0
              preproc['salient_span'].to_s
            else
              call(:build_email_text, subj, body)
            end

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

          result = nil

          if %w[embedding hybrid].include?(mode)
            emb_model      = (input['embedding_model'].presence || 'text-embedding-005')
            emb_model_path = call(:build_embedding_model_path, connection, emb_model)

            email_inst = { 'content' => email_text, 'task_type' => 'RETRIEVAL_QUERY' }
            cat_insts  = cats.map do |c|
              txt = [c['name'], c['description'], *(c['examples'] || [])].compact.join("\n")
              { 'content' => txt, 'task_type' => 'RETRIEVAL_DOCUMENT' }
            end

            emb_resp = call(:predict_embeddings, connection, emb_model_path, [email_inst] + cat_insts, {}, corr)
            preds    = call(:safe_array, emb_resp && emb_resp['predictions'])
            error('Embedding model returned no predictions') if preds.empty?

            email_vec = call(:extract_embedding_vector, preds.first)
            cat_vecs  = preds.drop(1).map { |p| call(:extract_embedding_vector, p) }

            sims = cat_vecs.each_with_index.map { |v, i| [i, call(:vector_cosine_similarity, email_vec, v)] }
            sims.sort_by! { |(_i, s)| -s }

            scores     = sims.map { |(i, s)| { 'category' => cats[i]['name'], 'score' => (((s + 1.0) / 2.0).round(6)), 'cosine' => s.round(6) } }
            top        = scores.first
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
              referee   = call(:llm_referee, connection, input['generative_model'], email_text, shortlist, cats, input['fallback_category'], corr)
              result['referee'] = referee

              if referee['category'].present? && shortlist.include?(referee['category'])
                result['chosen']     = referee['category']
                result['confidence'] = [result['confidence'], referee['confidence']].compact.max
              end
              if result['confidence'].to_f < min_conf && input['fallback_category'].present?
                result['chosen'] = input['fallback_category']
              end
            end

          elsif mode == 'generative'
            error('generative_model is required when mode=generative') if input['generative_model'].blank?
            referee = call(:llm_referee, connection, input['generative_model'], email_text, cats.map { |c| c['name'] }, cats, input['fallback_category'], corr)
            chosen =
              if referee['confidence'].to_f < min_conf && input['fallback_category'].present?
                input['fallback_category']
              else
                referee['category']
              end

            result = {
              'mode'       => mode,
              'chosen'     => chosen,
              'confidence' => referee['confidence'],
              'referee'    => referee
            }

          else
            error("Unknown mode: #{mode}")
          end

          # Post-processing / salience blend & attachments
          if preproc && preproc['importance']
            blend = [[(input['confidence_blend'] || 0.15).to_f, 0.0].max, 0.5].min
            result['confidence'] = [[result['confidence'].to_f + blend * (preproc['importance'].to_f - 0.5), 0.0].max, 1.0].min
          end
          result['preproc'] = preproc if preproc
          
          # Attach telemetry envelope
          result = result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

          # Build and emit to Google Cloud
          facets = call(:compute_facets_for!, 'gen_categorize_email', result)
          call(:tail_log_emit!, connection, :gen_categorize_email, started_at, t0, result, nil, facets)

          result
        rescue => e
          # Extract Google error
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')

          # Construct telmetry envelope
          env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg)

          # Emit logging to Google Cloud
          call(:tail_log_emit!, connection, :gen_categorize_email, started_at, t0, nil, e, nil)

          # Construct and emit debug attachment, as applicable
          if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
            env['debug'] = call(:debug_pack, true, url, req_body, g)
          end
          error(env)   # <-- raise so Workato marks step failed and retries if applicable
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
          },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },
    email_extract_salient_span: {
      title: 'Email Extract salient span',
      subtitle: 'Pull the most important sentence/paragraph from an email',
      display_priority: 100,
      help: lambda do |_|
        { 
          body: 'Heuristically trims boilerplate/quotes, then asks the model for the single most important span (<= 500 chars), with rationale, tags, and optional call-to-action metadata.',
          learn_more_url: 'https://ai.google.dev/gemini-api/docs/models',
          learn_more_text: 'Find a current list of available Gemini models'
        }
      end,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, config_fields|
        [
          { name: 'subject', optional: true },
          { name: 'body',    optional: false, hint: 'Raw email body (HTML or plain text). Quoted replies and signatures are pruned automatically.' },

          { name: 'generative_model', label: 'Generative model', control_type: 'text', optional: true, default: 'gemini-2.0-flash' },
          { name: 'max_span_chars', type: 'integer', optional: true, default: 500, hint: 'Hard cap for the extracted span.' },
          { name: 'include_entities', type: 'boolean', control_type: 'checkbox', optional: true, default: true,
            hint: 'Try to extract named people/teams/products mentioned in the salient span.' },
          { name: 'temperature', type: 'number', optional: true, hint: 'Default 0 (deterministic).' },

          # Debug
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'salient_span' },
          { name: 'reason' },
          { name: 'importance', type: 'number' },
          { name: 'tags', type: 'array', of: 'string' },
          { name: 'call_to_action' },
          { name: 'deadline_iso' },
          { name: 'entities', type: 'array', of: 'object', properties: [
              { name: 'type' }, { name: 'text' }
            ]
          },
          { name: 'focus_preview' },   # the pruned text the model actually saw
          { name: 'responseId' },
          { name: 'usage', type: 'object', properties: [
              { name: 'promptTokenCount', type: 'integer' },
              { name: 'candidatesTokenCount', type: 'integer' },
              { name: 'totalTokenCount', type: 'integer' }
            ]
          },
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message' }, { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id' }
          ]},
          { name: 'debug', type: 'object' }
        ]
      end,
      execute: lambda do |connection, input|
        started_at = Time.now.utc.iso8601 # for logging

        t0   = Time.now
        corr = call(:build_correlation_id)
        url = nil; req_body = nil; req_params = nil
        begin
          # 1) Heuristic focus text (strip quotes, signatures, legal footers, tracking junk)
          subj  = (input['subject'] || '').to_s
          body  = (input['body']    || '').to_s
          error('body is required') if body.strip.empty?

          plain = call(:email_minify, subj, body)               # HTML → text, whitespace/emoji control
          focus = call(:email_focus_trim, plain, 8000)          # keep the meat; drop quoted chains & footers

          # 2) Build schema-constrained prompt
          max_span = call(:clamp_int, (input['max_span_chars'] || 500), 80, 2000)
          want_entities = call(:normalize_boolean, input['include_entities'])

          system_text = "You extract the single most important sentence or short paragraph from an email. " \
                        "Rules: (1) Return VALID JSON only. (2) Do NOT output greetings, signatures, legal footers, " \
                        "auto-replies, or vague pleasantries (e.g., 'Hello', 'Thanks', 'Please see below'). " \
                        "(3) Keep under #{max_span} characters; do not truncate mid-sentence. " \
                        "(4) importance is a calibrated score in [0,1]. If a clear action is requested, set call_to_action; " \
                        "if a clear deadline exists, return ISO-8601 in deadline_iso; otherwise leave null."

          schema_props = {
            'salient_span'   => {
              'type' => 'string',
              'minLength' => 12
            },
            'reason'         => { 'type' => 'string' },
            'importance'     => { 'type' => 'number' },
            'tags'           => { 'type' => 'array', 'items' => { 'type' => 'string' } },
            'call_to_action' => { 'type' => 'string' },
            'deadline_iso'   => { 'type' => 'string' }
          }
          if want_entities
            schema_props['entities'] = {
              'type'  => 'array',
              'items' => { 'type' => 'object', 'additionalProperties' => false,
                'properties' => { 'type' => { 'type' => 'string' }, 'text' => { 'type' => 'string' } },
                'required'   => ['text']
              }
            }
          end

          gen_cfg = {
            'temperature'      => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
            'maxOutputTokens'  => 512,
            'responseMimeType' => 'application/json',
            'responseSchema'   => {
              'type'                 => 'object',
              'additionalProperties' => false,
              'properties'           => schema_props,
              'required'             => ['salient_span','importance']
            }
          }

          model = (input['generative_model'].presence || 'gemini-2.0-flash').to_s
          model_path = call(:build_model_path_with_global_preview, connection, model)

          contents = [
            { 'role' => 'user', 'parts' => [
                { 'text' => [
                    ("Subject: #{subj}".strip unless subj.strip.empty?),
                    "Email (trimmed):\n#{focus}"
                  ].compact.join("\n\n")
                }
              ]
            }
          ]

          payload = {
            'contents'          => contents,
            'systemInstruction' => { 'role' => 'system', 'parts' => [ { 'text' => system_text } ] },
            'generationConfig'  => gen_cfg
          }

          loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
          url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
          req_params = "model=#{model_path}"
          req_body = call(:json_compact, payload)
          resp = post(url).headers(call(:request_headers_auth, connection, corr, connection['user_project'], req_params)).payload(req_body)

          text   = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
          parsed = call(:safe_parse_json, text)
          # Post-parse repair: filter obvious salutations/low-content and enforce the cap
          span = parsed['salient_span'].to_s.strip
          if span.empty? || span =~ /\A(hi|hello|hey)\b[:,\s]*\z/i || span.length < 8
            # Fallback: pick first substantive sentence from focus
            head = (focus || '').to_s
            # Drop leading greeting lines
            head = head.sub(/\A\s*(subject:\s*[^\n]+\n+)?\s*(hi|hello|hey)[^a-z0-9]*\n+/i, '')
            cand = head.split(/(?<=[.!?])\s+/).find { |s| s.strip.length >= 12 && s !~ /\A(hi|hello|hey)\b/i } || head[0, max_span]
            span = cand[0, max_span].to_s.strip
          else
            span = span[0, max_span]
          end

          out = {
            'salient_span'   => span,
            'reason'         => parsed['reason'],
            'importance'     => parsed['importance'],
            'tags'           => parsed['tags'],
            'call_to_action' => parsed['call_to_action'],
            'deadline_iso'   => parsed['deadline_iso'],
            'entities'       => parsed['entities'],
            'focus_preview'  => focus,
            'responseId'     => resp['responseId'],
            'usage'          => resp['usageMetadata']
          }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end

          # Compute facets (tokens_total, etc.) and emit
          facets = call(:compute_facets_for!, 'email_extract_salient_span', out)
          call(:tail_log_emit!, connection, :email_extract_salient_span, started_at, t0, out, nil, facets)
          out
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          out = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
          call(:tail_log_emit!, connection, :email_extract_salient_span, started_at, t0, nil, e, nil)
          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end
          out
        end
      end,
      sample_output: lambda do
        {
          'salient_span'   => 'Can you approve the Q4 budget increase by Friday, October 24?',
          'reason'         => 'Explicit ask with a concrete deadline that blocks downstream work.',
          'importance'     => 0.92,
          'tags'           => ['approval','budget','deadline'],
          'call_to_action' => 'Approve Q4 budget increase',
          'deadline_iso'   => '2025-10-24T17:00:00Z',
          'entities'       => [ { 'type' => 'team', 'text' => 'Finance' } ],
          'focus_preview'  => 'Subject: Budget approval needed\n\nHi — we need your approval...',
          'responseId'     => 'resp-xyz',
          'usage'          => { 'promptTokenCount' => 175, 'candidatesTokenCount' => 96, 'totalTokenCount' => 271 },
          'ok'             => true,
          'telemetry'      => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 18, 'correlation_id' => 'sample' }
        }
      end
    },
    gen_generate: {
      title: 'Query a generative endpoint (configurable)',
      subtitle: 'Plain / Grounded / RAG-lite',
      help: lambda do |_|
        {
          body: "Select an option from the `Mode` field, then fill only the fields rendered by the recipe builder. "\
                "Required fields per mode: `RAG-LITE`: [question, context_chunks, max_chunks, salience_text, " \
                'salience_id, salience_score, max_prompt_tokens, reserve_output_tokens, count_tokens_model, ' \
                'trim_strategy, temperature].  ' \
                '`VERTEX-SEARCH ONLY`: [vertex_ai_search_datastore, vertex_ai_search_serving_config].   ' \
                '`RAG-STORE ONLY`: [rag_corpus, rag_retrieval_config, similarity_top_k, vector_distance_threshold,'\
                'vector_similarity_threshold, rank_service_model, llm_ranker_model]. ',
          learn_more_url: 'https://ai.google.dev/gemini-api/docs/models',
          learn_more_text: 'Find a current list of available Gemini models'
        }
      end,
      display_priority: 90,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields:  lambda { |od, c, cf| od['gen_generate_input'] },
      output_fields: lambda do |od, c|
        Array(od['generate_content_output']) + [
          { name: 'confidence', type: 'number' },
          { name: 'parsed', type: 'object', properties: [
              { name: 'answer' },
              { name: 'citations', type: 'array', of: 'object', properties: [
                  { name: 'chunk_id' }, { name: 'source' }, { name: 'uri' }, { name: 'score', type: 'number' }
              ]}
          ]}
        ] + Array(od['envelope_fields'])
      end,
      execute: lambda do |connection, raw_input|
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        input = call(:normalize_input_keys, raw_input)
        begin
          result = call(:gen_generate_core!, connection, input)
          facets = call(:compute_facets_for!, 'gen_generate', result)
          call(:tail_log_emit!, connection, :gen_generate, started_at, t0, result, nil, facets)
          result
        rescue => e
          call(:tail_log_emit!, connection, :gen_generate, started_at, t0, nil, e, nil)
          raise
        end
      end,
      sample_output: lambda do
        { 'responseId'=>'resp-x',
          'candidates'=>[{'content'=>{'parts'=>[{'text'=>'...'}]}}],
          'confidence'=>0.84,
          'parsed'=>{'answer'=>'...','citations'=>[{'chunk_id'=>'doc-1#c2','score'=>0.88}]} ,
          'ok'=>true,
          'telemetry'=>{ 'http_status'=>200, 'message'=>'OK', 'duration_ms'=>12, 'correlation_id'=>'sample',
                         'confidence'=>{'basis'=>'citations_topk_avg','k'=>3,'n'=>1} } }
      end
    },
    rank_texts_with_ranking_api: {
      title: 'Rerank contexts',
      subtitle: 'projects.locations.rankingConfigs:rank',
      description: '',
      help: lambda do |input, picklist_label|
        {
          body: 'Rerank candidate texts for a query using Vertex Ranking.',
          learn_more_url: 'https://cloud.google.com/vertex-ai/generative-ai/docs/rag-engine/retrieval-and-ranking',
          learn_more_text: 'Check out Google docs for retrieval and ranking'
        }
      end,
      display_priority: 89,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'query_text', optional: false },
          { name: 'records', type: 'array', of: 'object', optional: false, properties: [
              { name: 'id', optional: false }, { name: 'content', optional: false }, { name: 'metadata', type: 'object' }
            ], hint: 'id + content required.' },
          { name: 'rank_model', optional: true, hint: 'e.g., semantic-ranker-default@latest' },
          { name: 'top_n', type: 'integer', optional: true },
          { name: 'ignore_record_details_in_response', type: 'boolean', control_type: 'checkbox', optional: true,
            hint: 'If true, response returns only {id, score}. Useful when you only need scores.' },
          { name: 'ranking_config_name', optional: true, hint: 'Full name or simple id. Blank → default_ranking_config' },
          { name: 'emit_metrics', type: 'boolean', control_type: 'checkbox', optional: true, default: true },
          { name: 'metrics_namespace', optional: true },
          # --- New tidy knobs ---
          { name: 'emit_shape', control_type: 'select', pick_list: 'rerank_emit_shapes', optional: true, default: 'context_chunks',
            hint: 'Choose output shape: records-only, enriched records, or generator-ready context_chunks.' },
          { name: 'source_key', optional: true, hint: 'Metadata key to use for source (default: "source")' },
          { name: 'uri_key',    optional: true, hint: 'Metadata key to use for uri (default: "uri")' },
          # Optional override when your connection 'location' is regional (e.g., us-central1)
          { name: 'ai_apps_location', label: 'AI-Apps location (override)', control_type: 'select', pick_list: 'ai_apps_locations', optional: true,
            hint: 'Ranking/Search use multi-regions only: global, us, or eu. Leave blank to derive from connection location.' }
        ]
      end,
      output_fields: lambda do |object_definitions, _connection|
        [
          # Always present; shape controls richness
          { name: 'records', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'score', type: 'number' }, { name: 'rank', type: 'integer' },
              { name: 'content' }, { name: 'metadata', type: 'object' }
            ] },
          # Present when emit_shape == 'context_chunks'
          { name: 'context_chunks', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'text' }, { name: 'score', type: 'number' },
              { name: 'source' }, { name: 'uri' },
              { name: 'metadata', type: 'object' },
              { name: 'metadata_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
              { name: 'metadata_json' }
            ] }
        ] + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        started_at = Time.now.utc.iso8601
        t0   = Time.now
        corr = call(:build_correlation_id)
        url = nil; req_body = nil

        call(:ensure_project_id!, connection)
        # Normalize connection region → AI-Apps multi-region (global|us|eu), allow per-action override
        loc = call(:aiapps_loc_resolve, connection, input['ai_apps_location'])

        # Resolve config id (not full resource) to avoid double-embedding path parts.
        config_id =
          if input['ranking_config_name'].to_s.strip.empty?
            'default_ranking_config'
          else
            input['ranking_config_name'].to_s.split('/').last
          end

        req_params = "ranking_config=projects/#{connection['project_id']}/locations/#{loc}/rankingConfigs/#{config_id}"

        # Build absolute URL to Discovery Engine Ranking API (no base_uri usage)
        # POST https://discoveryengine.googleapis.com/{ver}/projects/*/locations/*/rankingConfigs/*:rank
        ver = (connection['discovery_api_version'].presence || 'v1alpha').to_s
        url = call(
          :discovery_url,
          connection,
          loc,
          "projects/#{connection['project_id']}/locations/#{loc}/rankingConfigs/#{config_id}:rank",
          ver
        )

        body = {
          # Ranking expects a scalar string for 'query'
          'query'   => input['query_text'].to_s,
          'records' => call(:safe_array, input['records']).map { |r|
            {
              'id'       => r['id'].to_s,
              'content'  => r['content'].to_s,
              'metadata' => (r['metadata'].is_a?(Hash) ? r['metadata'] : nil)
            }.delete_if { |_k, v| v.nil? }
          },
          'model'   => (input['rank_model'].to_s.strip.empty? ? nil : input['rank_model'].to_s.strip),
          'topN'    => (input['top_n'].to_i > 0 ? input['top_n'].to_i : nil),
          'ignoreRecordDetailsInResponse' => (input['ignore_record_details_in_response'] == true ? true : nil)
        }.delete_if { |_k, v| v.nil? }

        req_body = call(:json_compact, body)

        begin
        resp = post(url)
                 .headers(call(:request_headers_auth, connection, corr, connection['user_project'], req_params))
                 .payload(req_body)

        code = call(:telemetry_success_code, resp)
        # Minimal ranked list from API (or accurate when API omits details)
        ranked_min = Array(resp['records']).each_with_index.map { |r, i|
          { 'id' => r['id'].to_s, 'score' => r['score'].to_f, 'rank' => i + 1 }
        }

        # Enrich from caller input; centralized helpers keep this tidy
        enriched = call(:rerank_enrich_records, input['records'], ranked_min)

        # Shape
        shape = (input['emit_shape'].presence || 'context_chunks').to_s
        out_records =
          case shape
          when 'records_only'     then ranked_min
          when 'enriched_records' then enriched
          else                          enriched
          end

        out = { 'records' => out_records }
        if shape == 'context_chunks'
          out['context_chunks'] = call(:context_chunks_from_enriched, enriched,
                                       (input['source_key'].presence || 'source'),
                                       (input['uri_key'].presence    || 'uri'))
        end
        out = out.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        # annotate ranking call provenance for observability
        (out['telemetry'] ||= {})['ranking'] = {
          'api'       => 'discoveryengine.ranking:rank',
          'version'   => (connection['discovery_api_version'].presence || 'v1alpha'),
          'location'  => call(:aiapps_loc_resolve, connection, input['ai_apps_location']),
          'config_id' => config_id,
          'model'     => (input['rank_model'].to_s.strip if input['rank_model'].present?)
        }.compact

        # Facets: capture rank mode/model when present
        rank_facets = {
          'rank_mode'  => out.dig('telemetry','ranking','api') ? 'rank_service' : nil,
          'rank_model' => out.dig('telemetry','ranking','model')
        }.delete_if { |_k,v| v.nil? }
        call(:tail_log_emit!, connection, :rank_texts_with_ranking_api, started_at, t0, out, nil, rank_facets)
        out
      rescue => e
        g   = call(:extract_google_error, e)
        msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
        env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg)
        call(:tail_log_emit!, connection, :rank_texts_with_ranking_api, started_at, t0, nil, e, nil)
        # Optional debug attachment in non-prod:
        if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
          env['debug'] = call(:debug_pack, true, url, req_body, g)
        end
        error(env)   # <-- raise so Workato marks step failed and retries if applicable
      end
      end,
      sample_output: lambda do |_connection, _input|
        {
          'records' => [
            { 'id' => 'ctx-1', 'score' => 0.92, 'rank' => 1, 'content' => '...', 'metadata' => { 'source' => 'handbook', 'uri' => 'https://...' } }
          ],
          'context_chunks' => [
            { 'id' => 'ctx-1', 'text' => '...', 'score' => 0.92, 'source' => 'handbook', 'uri' => 'https://...',
              'metadata' => { 'page' => 7 }, 'metadata_kv' => [{ 'key' => 'page', 'value' => 7 }], 'metadata_json' => '{"page":7}' }
          ],
          'ok' => true, 'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 9, 'correlation_id' => 'sample' }
        }
      end
    },

    # RAG store engine
    rag_retrieve_contexts: {
      title: 'RAG Engine: Fetch contexts',
      subtitle: 'projects.locations:retrieveContexts (Vertex RAG Store)',
      help: lambda do |_|
        {
          body:
            'Retrieves contexts from a Vertex RAG corpus. ' \
            'Modifiers: top_k limits pre-ranking candidates; use EITHER vector_distance_threshold OR vector_similarity_threshold (not both). ' \
            'Pick ONE ranker: rank_service_model (semantic ranker) OR llm_ranker_model (Gemini). ' \
            'Guidance: if your index uses COSINE distance, distance≈1−similarity (so distance≤0.30 ≈ similarity≥0.70). ' \
            'Start with top_k=20, then tighten by threshold; add a ranker only if you need stricter ordering.',
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/rag-api-v1',
          learn_more_text: 'Find out more about the RAG Engine API'
        }
      end,
      display_priority: 86,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, _connection, _config_fields|
        [
          { name: 'rag_corpus', optional: false,
            hint: 'Accepts either full resource name (e.g., "projects/{project}/locations/{region}/ragCorpora/{corpus}") or the "corpus"' },
          { name: 'question', optional: false },
          { name: 'restrict_to_file_ids', type: 'array', of: 'string', optional: true },
          { name: 'max_contexts', type: 'integer', optional: true, default: 20 },

          # Preferred object (teams can use this)
          { name: 'rag_retrieval_config', label: 'Retrieval config', type: 'object',
            properties: object_definitions['rag_retrieval_config'], optional: true,
            hint: 'Set top_k, exactly one threshold (distance OR similarity), and exactly one ranker (semantic OR LLM).' }

          # (If you kept legacy flat fields for BC, they can remain here; not required.)
        ]
      end,
      output_fields: lambda do |object_definitions, _connection|
        [
          { name: 'question' },
          { name: 'contexts', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'text' }, { name: 'score', type: 'number' },
              { name: 'source' }, { name: 'uri' },
              { name: 'metadata', type: 'object' },
              { name: 'metadata_kv', label: 'metadata (KV)', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
              { name: 'metadata_json', label: 'metadata (JSON)' }
            ]
          }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message' }, { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id' },
            { name: 'retrieval', type: 'object' },
            { name: 'rank', type: 'object' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        started_at = Time.now.utc.iso8601
        t0   = Time.now
        corr = call(:build_correlation_id)
        retr_url = nil; retr_req_body = nil

        proj = connection['project_id']
        loc  = connection['location']
        raise 'Connection missing project_id' if proj.blank?
        raise 'Connection missing location'   if loc.blank?

        corpus = call(:normalize_rag_corpus, connection, input['rag_corpus'])
        error('rag_corpus is required') if corpus.blank?

        loc     = (connection['location'] || '').downcase
        parent  = "projects/#{connection['project_id']}/locations/#{loc}"
        req_par = "parent=#{parent}"

        # Merge + validate retrieval options from object/flat inputs
        opts = call(:build_retrieval_opts_from_input!, input)

        payload = call(
          :build_rag_retrieve_payload,
          input['question'],
          corpus,
          input['restrict_to_file_ids'],
          opts
        )

        retr_url  = call(:aipl_v1_url, connection, loc, "#{parent}:retrieveContexts")
        retr_req_body = call(:json_compact, payload)
        # Retrieval must not depend on user-project billing; align with rag_answer
        resp = post(retr_url)
                 .headers(call(:headers_rag, connection, corr, req_par))
                 .payload(retr_req_body)


        raw   = call(:normalize_retrieve_contexts!, resp)
        maxn  = call(:clamp_int, (input['max_contexts'] || 20), 1, 200)
        mapped= call(:map_context_chunks, raw, maxn)

        out = {
          'question' => input['question'],
          'contexts' => mapped
        }.merge(call(:telemetry_envelope, t0, corr, true, call(:telemetry_success_code, resp), 'OK'))

        # Telemetry preview from canonical opts:
        (out['telemetry'] ||= {})['retrieval'] = {}.tap do |h|
          h['top_k'] = opts['topK'].to_i if opts['topK']
          if opts['vectorDistanceThreshold']
            h['filter'] = { 'type' => 'distance',  'value' => opts['vectorDistanceThreshold'].to_f }
          elsif opts['vectorSimilarityThreshold']
            h['filter'] = { 'type' => 'similarity','value' => opts['vectorSimilarityThreshold'].to_f }
          end
        end
        if opts['rankServiceModel']
          (out['telemetry'] ||= {})['rank'] = { 'mode' => 'rank_service', 'model' => opts['rankServiceModel'] }
        elsif opts['llmRankerModel']
          (out['telemetry'] ||= {})['rank'] = { 'mode' => 'llm', 'model' => opts['llmRankerModel'] }
        end

        facets = call(:compute_facets_for!, 'rag_retrieve_contexts', out)
        call(:tail_log_emit!, connection, :rag_retrieve_contexts, started_at, t0, out, nil, facets)
        out
      rescue => e
        g = call(:extract_google_error, e)
        msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
        env = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
        call(:tail_log_emit!, connection, :rag_retrieve_contexts, started_at, t0, nil, e, nil)
        env

      end,
      sample_output: lambda do
        {
          'question' => 'What is the PTO carryover policy?',
          'contexts' => [
            { 'id' => 'doc-42#c3', 'text' => 'Employees may carry over up to 40 hours...', 'score' => 0.91,
              'source' => 'handbook', 'uri' => 'https://drive.google.com/file/d/abc...',
              'metadata' => { 'page' => 7 },
              'metadata_kv' => [ { 'key' => 'page', 'value' => 7 } ],
              'metadata_json' => '{"page":7}' }
          ],
          'ok' => true,
          'telemetry' => {
            'http_status' => 200, 'message' => 'OK', 'duration_ms' => 22, 'correlation_id' => 'sample',
            'retrieval' => { 'top_k' => 20, 'filter' => { 'type' => 'distance', 'value' => 0.35 } },
            'rank' => { 'mode': 'rank_service', 'model': 'semantic-ranker-512@latest' }
          }
        }
      end
    },
    rag_answer: {
      title: 'RAG Engine: Get grounded response (one-shot)',
      subtitle: 'Retrieve contexts from a corpus and generate a cited answer',
      help: lambda do |input, _picklist_label|
        { body:
          'One-shot retrieve+answer with citations. ' \
          'Tuning: top_k controls candidate pool; apply ONE threshold (distance OR similarity) to prune; pick ONE ranker (rank_service_model OR llm_ranker_model). ' \
          'Interpretation: surfaced context scores reflect retrieval similarity; re-ranking may change order but not the underlying scores. ' \
          'Start with top_k=12, threshold to drop tails (e.g., similarity≥0.75 or distance≤0.25 for COSINE), then add a ranker if you still see off-topic chunks.',
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/rag-api-v1',
          learn_more_text: 'Find out more about the RAG Engine API'
        }
      end,
      display_priority: 86,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, _connection, _config_fields|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'text', hint: 'Find a current list of available Gemini models at `https://ai.google.dev/gemini-api/docs/models`'},
          { name: 'rag_corpus', optional: false, hint: 'RAG corpus: projects/{project}/locations/{region}/ragCorpora/{corpus}' },
          { name: 'question', optional: false },
          { name: 'restrict_to_file_ids', type: 'array', of: 'string', optional: true },
          { name: 'max_contexts', type: 'integer', optional: true, default: 12 },

          # Preferred (object) retrieval config
          { name: 'rag_retrieval_config', label: 'Retrieval config', type: 'object',
            properties: object_definitions['rag_retrieval_config'], optional: true,
            hint: 'Use this to set top_k, a single threshold (distance OR similarity), and one ranker (semantic OR LLM).' },

          { name: 'system_preamble', optional: true, hint: 'e.g., Only answer from retrieved contexts; say “I don’t know” otherwise.' },
          { name: 'temperature', type: 'number', optional: true, hint: 'Default 0' },
          { name: 'emit_context_chunks', type: 'boolean', control_type: 'checkbox', optional: true, default: true,
            hint: 'If enabled, returns generator-ready context_chunks alongside the answer.' },
          { name: 'return_rationale', type: 'boolean', control_type: 'checkbox', optional: true, default: true,
            hint: 'If enabled, model returns a brief rationale (1–2 sentences) for traceability.' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions, _connection|
        [
          { name: 'answer' },
          { name: 'rationale' },
          { name: 'citations', type: 'array', of: 'object', properties: [
              { name: 'chunk_id' }, { name: 'source' }, { name: 'uri' }, { name: 'score', type: 'number' }
            ]
          },
          { name: 'context_chunks', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'text' }, { name: 'score', type: 'number' },
              { name: 'source' }, { name: 'uri' },
              { name: 'metadata', type: 'object' },
              { name: 'metadata_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
              { name: 'metadata_json' }
            ]
          },
          { name: 'responseId' },
          { name: 'usage', type: 'object', properties: [
              { name: 'promptTokenCount', type: 'integer' },
              { name: 'candidatesTokenCount', type: 'integer' },
              { name: 'totalTokenCount', type: 'integer' }
            ]
          },
          { name: 'request_preview', type: 'object' },
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message' }, { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id' },
            { name: 'retrieval', type: 'object' },
            { name: 'rank', type: 'object' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        # Begin logging
        started_at = Time.now.utc.iso8601
        t0   = Time.now
        corr = call(:build_correlation_id)
        retr_url = nil; retr_req_body = nil
        gen_url  = nil;  gen_req_body  = nil

        # Validate connection
        proj = connection['project_id']
        loc  = (connection['location'] || '').to_s.downcase
        error('Connection is missing project_id') if proj.to_s.empty?
        error('Connection is missing location (must be regional)') if loc.empty? || loc == 'global'

        # Retrieve contexts (inline)
        corpus = call(:normalize_rag_corpus, connection, input['rag_corpus'])
        error('rag_corpus is required') if corpus.blank?

        parent        = "projects/#{connection['project_id']}/locations/#{loc}"
        req_params_re = "parent=#{parent}"

        # Merge + validate retrieval options from object/flat inputs
        opts = call(:build_retrieval_opts_from_input!, input)

        retrieve_payload = call(
          :build_rag_retrieve_payload,
          input['question'],
          corpus,
          input['restrict_to_file_ids'],
          opts
        )
        retr_url      = call(:aipl_v1_url, connection, loc, "#{parent}:retrieveContexts")
        retr_req_body = call(:json_compact, retrieve_payload)
        retr_resp = post(retr_url)
                      .headers(call(:request_headers_auth, connection, corr, nil, req_params_re))
                      .payload(retr_req_body)
        raw_ctxs = call(:normalize_retrieve_contexts!, retr_resp)

        maxn   = call(:clamp_int, (input['max_contexts'] || 12), 1, 100)
        chunks = call(:map_context_chunks, raw_ctxs, maxn)
        error('No contexts retrieved; check corpus/permissions/region') if chunks.empty?

        # Generate structured answer with parsed contexts
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        gen_cfg = {
          'temperature'       => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
          'maxOutputTokens'   => 1024,
          'responseMimeType'  => 'application/json',
          'responseSchema'    => {
            'type'        => 'object', 'additionalProperties' => false,
            'properties'  => {
              'answer'    => { 'type' => 'string' },
              'rationale' => { 'type' => 'string' },
              'citations' => {
                'type'    => 'array',
                'items'   => {
                  'type'  => 'object', 'additionalProperties' => false,
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

        want_rat = call(:normalize_boolean, input['return_rationale'])
        sys_text = (input['system_preamble'].presence ||
          "Answer using ONLY the retrieved context chunks. If the context is insufficient, reply with “I don’t know.” "\
          "Keep answers concise and include citations with chunk_id, source, uri, and score."\
          "#{want_rat ? ' Also include a brief rationale (1–2 sentences) explaining which chunks support the answer.' : ''}")
        sys_inst = { 'role' => 'system', 'parts' => [ { 'text' => sys_text } ] }

        ctx_blob = call(:format_context_chunks, chunks)
        contents = [
          { 'role' => 'user', 'parts' => [ { 'text' => "Question:\n#{input['question']}\n\nContext:\n#{ctx_blob}" } ] }
        ]

        gen_payload = {
          'contents'          => contents,
          'systemInstruction' => sys_inst,
          'generationConfig'  => gen_cfg
        }

        # Route by model path location
        loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
        gen_url      = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
        gen_req_body = call(:json_compact, gen_payload)
        req_params_g = "model=#{model_path}"
        gen_resp = post(gen_url)
                    .headers(call(:request_headers_auth, connection, corr, connection['user_project'], req_params_g))
                    .payload(gen_req_body)
        text   = gen_resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
        parsed = call(:safe_parse_json, text)

        out = {
          'answer'     => (parsed['answer'] || text),
          'rationale'  => (parsed['rationale'] || nil),
          'citations'  => (parsed['citations'] || []),
          'responseId' => gen_resp['responseId'],
          'usage'      => gen_resp['usageMetadata']
        }.merge(call(:telemetry_envelope, t0, corr, true, call(:telemetry_success_code, gen_resp), 'OK'))

        # Enrich citations from retrieved chunks (best-effort)
        begin
          if out['citations'].is_a?(Array) && chunks.is_a?(Array)
            by_id = {}; chunks.each { |c| by_id[c['id'].to_s] = c }
            out['citations'] = out['citations'].map do |c|
              h = c.is_a?(Hash) ? c.dup : {}
              cid = h['chunk_id'].to_s
              if cid != '' && by_id[cid]
                h['source'] ||= by_id[cid]['source']
                h['uri']    ||= by_id[cid]['uri']
                h['score']  ||= by_id[cid]['score']
              end
              h
            end
          end
        rescue
          # non-fatal; leave citations as-is
        end

        # Optional emission of generator-ready context chunks
        if call(:normalize_boolean, input['emit_context_chunks'])
          out['context_chunks'] = chunks
        end

        # Telemetry preview from canonical opts:
        (out['telemetry'] ||= {})['retrieval'] = {}.tap do |h|
          h['top_k'] = opts['topK'].to_i if opts['topK']
          if opts['vectorDistanceThreshold']
            h['filter'] = { 'type' => 'distance',  'value' => opts['vectorDistanceThreshold'].to_f }
          elsif opts['vectorSimilarityThreshold']
            h['filter'] = { 'type' => 'similarity','value' => opts['vectorSimilarityThreshold'].to_f }
          end
        end
        if opts['rankServiceModel']
          (out['telemetry'] ||= {})['rank'] = { 'mode' => 'rank_service', 'model' => opts['rankServiceModel'] }
        elsif opts['llmRankerModel']
          (out['telemetry'] ||= {})['rank'] = { 'mode' => 'llm', 'model' => opts['llmRankerModel'] }
        end

        # Debug preview in non-prod
        unless call(:normalize_boolean, connection['prod_mode'])
          if call(:normalize_boolean, input['debug'])
            out = out.merge(call(:request_preview_pack, gen_url, 'POST',
                                call(:request_headers_auth, connection, corr, connection['user_project'], req_params_g),
                                gen_req_body))
          end
        end

        facets = call(:compute_facets_for!, 'rag_answer', out)
        call(:tail_log_emit!, connection, :rag_answer, started_at, t0, out, nil, facets)
        out
      rescue => e
        g   = call(:extract_google_error, e)
        msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
        env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg)
        if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
          env['debug'] = call(:debug_pack, true, gen_url || retr_url, (gen_req_body || retr_req_body), g)
        end
        call(:tail_log_emit!, connection, :rag_answer, started_at, t0, nil, e, nil)
        error(env)
      end,

      sample_output: lambda do
        {
          'answer' => 'Employees may carry over up to 40 hours of PTO.',
          'rationale' => 'Policy paragraph [doc-42#c3] states the carryover cap explicitly.',
          'citations' => [
            { 'chunk_id' => 'doc-42#c3', 'source' => 'handbook', 'uri' => 'https://drive.google.com/file/d/abc...', 'score' => 0.91 }
          ],
          'context_chunks' => [
            { 'id' => 'doc-42#c3', 'text' => 'Employees may carry over up to 40 hours...', 'score' => 0.91,
              'source' => 'handbook', 'uri' => 'https://drive.google.com/file/d/abc...',
              'metadata' => { 'page' => 7 }, 'metadata_kv' => [{ 'key' => 'page', 'value' => 7 }], 'metadata_json' => '{"page":7}' }
          ],
          'responseId' => 'resp-123',
          'usage' => { 'promptTokenCount' => 298, 'candidatesTokenCount' => 156, 'totalTokenCount' => 454 },
          'ok' => true,
          'telemetry' => {
            'http_status' => 200, 'message' => 'OK', 'duration_ms' => 44, 'correlation_id' => 'sample',
            'retrieval' => { 'top_k' => 12, 'filter' => { 'type' => 'similarity', 'value' => 0.8 } },
            'rank' => { 'mode': 'llm', 'model': 'gemini-2.0-flash' }
          }
        }
      end
    },

    # Utility
    embed_text: {
      title: 'Embed text',
      subtitle: 'Get embeddings from a publisher embedding model',
      help: lambda do |_|
        {
          body: 'POST :predict on a publisher embedding model',
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/text-embeddings-api',
          learn_more_text: 'Find more information about the Text embeddings API'
        }
      end,
      display_priority: 7,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'model', label: 'Embedding model', optional: false, control_type: 'text', default: 'text-embedding-005' },
          { name: 'texts', type: 'array', of: 'string', optional: false },
          { name: 'task', hint: 'Optional: RETRIEVAL_QUERY or RETRIEVAL_DOCUMENT' },
          { name: 'autoTruncate', type: 'boolean', hint: 'Truncate long inputs automatically' },
          { name: 'outputDimensionality', type: 'integer', optional: true, convert_input: 'integer_conversion',
            hint: 'Optional dimensionality reduction (see model docs).' }
        ]
      end,
      output_fields: lambda do |_object_definitions, _connection|
        Array(object_definitions['embed_output']) + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        corr = call(:build_correlation_id)
        url = nil; req_body = nil

        begin
          model_path = call(:build_embedding_model_path, connection, input['model'])

          # Guard: texts length cannot exceed model batch limit (friendly message)
          max_per_call = call(:embedding_max_instances, model_path)
          texts = call(:safe_array, input['texts'])
          error("Too many texts (#{texts.length}). Max per request for this model is #{max_per_call}. Chunk upstream.") if texts.length > 0 && texts.length > max_per_call

          allowed_tasks = %w[
            RETRIEVAL_QUERY RETRIEVAL_DOCUMENT SEMANTIC_SIMILARITY
            CLASSIFICATION CLUSTERING QUESTION_ANSWERING FACT_VERIFICATION
            CODE_RETRIEVAL_QUERY
          ]
          task = input['task'].to_s.strip
          task = nil if task.blank?
          error("Invalid task_type: #{task}. Allowed: #{allowed_tasks.join(', ')}") \
            if task && !allowed_tasks.include?(task)

          instances = call(:safe_array, input['texts']).map { |t|
            h = { 'content' => t }
            h['task_type'] = task if task
            h
          }

          # Coerce/validate embedding parameters to correct JSON types
          params = call(:sanitize_embedding_params, {
            'autoTruncate'         => input['autoTruncate'],
            'outputDimensionality' => input['outputDimensionality']
          })

          result = call(:predict_embeddings, connection, model_path, instances, params, corr)
          result = result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
          call(:tail_log_emit!, connection, :embed_text, started_at, t0, result, nil)
          result
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg)
          # Optional debug attachment in non-prod:
          if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
            env['debug'] = call(:debug_pack, true, url, req_body, g)
          end
          call(:tail_log_emit!, connection, :embed_text, started_at, t0, nil, e)
          error(env)   # <-- raise so Workato marks step failed and retries if applicable
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
          'metadata' => { 'billableCharacterCount' => 230 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },
    count_tokens: {
      title: 'Count tokens',
      description: 'POST :countTokens on a publisher model',
      help: lambda do |_|
        {
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/count-tokens',
          learn_more_text: 'Check out Google docs for the CountTokens API'
        }
      end,
      display_priority: 5,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, config_fields|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'text' },
          { name: 'contents', type: 'array', of: 'object', properties: object_definitions['content'], optional: false },
          { name: 'system_preamble', label: 'System preamble (text)', optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'totalTokens', type: 'integer' },
          { name: 'totalBillableCharacters', type: 'integer' },
          { name: 'promptTokensDetails', type: 'array', of: 'object' }
        ] + Array(object_definitions['envelope_fields'])
      end,
      execute:  lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        corr = call(:build_correlation_id)
        url = nil; req_body = nil

        # Compute model path
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        # Build payload
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
        url = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:countTokens")

        begin
          req_body = call(:json_compact, {
            'contents'          => contents,
            'systemInstruction' => sys_inst
          })
          resp = post(url)
                    .headers(call(:request_headers_auth, connection, corr, connection['user_project'], "model=#{model_path}"))
                    .payload(req_body)
          code = call(:telemetry_success_code, resp)
          result = resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          call(:tail_log_emit!, connection, :count_tokens, started_at, t0, result, nil)
          result
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg)
          # Optional debug attachment in non-prod:
          if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
            env['debug'] = call(:debug_pack, true, url, req_body, g)
          end
          call(:tail_log_emit!, connection, :count_tokens, started_at, t0, nil, e)
          error(env)   # <-- raise so Workato marks step failed and retries if applicable
        end
      end,
      sample_output: lambda do
        { 'totalTokens' => 31, 'totalBillableCharacters' => 96,
          'promptTokensDetails' => [ { 'modality' => 'TEXT', 'tokenCount' => 31 } ],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' } }
      end
    },
    operations_get: {
      title: 'Get (poll) long running operation',
      subtitle: 'google.longrunning.operations.get',
      help: lambda do |_|
        {
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/docs/general/long-running-operations',
          learn_more_text: 'Find out more about Long running operations'
        }
      end,
      display_priority: 5,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'operation', optional: false,
            hint: 'Operation name or full path, e.g., projects/{p}/locations/{l}/operations/{id}' }
        ]
      end,
      output_fields: lambda do |_object_definitions, _connection|
        [
          { name: 'name' }, { name: 'done', type: 'boolean' },
          { name: 'metadata', type: 'object' }, { name: 'error', type: 'object' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        corr = call(:build_correlation_id)
        begin
          call(:ensure_project_id!, connection)
          # Accept either full /v1/... or name-only
          op = input['operation'].to_s.sub(%r{^/v1/}, '')
          loc = (connection['location'].presence || 'us-central1').to_s.downcase
          url = call(:aipl_v1_url, connection, loc, op.start_with?('projects/') ? op : "projects/#{connection['project_id']}/locations/#{loc}/operations/#{op}")
          resp = get(url).headers(call(:request_headers_auth, connection, corr, connection['user_project'], nil))
          code = call(:telemetry_success_code, resp)
          result = resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          call(:tail_log_emit!, connection, :operations_get, started_at, t0, result, nil)
          result
        rescue => e
          env = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
          call(:tail_log_emit!, connection, :operations_get, started_at, t0, nil, e)
          env
        end
      end,

      sample_output: lambda do
        { 'name' => 'projects/p/locations/us-central1/operations/123', 'done' => false,
          'ok' => true, 'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 8, 'correlation_id' => 'sample' } }
      end
    },
    logs_write: {
      title: 'Logs: write (Cloud Logging)',
      subtitle: 'Send structured logs to GCP',
      help: lambda do |_| 
        { 
          body: 'Write structured logs to Google Cloud Logging. Canonical severities accepted by GCP logging facets include: '\
                'DEFAULT, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY. '
        }
      end,
      input_fields: lambda do |object_definitions, connection|
        [
          { name: 'project_id', optional: false },
          { name: 'log_id', label: 'Log ID', optional: false, hint: 'e.g., workato_vertex_rag' },
          { name: 'entries', type: 'array', of: 'object', optional: false, properties: [
            {
              name: 'severity',
              label: 'Severity',
              control_type: 'select',
              pick_list: 'logging_severities',
              optional: true,
              default: 'INFO',
              toggle_hint: 'Or enter a custom severity',
              toggle_field: {
                name: 'severity_custom',
                label: 'Custom severity (text)',
                type: 'string',
                hint: 'E.g., INFO, WARNING, ERROR — or your own label'
              }
            },
            { name: 'labels', type: 'object', properties: [] },
            { name: 'jsonPayload', type: 'object', properties: [] }
          ]}
        ]
      end,
      output_fields: lambda do |_object_definitions, _connection|
        [{ name: 'status' }, { name: 'http_status' }]
      end,
      execute: lambda do |connection, input|
        # Canonical severities accepted by Cloud Logging facets:
        # DEFAULT, DEBUG, INFO, NOTICE, WARNING, ERROR, CRITICAL, ALERT, EMERGENCY
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        corr = call(:build_correlation_id)
        project = input['project_id']
        log_id  = input['log_id']
        nonstandard_prefix = 'NONSTANDARD/' # backend-only tag for abnormal values
        body = {
          entries: (input['entries'] || []).map do |e|
          # Prefer custom severity (toggle), then dropdown, then default; normalize afterwards
          sev_raw = begin
            sc = e['severity_custom'].to_s.strip
            sd = e['severity'].to_s.strip
            sc.empty? ? (sd.empty? ? 'INFO' : sd) : sc
          end
          sev = call(:normalize_severity, sev_raw, nonstandard_prefix)
            {
              logName: "projects/#{project}/logs/#{log_id}",
              resource: { type: 'global', labels: { project_id: project } },
              severity: sev,
              labels: e['labels'],
              jsonPayload: e['jsonPayload']
            }.compact
          end
        }
        begin
          # Reuse standard header builder (adds auth, optional x-goog-user-project, x-goog-request-params)
          req_headers = call(:headers_logging, connection, corr, nil)
          resp = post("https://logging.googleapis.com/v2/entries:write")
                    .headers(req_headers)
                    .payload(call(:json_compact, body))
                    .request_format_json
          # Shape the return to match output_fields
          http_status = (resp['status'] || resp['status_code'] || 200).to_i
          out = { 'status' => 'ok', 'http_status' => http_status }
          call(:tail_log_emit!, connection, :logs_write, started_at, t0, out, nil)
          out
        rescue => e
          call(:tail_log_emit!, connection, :logs_write, started_at, t0, nil, e)
          # Re-raise normalized so Workato retry/metrics apply; include code when we can parse it
          code = call(:telemetry_parse_error_code, e)
          code = 500 if code.to_i == 0
          error({
            'status'  => 'error',
            'message' => e.to_s,
            'code'    => code
          })
        end
      end
    }
  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {
    # What to emit from rerank for ergonomics/BC:
    # - records_only:     [{id, score, rank}]
    # - enriched_records: [{id, score, rank, content, metadata}]
    # - context_chunks:   enriched records + generator-ready context_chunks
    rerank_emit_shapes: lambda do |_connection|
      [
        ['Records only (id, score, rank)','records_only'],
        ['Enriched records (adds content/metadata)','enriched_records'],
        ['Context chunks (enriched + context_chunks)','context_chunks']
      ]
    end,
    gen_generate_modes: lambda do |_connection|
      [
        ['Plain (no grounding)','plain'],
        ['Grounded via Google Search','grounded_google'],
        ['Grounded via Vertex AI Search','grounded_vertex'],
        ['RAG-lite (provided context)','rag_with_context'],
        ['Grounded via Vertex RAG Store (tool)','grounded_rag_store']
      ]
    end,
    trim_strategies: lambda do  |_connection|
      [['Drop lowest score first','drop_low_score'],
      ['Diverse (MMR-like)','diverse_mmr'],
      ['Truncate by characters','truncate_chars']]
    end,
    discovery_hosts: lambda do |_connection|
      [
        ['Discovery Engine (global)', 'discoveryengine.googleapis.com'],
        ['Discovery Engine (US multi-region)', 'us-discoveryengine.googleapis.com']
      ]
    end,
    modes_classification: lambda do |_connection|
      [%w[Embedding embedding], %w[Generative generative], %w[Hybrid hybrid]]
    end,
    modes_grounding: lambda do |_connection|
      [%w[Google\ Search google_search], %w[Vertex\ AI\ Search vertex_ai_search]]
    end,
    roles: lambda do |_connection|
      # Contract-conformant roles (system handled via system_preamble)
      [['user','user'], ['model','model']]
    end,
    rag_source_families: lambda do |_connection|
      [
        ['Google Cloud Storage', 'gcs'],
        ['Google Drive', 'drive']
      ]
    end,
    drive_input_type: lambda do |_connection|
      [
        ['Drive Files', 'files'],
        ['Drive Folder', 'folder']
      ]
    end,
    distance_measures: lambda do |_connection|
      [
        ['Cosine distance', 'COSINE_DISTANCE'],
        ['Dot product',     'DOT_PRODUCT'],
        ['Euclidean',       'EUCLIDEAN_DISTANCE']
      ]
    end,
    iam_services: lambda do |_connection|
      [['Vertex AI','vertex'], ['AI Applications (Discovery)','discovery']]
    end,
    discovery_versions: lambda do |_connection|
      [
        ['v1alpha', 'v1alpha'],
        ['v1beta',  'v1beta'],
        ['v1',      'v1']
      ]
    end,
    ai_apps_locations: lambda do |_connection|
      [
        ['Global (recommended default)', 'global'],
        ['US (multi-region)', 'us'],
        ['EU (multi-region)', 'eu']
      ]
    end,
    logging_severities: lambda do |_connection|
      %w[DEFAULT DEBUG INFO NOTICE WARNING ERROR CRITICAL ALERT EMERGENCY].map { |s| [s, s] }
    end

  },
  
  # --------- METHODS ------------------------------------------------------
  methods: {
    # Build canonical retrieval opts from either the object field
    #   input['rag_retrieval_config'] OR legacy flat fields on the action.
    # Returns a Hash suitable for build_rag_retrieve_payload.
    build_retrieval_opts_from_input!: lambda do |input|
      cfg = (input['rag_retrieval_config'].is_a?(Hash) ? input['rag_retrieval_config'] : {})
      filt = cfg['filter'].is_a?(Hash) ? cfg['filter'] : {}
      rank = cfg['ranking'].is_a?(Hash) ? cfg['ranking'] : {}

      topk = cfg['top_k'] || input['similarity_top_k']
      dist = filt['vector_distance_threshold']   || input['vector_distance_threshold']
      sim  = filt['vector_similarity_threshold'] || input['vector_similarity_threshold']
      rsm  = rank['rank_service_model']          || input['rank_service_model']
      llm  = rank['llm_ranker_model']            || input['llm_ranker_model']

      # Validate oneof unions
      call(:guard_threshold_union!, dist, sim)
      call(:guard_ranker_union!, rsm, llm)

      {
        'topK'                         => topk,
        'vectorDistanceThreshold'      => dist,
        'vectorSimilarityThreshold'    => sim,
        'rankServiceModel'             => rsm,
        'llmRankerModel'               => llm
      }.delete_if { |_k, v| v.nil? || v == '' }
    end,
    # Heuristic: average of top-K citation scores, clamped to [0,1]. Returns nil if no scores.
    overall_confidence_from_citations: lambda do |citations, k=3|
      arr = Array(citations).map { |c| (c.is_a?(Hash) ? c['score'] : nil) }.compact.map(&:to_f)
      return nil if arr.empty?
      topk = arr.sort.reverse.first([[k, 1].max, arr.length].min)
      avg  = topk.sum / topk.length.to_f
      [[avg, 0.0].max, 1.0].min.round(4)
    end,

    # --- Tail logging helpers -------------------------------------------------
    # Lightweight context to track timing/correlation across an action run.
    tail_log_begin!: lambda do |connection, action_id, input|
      {
        action_id: action_id.to_s,
        started_at: (Time.now.utc.iso8601 rescue Time.at((t0 = Time.now*1000).to_i/1000.0).utc.iso8601),
        t0: (t0 = Time.now rescue Time.now),
        corr_id: (SecureRandom.uuid rescue "#{rand}-#{Time.now.to_i}"),
        project_id: connection['project_id'] || connection['gcp_project_id'],
        log_id: 'workato_vertex_rag' # keep internal; no builder knob required
      }
    end,
    tail_log_end!: lambda do |connection, ctx, result, error|
      # Build a compact envelope; scrub obvious big/PII fields
      latency_ms = (((t0 = Time.now - ctx[:t0]) * 1000).round rescue nil)
      status = error ? 'error' : 'ok'

      # Pull some soft identifiers if you have them in the connection
      env  = connection['environment'] || 'prod'
      app  = 'vertex_rag_engine'

      entry = {
        logName: "projects/#{ctx[:project_id]}/logs/#{ctx[:log_id]}",
        resource: { type: 'global', labels: { project_id: ctx[:project_id] } },
        severity: (error ? 'ERROR' : 'INFO'),
        labels: {
          connector: app,
          action: ctx[:action_id],
          correlation_id: ctx[:corr_id],
          env: env
        },
        jsonPayload: {
          status: status,
          started_at: ctx[:started_at],
          ended_at: (Time.now.utc.iso8601 rescue nil),
          latency_ms: latency_ms,
          request_meta: {
            # include only safe breadcrumbs; avoid bodies/tokens
            recipe_id: connection['__recipe_id'],
            job_id: connection['__job_id'],
            region: connection['location'] || connection['region']
          },
          result_meta: call(:_shrink_result_meta, result),
          error: call(:_normalize_error, error)
        }
      }.compact

      call(:_tl_post_logs, connection, [entry])
    rescue => swallow
      # Never raise from logging; tail logger must be fire-and-forget
      nil
    end,
    _normalize_error: lambda do |e|
      return nil unless e
      # Workato errors vary; capture stable fields only
      h = {
        class: e.class.to_s,
        message: e.message.to_s[0, 1024],
        http_status: (e.dig(:response, :status) rescue nil),
        code: (e.dig(:error, :code) rescue nil)
      }.compact
      h
    end,
    tail_log_emit!: lambda do |connection, action_id, started_at, t0, result, error, facets=nil|
      # Resolve project (guard) and respect feature flag
      begin
        call(:ensure_project_id!, connection)
      rescue
        # If ensure fails we still fast-exit; never raise from logger
      end
      project = (connection['project_id'] || connection['gcp_project_id']).to_s.strip
      return nil if project.empty? || connection['disable_tail_logging'] == true

      # Timing
      now = Time.now
      begun = started_at || now.utc.iso8601
      t0f = (t0 || now.to_f).to_f
      latency_ms = ((now.to_f - t0f) * 1000).round

      # Envelope
      env  = connection['environment'] || 'prod'
      app  = 'vertex_rag_engine'
      # Prefer action-scoped correlation id from the result envelope; fallback to connection or fresh UUID
      corr = begin
        (result && result['telemetry'] && result['telemetry']['correlation_id']) ||
        connection['correlation_id'] ||
        SecureRandom.uuid
      rescue
        SecureRandom.uuid
      end
      log_id = 'workato_vertex_rag'
      # Normalize severity (maps WARN->WARNING, etc.; prefixes NONSTANDARD/ for unknowns)
      severity = call(:normalize_severity, (error ? 'ERROR' : 'INFO'))

      entry = {
        logName: "projects/#{project}/logs/#{log_id}",
        resource: { type: 'global', labels: { project_id: project } },
        severity: severity,
        labels: {
          connector: app,
          action: action_id.to_s,
          correlation_id: corr,
          env: env,
          # Duplicate a couple of request IDs into labels for faster filtering
          recipe_id: connection['__recipe_id'],
          job_id:    connection['__job_id']
        },
        jsonPayload: {
          status: (error ? 'error' : 'ok'),
          started_at: begun,
          ended_at: now.utc.iso8601,
          latency_ms: latency_ms,
          request_meta: {
            recipe_id: connection['__recipe_id'],
            job_id: connection['__job_id'],
            region: connection['location'] || connection['region']
          },
          result_meta: call(:_tl_shrink_meta, result),
          error: call(:_tl_norm_error, error)
        }
      }
      # Attach compact facets if enabled and provided
      if call(:_facets_enabled?, connection)
        fac = facets.is_a?(Hash) ? facets.dup : {}
        entry[:jsonPayload][:facets] = fac if fac.any?
      end
      call(:_tl_post_logs, connection, [entry])
    rescue
      nil
    end,
    _tl_norm_error: lambda do |e|
      return nil unless e
      { class: e.class.to_s, message: e.message.to_s[0, 1024],
        http_status: (e.dig(:response, :status) rescue nil),
        code: (e.dig(:error, :code) rescue nil) }.compact
    end,
    _tl_shrink_meta: lambda do |r|
      return nil if r.nil?
      case r
      when Hash
        kept = %w[id name count total items_length model model_version index datapoints_upserted]
        h = {}
        kept.each do |k|
          v = r[k] || r[k.to_sym]
          next if v.nil?
          h[k] = v.is_a?(Array) ? v.length : v
        end
        h.empty? ? { summary: 'present' } : h
      when Array then { items_length: r.length }
      else { summary: r.to_s[0, 128] }
      end
    end,
    _tl_post_logs: lambda do |connection, entries|
      return nil if entries.nil? || entries.empty?
      # Build routing hint: x-goog-request-params requires log_name
      project = (connection['project_id'] || connection['gcp_project_id']).to_s.strip
      log_id  = begin
        # entries[0][:logName] = "projects/<p>/logs/<log_id>"
        ln = entries.first && (entries.first[:logName] || entries.first['logName']).to_s
        ln.split('/').last
      rescue
        nil
      end
      req_params = (project.empty? || log_id.to_s.empty?) ? nil : "log_name=projects/#{project}/logs/#{log_id}"
      # Use the standard auth header builder so 401s trigger refresh_on
      corr = connection['correlation_id'] || SecureRandom.uuid
      hdrs = call(:headers_logging, connection, corr, req_params)
      # Fire the write using JSON (required by Cloud Logging). Do NOT swallow here;
      # tail_log_emit! already rescues around this call.
      body = { 'entries' => entries }
      post("https://logging.googleapis.com/v2/entries:write")
        .headers(hdrs)
        .payload(call(:json_compact, body))
        .request_format_json
      nil
    end,
    normalize_severity: lambda do |s, prefix = 'NONSTANDARD/'|
      raw = s.to_s.upcase.strip
      return 'INFO' if raw.empty?
      canonical = %w[DEFAULT DEBUG INFO NOTICE WARNING ERROR CRITICAL ALERT EMERGENCY]
      alias_map = {
        'WARN'=>'WARNING','ERR'=>'ERROR','SEVERE'=>'ERROR',
        'FATAL'=>'CRITICAL','CRIT'=>'CRITICAL','TRACE'=>'DEBUG','DBG'=>'DEBUG','EMERG'=>'EMERGENCY'
      }
      mapped = alias_map[raw] || raw
      mapped = mapped[0, 64]
      canonical.include?(mapped) ? mapped : "#{prefix}#{mapped}"
    end,

    # -- Facets plumbing ----------------------------------------------------
    _facets_enabled?: lambda do |connection|
      # default true; explicit false disables
      v = connection['enable_facets_logging']
      v.nil? ? true : (v == true)
    end,
    _facet_bool:   lambda { |v| v == true },
    _facet_int:    lambda { |v| (v.is_a?(Numeric) || v.to_s =~ /\A-?\d+\z/) ? v.to_i : nil },
    _facet_float:  lambda { |v| (v.is_a?(Numeric) || v.to_s =~ /\A-?\d+(\.\d+)?\z/) ? v.to_f : nil },
    _facet_str:    lambda { |v, max=128| s = v.to_s; s.empty? ? nil : s[0, max] },
    _facet_any?:   lambda { |h| h.is_a?(Hash) && h.any? },
    _facet_safety_blocked?: lambda do |cand|
      Array(cand && cand['safetyRatings']).any? { |r| r.is_a?(Hash) && r['blocked'] == true }
    end,
    _facet_finish_reason: lambda do |resp|
      Array(resp['candidates']).first && Array(resp['candidates']).first['finishReason']
    end,
    _facet_tokens: lambda do |usage|
      return {} unless usage.is_a?(Hash)
      {
        'tokens_prompt'     => usage['promptTokenCount'],
        'tokens_candidates' => usage['candidatesTokenCount'],
        'tokens_total'      => usage['totalTokenCount']
      }.delete_if { |_k,v| v.nil? }
    end,
    compute_facets_for!: lambda do |action_id, out, extras = {}|
      # out: action result hash (success path). Compact, redaction-safe, no PII.
      return {} unless out.is_a?(Hash)
      h = {}
      tel = out['telemetry'].is_a?(Hash) ? out['telemetry'] : {}
      ret = tel['retrieval'].is_a?(Hash) ? tel['retrieval'] : {}
      rnk = tel['rank'].is_a?(Hash)      ? tel['rank']      : {}

      # Retrieval knobs
      h['retrieval_top_k']       = call(:_facet_int, ret['top_k'])
      if ret['filter'].is_a?(Hash)
        h['retrieval_filter']     = call(:_facet_str, ret['filter']['type'])
        h['retrieval_filter_val'] = call(:_facet_float, ret['filter']['value'])
      end

      # Ranker provenance
      h['rank_mode']  = call(:_facet_str, rnk['mode'])
      h['rank_model'] = call(:_facet_str, rnk['model'], 256)

      # Context counts (works for both retrieve→contexts and rag-lite→context_chunks)
      ctxs = out['contexts'] || out['context_chunks']
      h['contexts_returned'] = Array(ctxs).length

      # Token usage (usage or usageMetadata)
      usage = out['usage'] || out['usageMetadata']
      h.merge!(call(:_facet_tokens, usage))

      # Generation outcome fields (if present)
      # Try structured finishReason first (from candidates), otherwise from parsed meta if caller attached it later.
      fr = call(:_facet_finish_reason, out)
      h['gen_finish_reason'] = call(:_facet_str, fr) if fr

      # Safety: true if any candidate blocked
      c0 = Array(out['candidates']).first || {}
      h['safety_blocked'] = call(:_facet_bool, call(:_facet_safety_blocked?, c0)) if c0 && c0.any?

      # Confidence proxy (rag-lite)
      h['confidence'] = call(:_facet_float, out['confidence']) if out.key?('confidence')

      # Abstention detection (simple heuristic)
      ans = out['answer']
      if ans.is_a?(String)
        h['answered_unknown'] = true if ans.strip =~ /\A(?i:(i\s+don[’']?t\s+know|cannot\s+answer|no\s+context|not\s+enough\s+context))/
      end

      # Merge caller extras (pre-sanitized small scalars only)
      extras = extras.is_a?(Hash) ? extras.dup : {}
      extras.delete_if { |_k,v| v.is_a?(Array) || v.is_a?(Hash) } # avoid bloat
      h.merge!(extras)

      # Final compaction: drop nils/empties
      h.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
      h
    end,

    gen_generate_core!: lambda do |connection, input|
      t0   = Time.now
      corr = call(:build_correlation_id)
      mode = (input['mode'] || 'plain').to_s

      # Model + location
      model_path     = call(:build_model_path_with_global_preview, connection, input['model'])
      loc_from_model = (model_path[/\/locations\/([^\/]+)/,1] || (connection['location'].presence || 'global')).to_s.downcase
      url            = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
      req_params     = "model=#{model_path}"

      # Base fields
      contents = call(:sanitize_contents!, input['contents'])
      sys_inst = call(:system_instruction_from_text, input['system_preamble'])
      gen_cfg  = call(:sanitize_generation_config, input['generation_config'])
      safety   = call(:sanitize_safety!, input['safetySettings'])
      tool_cfg = call(:safe_obj, input['toolConfig'])
      tools    = nil

      case mode
      when 'plain'
        # no special tools
      when 'grounded_google'
        tools = [ { 'googleSearch' => {} } ]
      when 'grounded_vertex'
        ds   = input['vertex_ai_search_datastore'].to_s
        scfg = input['vertex_ai_search_serving_config'].to_s
        error('Provide exactly one of vertex_ai_search_datastore OR vertex_ai_search_serving_config') \
          if (ds.blank? && scfg.blank?) || (ds.present? && scfg.present?)
        vas = {}; vas['datastore'] = ds unless ds.blank?; vas['servingConfig'] = scfg unless scfg.blank?
        tools = [ { 'retrieval' => { 'vertexAiSearch' => vas } } ]
      when 'grounded_rag_store'
        # Build the retrieval tool payload for Vertex RAG Store
        corpus = call(:normalize_rag_corpus, connection, input['rag_corpus'])
        error('rag_corpus is required for grounded_rag_store') if corpus.blank?
        # Guards for unions
        call(:guard_threshold_union!, input['vector_distance_threshold'], input['vector_similarity_threshold'])
        call(:guard_ranker_union!, input['rank_service_model'], input['llm_ranker_model'])

        vr = { 'ragResources' => [ { 'ragCorpus' => corpus } ] }
        # Optional knobs
        filt = {}
        if input['vector_distance_threshold'].present?
          filt['vectorDistanceThreshold'] = call(:safe_float, input['vector_distance_threshold'])
        elsif input['vector_similarity_threshold'].present?
          filt['vectorSimilarityThreshold'] = call(:safe_float, input['vector_similarity_threshold'])
        end
        rconf = {}
        if input['rank_service_model'].to_s.strip != ''
          rconf['rankService'] = { 'modelName' => input['rank_service_model'].to_s.strip }
        elsif input['llm_ranker_model'].to_s.strip != ''
          rconf['llmRanker']   = { 'modelName' => input['llm_ranker_model'].to_s.strip }
        end
        retrieval_cfg = {}
        retrieval_cfg['similarityTopK'] = call(:clamp_int, (input['similarity_top_k'] || 0), 1, 200) if input['similarity_top_k'].present?
        retrieval_cfg['filter']  = filt unless filt.empty?
        retrieval_cfg['ranking'] = rconf unless rconf.empty?

        tools = [ { 'retrieval' => { 'vertexRagStore' => vr.merge( (retrieval_cfg.empty? ? {} : { 'ragRetrievalConfig' => retrieval_cfg }) ) } } ]
      when 'rag_with_context'
        # Build RAG-lite prompt + JSON schema
        q        = input['question'].to_s
        maxn     = call(:clamp_int, (input['max_chunks'] || 20), 1, 100)
        chunks   = call(:safe_array, input['context_chunks']).first(maxn)
        sal_text = input['salience_text'].to_s.strip
        sal_id   = (input['salience_id'].presence || 'salience').to_s
        sal_scr  = (input['salience_score'].presence || 1.0).to_f
        items    = []
        items << { 'id'=>sal_id,'text'=>sal_text,'score'=>sal_scr,'source'=>'salience' } if sal_text.present?
        items.concat(chunks)

        target_total  = (input['max_prompt_tokens'].presence || 3000).to_i
        reserve_out   = (input['reserve_output_tokens'].presence || 512).to_i
        budget_prompt = [target_total - reserve_out, 400].max
        model_for_cnt = (input['count_tokens_model'].presence || input['model']).to_s
        strategy      = (input['trim_strategy'].presence || 'drop_low_score').to_s

        base = []; base << items.shift if items.first && items.first['source']=='salience'
        items = items.map { |c| c.merge('text'=>call(:truncate_chunk_text, c['text'], 800)) }
        ordered = case strategy
                  when 'diverse_mmr'   then call(:mmr_diverse_order, items.sort_by { |c| [-(c['score']||0.0).to_f, c['id'].to_s] }, alpha: 0.7, per_source_cap: 3)
                  when 'drop_low_score' then items.sort_by { |c| [-(c['score']||0.0).to_f, c['id'].to_s] }
                  else items
                  end
        ordered = call(:drop_near_duplicates, ordered, 0.9)
        pool    = base + ordered

        sys_text = input['system_preamble'].presence ||
          'Answer using ONLY the provided context chunks. If the context is insufficient, reply with “I don’t know.” Keep answers concise and cite chunk IDs.'
        kept    = call(:select_prefix_by_budget, connection, pool, q, sys_text, budget_prompt, model_for_cnt)
        blob    = call(:format_context_chunks, kept)
        gen_cfg = {
          'temperature'      => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
          'maxOutputTokens'  => reserve_out,
          'responseMimeType' => 'application/json',
          'responseSchema'   => {
            'type'=>'object','additionalProperties'=>false,
            'properties'=>{
              'answer'=>{'type'=>'string'},
              'citations'=>{'type'=>'array','items'=>{'type'=>'object','additionalProperties'=>false,
                'properties'=>{'chunk_id'=>{'type'=>'string'},'source'=>{'type'=>'string'},'uri'=>{'type'=>'string'},'score'=>{'type'=>'number'}}}}
            },
            'required'=>['answer']
          }
        }
        sys_inst = call(:system_instruction_from_text, sys_text)
        contents = [{ 'role'=>'user','parts'=>[{'text'=>"Question:\n#{q}\n\nContext:\n#{blob}"}]}]
      else
        error("Unknown mode: #{mode}")
      end

      payload = {
        'contents'          => contents,
        'systemInstruction' => sys_inst,
        'tools'             => tools,
        'toolConfig'        => tool_cfg,
        'safetySettings'    => safety,
        'generationConfig'  => gen_cfg
      }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

      resp = post(url).headers(call(:request_headers_auth, connection, corr, connection['user_project'], req_params))
                      .payload(call(:json_compact, payload))
      code = call(:telemetry_success_code, resp)

      out  = resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
      if mode == 'rag_with_context'
        text   = resp.dig('candidates',0,'content','parts',0,'text').to_s
        parsed = call(:safe_parse_json, text)
        out['parsed'] = { 'answer'=>parsed['answer'] || text, 'citations'=>parsed['citations'] || [] }
        # Compute overall confidence from cited chunk scores, if available
        conf = call(:overall_confidence_from_citations, out['parsed']['citations'])
        out['confidence'] = conf if conf
        (out['telemetry'] ||= {})['confidence'] = { 'basis' => 'citations_topk_avg', 'k' => 3,
                                                    'n' => Array(out.dig('parsed','citations')).length }
      end
      out
    rescue => e
      g   = call(:extract_google_error, e)
      msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
      env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg)
      if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
        env['debug'] = call(:debug_pack, true, url, payload, g)
      end
      error(env)
    end,
    json_parse_safe: lambda do |body|
      call(:safe_json, body) || {}
    end,


    # --- HTTP -------------------------------------------------------------
    http_call!: lambda do |verb, url|
      v = verb.to_s.upcase
      case v
      when 'GET'    then get(url)
      when 'POST'   then post(url)
      when 'PUT'    then put(url)
      when 'PATCH'  then patch(url)
      when 'DELETE' then delete(url)
      else error("Unsupported HTTP verb: #{verb}")
      end
    end,
    http_body_json: lambda do |resp|
      # Accepts Workato HTTP response object OR already-parsed Hash/Array/String.
      # Normalizes to a Hash (or {}).
      if resp.is_a?(Hash) && resp.key?('body')
        parsed = call(:safe_json, resp['body'])
        parsed.nil? ? {} : parsed
      elsif resp.is_a?(String)
        call(:safe_json, resp) || {}
      elsif resp.is_a?(Hash) || resp.is_a?(Array)
        resp
      else
        {}
      end
    end,
    request_preview_pack: lambda do |url, verb, headers, payload|
      {
        'request_preview' => {
          'method'  => verb.to_s.upcase,
          'url'     => url.to_s,
          'headers' => headers || {},
          'payload' => call(:redact_json, payload)
        }
      }
    end,
    # Stabilized output shaper for neighbors
    shape_neighbors: lambda do |resp|
      body = call(:http_body_json, resp)
      list = Array(body['nearestNeighbors']).map do |nn|
        neighbors = Array(nn['neighbors']).map do |n|
          {
            'datapoint' => n['datapoint'],
            'distance'  => n['distance'].to_f.round(6),
            'crowdingTagCount' => n['crowdingTagCount'].to_i
          }
        end
        # Deterministic order: by distance ASC, then datapointId
        { 'neighbors' => neighbors.sort_by { |x|
            [x['distance'], x.dig('datapoint','datapointId').to_s]
          }
        }
      end
      { 'nearestNeighbors' => list }
    end,

    # ---------- Rerank helpers (tidy + reusable) --------------------------
    rerank_enrich_records: lambda do |input_records, ranked_min|
      # Build index of caller-provided records by id
      idx = {}
      call(:safe_array, input_records).each do |r|
        next unless r
        id  = (r['id'] || r[:id]).to_s
        next if id.empty?
        idx[id] = {
          'content'  => (r['content']  || r[:content]).to_s,
          'metadata' => (r['metadata'].is_a?(Hash) ? r['metadata'] : nil)
        }
      end
      # Merge preserving rank order
      call(:safe_array, ranked_min).map do |r|
        src = idx[r['id']]
        r.merge('content' => (src && src['content']),
                'metadata' => (src && src['metadata']))
      end
    end,
    derive_source_uri: lambda do |md, source_key='source', uri_key='uri'|
      m = md.is_a?(Hash) ? md : {}
      source = m[source_key] || m['source'] || m['displayName']
      uri = m[uri_key]
      %w[sourceUri gcsUri url uri].each { |k| uri ||= m[k] }
      { 'source' => source, 'uri' => uri }
    end,
    context_chunks_from_enriched: lambda do |enriched_records, source_key='source', uri_key='uri'|
      call(:safe_array, enriched_records).map do |r|
        md = r['metadata'] || {}
        der = call(:derive_source_uri, md, source_key, uri_key)
        {
          'id'            => r['id'],
          'text'          => r['content'].to_s,
          'score'         => r['score'],
          'source'        => der['source'],
          'uri'           => der['uri'],
          'metadata'      => md,
          'metadata_kv'   => md.map { |k,v| { 'key' => k.to_s, 'value' => v } },
          'metadata_json' => (md.nil? || md.empty?) ? nil : md.to_json
        }
      end
    end,

    # --- Telemetry and resilience -----------------------------------------
    telemetry_envelope: lambda do |started_at, correlation_id, ok, code, message|
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
    end,
    telemetry_envelope_ex: lambda do |started_at, correlation_id, ok, code, message, extras={}|
      # Envelope with optional extras folded into telemetry (non-breaking)
      env = call(:telemetry_envelope, started_at, correlation_id, ok, code, message)
      if extras.is_a?(Hash) && !extras.empty?
        env['telemetry'] = (env['telemetry'] || {}).merge(extras)
      end
      env
    end,
    telemetry_success_code: lambda do |resp|
      # Works with Workato HTTP response (has status/status_code) or defaults to 200
      (resp.is_a?(Hash) && (resp['status'] || resp['status_code'])) ? (resp['status'] || resp['status_code']).to_i : 200
    end,
    telemetry_parse_error_code: lambda do |err|
      # Prefer Workato HTTP error objects first
      begin
        if err.respond_to?(:[])

          code = err['status'] || err.dig('response', 'status') ||
                 err.dig('response', 'status_code') || err.dig('error', 'code')
          return code.to_i if code
        end
      rescue; end
      # Try JSON body error.code
      begin
        body = (err.respond_to?(:[]) && err.dig('response','body')).to_s
        j = JSON.parse(body) rescue nil
        c = j && j.dig('error','code')
        return c.to_i if c
      rescue; end
      m = err.to_s.match(/\b(\d{3})\b/)
      m ? m[1].to_i : 500
    end,
    build_correlation_id: lambda do
      SecureRandom.uuid
    end,
    extract_google_error: lambda do |err|
      begin
        body = (err.respond_to?(:[]) && err.dig('response','body')).to_s
        json = JSON.parse(body) rescue nil
        if json
          # google.rpc.Status shape
          if json['error']
            det   = json['error']['details'] || []
            bad   = det.find { |d| (d['@type'] || '').end_with?('google.rpc.BadRequest') } || {}
            vlist = (bad['fieldViolations'] || bad['violations'] || []).map do |v|
              {
                'field'  => v['field'] || v['fieldPath'] || v['subject'],
                'reason' => v['description'] || v['message'] || v['reason']
              }.compact
            end.reject(&:empty?)
            return {
              'code'       => json['error']['code'],
              'message'    => json['error']['message'],
              'details'    => json['error']['details'],
              'violations' => vlist,
              'raw'        => json
            }
          end
          # some endpoints return {message:"..."} at top level
          return { 'message' => json['message'], 'raw' => json } if json['message']
        end
      rescue
      end
      {}
    end,
    deep_dup: lambda do |o|
      case o
      when Hash  then o.each_with_object({}) { |(k,v),h| h[k] = call(:deep_dup, v) }
      when Array then o.map { |e| call(:deep_dup, e) }
      else o
      end
    end,
    redact_json: lambda do |obj|
      # Shallow redaction of obvious secrets in request bodies; extend as needed
      begin
        j = obj.is_a?(String) ? JSON.parse(obj) : call(:deep_dup, obj)
      rescue
        return obj
      end
      if j.is_a?(Hash)
        %w[access_token authorization api_key apiKey bearer token id_token refresh_token client_secret private_key].each do |k|
          j[k] = '[REDACTED]' if j.key?(k)
        end
      end
      j
    end,
    debug_pack: lambda do |enabled, url, body, google_error|
      return nil unless enabled
      {
        'request_url'  => url.to_s,
        'request_body' => call(:redact_json, body),
        'error_body'   => (google_error && google_error['raw']) || google_error
      }
    end,
    after_response_shared: lambda do |code, body, headers, message|
      parsed = call(:json_parse_safe, body)

      meta = (parsed['_meta'] ||= {})
      meta['http_status']    = code
      meta['request_id']     = headers['X-Request-Id'] || headers['x-request-id']
      meta['retry_after']    = headers['Retry-After']
      meta['rate_limit_rem'] = headers['X-RateLimit-Remaining']
      meta['etag']           = headers['ETag']
      meta['last_modified']  = headers['Last-Modified']
      meta['model_version']  = headers['x-goog-model-id'] || headers['X-Model-Version']

      meta['next_page_token'] ||= parsed['nextPageToken'] || parsed['next_page_token']

      if parsed['error'].is_a?(Hash)
        e = parsed['error']
        error({
          'code'     => e['code'] || code,
          'status'   => e['status'],
          'message'  => e['message'] || message || 'Request failed',
          'details'  => e['details']
        })
      else
        parsed
      end
    end,
    after_error_response_shared: lambda do |code, body, headers, message|
      json = call(:json_parse_safe, body)
      normalized = { 'code' => code }

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

      error(normalized) # re-raise so Workato’s retry/metrics kick in
    end,

    # --- Auth (JWT → OAuth) -----------------------------------------------
    const_default_scopes: lambda { ['https://www.googleapis.com/auth/cloud-platform'].freeze },
    b64url: lambda do |bytes|
      Base64.urlsafe_encode64(bytes).gsub(/=+$/, '')
    end,
    jwt_sign_rs256: lambda do |claims, private_key_pem|
      header = { alg: 'RS256', typ: 'JWT' }
      enc_h  = call(:b64url, header.to_json)
      enc_p  = call(:b64url, claims.to_json)
      input  = "#{enc_h}.#{enc_p}"
      rsa = OpenSSL::PKey::RSA.new(private_key_pem.to_s)
      sig = rsa.sign(OpenSSL::Digest::SHA256.new, input)
      "#{input}.#{call(:b64url, sig)}"
    end,
    auth_normalize_scopes: lambda do |scopes|
      arr = case scopes
            when nil    then ['https://www.googleapis.com/auth/cloud-platform']
            when String then scopes.split(/\s+/)
            when Array  then scopes
            else              ['https://www.googleapis.com/auth/cloud-platform']
            end
      arr.map(&:to_s).reject(&:empty?).uniq
    end,
    auth_token_cache_get: lambda do |connection, scope_key|
      cache = (connection['__token_cache'] ||= {})
      tok   = cache[scope_key]
      return nil unless tok.is_a?(Hash) && tok['access_token'].present? && tok['expires_at'].present?
      exp = Time.parse(tok['expires_at']) rescue nil
      return nil unless exp && Time.now < (exp - 60)
      tok
    end,
    auth_token_cache_put: lambda do |connection, scope_key, token_hash|
      cache = (connection['__token_cache'] ||= {})
      cache[scope_key] = token_hash
      token_hash
    end,
    auth_issue_token!: lambda do |connection, scopes|
      # Safely parse sa key json
      key = JSON.parse(connection['service_account_key_json'].to_s)
      # Guard for sa key exists
      error('Invalid service account key: missing client_email') if key['client_email'].to_s.strip.empty?
      error('Invalid service account key: missing private_key') if key['private_key'].to_s.strip.empty?
      # Normalize pk newlines to satisfy OpenSSL
      pk = key['private_key'].to_s.gsub(/\\n/, "\n")
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

      # Build assertion
      assertion = call(:jwt_sign_rs256, payload, pk)

      # POST to ep
      res = post(token_url)
              .payload(grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: assertion)
              .request_format_www_form_urlencoded

      # Parse response
      {
        'access_token' => res['access_token'],
        'token_type'   => res['token_type'],
        'expires_in'   => res['expires_in'],
        'expires_at'   => (Time.now + res['expires_in'].to_i).utc.iso8601,
        'scope_key'    => scope_str
      }
    end,
    auth_build_access_token!: lambda do |connection, scopes: nil|
      set = call(:auth_normalize_scopes, scopes)
      scope_key = set.join(' ')
      if (cached = call(:auth_token_cache_get, connection, scope_key))
        return cached['access_token']
      end
      fresh = call(:auth_issue_token!, connection, set)
      call(:auth_token_cache_put, connection, scope_key, fresh)['access_token']
    end,
    # --- Purpose-specific header helpers ---------------------------------
    headers_rag: lambda do |connection, correlation_id, request_params=nil|
      # Retrieval calls: NEVER send x-goog-user-project
      call(:request_headers_auth, connection, correlation_id, nil, request_params)
    end,
    headers_logging: lambda do |connection, correlation_id, request_params=nil|
      # Logging calls: include x-goog-user-project when configured
      call(:request_headers_auth, connection, correlation_id, connection['user_project'], request_params)
    end,
    request_headers_auth: lambda do |connection, correlation_id, user_project=nil, request_params=nil|
      # Ensure we always carry a valid Bearer token (don’t rely solely on authorization.apply)
      token = connection['access_token'].to_s
      if token.empty?
        begin
          scopes = call(:const_default_scopes)
          token  = call(:auth_build_access_token!, connection, scopes: scopes)
          connection['access_token'] = token
        rescue
          # If token minting fails, still build headers; caller will raise and be logged as error.
          token = ''
        end
      end
      h = {
        'X-Correlation-Id' => correlation_id.to_s,
        'Content-Type'     => 'application/json',
        'Accept'           => 'application/json'
      }
      h['Authorization'] = "Bearer #{auth}" unless auth.empty?
      up = user_project.to_s.strip
      h['x-goog-user-project']   = up unless up.empty?
      rp = request_params.to_s.strip
      h['x-goog-request-params'] = rp unless rp.empty?
      h
    end,
    request_headers: lambda do |correlation_id, extra=nil|
      base = {
        'X-Correlation-Id' => correlation_id.to_s,
        'Content-Type'     => 'application/json',
        'Accept'           => 'application/json'
      }
      extra.is_a?(Hash) ? base.merge(extra) : base
    end,
    # --- URL and resource building ----------------------------------------
    # Map a Vertex "region" (e.g., us-central1, europe-west1) to AI-Apps multi-region (global|us|eu)
    region_to_aiapps_loc: lambda do |raw|
      v = raw.to_s.strip.downcase
      return 'global' if v.empty? || v == 'global'
      return 'us' if v.start_with?('us-')
      return 'eu' if v.start_with?('eu-') || v.start_with?('europe-')
      # If user typed a multi-region already, pass through:
      return v if %w[us eu global].include?(v)
      # Safe fallback: prefer global (Ranking supports global/us/eu only)
      'global'
    end,

    # Resolve AI-Apps (Discovery/Ranking) location with optional override
    aiapps_loc_resolve: lambda do |connection, override=nil|
      o = override.to_s.strip.downcase
      return o if %w[us eu global].include?(o)
      call(:region_to_aiapps_loc, connection['location'])
    end,
    aipl_service_host: lambda do |connection, loc=nil|
      l = (loc || connection['location']).to_s.downcase
      (l.blank? || l == 'global') ? 'aiplatform.googleapis.com' : "#{l}-aiplatform.googleapis.com"
    end,
    aipl_v1_url: lambda do |connection, loc, path|
      "https://#{call(:aipl_service_host, connection, loc)}/v1/#{path}"
    end,
    aipl_v1alpha_url: lambda do |connection, loc, path|
      "https://#{call(:aipl_service_host, connection, loc)}/v1alpha/#{path}"
    end,
    endpoint_predict_url: lambda do |connection, endpoint|
      ep = call(:normalize_endpoint_identifier, endpoint).to_s
      # Allow fully-qualified dedicated endpoint URLs.
      return (ep.include?(':predict') ? ep : "#{ep}:predict") if ep.start_with?('http')

      # Prefer region from the resource name; fallback to connection.
      m   = ep.match(%r{^projects/[^/]+/locations/([^/]+)/endpoints/})
      loc = (m && m[1]) || (connection['location'] || '').to_s.downcase
      error("This action requires a regional location. Current location is '#{loc}'.") if loc.blank? || loc == 'global'

      host = call(:aipl_service_host, connection, loc)
      "https://#{host}/v1/#{call(:build_endpoint_path, connection, ep)}:predict"
    end,
    build_endpoint_path: lambda do |connection, endpoint|
      ep = call(:normalize_endpoint_identifier, endpoint)
      ep = ep.sub(%r{^/v1/}, '') # defensive
      ep.start_with?('projects/') ? ep :
        "projects/#{connection['project_id']}/locations/#{connection['location']}/endpoints/#{ep}"
    end,
    build_model_path_with_global_preview: lambda do |connection, model|
      # Enforce project location (inference from connection)
      call(:ensure_project_id!, connection)

      # Normalize model identifier
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
    end,
    build_embedding_model_path: lambda do |connection, model|
      # Enforce project location (inference from connection)
      call(:ensure_project_id!, connection)

      # Normalize model identifier
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
    end,
    build_rag_corpus_path: lambda do |connection, corpus|
      v = corpus.to_s.strip
      return '' if v.empty?
      return v.sub(%r{^/v1/}, '') if v.start_with?('projects/')
      call(:ensure_project_id!, connection)
      loc = (connection['location'] || '').to_s.downcase
      error("RAG corpus requires regional location; got '#{loc}'") if loc.blank? || loc == 'global'
      "projects/#{connection['project_id']}/locations/#{loc}/ragCorpora/#{v}"
    end,
    build_rag_file_path: lambda do |connection, rag_file|
      v = rag_file.to_s.strip
      return '' if v.empty?
      return v.sub(%r{^/v1/}, '') if v.start_with?('projects/')
      # allow short form: {corpus_id}/ragFiles/{file_id}
      if v.start_with?('ragCorpora/')
        call(:ensure_project_id!, connection)
        loc = (connection['location'] || '').to_s.downcase
        error("RAG file requires regional location; got '#{loc}'") if loc.blank? || loc == 'global'
        return "projects/#{connection['project_id']}/locations/#{loc}/#{v}"
      end
      # otherwise expect full name
      error('rag_file must be a full resource name like projects/{p}/locations/{l}/ragCorpora/{c}/ragFiles/{id}')
    end,
    discovery_host: lambda do |connection, loc=nil|
      # Accept regional input and normalize to multi-region for host selection
      l = call(:aiapps_loc_resolve, connection, loc)
      # AI Applications commonly use 'global' or 'us' multi-region
      host = (l == 'us') ? 'us-discoveryengine.googleapis.com' : 'discoveryengine.googleapis.com'
      (connection['discovery_host_custom'].presence || host)
    end,
    discovery_url: lambda do |connection, loc, path, version=nil, host_override=nil|
      ver  = (version.presence || connection['discovery_api_version'].presence || 'v1alpha').to_s
      host = host_override.presence || call(:discovery_host, connection, loc)
      "https://#{host}/#{ver}/#{path.sub(%r{^/}, '')}"
    end,

    # --- Guards, normalization --------------------------------------------
    ensure_project_id!: lambda do |connection|
      # Method mutates caller-visible state, but this is a known and desired side effect. 
      pid = (connection['project_id'].presence ||
              (JSON.parse(connection['service_account_key_json'].to_s)['project_id'] rescue nil)).to_s
      error('Project ID is required (not found in connection or key)') if pid.blank?
      connection['project_id'] = pid
      pid
    end,
    ensure_regional_location!: lambda do |connection|
      loc = (connection['location'] || '').downcase
      error("This action requires a regional location (e.g., us-central1). Current location is '#{loc}'.") if loc.blank? || loc == 'global'
    end,
    embedding_region: lambda do |connection|
      loc = (connection['location'] || '').to_s.downcase
      loc.present? ? loc : 'global'
    end,
    normalize_endpoint_identifier: lambda do |raw|
      return '' if raw.nil? || raw == true || raw == false
      if raw.is_a?(Hash)
        v = raw['value'] || raw[:value] || raw['id'] || raw[:id] || raw['path'] || raw[:path] || raw['name'] || raw[:name] || raw.to_s
        return v.to_s.strip
      end; raw.to_s.strip
    end,
    normalize_model_identifier: lambda do |raw|
      # Normalize a user-provided "model" into a String.
      # Accepts String or Hash (from datapills/pick lists). Prefers common keys.
      return '' if raw.nil? || raw == true || raw == false
      if raw.is_a?(Hash)
        v = raw['value'] || raw[:value] || raw['id'] || raw[:id] || raw['path'] || raw[:path] || raw['name'] || raw[:name] || raw.to_s
        return v.to_s.strip
      end; raw.to_s.strip
    end,
    normalize_boolean: lambda do |v|
      %w[true 1 yes on].include?(v.to_s.strip.downcase)
    end,
    normalize_input_keys: lambda do |input|
      (input || {}).to_h.transform_keys(&:to_s)
    end,
    normalize_drive_resource_id: lambda do |raw|
      # Accepts:
      #   - raw ID strings: "1AbC_def-123"
      #   - datapill Hashes: {id:"..."}, {fileId:"..."}, {value:"..."}, etc.
      #   - full URLs: https://drive.google.com/file/d/<id>/..., ?id=<id>, /folders/<id>
      # Returns bare ID: /[A-Za-z0-9_-]+/
      return '' if raw.nil? || raw == false
      v =
        if raw.is_a?(Hash)
          raw['id'] || raw[:id] ||
          raw['fileId'] || raw[:fileId] ||
          raw['value'] || raw[:value] ||
          raw['name'] || raw[:name] ||
          raw['path'] || raw[:path] ||
          raw.to_s     # last resort (but avoid using this path)
        else
          raw
        end.to_s.strip
      # Common placeholders → empty
      return '' if v.empty? || %w[null nil none undefined - (blank)].include?(v.downcase)

      # Strip common Drive URL patterns
      if v.start_with?('http://', 'https://')
        # /file/d/<id>/...   or   /folders/<id>
        if (m = v.match(%r{/file/d/([^/?#]+)}))      then v = m[1]
        elsif (m = v.match(%r{/folders/([^/?#]+)}))  then v = m[1]
        elsif (m = v.match(/[?&]id=([^&#]+)/))       then v = m[1]
        end
        # Drop resourcekey etc.
      end

      # As a final guard, collapse any accidental Hash#to_s artifacts: {"id"=>"..."}
      if v.include?('=>') || v.include?('{') || v.include?('}')
        # Try to salvage with a simple JSON parse if it looks like a hash string
        begin
          j = JSON.parse(v) rescue nil
          if j.is_a?(Hash)
            v = j['id'] || j['fileId'] || j['value'] || ''
          end
        rescue; end
      end

      # Keep only legal Drive ID charset
      prior = v.dup
      v = v[/[A-Za-z0-9_-]+/].to_s
      # If a link was provided but no usable token, treat as empty (forces upstream “invalid” logic)
      v = '' if v.length < 8 && prior.start_with?('http')
      v
    end,
    normalize_drive_file_id:   lambda { |raw| call(:normalize_drive_resource_id, raw) },
    normalize_drive_folder_id: lambda { |raw| call(:normalize_drive_resource_id, raw) },
    normalize_retrieve_contexts!: lambda do |raw_resp|
      # Accept both shapes:
      #   { "contexts": [ {...}, {...} ] }
      #   { "contexts": { "contexts": [ ... ] } }  # some beta responses
      arr = raw_resp['contexts']
      arr = arr['contexts'] if arr.is_a?(Hash) && arr.key?('contexts')
      Array(arr)
    end,
    normalize_index_endpoint_identifier: lambda do |raw|
      return '' if raw.nil? || raw == false
      raw.to_s.strip
    end,
    normalize_index_identifier: lambda do |raw|
      return '' if raw.nil? || raw == false
      raw.to_s.strip
    end,
    guard_threshold_union!: lambda do |dist, sim|
      return true if dist.nil? || dist.to_s == ''
      return true if sim.nil?  || sim.to_s  == ''
      error('Provide only one of vector_distance_threshold OR vector_similarity_threshold')
    end,
    guard_ranker_union!: lambda do |rank_service_model, llm_ranker_model|
      r = rank_service_model.to_s.strip
      l = llm_ranker_model.to_s.strip
      return true if r.empty? || l.empty?
      error('Provide only one of rank_service_model OR llm_ranker_model')
    end,
    build_rag_retrieve_payload: lambda do |question, rag_corpus, restrict_ids = [], opts = {}|
      # Backward-compatible extension: optional opts hash supports:
      #   'topK'                         -> Integer
      #   'vectorDistanceThreshold'      -> Float (mutually exclusive with similarity)
      #   'vectorSimilarityThreshold'    -> Float (mutually exclusive with distance)
      #   'rankServiceModel'             -> String (semantic ranker)
      #   'llmRankerModel'               -> String (Gemini ranker)

      rag_res = { 'ragCorpus' => rag_corpus }
      ids     = call(:sanitize_drive_ids, restrict_ids, allow_empty: true, label: 'restrict_to_file_ids')
      rag_res['ragFileIds'] = ids if ids.present?

      query = { 'text' => question.to_s }
      # Attach ragRetrievalConfig only when needed (keeps payload minimal)
      rr_cfg = {}
      if opts.is_a?(Hash)
        if opts['topK']
          rr_cfg['topK'] = call(:clamp_int, (opts['topK'] || 0), 1, 200)
        end
        # Filter union
        dist = opts['vectorDistanceThreshold']
        sim  = opts['vectorSimilarityThreshold']
        call(:guard_threshold_union!, dist, sim)
        filt = {}
        filt['vectorDistanceThreshold']   = call(:safe_float, dist) if !dist.nil?
        filt['vectorSimilarityThreshold'] = call(:safe_float, sim)  if !sim.nil?
        rr_cfg['filter'] = filt unless filt.empty?

        # Ranking union
        rsm = (opts['rankServiceModel'].to_s.strip)
        llm = (opts['llmRankerModel'].to_s.strip)
        call(:guard_ranker_union!, rsm, llm)
        if rsm != ''
          rr_cfg['ranking'] = { 'rankService' => { 'modelName' => rsm } }
        elsif llm != ''
          rr_cfg['ranking'] = { 'llmRanker'   => { 'modelName' => llm } }
        end
      end
      query['ragRetrievalConfig'] = rr_cfg unless rr_cfg.empty?

      {
        'query'          => query,
        # union member supplied at top-level (not wrapped in "dataSource")
        'vertexRagStore' => { 'ragResources'  => [rag_res] }
      }
    end,
    map_context_chunks: lambda do |raw_contexts, maxn = 20|
      call(:safe_array, raw_contexts).first(maxn).each_with_index.map do |c, i|
        md = (c['metadata'] || {}).to_h
        {
          'id'       => (c['chunkId'] || "ctx-#{i+1}"),
          'text'     => c['text'].to_s,
          'score'    => (c['score'] || c['relevanceScore'] || 0.0).to_f,
          'source'   => (c['sourceDisplayName'] || c.dig('metadata','source')),
          'uri'      => (c['sourceUri']        || c.dig('metadata','uri')),
          'metadata' => md,
          'metadata_kv' => md.map { |k,v| { 'key' => k.to_s, 'value' => v } },
          'metadata_json' => (md.empty? ? nil : md.to_json)

        }
      end
    end,
    build_rag_import_payload!: lambda do |input|
      # Validates XOR and constructs { importRagFilesConfig: { ... } }
      has_gcs   = call(:safe_array, input['gcs_uris']).present?
      has_drive = input['drive_folder_id'].present? || call(:safe_array, input['drive_file_ids']).present?
      error('Provide exactly one source family: GCS or Drive') if (has_gcs && has_drive) || (!has_gcs && !has_drive)

      cfg = {}
      if has_gcs
        uris = call(:safe_array, input['gcs_uris']).map(&:to_s)
        bad  = uris.find { |u| u.include?('*') }
        error("gcs_uris does not support wildcards; got: #{bad}") if bad
        cfg['gcsSource'] = { 'uris' => uris }
      else
        res_ids = []
        if input['drive_folder_id'].present?
          folder_id = call(:normalize_drive_folder_id, input['drive_folder_id'])
          error('drive_folder_id is not a valid Drive ID') if folder_id.blank?
          res_ids << { 'resourceId' => folder_id, 'resourceType' => 'RESOURCE_TYPE_FOLDER' }
        end
        drive_ids = call(:sanitize_drive_ids, input['drive_file_ids'], allow_empty: true, label: 'drive_file_ids')
        drive_ids.each do |fid|
          res_ids << { 'resourceId' => fid, 'resourceType' => 'RESOURCE_TYPE_FILE' }
        end
        error('Provide drive_folder_id or non-empty drive_file_ids (share with the Vertex RAG Data Service Agent)') if res_ids.empty?
        cfg['googleDriveSource'] = { 'resourceIds' => res_ids }
      end

      # Optional knobs
      if input.key?('maxEmbeddingRequestsPerMin')
        cfg['maxEmbeddingRequestsPerMin'] = call(:safe_integer, input['maxEmbeddingRequestsPerMin'])
      end
      cfg['rebuildAnnIndex'] = true if input['rebuildAnnIndex'] == true
      sink = input['importResultGcsSink']
      if sink.is_a?(Hash) && sink['outputUriPrefix'].present?
        cfg['importResultGcsSink'] = { 'outputUriPrefix' => sink['outputUriPrefix'].to_s }
      end

      { 'importRagFilesConfig' => cfg }
    end,
    build_index_path: lambda do |connection, index|
      id = call(:normalize_index_identifier, index)
      return id.sub(%r{^/v1/}, '') if id.start_with?('projects/')
      call(:ensure_project_id!, connection)
      loc = (connection['location'] || '').to_s.downcase
      error("Index requires regional location; got '#{loc}'") if loc.blank? || loc == 'global'
      "projects/#{connection['project_id']}/locations/#{loc}/indexes/#{id}"
    end,
    build_index_endpoint_path: lambda do |connection, ep|
      v = call(:normalize_index_endpoint_identifier, ep)
      return v.sub(%r{^/v1/}, '') if v.start_with?('projects/')
      call(:ensure_project_id!, connection)
      loc = (connection['location'] || '').to_s.downcase
      error("IndexEndpoint requires regional location; got '#{loc}'") if loc.blank? || loc == 'global'
      "projects/#{connection['project_id']}/locations/#{loc}/indexEndpoints/#{v}"
    end,

    # --- Sanitizers and conversion ----------------------------------------
    safe_array: lambda do |v|
      return [] if v.nil? || v == false
      return v  if v.is_a?(Array)
      [v]
    end,
    safe_json: lambda do |body|
      begin
        case body
        when String then JSON.parse(body)
        when Hash, Array then body # already JSON-like
        else JSON.parse(body.to_s)
        end
      rescue
        nil
      end
    end,
    safe_integer: lambda do |v|
      return nil if v.nil?; Integer(v) rescue v.to_i
    end,
    safe_float: lambda do |v|
      return nil if v.nil?; Float(v)   rescue v.to_f
    end,
    clamp_int: lambda do |n, min, max|
      [[n.to_i, min].max, max].min
    end,
    sanitize_embedding_params: lambda do |raw|
      h = {}
      # Only include autoTruncate when explicitly true (keeps payload minimal)
      h['autoTruncate'] = true if raw['autoTruncate'] == true

      if raw['outputDimensionality'].present?
        od = call(:safe_integer, raw['outputDimensionality'])
        error('outputDimensionality must be a positive integer') if od && od < 1
        h['outputDimensionality'] = od if od
      end
      h
    end,
    sanitize_generation_config: lambda do |cfg|
      return nil if cfg.nil? || (cfg.respond_to?(:empty?) && cfg.empty?)
      g = cfg.dup
      # String
      g['responseMimeType'] = g['responseMimeType'].to_s if g.key?('responseMimeType')
      # Floats
      g['temperature']     = call(:safe_float,  g['temperature'])     if g.key?('temperature')
      g['topP']            = call(:safe_float,  g['topP'])            if g.key?('topP')
      # Integers
      g['topK']            = call(:safe_integer,g['topK'])            if g.key?('topK')
      g['maxOutputTokens'] = call(:safe_integer,g['maxOutputTokens']) if g.key?('maxOutputTokens')
      if g.key?('candidateCount')
        c = call(:safe_integer, g['candidateCount'])
        # Known safe window for Gemini text generation (defensive)
        g['candidateCount'] = (c && c >= 1 && c <= 4) ? c : nil
      end
      # Arrays
      g['stopSequences']   = call(:safe_array, g['stopSequences']).map(&:to_s) if g.key?('stopSequences')
      # Strip
      g.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
      g
    end,
    sanitize_contents_roles: lambda do |contents|
      call(:safe_array, contents).map do |c|
        h = c.is_a?(Hash) ? c.transform_keys(&:to_s) : {}
        role = (h['role'] || 'user').to_s.downcase
        next nil if role == 'system' # we move system text via systemInstruction
        error("Invalid role: #{role}") unless %w[user model].include?(role)
        h['role'] = role
        h
      end.compact
    end,
    sanitize_drive_ids: lambda do |raw_list, allow_empty: false, label: 'drive_file_ids'|
      # 1) normalize → 2) drop empties → 3) de-dup
      norm = call(:safe_array, raw_list)
               .map { |x| call(:normalize_drive_file_id, x) }
               .reject { |x| x.to_s.strip.empty? }
               .uniq
      return [] if norm.empty? && allow_empty
      error("No valid Drive IDs found in #{label}. Remove empty entries or fix links.") if norm.empty?
      # Validate basic length/pattern to catch obvious junk that isn’t empty
      bad = norm.find { |id| id !~ /\A[A-Za-z0-9_-]{8,}\z/ }
      error("Invalid Drive ID in #{label}: #{bad}") if bad
      norm
    end,
    sanitize_feature_vector: lambda do |arr|
      call(:safe_array, arr).map { |x| call(:safe_float, x) }.reject { |x| x.nil? }
    end,

    # --- Embeddings -------------------------------------------------------
    extract_embedding_vector: lambda do |pred|
      # Extracts float vector from embedding prediction (both shapes supported)
      vec = pred.dig('embeddings', 'values') ||
            pred.dig('embeddings', 0, 'values') ||
            pred['values']
      error('Embedding prediction missing values') if vec.blank?
      vec.map(&:to_f)
    end,
    vector_cosine_similarity: lambda do |a, b|
      return 0.0 if a.blank? || b.blank?
      error("Embedding dimensions differ: #{a.length} vs #{b.length}") if a.length != b.length
      dot = 0.0; sum_a = 0.0; sum_b = 0.0
      a.each_index do |i|
        ai = a[i].to_f; bi = b[i].to_f
        dot += ai * bi; sum_a += ai * ai; sum_b += bi * bi
      end
      denom = Math.sqrt(sum_a) * Math.sqrt(sum_b)
      denom.zero? ? 0.0 : (dot / denom)
    end,
    embedding_max_instances: lambda do |model_path_or_id|
      id = model_path_or_id.to_s.split('/').last
      if id.start_with?('gemini-embedding-001')
        1
      else
        250
      end
    end,
    predict_embeddings: lambda do |connection, model_path, instances, params={}, corr=nil|
      max  = call(:embedding_max_instances, model_path)
      preds = []
      billable = 0
      # Derive location from the model path (projects/.../locations/{loc}/...)
      loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
      req_params = "model=#{model_path}"

      (instances || []).each_slice(max) do |slice|
        url  = call(:aipl_v1_url, connection, loc, "#{model_path}:predict")
        resp = post(url)
                .headers(call(:request_headers_auth, connection, (corr || call(:build_correlation_id)), connection['user_project'], req_params))
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
    end,

    # --- Generative -------------------------------------------------------
    system_instruction_from_text: lambda do |text|
      return nil if text.blank?
      { 'role' => 'system', 'parts' => [ { 'text' => text.to_s } ] }
    end,
    format_context_chunks: lambda do |chunks|
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
    end,
    safe_parse_json: lambda do |s|
      JSON.parse(s) rescue { 'answer' => s }
    end,
    llm_referee: lambda do |connection, model, email_text, shortlist_names, all_cats, fallback_category = nil, corr=nil|
      # Minimal, schema-constrained JSON referee using Gemini
      model_path = call(:build_model_path_with_global_preview, connection, model)
      req_params = "model=#{model_path}"

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
      resp = post(url)
               .headers(call(:request_headers_auth, connection, (corr || call(:build_correlation_id)), connection['user_project'], req_params))
               .payload(call(:json_compact, payload))

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
    end,

    # --- RAG helpers ------------------------------------------------------\
    normalize_rag_corpus: lambda do |connection, raw|
      v = raw.to_s.strip
      return '' if v.blank?
      return v if v.start_with?('projects/')
      # Allow short form: just corpus id -> expand using connection project/region
      call(:ensure_project_id!, connection)
      loc = (connection['location'] || '').to_s.downcase
      error("RAG corpus requires regional location; got '#{loc}'") if loc.blank? || loc == 'global'
      "projects/#{connection['project_id']}/locations/#{loc}/ragCorpora/#{v}"
    end,

    # --- Miscellaneous ----------------------------------------------------
    build_email_text: lambda do |subject, body|
      # Build a single email text body for classification
      s = subject.to_s.strip
      b = body.to_s.strip
      parts = []
      parts << "Subject: #{s}" if s.present?
      parts << "Body:\n#{b}"    if b.present?
      parts.join("\n\n")
    end,
    safe_map: lambda do |v|
      # Like Array#map but safe against nil/false/non-arrays.
      call(:safe_array, v).map { |x| yield(x) }
    end,
    json_compact: lambda do |obj|
      # Compact a JSON-able Hash/Array without mutating the caller:
      # - Removes nil
      # - Removes empty arrays/objects/strings
      # - Preserves false/0
      case obj
      when Hash
        obj.each_with_object({}) do |(k, v), h|
          next if v.nil?
          cv = call(:json_compact, v)
          # keep false/0; drop only empty containers/strings
          keep =
            case cv
            when String then !cv.empty?
            when Array  then !cv.empty?
            when Hash   then !cv.empty?
            else true
            end
          h[k] = cv if keep
        end
      when Array
        obj.map { |e| call(:json_compact, e) }.reject do |cv|
          case cv
          when String then cv.empty?
          when Array  then cv.empty?
          when Hash   then cv.empty?
          else false
          end
        end
      else
        obj
      end
    end,
    sanitize_hash: lambda do |v|
      v.is_a?(Hash) ? v : (v.is_a?(String) ? (JSON.parse(v) rescue nil) : nil)
    end,
    safe_obj: lambda do |v|
      call(:sanitize_hash, v)
    end,
    safe_array_of_hashes: lambda do |v|
      arr = call(:safe_array, v)
      arr.map { |x| call(:sanitize_hash, x) }.compact
    end,
    sanitize_tools!: lambda do |raw|
      a = call(:safe_array_of_hashes, raw)
      return nil if a.empty?

      allowed_keys = %w[googleSearch retrieval codeExecution functionDeclarations]
      cleaned = a.map do |t|
        keep = {}
        allowed_keys.each do |k|
          v = t[k]
          keep[k] = v if v.is_a?(Hash) || v.is_a?(Array) || v == {}
        end
        keep.empty? ? nil : keep
      end.compact

      cleaned.empty? ? nil : cleaned
    end,
    extract_candidate_text: lambda do |resp|
      # Returns [text, meta] where meta carries finishReason/safety/promptFeedback
      cands = call(:safe_array, resp['candidates'])
      pf    = resp['promptFeedback']
      return [nil, { 'promptFeedback' => pf }] if cands.empty?

      c0 = cands.first || {}
      parts = call(:safe_array, c0.dig('content','parts'))
      text  = parts.first && parts.first['text']
      meta  = {
        'finishReason'  => c0['finishReason'],
        'safetyRatings' => c0['safetyRatings'],
        'promptFeedback'=> pf
      }.compact
      [text, meta]
    end,

    sanitize_safety!: lambda do |raw|
      a = call(:safe_array_of_hashes, raw).map do |h|
        c = h['category'] || h[:category]
        th = h['threshold'] || h[:threshold]
        (c && th) ? { 'category' => c.to_s, 'threshold' => th.to_s } : nil
      end.compact
      a.empty? ? nil : a
    end,
    sanitize_contents!: lambda do |raw|
      call(:safe_array, raw).map do |c|
        h = c.is_a?(Hash) ? c.transform_keys(&:to_s) : {}
        role = (h['role'] || 'user').to_s.downcase
        next nil if role == 'system' # system handled via systemInstruction
        error("Invalid role: #{role}") unless %w[user model].include?(role)
        parts = call(:safe_array, h['parts']).map do |p|
          ph = p.is_a?(Hash) ? p.transform_keys(&:to_s) : {}
          # keep any part that has at least something meaningful
          ph if ph['text'].to_s != '' || ph['inlineData'].is_a?(Hash) || ph['fileData'].is_a?(Hash) ||
                ph['functionCall'].is_a?(Hash) || ph['functionResponse'].is_a?(Hash) ||
                ph['executableCode'].is_a?(Hash) || ph['codeExecutionResult'].is_a?(Hash)
        end.compact
        next nil if parts.empty?
        { 'role' => role, 'parts' => parts }
      end.compact
    end,

    # Strip HTML → text, collapse whitespace, normalize dashes/quotes a bit
    email_minify: lambda do |subject, body|
      txt = body.to_s.dup

      # crude HTML to text (Workato runtime lacks Nokogiri; keep it simple)
      txt = txt.gsub(/<\/(p|div|br)>/i, "\n")
              .gsub(/<[^>]+>/, ' ')
              .gsub(/\r/, '')
              .gsub(/[ \t]+/, ' ')
              .gsub(/\n{3,}/, "\n\n")
              .strip

      # prepend subject if useful for salience
      if subject.to_s.strip.length > 0
        "Subject: #{subject.to_s.strip}\n\n#{txt}"
      else
        txt
      end
    end,
    # Remove reply chains, signatures, legal footers; keep top-most author content
    email_focus_trim: lambda do |plain_text, max_chars = 8000|
      t = plain_text.to_s

      # Remove common quoted-reply sections
      t = t.split(/\n-{2,}\s*Original Message\s*-{2,}\n/i).first || t
      t = t.split(/\nOn .* wrote:\n/).first       || t
      t = t.split(/\nFrom: .*?\nSent: .*?\nTo:/m).first || t
      t = t.gsub(/(^|\n)>[^\n]*\n?/, "\n")        # strip lines starting with '>'

      # Drop common signature delimiters / legal footers
      t = t.split(/\n--\s*\n/).first || t
      t = t.gsub(/\nThis message .*? confidentiality.*$/mi, '') # rough legal footer killer

      t = t.strip
      # Keep head of the message if extremely long
      t.length > max_chars ? t[0, max_chars] : t
    end,
    extract_salient_span!: lambda do |connection, subject, body, model='gemini-2.0-flash', max_span=500, temperature=0, corr=nil|
      plain = call(:email_minify, subject, body)
      focus = call(:email_focus_trim, plain, 8000)

      system_text = "Extract the single most important sentence or short paragraph from an email. " \
                    "Return valid JSON only. Keep the extracted span under #{max_span} characters. " \
                    "importance is a calibrated score in [0,1]."

      gen_cfg = {
        'temperature'      => temperature.to_f,
        'maxOutputTokens'  => 512,
        'responseMimeType' => 'application/json',
        'responseSchema'   => {
          'type' => 'object', 'additionalProperties' => false,
          'properties' => {
            'salient_span' => { 'type' => 'string' },
            'reason'       => { 'type' => 'string' },
            'importance'   => { 'type' => 'number' }
          },
          'required' => ['salient_span','importance']
        }
      }

      model_path = call(:build_model_path_with_global_preview, connection, model)
      req_params = "model=#{model_path}"
      contents = [
        { 'role' => 'user', 'parts' => [ { 'text' =>
          [
            (subject.to_s.strip.empty? ? nil : "Subject: #{subject.to_s.strip}"),
            "Email (trimmed):\n#{focus}"
          ].compact.join("\n\n")
        } ] }
      ]
      loc = (connection['location'].presence || 'global').to_s.downcase
      url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
      resp = post(url)
               .headers(call(:request_headers_auth, connection, (corr || call(:build_correlation_id)), nil, req_params))
               .payload({
                  'contents' => contents,
                  'systemInstruction' => { 'role' => 'system', 'parts' => [ { 'text' => system_text } ] },
                  'generationConfig'  => gen_cfg
                })

      text   = resp.dig('candidates',0,'content','parts',0,'text').to_s
      parsed = call(:safe_parse_json, text)
      {
        'focus_preview' => focus,
        'salient_span'  => parsed['salient_span'].to_s,
        'reason'        => parsed['reason'],
        'importance'    => parsed['importance'].to_f
      }
    end,
    count_tokens_quick!: lambda do |connection, model_id, contents, system_text=nil, corr=nil|
      # Build path for whichever model we’re counting
      model_path = call(:build_model_path_with_global_preview, connection, model_id)
      loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
      url = call(:aipl_v1_url, connection, loc, "#{model_path}:countTokens")
      req_params = "model=#{model_path}"

      payload = {
        'contents' => contents,
        'systemInstruction' => call(:system_instruction_from_text, system_text)
      }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

      begin
        post(url).headers(call(:request_headers_auth, connection, (corr || call(:build_correlation_id)), connection['user_project'], req_params))
                 .payload(call(:json_compact, payload))

      rescue
        nil
      end
    end,
    text_tokenize_words: lambda do |s|
      s.to_s.downcase.scan(/[a-z0-9]+/)
    end,
    jaccard_overlap: lambda do |a_tokens, b_tokens|
      a = a_tokens; b = b_tokens
      return 0.0 if a.empty? || b.empty?
      ai = a & b
      au = (a | b)
      (ai.length.to_f / au.length.to_f)
    end,
    mmr_diverse_order: lambda do |items, alpha: 0.7, per_source_cap: 3|
      # items: [{ 'id','text','score','source', ... }]
      pool = (items || []).map do |c|
        c = c.dup
        c['_tokens'] = call(:text_tokenize_words, c['text'])
        c
      end
      kept, kept_by_source = [], Hash.new(0)
      while pool.any?
        best = nil
        best_adj = -1.0/0.0
        pool.each do |cand|
          src = cand['source'].to_s
          next if per_source_cap && kept_by_source[src] >= per_source_cap && src != 'salience'
          overlap = 0.0
          kept.each do |k|
            overlap = [overlap, call(:jaccard_overlap, cand['_tokens'], k['_tokens'])].max
          end
          adj = alpha * (cand['score'] || 0.0).to_f - (1.0 - alpha) * overlap
          if adj > best_adj
            best = cand; best_adj = adj
          end
        end
        break unless best
        kept << best
        kept_by_source[best['source'].to_s] += 1
        pool.delete(best)
      end
      kept.each { |c| c.delete('_tokens') }
      kept
    end,
    collapse_ws: lambda { |s| s.to_s.gsub(/\s+/, ' ').strip },
    truncate_chunk_text: lambda do |text, max_chars=800|
      t = call(:collapse_ws, text)
      t.length > max_chars ? t[0, max_chars] : t
    end,
    drop_near_duplicates: lambda do |items, jaccard=0.9|
      kept = []
      items.each do |c|
        toks = call(:text_tokenize_words, c['text'])
        next if kept.any? { |k| call(:jaccard_overlap, toks, k['_toks']) >= jaccard }
        c = c.dup; c['_toks'] = toks; kept << c
      end
      kept.each { |k| k.delete('_toks') }
      kept
    end,
    tokens_fit?: lambda do |connection, model_for_count, question, sys, chunks_blob, budget|
      contents = [{ 'role' => 'user', 'parts' => [{ 'text' => "Question:\n#{question}\n\nContext:\n#{chunks_blob}" }]}]
      cnt = call(:count_tokens_quick!, connection, model_for_count, contents, sys)
      cnt && cnt['totalTokens'].to_i <= budget
    end,
    select_prefix_by_budget: lambda do |connection, ordered_items, question, sys, budget, model_for_count|
      # Exponential ramp to find upper bound, then binary search → O(log n) countTokens calls
      lo = 0
      hi = [1, ordered_items.length].min
      fmt = lambda { |k| call(:format_context_chunks, ordered_items.first(k)) }
      while hi <= ordered_items.length && call(:tokens_fit?, connection, model_for_count, question, sys, fmt.call(hi), budget)
        lo = hi
        hi = [hi * 2, ordered_items.length].min
        break if hi == lo
      end
      return ordered_items.first(lo) if hi == lo
      while lo < hi
        mid = (lo + hi + 1) / 2
        if call(:tokens_fit?, connection, model_for_count, question, sys, fmt.call(mid), budget)
          lo = mid
        else
          hi = mid - 1
        end
      end
      ordered_items.first(lo)
    end

  },

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {},

  # --------- CUSTOM ACTION SUPPORT ----------------------------------------
  custom_action: true,
  custom_action_help: {
    body: "For actions calling host 'aiplatform.googleapis.com/v1', use relative paths. " \
          "For actions calling other endpoints (e.g. discovery engine), provide the absolute URL."
  }
}

# ----------- VERTEX API NOTES ---------------------------------------------
# ENDPOINTS
  # Embed
    # - host=`https://{LOCATION}-aiplatform.googleapis.com`
    # - path=`/v1/projects/{project}/locations/{location}/publishers/google/models/{embeddingModel}:predict`
  # Generate 
    # - host=`https://aiplatform.googleapis.com` 
    # - path=`/v1/{model}:generateContent`
  # Count tokens
    # - host=`https://LOCATION-aiplatform.googleapis.com`
    # - path=`/v1/projects/{project}/locations/{location}/publishers/google/models/{model}:countTokens`
  # Rank 
    # - host=`https://discoveryengine.googleapis.com` 
    # - path=`/v1alpha/{rankingConfig=projects/*/locations/*/rankingConfigs/*}:rank`
# DOCUMENTATION
  # 1. RAG Engine 
    # 1a. (https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/rag-api-v1)
    # 1b. (https://cloud.google.com/vertex-ai/generative-ai/docs/model-reference/rag-output-explained)
    # 1c. (https://cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations/retrieveContexts)
  # 2. Embedding (https://ai.google.dev/gemini-api/docs/embeddings)
  # 3. Ranking (https://docs.cloud.google.com/generative-ai-app-builder/docs/ranking)
  # 4. Count tokens
    # 4a. publisher model  (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/countTokens)
    # 4b. endpoint/tuned model (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.endpoints/countTokens)
# --------------------------------------------------------------------------
