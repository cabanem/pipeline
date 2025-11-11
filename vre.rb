# frozen_string_literal: true
require 'openssl'
require 'base64'
require 'json'
require 'time'
require 'securerandom'


{
  title: 'Vertex RAG Engine',
  subtitle: 'RAG Engine',
  version: '1.0.1',
  description: 'RAG engine via service account (JWT)',
  author: 'Emily Cabaniss',
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
        options: [['v1alpha','v1alpha'], ['v1beta','v1beta'], ['v1','v1']], hint: 'v1alpha for AI Applications; switch to v1beta/v1 if/when you migrate.' },
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
    # Matches the hash produced by methods.local_log_entry
    log_entry: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'ts' },
          { name: 'action' },
          { name: 'started_at' },
          { name: 'ended_at' },
          { name: 'latency_ms', type: 'integer' },
          { name: 'status' },
          { name: 'correlation' },
          { name: 'http_status', type: 'integer' },
          { name: 'message' },
          { name: 'error_class' },
          { name: 'error_msg' },
          # extras is where you already stash compact, action-specific metrics
          { name: 'extras', type: 'object' }
        ]
      end
    },
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
          { name: 'correlation_id', label: 'Correlation ID', optional: true, hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
          # Show 'contents' for plain/grounded modes; hide for rag_with_context
          { name: 'contents',
            type: 'array', of: 'object', properties: object_definitions['content'], optional: true,
            ngIf: 'input.mode != "rag_with_context"' },

          { name: 'system_preamble', label: 'System Instructions', control_type: 'text-area', extends_schema: false,
            optional: true, hint: 'Provide system-level instructions to guide the model\'s behavior (e.g., tone, role, constraints). This becomes the systemInstruction for the model.'},
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
          { name: 'rag_corpus', optional: true, hint: 'projects/{project}/locations/{region}/ragCorpora/{corpus}', ngIf: 'input.mode == "grounded_rag_store"' },
          { name: 'rag_retrieval_config', label: 'Retrieval config', type: 'object',
            properties: object_definitions['rag_retrieval_config'], ngIf: 'input.mode == "grounded_rag_store"', optional: true }
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
    envelope_fields_1: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message', type: 'string' },
            { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id', type: 'string' },
            { name: 'facets', type: 'object' },
            { name: 'local_logs', type: 'array', of: 'object' }
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
    # -- NEW ---
    # Shared envelope for all action outputs (ok, telemetry, optional debug)
    envelope_fields: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
              { name: 'http_status',  type: 'integer' },
              { name: 'message' },
              { name: 'duration_ms',  type: 'integer' },
              { name: 'correlation_id' },
              { name: 'facets',       type: 'object' }
            ]
          },
          { name: 'debug', type: 'object', optional: true }
        ]
      end
    },
    # Tiny reusable input group for observability
    observability_input_fields: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'correlation_id', label: 'Correlation ID', sticky: true, optional: true,
            hint: 'Sticky ID for stitching logs/metrics across steps.' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true,
            hint: 'Adds request/response preview to output in non-prod connections.' }
        ]
      end
    }, 
    email_envelope: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'subject' }, { name: 'body' }, { name: 'from' },
          { name: 'headers', type: 'object' },
          { name: 'attachments', type: 'array', of: 'object', properties: [
              { name: 'filename' }, { name: 'mimeType' }, { name: 'size' }
          ]},
          { name: 'auth', type: 'object' }
        ]
      end
    },
    category_def: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'name', optional: false },
          { name: 'description' },
          { name: 'examples', type: 'array', of: 'string' }
        ]
      end
    },
    rule_rows_table: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'rule_id' }, { name: 'family' }, { name: 'field' }, { name: 'operator' },
          { name: 'pattern' }, { name: 'weight' }, { name: 'action' }, { name: 'cap_per_email' },
          { name: 'category' }, { name: 'flag_a' }, { name: 'flag_b' },
          { name: 'enabled' }, { name: 'priority' }, { name: 'notes' }
        ]
      end
    },
    rulepack_compiled: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'hard_exclude', type: 'object' },
          { name: 'soft_signals', type: 'array', of: 'object' },
          { name: 'thresholds',   type: 'object' },
          { name: 'guards',       type: 'object' }
        ]
      end
    },
    contexts_ranked: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'id' }, { name: 'text' }, { name: 'score', type: 'number' },
          { name: 'source' }, { name: 'uri' }, { name: 'metadata', type: 'object' }
        ]
      end
    },
    context_chunk: {
      fields: lambda do |connection, config_fields|
        [
          { name: 'id' },
          { name: 'uri', optional: true },
          { name: 'content', control_type: 'text-area' },
          { name: 'score', type: :number, optional: true },
          { name: 'metadata', type: :object, optional: true }
        ]
      end,
      sample_output: { id: 'c1', uri: 'gs://bucket/a.txt', content: '...', score: 0.83 },
      additional_properties: false
    },
    intent_out: {
      fields: lambda do |_connection, _config_fields|
        [ { name: 'label' }, { name: 'confidence', type: 'number' }, { name: 'basis' } ]
      end
    },
    policy_out: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'decision' }, { name: 'confidence', type: 'number' },
          { name: 'matched_signals', type: 'array', of: 'string' },
          { name: 'reasons', type: 'array', of: 'string' }
        ]
      end
    },
    referee_out: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'category' }, { name: 'confidence', type: 'number' }, { name: 'reasoning' },
          { name: 'distribution', type: 'array', of: 'object', properties: [
              { name: 'category' }, { name: 'prob', type: 'number' }
          ]}
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
    deterministic_filter: {
      title: 'Filter: Deterministic (with intent)',
      subtitle: 'Hard/soft rules + lightweight intent inference',
      display_priority: 510,
      help: lambda do |_|
        { body: 'Runs hard/soft rules and infers coarse intent from headers/auth/keywords.' }
      end,

      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        call(:ui_df_inputs, object_definitions, config_fields) +
          Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'pre_filter', type: 'object', properties: [
              { name: 'hit', type: 'boolean' }, { name: 'action' }, { name: 'reason' },
              { name: 'score', type: 'integer' }, { name: 'matched_signals', type: 'array', of: 'string' },
              { name: 'decision' }
          ]},
          { name: 'intent', type: 'object', properties: Array(object_definitions['intent_out']) },
          { name: 'email_text' },
          { name: 'email_type' },
          { name: 'gate', type: 'object', properties: [
              { name: 'prelim_pass', type: 'boolean' }, { name: 'hard_block', type: 'boolean' },
              { name: 'hard_reason' }, { name: 'soft_score', type: 'integer' }, { name: 'decision' }, { name: 'generator_hint' }
            ]},
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :deterministic_filter, input)

        env = call(:norm_email_envelope!, (input['email'] || input))
        subj, body, email_text = env['subject'], env['body'], env['email_text']
        #subj = (input['subject'] || '').to_s.strip
        #body = (input['body']    || '').to_s.strip
        #error('Provide subject and/or body') if subj.empty? && body.empty?
        #email_text = call(:build_email_text, subj, body)

        # Build/choose rulepack
        rules =
          if input['rules_mode'] == 'json' && input['rules_json'].present?
            call(:safe_json, input['rules_json'])
          elsif input['rules_mode'] == 'rows' && Array(input['rules_rows']).any?
            call(:hr_compile_rulepack_from_rows!, input['rules_rows'])
          else
            nil
          end

        pre = { 'hit' => false }
        email_type = 'direct_request'
        hard_block = false
        hard_reason = nil
        if rules.is_a?(Hash)
          hard = call(:hr_eval_hard?, {
            'subject'=>subj, 'body'=>body, 'from'=>env['from'],
            'headers'=>env['headers'], 'attachments'=>env['attachments'], 'auth'=>env['auth']
          }, (rules['hard_exclude'] || {}))

          if hard[:hit]
            hard_reason = hard[:reason] # e.g., forwarded_chain | internal_discussion | mailing_list | bounce | safety_block
            # Map hard reasons to a deterministic decision and email_type
            decision_map = {
              'forwarded_chain'     => 'REVIEW',
              'internal_discussion' => 'REVIEW',
              'mailing_list'        => 'IRRELEVANT',
              'bounce'              => 'IRRELEVANT',
              'safety_block'        => 'REVIEW'
            }
            email_type = 'forwarded_chain'     if hard_reason == 'forwarded_chain'
            email_type = 'internal_discussion' if hard_reason == 'internal_discussion'
            hard_block = %w[forwarded_chain internal_discussion safety_block mailing_list bounce].include?(hard_reason)
            dec = (decision_map[hard_reason] || 'IRRELEVANT')
            gate = { 'prelim_pass'=>false, 'hard_block'=>hard_block, 'hard_reason'=>hard_reason, 'soft_score'=>0, 'decision'=>dec, 'generator_hint'=>'blocked' }
            out = {
              'pre_filter' => hard.merge({ 'decision' => dec }),
              'intent'     => nil,
              'email_text' => email_text,
              'email_type' => email_type,
              'gate'       => gate
            }
            # Compute facets
            return call(:step_ok!, ctx, out, 200, 'OK', { 
              'decision_path' => 'hard_exit',
              'final_decision' => dec,
              'hard_rule_triggered' => hard[:reason],
              'rules_evaluated' => true,
              'intent_label' => 'none',
              'email_length' => email_text.length
            })
          end

          soft = call(:hr_eval_soft, {
            'subject'=>subj, 'body'=>body, 'from'=>env['from'],
            'headers'=>env['headers'], 'attachments'=>env['attachments']
          }, (rules['soft_signals'] || []))
          decision = call(:hr_eval_decide, soft[:score], (rules['thresholds'] || {}))
          pre = { 'hit'=>false, 'score'=>soft[:score], 'matched_signals'=>soft[:matched], 'decision'=>decision }
        end

        # Lightweight intent (deterministic)
        headers = (env['headers'] || {}).transform_keys(&:to_s)
        s = "#{subj}\n\n#{body}".downcase
        intent = { 'label' => 'unknown', 'confidence' => 0.0, 'basis' => 'deterministic' }
        if headers['auto-submitted'].to_s.downcase != '' && headers['auto-submitted'].to_s.downcase != 'no'
          intent = { 'label'=>'auto_reply','confidence'=>0.95,'basis'=>'header:auto-submitted' }
        elsif headers.key?('x-autoreply') || s =~ /(out of office|auto.?reply|automatic reply)/
          intent = { 'label'=>'auto_reply','confidence'=>0.9,'basis'=>'header/subject' }
        elsif headers.key?('list-unsubscribe') || headers.key?('list-id') || headers['precedence'].to_s =~ /(bulk|list)/i
          intent = { 'label'=>'marketing','confidence'=>0.8,'basis'=>'list-headers' }
        elsif s =~ /(invoice|receipt|order\s?#|shipment|ticket\s?#)/i
          intent = { 'label'=>'transactional','confidence'=>0.7,'basis'=>'keywords' }
        end

        # Preliminary generator gate (upstream of policy): block if any hard_block; require direct_request; disallow IRRELEVANT
        prelim_pass = (!hard_block) && (email_type == 'direct_request') && (pre['decision'] != 'IRRELEVANT')
        gate = {
          'prelim_pass'    => prelim_pass,
          'hard_block'     => hard_block,
          'hard_reason'    => hard_reason,
          'soft_score'     => pre['score'] || 0,
          'decision'       => pre['decision'],
          'generator_hint' => (prelim_pass ? 'pass' : 'blocked')
        }

        # Standard outputs
        out = {
          'pre_filter' => pre,
          'intent'     => intent,
          'email_text' => email_text,
          'email_type' => email_type,
          'gate'       => gate
        }
        # Compute facets
        call(:step_ok!, ctx, out, 200, 'OK', { 
          'decision_path' => 'soft_eval',
          'final_decision' => pre['decision'],
          'intent_label' => intent['label'],
          'intent_confidence' => intent['confidence'],
          'intent_basis' => intent['basis'],
          'soft_score' => pre['score'] || 0,
          'signals_matched' => (pre['matched_signals'] || []).length,
          'rules_evaluated' => rules.is_a?(Hash),
          'hard_rules_count' => rules.is_a?(Hash) ? (rules['hard_exclude'] || {}).values.flatten.length : 0,
          'soft_signals_count' => rules.is_a?(Hash) ? (rules['soft_signals'] || []).length : 0,
          'has_attachments' => env['attachments'].present? && env['attachments'].any?,
          'email_length' => email_text.length,
          'special_headers' => headers.keys.any? { |k| k.match(/^(x-|list-|auto-|precedence)/i) },
          'email_type' => email_type,
          'generator_hint' => gate['generator_hint']
        })
      end,
      sample_output: lambda do
        call(:sample_deterministic_filter)
      end
    },
    ai_policy_filter: {
      title: 'Filter: AI policy',
      subtitle: 'Fuzzy triage via LLM under a strict JSON schema',
      display_priority: 501,
      help: lambda do |_|
        { body: 'Constrained LLM decides IRRELEVANT/REVIEW/KEEP. Short-circuits only if confident.' }
      end,

      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        call(:ui_policy_inputs, object_definitions, config_fields) +
          Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'policy', type: 'object', properties: Array(object_definitions['policy_out']) },
          { name: 'short_circuit', type: 'boolean' },
          { name: 'email_type' },
          { name: 'generator_gate', type: 'object', properties: [
              { name: 'pass_to_responder', type: 'boolean' }, { name: 'reason' }, { name: 'generator_hint' }
            ]},
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :ai_policy_filter, input)
        
        # Build model path
        model_path = call(:build_model_path_with_global_preview, connection, (input['model'] || 'gemini-2.0-flash'))
        loc = (model_path[/\/locations\/([^\/]+)/,1] || (connection['location'].presence || 'global')).to_s.downcase
        url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
        req_params = "model=#{model_path}"

        # Parse policy specification and extract schema if present
        policy_spec = nil
        output_format = nil
        
        if input['policy_mode'].to_s == 'json' && input['policy_json'].present?
          parsed_policy = call(:safe_json_obj!, input['policy_json'])
          
          # Handle both direct policy spec and nested policy_config
          if parsed_policy['policy_config'].is_a?(Hash)
            policy_spec = parsed_policy['policy_config']
            output_format = policy_spec['output_format']
          else
            policy_spec = parsed_policy
            output_format = parsed_policy['output_format']
          end
        end

        # Build LLM configuration from schema if available
        llm_config = output_format ? call(:build_llm_config_from_schema, output_format) : {}
        
        # Build system text - combine base instructions with schema instructions
        system_text = <<~SYS
          You are a strict email policy triage. Your ENTIRE response must be valid JSON.
          
          CRITICAL OUTPUT REQUIREMENTS:
          1. Output ONLY valid JSON - no text before or after
          2. DO NOT include markdown formatting or code blocks
          3. Use EXACTLY these field names (case-sensitive):
             - decision: must be exactly one of: "IRRELEVANT", "REVIEW", "KEEP"
             - confidence: number between 0 and 1
             - matched_signals: array of strings (max 10 items)
             - reasons: array of strings (max 5 items)
          4. ALL fields are required
          
          Example valid response:
          {"decision":"KEEP","confidence":0.85,"matched_signals":["direct_question"],"reasons":["Clear PTO request"]}
        SYS
        
        # Add schema-specific instructions if available
        if output_format && output_format['instructions']
          system_text += "\n\n#{output_format['instructions']}"
        end
        
        # Add example if provided by schema
        if llm_config['prompt_addition']
          system_text += llm_config['prompt_addition']
        end
        
        # Add policy spec to system text if present
        if policy_spec
          system_text += "\n\nPolicy spec JSON:\n#{call(:json_compact, policy_spec)}"
        end

        # Build generation config - use schema-derived or fallback to default
        if llm_config['response_schema']
          gen_config = {
            'temperature' => 0,
            'maxOutputTokens' => 256,
            'responseMimeType' => 'application/json',
            'responseSchema' => llm_config['response_schema']
          }
        else
          # Fallback to original hardcoded schema
          gen_config = {
            'temperature' => 0, 
            'maxOutputTokens' => 256,
            'responseMimeType' => 'application/json',
            'responseSchema' => {
              'type' => 'object',
              'additionalProperties' => false,
              'properties' => {
                'decision' => {'type' => 'string'},
                'confidence' => {'type' => 'number'},
                'matched_signals' => {'type' => 'array','items' => {'type' => 'string'}},
                'reasons' => {'type' => 'array','items' => {'type' => 'string'}}
              }, 
              'required' => ['decision']
            }
          }
        end

        # Build the request payload
        payload = {
          'systemInstruction' => { 'role' => 'system','parts' => [{'text' => system_text}] },
          'contents' => [ { 'role' => 'user', 'parts' => [ { 'text' => "Email:\n#{input['email_text']}" } ] } ],
          'generationConfig' => gen_config
        }

        # Make the API request
        resp = post(url).headers(call(:request_headers_auth, connection, ctx['cid'], connection['user_project'], req_params))
                        .payload(call(:json_compact, payload))
        
        # Extract and parse the response
        text = resp.dig('candidates',0,'content','parts',0,'text').to_s
        # Strict JSON extraction and validation
        clean_text = text.gsub(/```json\n?/, '').gsub(/```\n?/, '').strip
        
        # Ensure it looks like JSON
        unless clean_text.start_with?('{') && clean_text.end_with?('}')
          # Try to extract JSON from the text
          json_match = clean_text.match(/\{[^{}]*\}/)
          clean_text = json_match ? json_match[0] : '{"decision":"REVIEW","confidence":0.0,"matched_signals":[],"reasons":["Parse error"]}'
        end
        
        policy = begin
          JSON.parse(clean_text)
        rescue => e
          { 'decision' => 'REVIEW', 'confidence' => 0.0, 'matched_signals' => [], 'reasons' => ['JSON parse failed'] }
        end
        
        # Validate and correct response against schema if available
        if output_format
          policy = call(:validate_policy_schema!, policy, output_format)
        end
        
        # Ensure decision is valid (original logic preserved)
        if !['IRRELEVANT', 'REVIEW', 'KEEP'].include?(policy['decision'])
          policy['decision'] = 'REVIEW'  # Safe default
        end
        
        # Calculate short circuit (original logic preserved)
        short_circuit = (policy['decision'] == 'IRRELEVANT' && 
                        policy['confidence'].to_f >= (input['confidence_short_circuit'] || 0.8).to_f)

        # Generator gate logic (original logic preserved)
        email_type = (input['email_type'].presence || 'direct_request')
        min_conf = (input['min_confidence_for_generation'].presence || 0.60).to_f
        decision = policy['decision'].to_s.upcase
        conf = policy['confidence'].to_f
        
        block_reasons = []
        block_reasons << 'non_direct_request' if email_type != 'direct_request'
        block_reasons << 'policy_irrelevant' if decision == 'IRRELEVANT'
        block_reasons << 'low_confidence' if conf < min_conf
        
        pass_to_responder = block_reasons.empty?
        generator_hint = pass_to_responder ? 'pass' : 'blocked'

        # Build output (original structure preserved)
        out = {
          'policy' => policy,
          'short_circuit' => short_circuit,
          'email_type' => email_type,
          'generator_gate' => {
            'pass_to_responder' => pass_to_responder,
            'reason' => (block_reasons.any? ? block_reasons.join(',') : 'meets_minimums'),
            'generator_hint' => generator_hint
          }
        }
        
        # Build facets and complete output (original logic preserved)
        call(:step_ok!, ctx, out, call(:telemetry_success_code, resp), 'OK', {
          'decision' => policy['decision'],
          'confidence' => policy['confidence'],
          'short_circuit' => short_circuit,
          'email_type' => email_type,
          'generator_hint' => generator_hint,
          'signals_count' => (policy['matched_signals'] || []).length,
          'reasons_count' => (policy['reasons'] || []).length,
          'model_used' => input['model'] || 'gemini-2.0-flash',
          'policy_mode' => input['policy_mode'] || 'none',
          'schema_enhanced' => output_format.present?  # New facet to track schema usage
        })
        
      rescue => e
        call(:step_err!, ctx, e)
      end,
      sample_output: lambda do
        call(:sample_ai_policy_filter)
      end
    },
    embed_text_against_categories: {
      title: 'Categorize: Embed email vs categories',
      subtitle: 'Cosine similarity → scores + shortlist',
      display_priority: 499,
      help: lambda do |_|
        { body: 'Embeds email and categories, returns similarity scores and a top-K shortlist.' }
      end,

      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        call(:ui_embed_inputs, object_definitions, config_fields) +
          Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, _conn|
        [
          { name: 'scores', type: 'array', of: 'object', properties: [
              { name: 'category' }, { name: 'score', type: 'number' }, { name: 'cosine', type: 'number' }
          ]},
          { name: 'shortlist', type: 'array', of: 'string' },
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :embed_text_against_categories, input)
        # Select categories source (array vs JSON)
        cats_raw =
          if input['categories_mode'].to_s == 'json' && input['categories_json'].present?
            call(:safe_json_arr!, input['categories_json'])
          else
            input['categories']
          end
        cats = call(:norm_categories!, cats_raw)

        emb_model      = (input['embedding_model'].presence || 'text-embedding-005')
        model_path     = call(:build_embedding_model_path, connection, emb_model)

        email_inst = { 'content'=>input['email_text'].to_s, 'task_type'=>'RETRIEVAL_QUERY' }
        cat_insts  = cats.map do |c|
          { 'content'=>[c['name'], c['description'], *call(:safe_array,c['examples'])].compact.join("\n"),
            'task_type'=>'RETRIEVAL_DOCUMENT' }
        end

        emb_resp = call(:predict_embeddings, connection, model_path, [email_inst] + cat_insts, {}, ctx['cid'])
        preds    = call(:safe_array, emb_resp && emb_resp['predictions'])
        error('Embedding model returned no predictions') if preds.empty?

        email_vec = call(:extract_embedding_vector, preds.first)
        cat_vecs  = preds.drop(1).map { |p| call(:extract_embedding_vector, p) }
        sims = cat_vecs.each_with_index.map { |v, i| [i, call(:vector_cosine_similarity, email_vec, v)] }
        sims.sort_by! { |(_i, s)| -s }

        scores = sims.map { |(i,s)| { 'category'=>cats[i]['name'], 'score'=>(((s+1.0)/2.0).round(6)), 'cosine'=>s.round(6) } }
        k = [[(input['shortlist_k'] || 3).to_i, 1].max, cats.length].min
        shortlist = scores.first(k).map { |h| h['category'] }

        out = { 'scores'=>scores, 'shortlist'=>shortlist }
        call(:step_ok!, ctx, out, 200, 'OK', { 
          'k' => k,
          'categories_count' => cats.length,
          'shortlist_k' => k,
          'top_score' => scores.first ? scores.first['score'] : 0,
          'embedding_model' => emb_model,
          'categories_mode' => input['categories_mode'] || 'array'
        })

      end,
      sample_output: lambda do
        call(:sample_embed_text_against_categories)
      end
    },
    rerank_shortlist: {
      title: 'Categorize: Re-rank shortlist',
      subtitle: 'Optional LLM listwise re-ordering of categories',
      display_priority: 498,
      help: lambda do |_|
        { body: 'Uses LLM to produce a probability distribution over the shortlist and re-orders it.' }
      end,

      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ],  
      input_fields: lambda do |object_definitions, connection, config_fields|
        call(:ui_rerank_inputs, object_definitions, config_fields) +
          Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'ranking', type: 'array', of: 'object', properties: [
              { name: 'category' }, { name: 'prob', type: 'number' }
          ]},
          { name: 'shortlist', type: 'array', of: 'string' },
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :rerank_shortlist, input)
        mode = (input['mode'] || 'none').to_s

        if mode == 'none'
          sl = call(:safe_array, input['shortlist'])
          ranking = sl.map { |c| { 'category'=>c, 'prob'=>nil } }
          out = { 'ranking'=>ranking, 'shortlist'=>sl }
          return call(:step_ok!, ctx, out, 200, 'OK', { 
            'mode' => 'none',
            'categories_ranked' => sl.length
          })
        end

        # LLM listwise: reuse referee to get distribution over shortlist
        cats_raw =
          if input['categories_mode'].to_s == 'json' && input['categories_json'].present?
            call(:safe_json_arr!, input['categories_json'])
          else
            input['categories']
          end
        cats = call(:norm_categories!, cats_raw)
        ref  = call(:llm_referee, connection, (input['generative_model'] || 'gemini-2.0-flash'),
                    input['email_text'], call(:safe_array, input['shortlist']), cats, nil, ctx['cid'], nil)
        dist = call(:safe_array, ref['distribution']).map { |d| { 'category'=>d['category'], 'prob'=>d['prob'].to_f } }
        # Ensure all shortlist items present
        missing = call(:safe_array, input['shortlist']) - dist.map { |d| d['category'] }
        dist.concat(missing.map { |m| { 'category'=>m, 'prob'=>0.0 } })
        ranking = dist.sort_by { |h| -h['prob'].to_f }
        out = { 'ranking'=>ranking, 'shortlist'=>ranking.map { |r| r['category'] } }
        call(:step_ok!, ctx, out, 200, 'OK', { 
          'mode' => 'llm',
          'top_prob' => ranking.first ? ranking.first['prob'] : 0,
          'categories_ranked' => ranking.length,
          'generative_model' => input['generative_model'] || 'gemini-2.0-flash',
          'categories_mode' => input['categories_mode'] || 'array'
        })
      end,
      sample_output: lambda do
        call(:sample_rerank_shortlist)
      end
    },
    rag_retrieve_contexts_enhanced: {
      title: 'RAG: Retrieve contexts (enhanced)',
      subtitle: 'projects.locations:retrieveContexts (Vertex RAG Engine, v1)',
      display_priority: 489,
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
          { name: 'correlation_id', optional: true, hint: 'For tracking related requests.' },
          { name: 'sanitize_pdf_content', 
            type: 'boolean', 
            control_type: 'checkbox',
            default: true,
            hint: 'Clean PDF extraction artifacts when detected (recommended)' },
          { name: 'on_error_behavior', 
            control_type: 'select',
            pick_list: [
              ['Skip failed contexts', 'skip'],
              ['Include error placeholders', 'include'],
              ['Fail entire request', 'fail']
            ],
            default: 'skip',
            hint: 'How to handle individual context processing errors' }
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
              { name: 'metadata_json' },
              { name: 'is_pdf', type: 'boolean' },
              { name: 'processing_error', type: 'boolean' }
            ]
          },
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
              { name: 'http_status', type: 'integer' },
              { name: 'message' },
              { name: 'duration_ms', type: 'integer' },
              { name: 'correlation_id' },
              { name: 'success_count', type: 'integer' },
              { name: 'error_count', type: 'integer' },
              { name: 'partial_failure', type: 'boolean' },
              { name: 'local_logs', type: 'array', of: 'object', optional: true }
            ]}
        ]
      end,
      execute: lambda do |connection, input|
        # Initialize tracking context
        ctx = call(:step_begin!, :rag_retrieve_contexts_enhanced, input)
        
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
            'X-Correlation-Id' => ctx['cid'],
            'x-goog-request-params' => "parent=projects/#{project}/locations/#{location}"
          }
          
          # Make request with error handling
          response = begin
            post(url).headers(headers).payload(body)
          rescue => e
            if e.message.include?('timeout') || e.message.include?('connection')
              # Return empty but valid response for network issues
              {
                'contexts' => { 'contexts' => [] },
                '_network_error' => "Network error: #{e.message[0..200]}"
              }
            else
              raise e  # Re-raise non-network errors
            end
          end
          
          # Extract and map contexts with graceful failure
          contexts = []
          #raw_contexts = call(:safe_extract_contexts, response)
          raw_contexts = if response && response['contexts'] && response['contexts']['contexts']
            response['contexts']['contexts']
          else
            []
          end
          
          if raw_contexts.empty? && !response['_network_error']
            # Add informational context for empty responses
            contexts << {
              'id' => 'no-results',
              'text' => 'No contexts were returned for this query.',
              'score' => 0.0,
              'source' => 'system',
              'uri' => nil,
              'metadata' => { 'info' => 'empty_response' },
              'metadata_kv' => [{ 'key' => 'info', 'value' => 'empty_response' }],
              'metadata_json' => '{"info":"empty_response"}',
              'is_pdf' => false,
              'processing_error' => false
            }
          else
            raw_contexts.each_with_index do |ctx_item, idx|
              begin
                # Extract metadata
                md = ctx_item['metadata'] || {}
                
                # Get source URI for PDF detection
                source_uri = ctx_item['sourceUri'] || ctx_item['uri']
                
                # DEBUGGING: Log what we're seeing
                is_pdf = call(:is_pdf_source?, source_uri, md)
                sanitize_enabled = input['sanitize_pdf_content'] != false
                
                # Extract text
                raw_text = (ctx_item['text'] || ctx_item.dig('chunk', 'text') || '').to_s
                
                # DEBUGGING: Check for PDF indicators
                has_double_escapes = raw_text.include?('\\\\n') || raw_text.include?('\\\\t')
                
                # Log for debugging (remove after fixing)
                if has_double_escapes || is_pdf
                  puts "DEBUG Context #{idx}: is_pdf=#{is_pdf}, sanitize=#{sanitize_enabled}, has_escapes=#{has_double_escapes}, uri=#{source_uri}"
                end
                
                # Apply PDF-specific cleaning based on preference and detection
                text = if sanitize_enabled && is_pdf
                  puts "DEBUG: Sanitizing PDF context #{idx}"
                  call(:sanitize_pdf_text, raw_text)
                elsif has_double_escapes && sanitize_enabled
                  # FALLBACK: Even if not detected as PDF, clean obvious PDF artifacts
                  puts "DEBUG: Cleaning escaped content in context #{idx}"
                  call(:sanitize_pdf_text, raw_text)
                else
                  # Regular cleaning for non-PDF content
                  raw_text.encode('UTF-8', invalid: :replace, undef: :replace, replace: ' ')
                          .gsub(/\s+/, ' ')
                          .strip
                end
                
                # Try JSON serialization with fallback
                metadata_json = begin
                  md.empty? ? nil : md.to_json
                rescue => json_err
                  { error: 'metadata_serialization_failed', keys: md.keys.take(10) }.to_json
                end
                
                contexts << {
                  'id' => ctx_item['chunkId'] || ctx_item['id'] || "ctx-#{idx + 1}",
                  'text' => text,
                  'score' => (ctx_item['score'] || ctx_item['relevanceScore'] || 0.0).to_f,
                  'source' => ctx_item['sourceDisplayName'] || source_uri&.split('/')&.last,
                  'uri' => source_uri,
                  'metadata' => md,
                  'metadata_kv' => md.map { |k, v| { 'key' => k.to_s, 'value' => v.to_s[0..1000] } }, # Limit value size
                  'metadata_json' => metadata_json,
                  'is_pdf' => call(:is_pdf_source?, source_uri, md),
                  'processing_error' => false
                }
                
              rescue => e
                # Handle individual context errors based on configuration
                case input['on_error_behavior']
                when 'fail'
                  raise e  # Re-raise to fail entire action
                when 'include'
                  # Add error placeholder context
                  contexts << {
                    'id' => "ctx-#{idx + 1}-error",
                    'text' => "[Error processing context: #{e.message[0..200]}]",
                    'score' => 0.0,
                    'source' => 'error',
                    'uri' => nil,
                    'metadata' => { 
                      'error' => e.message[0..500], 
                      'error_class' => e.class.name,
                      'original_id' => ctx_item['chunkId'] || ctx_item['id'] 
                    },
                    'metadata_kv' => [
                      { 'key' => 'error', 'value' => e.message[0..500] },
                      { 'key' => 'error_class', 'value' => e.class.name }
                    ],
                    'metadata_json' => nil,
                    'is_pdf' => false,
                    'processing_error' => true
                  }
                when 'skip', nil
                  # Skip this context, continue processing
                  next
                end
              end
            end
          end
          
          # Calculate success metrics
          error_count = contexts.count { |c| c['processing_error'] == true }
          pdf_count = contexts.count { |c| c['is_pdf'] && !c['processing_error'] }
          success_count = contexts.count { |c| !c['processing_error'] }
          
          # Build output
          out = {
            'question' => input['query_text'],
            'contexts' => contexts
          }
          
          # Add telemetry with success tracking
          extras = {
            'retrieval' => {
              'top_k' => retrieval_cfg['topK'],
              'filter' => retrieval_cfg['filter'] ? {
                'type' => retrieval_cfg['filter'].keys.first.to_s.sub('vector', '').sub('Threshold', ''),
                'value' => retrieval_cfg['filter'].values.first
              } : nil,
              'contexts_count' => contexts.length,
              'success_count' => success_count,
              'error_count' => error_count,
              'pdf_contexts_count' => pdf_count,
              'partial_failure' => error_count > 0
            }.compact,
            'rank' => retrieval_cfg['ranking'] ? {
              'mode' => retrieval_cfg['ranking'].keys.first,
              'model' => retrieval_cfg['ranking'].values.first['modelName']
            } : nil,
            'network_error' => response['_network_error']
          }.compact
          
          # Build appropriate message
          message = if response['_network_error']
            "Retrieved #{contexts.length} contexts (network issues detected)"
          elsif error_count > 0
            "Retrieved #{success_count} contexts (#{error_count} failed)"
          else
            "Retrieved #{contexts.length} contexts"
          end
          
          # Return success with telemetry
          call(:step_ok!, ctx, out, 200, message, extras)
          
        rescue => e
          # Handle errors with telemetry
          call(:step_err!, ctx, e)
        end
      end,
      sample_output: lambda do
        call(:sample_rag_retrieve_contexts_enhanced)
      end,
    },
    rank_texts_with_ranking_api: {
      title: 'Rerank contexts',
      subtitle: 'projects.locations.rankingConfigs:rank',
      description: '',
      help: lambda do |input, picklist_label|
        {
          body: 'Rerank retrieved contexts for a query within a known category using LLM-based ranking with category awareness.',
          learn_more_url: 'https://cloud.google.com/vertex-ai/generative-ai/docs/rag-engine/retrieval-and-ranking',
          learn_more_text: 'Check out Google docs for retrieval and ranking'
        }
      end,
      display_priority: 488,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'query_text', optional: false, hint: 'The user query to rank contexts against' },
          { name: 'records', type: 'array', of: 'object', optional: false, properties: [
              { name: 'id', optional: false }, 
              { name: 'content', optional: false }, 
              { name: 'metadata', type: 'object' }
            ], hint: 'Retrieved contexts to rank (id + content required)' },
          { name: 'category', optional: true, 
            hint: 'Pre-determined category (e.g., PTO, Billing, Support) to inform ranking' },
          { name: 'category_context', optional: true,
            hint: 'Additional context about the category to guide ranking (description, scope, etc.)' },
          { name: 'include_category_in_query', type: 'boolean', control_type: 'checkbox', optional: true, default: true,
            hint: 'Include category context in ranking query for better relevance' },
          { name: 'correlation_id', label: 'Correlation ID', optional: true, 
            hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
          
          # LLM ranking options (no longer conditional)
          { name: 'llm_model', optional: true, default: 'gemini-2.0-flash',
            hint: 'LLM model for semantic ranking' },
          { name: 'top_n', type: 'integer', optional: true, hint: 'Max contexts to return (default: all)' },
          
          # Advanced options toggle
          { name: 'show_advanced', label: 'Show advanced options', type: 'boolean', control_type: 'checkbox',
            default: false, sticky: true, extends_schema: true },
          
          # Context filtering
          { name: 'filter_by_category_metadata', type: 'boolean', control_type: 'checkbox', optional: true, default: false,
            ngIf: 'input.show_advanced', 
            hint: 'Pre-filter contexts by category match in metadata before ranking' },
          { name: 'category_metadata_key', optional: true, default: 'category',
            ngIf: 'input.show_advanced && input.filter_by_category_metadata', 
            hint: 'Metadata field containing category tags' },
          
          # LLM processing limits
          { name: 'llm_max_contexts', type: 'integer', optional: true, default: 50,
            ngIf: 'input.show_advanced',
            hint: 'Maximum number of contexts to process with LLM (for cost/performance)' },
          { name: 'include_confidence_distribution', type: 'boolean', control_type: 'checkbox', optional: true, default: false,
            ngIf: 'input.show_advanced',
            hint: 'Return probability distribution across contexts' },
          
          # Output shape control
          { name: 'emit_shape', control_type: 'select', pick_list: 'rerank_emit_shapes', 
            optional: true, default: 'context_chunks',
            hint: 'Output format: minimal records, enriched records, or RAG-ready context_chunks' },
          { name: 'source_key', optional: true, default: 'source',
            hint: 'Metadata key for document source' },
          { name: 'uri_key', optional: true, default: 'uri',
            hint: 'Metadata key for document URI' },
          
          # Location override
          { name: 'ai_apps_location', label: 'AI-Apps location (override)', control_type: 'select', 
            pick_list: 'ai_apps_locations', optional: true,
            hint: 'Force specific multi-region (global/us/eu). Usually auto-detected.' }
        ]
      end,
      output_fields: lambda do |object_definitions, _connection|
        [
          { name: 'records', type: 'array', of: 'object', properties: [
              { name: 'id' }, 
              { name: 'score', type: 'number' }, 
              { name: 'rank', type: 'integer' },
              { name: 'content' }, 
              { name: 'metadata', type: 'object' },
              { name: 'llm_relevance', type: 'number' },
              { name: 'category_alignment', type: 'number' }
            ] },
          { name: 'context_chunks', type: 'array', of: 'object', properties: [
              { name: 'id' }, 
              { name: 'text' }, 
              { name: 'score', type: 'number' },
              { name: 'source' }, 
              { name: 'uri' },
              { name: 'metadata', type: 'object' },
              { name: 'metadata_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
              { name: 'metadata_json' },
              { name: 'llm_relevance', type: 'number' },
              { name: 'category_alignment', type: 'number' }
            ] },
          { name: 'confidence_distribution', type: 'array', of: 'object', properties: [
              { name: 'id' }, 
              { name: 'probability', type: 'number' },
              { name: 'reasoning' }
            ] },
          { name: 'ranking_metadata', type: 'object', properties: [
              { name: 'category' },
              { name: 'llm_model' },
              { name: 'contexts_filtered', type: 'integer' },
              { name: 'contexts_ranked', type: 'integer' }
            ] },
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(object_definitions['envelope_fields_1'])
      end,
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :rank_texts_with_ranking_api, input)
        
        begin
          call(:ensure_project_id!, connection)
          loc = call(:aiapps_loc_resolve, connection, input['ai_apps_location'])
          
          # Extract category and prepare query
          category = input['category'].to_s.strip
          category_ctx = input['category_context'].to_s.strip
          query = input['query_text'].to_s
          
          # Enhance query with category context if requested
          if category.present? && input['include_category_in_query'] == true
            query_prefix = "Category: #{category}"
            query_prefix += " (#{category_ctx})" if category_ctx.present?
            enhanced_query = "#{query_prefix}\n\n#{query}"
          else
            enhanced_query = query
          end
          
          # Pre-filter contexts by category metadata if requested
          records_in = call(:safe_array, input['records'])
          
          # Validate that records have required fields
          records_in.each_with_index do |r, i|
            error("Record at index #{i} missing required 'id' field") unless r['id'].present?
            error("Record at index #{i} missing required 'content' field") unless r['content'].present?
          end
          
          records_to_rank = records_in
          
          if category.present? && input['filter_by_category_metadata'] == true
            meta_key = input['category_metadata_key'] || 'category'
            cat_lower = category.downcase
            
            records_to_rank = records_in.select do |r|
              md = r['metadata'].is_a?(Hash) ? r['metadata'] : {}
              cat_value = md[meta_key]
              
              case cat_value
              when String
                cat_value.downcase.include?(cat_lower)
              when Array
                cat_value.any? { |v| v.to_s.downcase.include?(cat_lower) }
              else
                false
              end
            end
            
            if records_to_rank.empty? && records_in.any?
              error("Category filtering removed all #{records_in.length} records. Check category metadata key '#{meta_key}' and value '#{category}'")
            end
          end
          
          # LLM-based ranking
          llm_model = input['llm_model'] || 'gemini-2.0-flash'
          distribution = nil
          enriched = []
          
          # Initialize all records with base scores
          enriched = records_to_rank.map do |orig|
            orig.merge(
              'score' => 0.0,
              'rank' => 999
            )
          end
          
          # Perform LLM ranking if category is present
          if category.present?
            # Limit contexts for LLM processing
            max_llm_contexts = (input['llm_max_contexts'] || 50).to_i
            contexts_for_llm = enriched.first(max_llm_contexts)
            
            # Call LLM ranker
            llm_result = call(:llm_category_aware_ranker,
                            connection,
                            llm_model,
                            enhanced_query,
                            category,
                            category_ctx,
                            contexts_for_llm,
                            ctx['cid'])
            
            if llm_result && llm_result['rankings']
              # Process LLM rankings
              llm_scores = {}
              llm_result['rankings'].each do |r|
                llm_scores[r['id']] = {
                  'relevance' => r['relevance'].to_f,
                  'category_alignment' => r['category_alignment'].to_f,
                  'reasoning' => r['reasoning']
                }
              end
              
              # Apply LLM scores
              enriched.each do |rec|
                if llm_data = llm_scores[rec['id']]
                  rec['llm_relevance'] = llm_data['relevance']
                  rec['category_alignment'] = llm_data['category_alignment']
                  # Score is weighted combination of relevance and category alignment
                  rec['score'] = 0.8 * llm_data['relevance'] + 0.2 * llm_data['category_alignment']
                else
                  # Records not evaluated by LLM get zero score
                  rec['score'] = 0.0
                end
              end
              
              # Build confidence distribution if requested
              if input['include_confidence_distribution'] == true
                total_score = enriched.sum { |x| x['score'].to_f }
                total_score = 0.001 if total_score <= 0
                
                distribution = enriched.map { |r|
                  llm_data = llm_scores[r['id']] || {}
                  {
                    'id' => r['id'],
                    'probability' => r['score'].to_f / total_score,
                    'reasoning' => llm_data['reasoning']
                  }
                }.sort_by { |d| -d['probability'] }
              end
            end
          else
            # If no category provided, use simple query-based relevance scoring
            contexts_for_llm = enriched.first((input['llm_max_contexts'] || 50).to_i)
            
            # Simplified LLM ranking without category
            llm_result = call(:llm_category_aware_ranker,
                            connection,
                            llm_model,
                            enhanced_query,
                            '',  # No category
                            '',  # No category context
                            contexts_for_llm,
                            ctx['cid'])
            
            if llm_result && llm_result['rankings']
              llm_result['rankings'].each do |r|
                matching_record = enriched.find { |rec| rec['id'] == r['id'] }
                if matching_record
                  matching_record['llm_relevance'] = r['relevance'].to_f
                  matching_record['score'] = r['relevance'].to_f
                end
              end
            end
          end
          
          # Re-rank by final scores
          enriched = enriched.sort_by { |r| [-r['score'].to_f, r['id']] }
                            .each_with_index { |r, i| r['rank'] = i + 1 }
          
          # Apply top_n limit if specified
          if input['top_n'].present?
            enriched = enriched.first(input['top_n'].to_i)
          end
          
          # Shape Output
          shape = input['emit_shape'] || 'context_chunks'
          result = {}
          
          case shape
          when 'records_only'
            result['records'] = enriched.map { |r|
              { 'id' => r['id'], 'score' => r['score'], 'rank' => r['rank'] }
            }
          when 'enriched_records'
            result['records'] = enriched
          else  # context_chunks
            chunks = enriched.map { |r|
              md = r['metadata'] || {}
              source_key = input['source_key'] || 'source'
              uri_key = input['uri_key'] || 'uri'
              
              chunk = {
                'id' => r['id'],
                'text' => r['content'].to_s,
                'score' => r['score'].to_f,
                'source' => md[source_key],
                'uri' => md[uri_key],
                'metadata' => md,
                'metadata_kv' => md.map { |k, v| { 'key' => k, 'value' => v } },
                'metadata_json' => md.empty? ? nil : md.to_json
              }
              
              chunk['llm_relevance'] = r['llm_relevance'] if r['llm_relevance']
              chunk['category_alignment'] = r['category_alignment'] if r['category_alignment']
              
              chunk
            }
            result['context_chunks'] = chunks
            result['records'] = enriched
          end
          
          result['confidence_distribution'] = distribution if distribution
          
          result['ranking_metadata'] = {
            'category' => category.presence,
            'llm_model' => llm_model,
            'contexts_filtered' => records_in.length - records_to_rank.length,
            'contexts_ranked' => records_to_rank.length
          }.compact
          
          # Build facets
          facets = {
            'ranking_api' => 'llm.category_ranker',
            'category' => category.presence,
            'llm_model' => llm_model,
            'emit_shape' => shape,
            'records_input' => records_in.length,
            'records_filtered' => records_in.length - records_to_rank.length,
            'records_ranked' => records_to_rank.length,
            'records_output' => enriched.length,
            'has_distribution' => distribution.present?,
            'top_score' => enriched.first&.dig('score'),
            'category_filtered' => input['filter_by_category_metadata'] == true
          }.compact
          
          call(:step_ok!, ctx, result, 200, 'OK', facets)
          
        rescue => e
          call(:step_err!, ctx, e)
        end
      end,
      sample_output: lambda do
        call(:sample_rank_texts_with_ranking_api)
      end
    },
    llm_referee_with_contexts: {
      title: 'Categorize: LLM as referee',
      subtitle: 'Adjudicate among shortlist; accepts ranked categories',
      display_priority: 487,
      help: lambda do |_|
        { body: 'Chooses final category using shortlist + category metadata; can append ranked contexts to the email text.' }
      end,
      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ], 
      input_fields: lambda do |object_definitions, connection, config_fields|
         call(:ui_ref_inputs, object_definitions, config_fields) + [
           # --- Salience sidecar (disabled by default; 2-call flow when LLM) ---
           { name: 'salience_mode', label: 'Salience extraction',
             control_type: 'select', optional: true, default: 'off', extends_schema: true,
             options: [['Off','off'], ['Heuristic (no LLM)','heuristic'], ['LLM (extra API call)','llm']],
             hint: 'Extract a salient sentence/paragraph before refereeing. LLM mode makes a separate API call.' },
           { name: 'salience_append_to_prompt', label: 'Append salience to prompt',
             type: 'boolean', control_type: 'checkbox', optional: true, default: true,
             ngIf: 'input.salience_mode != "off"',
             hint: 'If enabled, the salient span (and light metadata) is appended to the email text shown to the referee.' },
           { name: 'salience_max_chars', label: 'Salience max chars', type: 'integer',
             optional: true, default: 500, ngIf: 'input.salience_mode != "off"' },
           { name: 'salience_include_entities', label: 'Salience: include entities',
             type: 'boolean', control_type: 'checkbox', optional: true, default: true,
             ngIf: 'input.salience_mode == "llm"' },
           { name: 'salience_model', label: 'Salience model',
             control_type: 'text', optional: true, default: 'gemini-2.0-flash',
             ngIf: 'input.salience_mode == "llm"' },
           { name: 'salience_temperature', label: 'Salience temperature',
             type: 'number', optional: true, default: 0,
             ngIf: 'input.salience_mode == "llm"' }
         ] + Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'referee', type: 'object', properties: Array(object_definitions['referee_out']) },
          { name: 'chosen' },
          { name: 'confidence', type: 'number' },
          # Salience sidecar (top-level field; backward compatible)
          { name: 'salience', type: 'object', properties: [
               { name: 'span' },
               { name: 'reason' },
               { name: 'importance', type: 'number' },
               { name: 'tags', type: 'array', of: 'string' },
               { name: 'entities', type: 'array', of: 'object',
                 properties: [{ name: 'type' }, { name: 'text' }] },
               { name: 'cta' },
               { name: 'deadline_iso' },
               { name: 'focus_preview' },
               { name: 'responseId' },
               { name: 'usage', type: 'object', properties: [
                   { name: 'promptTokenCount', type: 'integer' },
                   { name: 'candidatesTokenCount', type: 'integer' },
                   { name: 'totalTokenCount', type: 'integer' }
               ]},
               { name: 'span_source' } # heuristic | llm
             ]},
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :llm_referee_with_contexts, input)

        # Add intent gate check
        if input['intent_kind'].present? && input['intent_kind'] != 'information_request'
          out = {
            'referee' => { 
              'category' => input['fallback_category'] || 'Other',
              'confidence' => 0.0,
              'reasoning' => 'Non-information intent blocked',
              'distribution' => []
            },
            'chosen' => input['fallback_category'] || 'Other',
            'confidence' => 0.0,
            'generator_gate' => {
              'pass_to_responder' => false,
              'reason' => 'non_information_intent',
              'generator_hint' => 'blocked'
            }
          }
          return call(:step_ok!, ctx, out, 200, 'OK', { 'blocked_reason' => 'non_information_intent' })
        end

        # Select categories source (array vs JSON)
        cats_raw =
          if input['categories_mode'].to_s == 'json' && input['categories_json'].present?
            call(:safe_json_arr!, input['categories_json'])
          else
            input['categories']
          end
        cats = call(:norm_categories!, cats_raw)

        # Optionally append ranked contexts to the email text for better decisions
        email_text = input['email_text'].to_s
        if Array(input['contexts']).any?
          blob = call(:format_context_chunks, input['contexts'])
          email_text = "#{email_text}\n\nContext:\n#{blob}"
        end
         salience = nil
         sal_err  = nil
         mode     = (input['salience_mode'] || 'off').to_s
         if mode != 'off'
           begin
             max_span = (input['salience_max_chars'].to_i rescue 500); max_span = [[max_span,80].max,2000].min
             # Heuristic extraction (no extra API call)
             if mode == 'heuristic'
               focus = email_text.to_s[0, 8000]
               # drop greetings
               focus = focus.sub(/\A\s*(subject:\s*[^\n]+\n+)?\s*(hi|hello|hey)[^a-z0-9]*\n+/i, '')
               cand = focus.split(/(?<=[.!?])\s+/).find { |s| s.strip.length >= 12 && s !~ /\A(hi|hello|hey)\b/i } || focus[0, max_span]
               span = cand.to_s.strip[0, max_span]
               salience = {
                 'span'=>span, 'reason'=>nil, 'importance'=>nil, 'tags'=>nil,
                 'entities'=>nil, 'cta'=>nil, 'deadline_iso'=>nil,
                 'focus_preview'=>focus, 'responseId'=>nil, 'usage'=>nil, 'span_source'=>'heuristic'
               }
             elsif mode == 'llm'
               # LLM extraction (separate API call)
               model = (input['salience_model'].presence || 'gemini-2.0-flash').to_s
               model_path = call(:build_model_path_with_global_preview, connection, model)
               loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
               url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
               req_params = "model=#{model_path}"
 
               schema_props = {
                 'salient_span'=>{'type'=>'string','minLength'=>12},
                 'reason'=>{'type'=>'string'},
                 'importance'=>{'type'=>'number'},
                 'tags'=>{'type'=>'array','items'=>{'type'=>'string'}},
                 'call_to_action'=>{'type'=>'string'},
                 'deadline_iso'=>{'type'=>'string'}
               }
               schema_props['entities'] = {
                 'type'=>'array',
                 'items'=>{'type'=>'object','additionalProperties'=>false,
                   'properties'=>{'type'=>{'type'=>'string'},'text'=>{'type'=>'string'}},
                   'required'=>['text']}
               } if call(:normalize_boolean, input['salience_include_entities'])
 
               system_text = "You extract the single most important sentence or short paragraph from an email. " \
                             "Rules: (1) Return VALID JSON only. (2) Do NOT output greetings, signatures, legal footers, " \
                             "auto-replies, or vague pleasantries. (3) Keep under #{max_span} characters; do not truncate mid-sentence. " \
                             "(4) importance is in [0,1]; set call_to_action/deadline_iso when clearly present."
 
               gen_cfg = {
                 'temperature'=> (input['salience_temperature'].present? ? input['salience_temperature'].to_f : 0),
                 'maxOutputTokens'=>512,
                 'responseMimeType'=>'application/json',
                 'responseSchema'=>{
                   'type'=>'object','additionalProperties'=>false,'properties'=>schema_props,
                   'required'=>['salient_span']
                 }
               }
               contents = [{ 'role'=>'user', 'parts'=>[{ 'text'=>"Email (trimmed):\n#{email_text.to_s[0,8000]}" }]}]
               payload = {
                 'contents'=>contents,
                 'systemInstruction'=>{ 'role'=>'system', 'parts'=>[{ 'text'=>system_text }]},
                 'generationConfig'=>gen_cfg
               }
               req_body = call(:json_compact, payload)
               resp = post(url).headers(call(:request_headers_auth, connection, ctx['cid'], connection['user_project'], req_params))
                              .payload(req_body)
 
               txt = resp.dig('candidates',0,'content','parts',0,'text').to_s
               parsed = call(:json_parse_gently!, txt)
               span = parsed['salient_span'].to_s.strip
               if span.empty? || span =~ /\A(hi|hello|hey)\b[:,\s]*\z/i || span.length < 8
                 focus = email_text.to_s[0, 8000]
                 focus = focus.sub(/\A\s*(subject:\s*[^\n]+\n+)?\s*(hi|hello|hey)[^a-z0-9]*\n+/i, '')
                 cand = focus.split(/(?<=[.!?])\s+/).find { |s| s.strip.length >= 12 && s !~ /\A(hi|hello|hey)\b/i } || focus[0, max_span]
                 span = cand.to_s.strip[0, max_span]
               end
               salience = {
                 'span'=>span,
                 'reason'=>parsed['reason'],
                 'importance'=>parsed['importance'],
                 'tags'=>parsed['tags'],
                 'entities'=>parsed['entities'],
                 'cta'=>parsed['call_to_action'],
                 'deadline_iso'=>parsed['deadline_iso'],
                 'focus_preview'=>email_text.to_s[0,8000],
                 'responseId'=>resp['responseId'],
                 'usage'=>resp['usageMetadata'],
                 'span_source'=>'llm'
               }
             end
           rescue => e
             sal_err = e.to_s
             salience = nil
           end
         end
         # Optionally append salience to the prompt sent to the referee
         if salience && call(:normalize_boolean, input['salience_append_to_prompt'])
           email_text = call(:maybe_append_salience, email_text, salience, salience['importance'])
         end
        shortlist = call(:safe_array, input['shortlist'])
        ref = call(:llm_referee, connection, (input['generative_model'] || 'gemini-2.0-flash'),
                  email_text, (shortlist.any? ? shortlist : nil), cats, input['fallback_category'], ctx['cid'], nil)

        min_conf = (input['min_confidence'].presence || 0.25).to_f
        chosen =
          if ref['confidence'].to_f < min_conf && input['fallback_category'].present?
            input['fallback_category']
          else
            ref['category']
          end

        out = {
          'referee'=>ref,
          'chosen'=>chosen,
          'confidence'=>[ref['confidence'], 0.0].compact.first.to_f
        }
        out['salience'] = salience if salience
 
        extras = {
          'chosen' => chosen,
          'confidence' => out['confidence'],
          'salience_mode' => input['salience_mode'] || 'off',
          'salience_len' => (salience && salience['span'] ? salience['span'].to_s.length : nil),
          'salience_importance' => (salience && salience['importance']),
          'salience_source' => (salience && salience['span_source']),
          'salience_err' => sal_err,
          'has_contexts' => Array(input['contexts']).any?,
          'shortlist_size' => shortlist.length,
          'generative_model' => input['generative_model'] || 'gemini-2.0-flash',
          'categories_mode' => input['categories_mode'] || 'array'
        }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

        # Add the missing step_ok! call with extras
        call(:step_ok!, ctx, out, 200, 'OK', extras)
      end,
      sample_output: lambda do
        call(:sample_llm_referee_with_contexts)  # or appropriate method name for each action
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
      display_priority: 470,
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
          ]},
          { name: 'complete_output', type: 'object', hint: 'All outputs consolidated for easy downstream use' },
          { name: 'facets', type: 'object', optional: true, hint: 'Analytics facets when available' }
        ] + Array(od['envelope_fields_1'])
      end,
      execute: lambda do |connection, raw_input|
        ctx = call(:step_begin!, :gen_generate, raw_input)
        
        begin
          input = call(:normalize_input_keys, raw_input)
          result = call(:gen_generate_core!, connection, input, ctx['cid'])
          
          # Extract key metrics for facets
          mode = (input['mode'] || 'plain').to_s
          model = input['model']
          finish_reason = call(:_facet_finish_reason, result) rescue result.dig('candidates', 0, 'finishReason')
          
          # Standard facets from compute_facets_for! plus action-specific ones
          facets = {
            'mode' => mode,
            'model' => model,
            'finish_reason' => finish_reason,
            'confidence' => result['confidence'],
            'has_citations' => result.dig('parsed', 'citations').is_a?(Array) && result.dig('parsed', 'citations').any?
          }.compact
          
          call(:step_ok!, ctx, result, 200, 'OK', facets)
        rescue => e
          call(:step_err!, ctx, e)
        end
      end,
      sample_output: lambda do
        call(:sample_gen_generate)
      end
    },

    # Unused/deprecated
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
      deprecated: true,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'correlation_id', label: 'Correlation ID', optional: true, hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
          { name: 'model', label: 'Embedding model', optional: false, control_type: 'text', default: 'text-embedding-005' },
          { name: 'texts', type: 'array', of: 'string', optional: false },
          { name: 'task', hint: 'Optional: RETRIEVAL_QUERY or RETRIEVAL_DOCUMENT' },
          { name: 'autoTruncate', type: 'boolean', hint: 'Truncate long inputs automatically' },
          { name: 'outputDimensionality', type: 'integer', optional: true, convert_input: 'integer_conversion',
            hint: 'Optional dimensionality reduction (see model docs).' }
        ]
      end,
      output_fields: lambda do |object_definitions, _connection|
        Array(object_definitions['embed_output']) + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        cid = call(:ensure_correlation_id!, input)
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

          result = call(:predict_embeddings, connection, model_path, instances, params, cid)
          result = result.merge(call(:telemetry_envelope, t0, cid, true, 200, 'OK'))

          # Facets + local log
          extras_for_facets = {
            'n_texts' => Array(input['texts']).length,
            'task'    => (task || nil),
            'model'   => input['model']
          }
          facets = call(:compute_facets_for!, 'embed_text', result, extras_for_facets)
          (result['telemetry'] ||= {})['facets'] = facets
          call(:local_log_attach!, result,
            call(:local_log_entry, :embed_text, started_at, t0, result, nil, {
              'n_texts'        => Array(input['texts']).length,
              'task'           => (task || nil),
              'model'          => input['model'],
              'billable_chars' => result.dig('metadata','billableCharacterCount'),
              'facets'         => facets
            }))
          result
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          env = call(:telemetry_envelope, t0, cid, false, call(:telemetry_parse_error_code, e), msg)
          # Optional debug attachment in non-prod:
          if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
            env['debug'] = call(:debug_pack, true, url, req_body, g)
          end
          # Local log (error path)
          call(:local_log_attach!, env,
            call(:local_log_entry, :embed_text, started_at, t0, nil, e, { 'google_error' => g }))
          error(env)
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
          'telemetry' => {
            'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr',
            'facets' => {
              'model' => 'text-embedding-005',
              'n_texts' => 2,
              'task' => 'RETRIEVAL_QUERY'
            }
          }
        }
      end
    },
    count_tokens: {
      title: '[DEPRECATED] Count tokens',
      description: 'POST :countTokens on a publisher model',
      help: lambda do |_|
        {
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/generative-ai/docs/model-reference/count-tokens',
          learn_more_text: 'Check out Google docs for the CountTokens API'
        }
      end,
      display_priority: 5,
      deprecated: true,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, config_fields|
        [
          { name: 'correlation_id', label: 'Correlation ID', optional: true, hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
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
        ] + Array(object_definitions['envelope_fields_1'])
      end,
      execute:  lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        started_at = Time.now.utc.iso8601
        t0 = Time.now
        cid = call(:ensure_correlation_id!, input)
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
                    .headers(call(:request_headers_auth, connection, cid, connection['user_project'], "model=#{model_path}"))
                    .payload(req_body)
          code = call(:telemetry_success_code, resp)
          result = resp.merge(call(:telemetry_envelope, t0, cid, true, code, 'OK'))

          # Facets + local log
          extras_for_facets = {
            'model'          => input['model'],
            'tokens_total'   => result['totalTokens'],
            'billable_chars' => result['totalBillableCharacters']
          }
          facets = call(:compute_facets_for!, 'count_tokens', result, extras_for_facets)
          (result['telemetry'] ||= {})['facets'] = facets
          call(:local_log_attach!, result,
            call(:local_log_entry, :count_tokens, started_at, t0, result, nil, {
              'model'            => input['model'],
              'tokens_total'     => result['totalTokens'],
              'billable_chars'   => result['totalBillableCharacters'],
              'facets'           => facets
            }))
          result
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          env = call(:telemetry_envelope, t0, cid, false, call(:telemetry_parse_error_code, e), msg)
          # Optional debug attachment in non-prod:
          if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
            env['debug'] = call(:debug_pack, true, url, req_body, g)
          end
          # Local log (error path)
          call(:local_log_attach!, env,
            call(:local_log_entry, :count_tokens, started_at, t0, nil, e, { 'google_error' => g }))
          error(env) 
        end
      end,
      sample_output: lambda do
        {
          'totalTokens' => 31,
          'totalBillableCharacters' => 96,
          'promptTokensDetails' => [ { 'modality' => 'TEXT', 'tokenCount' => 31 } ],
          'ok' => true,
          'telemetry' => {
            'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr',
            'facets' => {
              'model' => 'gemini-2.0-flash',
              'tokens_total' => 31,
              'billable_chars' => 96
            }
          }
        }
      end
    },
    email_extract_salient_span: {
      title: '[DEPRECATED] Email Extract salient span',
      subtitle: 'Pull the most important sentence/paragraph from an email',
      display_priority: 5,
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
      deprecated: true,

      input_fields: lambda do |object_definitions, connection, config_fields|
        [
          { name: 'subject', optional: true },
          { name: 'body',    optional: false, hint: 'Raw email body (HTML or plain text). Quoted replies and signatures are pruned automatically.' },
          { name: 'correlation_id', label: 'Correlation ID', optional: true, hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
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
        cid = call(:ensure_correlation_id!, input)
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

          # System Prompt #
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
          resp = post(url).headers(call(:request_headers_auth, connection, cid, connection['user_project'], req_params)).payload(req_body)

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
          }.merge(call(:telemetry_envelope, t0, cid, true, 200, 'OK'))

          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end

          # Compute facets (tokens_total, etc.) and attach a local log entry
          facets = call(:compute_facets_for!, 'email_extract_salient_span', out)
          call(:local_log_attach!, out,
            call(:local_log_entry, :email_extract_salient_span, started_at, t0, out, nil, {
              'importance' => out['importance'],
              'facets'     => facets
            }))
          out
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          out = {}.merge(call(:telemetry_envelope, t0, cid, false, call(:telemetry_parse_error_code, e), msg))
          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end
          # Log the error path locally too
          call(:local_log_attach!, out,
            call(:local_log_entry, :email_extract_salient_span, started_at, t0, nil, e, { 'google_error' => g }))
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
    operations_get: {
      title: 'Get (poll) long running operation',
      subtitle: 'google.longrunning.operations.get',
      help: lambda do |_|
        {
          learn_more_url: 'https://docs.cloud.google.com/vertex-ai/docs/general/long-running-operations',
          learn_more_text: 'Find out more about Long running operations'
        }
      end,
      display_priority: 4,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_object_definitions, _connection, _config_fields|
        [
          { name: 'correlation_id', label: 'Correlation ID', optional: true, hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
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
        cid = call(:ensure_correlation_id!, input)
        begin
          call(:ensure_project_id!, connection)
          # Accept either full /v1/... or name-only
          op = input['operation'].to_s.sub(%r{^/v1/}, '')
          loc = (connection['location'].presence || 'us-central1').to_s.downcase
          url = call(:aipl_v1_url, connection, loc, op.start_with?('projects/') ? op : "projects/#{connection['project_id']}/locations/#{loc}/operations/#{op}")
          resp = get(url).headers(call(:request_headers_auth, connection, cid, connection['user_project'], nil))
          code = call(:telemetry_success_code, resp)
          result = resp.merge(call(:telemetry_envelope, t0, cid, true, code, 'OK'))

          # Facets + local log
          extras_for_facets = {
            'operation' => (result['name'] || input['operation']),
            'done'      => result['done']
          }
          facets = call(:compute_facets_for!, 'operations_get', result, extras_for_facets)
          (result['telemetry'] ||= {})['facets'] = facets

          call(:local_log_attach!, result,
            call(:local_log_entry, :operations_get, started_at, t0, result, nil, {
              'operation' => (result['name'] || input['operation']),
              'done'      => result['done'],
              'facets'    => facets
            }))
          result
        rescue => e
          g   = call(:extract_google_error, e)
          env = {}.merge(call(:telemetry_envelope, t0, cid, false, call(:telemetry_parse_error_code, e), e.to_s))
          # Local log (error path)
          call(:local_log_attach!, env,
            call(:local_log_entry, :operations_get, started_at, t0, nil, e, { 'google_error' => g, 'operation' => input['operation'] }))
          env
        end
      end,
      sample_output: lambda do
        {
          'name' => 'projects/p/locations/us-central1/operations/123',
          'done' => false,
          'ok' => true,
          'telemetry' => {
            'http_status' => 200, 'message' => 'OK', 'duration_ms' => 8, 'correlation_id' => 'sample',
            'facets' => {
              'operation' => 'projects/p/locations/us-central1/operations/123',
              'done' => false
            }
          }
        }
      end
    },
    gen_categorize_email: {
      title: 'Deprecated: Categorize email',
      subtitle: 'Classify an email into a category',
      deprecated: true,
      help: lambda do |input, picklist_label|
        {
          body: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.',
          learn_more_url: 'https://ai.google.dev/gemini-api/docs/models',
          learn_more_text: 'Find a current list of available Gemini models'
        }
      end,
      display_priority: 0,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, config_fields|
        [
          { name: 'show_advanced', label: 'Show advanced options', extends_schema: true,
            type: 'boolean', control_type: 'checkbox', optional: true, default: false },

          # === (A) Message -------------------------------------------------
          { name: 'subject', label: 'Email subject', optional: true },
          { name: 'body',    label: 'Email body (text/HTML ok)', optional: true },
          # Advanced: email metadata for rules/pre-filter
          { name: 'from',    optional: true, ngIf: 'input.show_advanced == true',
            hint: 'Sender email; used by rules and heuristics.' },
          { name: 'headers', type: 'object', optional: true, ngIf: 'input.show_advanced == true',
            hint: 'Key→value map. Ex: Content-Type, List-Unsubscribe.' },
          { name: 'attachments', type: 'array', of: 'object', optional: true, ngIf: 'input.show_advanced == true',
            properties: [{ name: 'filename' }, { name: 'mimeType' }, { name: 'size' }],
            hint: 'Attachment list used by ext_in rules.' },
          { name: 'auth', type: 'object', optional: true, ngIf: 'input.show_advanced == true',
            hint: 'Auth flags, e.g., spf_dkim_dmarc_fail: true.' },

          # === (B) Categories ---------------------------------------------
          { name: 'categories', label: 'Categories', optional: false, type: 'array', of: 'object',
            properties: [{ name: 'name', optional: false }, { name: 'description' },
                         { name: 'examples', type: 'array', of: 'string' }],
            hint: 'Provide ≥2. Strings allowed (names only).' },

          # === (C) Mode & Models ------------------------------------------
          { name: 'mode', control_type: 'select', pick_list: 'modes_classification',
            optional: false, default: 'embedding',
            hint: 'embedding (deterministic), generative, or hybrid.' },
          { name: 'embedding_model', label: 'Embedding model', control_type: 'text',
            optional: true, default: 'text-embedding-005',
            ngIf: 'input.mode != "generative"' },
          { name: 'generative_model', label: 'Generative model', control_type: 'text',
            optional: true, ngIf: 'input.mode != "embedding"',
            hint: 'Gemini model for referee or pure generative mode.' },
          { name: 'referee_system_preamble', label: 'Referee system preamble',
            optional: true, ngIf: 'input.show_advanced == true && input.mode != "embedding"',
            hint: 'Override the built-in strict JSON classifier preamble.' },

          # === (D) Salience (optional) ------------------------------------
          # Caller-supplied salience only (no in-action extraction)
          { name: 'salient_span', label: 'Pre-extracted salient span',
            optional: true,
            hint: 'If provided, classification uses this span instead of full email.' },
          { name: 'salience_importance', label: 'Salience importance (0–1)',
            type: 'number', optional: true,
            hint: 'Optional; used for confidence blending.' },
          { name: 'salience_reason', label: 'Salience reason',
            optional: true,
            hint: 'Optional note describing why this span was chosen.' },

          # === (E) Rules (optional) ---------------------------------------
          { name: 'rules_mode', label: 'Rules mode',
            control_type: 'select', optional: true, default: 'none',
            pick_list: 'rules_modes', # stub below
            hint: 'Choose none / single-table rows / compiled JSON.' },
          { name: 'rules_rows', label: 'Rules (single-table rows)',
            optional: true, ngIf: 'input.rules_mode == "rows"',
            hint: 'Bind Lookup Table rows or enter rows manually.',
            type: 'array', of: 'object', properties: [
              { name: 'rule_id' }, { name: 'family' }, { name: 'field' }, { name: 'operator' },
              { name: 'pattern' }, { name: 'weight' }, { name: 'action' }, { name: 'cap_per_email' },
              { name: 'category' }, { name: 'flag_a' }, { name: 'flag_b' },
              { name: 'enabled' }, { name: 'priority' }, { name: 'notes' }
            ]},
          { name: 'rules_json', label: 'Rules (compiled JSON)', control_type: 'text-area',
            optional: true, ngIf: 'input.rules_mode == "json"', hint: 'If present, overrides rows.' },

          # === (F) Decision & Fallbacks -----------------------------------
          { name: 'min_confidence', label: 'Minimum confidence',
            type: 'number', optional: true, default: 0.25,
            hint: '0–1. Below this, fallback is used.' },
          { name: 'fallback_category', label: 'Fallback category',
            optional: true, default: 'Other' },
          { name: 'top_k', label: 'Referee shortlist (top-K)',
            type: 'integer', optional: true, default: 3,
            ngIf: 'input.mode == "hybrid" || (input.mode != "embedding" && input.show_advanced == true)',
            hint: 'How many candidates to pass to LLM referee.' },
          { name: 'return_explanation', label: 'Return explanation',
            type: 'boolean', control_type: 'checkbox', optional: true, default: false,
            ngIf: 'input.mode != "embedding"',
            hint: 'If true and a generative model is set, return reasoning.' },

          # === (G) Observability ------------------------------------------
          { name: 'correlation_id', label: 'Correlation ID',
            optional: true, sticky: true,
            hint: 'Use a sticky ID to stitch logs/metrics.' },
          { name: 'debug', label: 'Debug (non-prod only)',
            type: 'boolean', control_type: 'checkbox', optional: true,
            ngIf: 'input.show_advanced == true',
            hint: 'Adds request/response preview to output when not in prod.' }
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
          # Pre-filter outcome (hard exclude or soft signals triage). Optional.
          { name: 'pre_filter', type: 'object', properties: [
              { name: 'hit', type: 'boolean' },
              { name: 'action' },
              { name: 'reason' },
              { name: 'score', type: 'number' },
              { name: 'matched_signals', type: 'array', of: 'string' },
              { name: 'decision' }
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
        ] + Array(object_definitions['envelope_fields_1'])
      end,
      execute: lambda do |connection, input|
        # 1. Invariants
        started_at = Time.now.utc.iso8601 # for logging
        t0   = Time.now
        cid = call(:ensure_correlation_id!, input)
        url = nil; req_body = nil
        begin
          # 1a. Compile rulepack from Workato-native table (if provided)
          rules =
            if input['rules_json'].present?
              call(:safe_json, input['rules_json'])
            elsif Array(input['rules_rows']).any?
              call(:hr_compile_rulepack_from_rows!, input['rules_rows'])
            else
              nil
            end
          # 2. Build the request
          subj = (input['subject'] || '').to_s.strip
          body = (input['body']    || '').to_s.strip
          error('Provide subject and/or body') if subj.empty? && body.empty?

          # Salience: accept caller-provided span; no in-action extraction
          preproc = nil
          if input['salient_span'].to_s.strip.length > 0
            preproc = {
              'salient_span' => input['salient_span'].to_s,
              'reason'       => (input['salience_reason'].presence || nil),
              'importance'   => (input['salience_importance'].nil? ? nil : input['salience_importance'].to_f)
            }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
          end

          email_text =
            if preproc && preproc['salient_span'].to_s.strip.length > 0
              preproc['salient_span'].to_s
            else
              call(:build_email_text, subj, body)
            end

          # 2a. Optional pre-filter using compiled rules (short-circuit irrelevant)
          if rules.is_a?(Hash)
            hard_pack = rules['hard_exclude'].is_a?(Hash) ? rules['hard_exclude'] : {}
            soft_pack = rules['soft_signals'].is_a?(Array) ? rules['soft_signals'] : []

            hf = call(:hr_eval_hard?, {
              'subject' => subj, 'body' => body,
              'from' => input['from'], 'headers' => input['headers'],
              'attachments' => input['attachments'], 'auth' => input['auth']
            }, hard_pack)
            if hf[:hit]
              out = {
                'mode' => (input['mode'] || 'embedding'),
                'chosen' => (input['fallback_category'].presence || 'Irrelevant'),
                'confidence' => 0.0,
                'pre_filter' => hf
              }.merge(call(:telemetry_envelope, t0, cid, true, 200, 'OK'))
              facets = call(:compute_facets_for!, 'gen_categorize_email', out, { 'decision' => 'IRRELEVANT' })
              (out['telemetry'] ||= {})['facets'] = facets
              return call(:local_log_attach!, out,
                call(:local_log_entry, :gen_categorize_email, started_at, t0, out, nil, { 'pre_filter' => hf, 'facets' => facets }))
            end

            ss = call(:hr_eval_soft, {
              'subject' => subj, 'body' => body,
              'from' => input['from'], 'headers' => input['headers'], 'attachments' => input['attachments']
            }, soft_pack)
            decision = call(:hr_eval_decide, ss[:score], (rules['thresholds'].is_a?(Hash) ? rules['thresholds'] : {}))
            if decision == 'IRRELEVANT'
              out = {
                'mode' => (input['mode'] || 'embedding'),
                'chosen' => (input['fallback_category'].presence || 'Irrelevant'),
                'confidence' => 0.0,
                'pre_filter' => { 'score' => ss[:score], 'matched_signals' => ss[:matched], 'decision' => decision }
              }.merge(call(:telemetry_envelope, t0, cid, true, 200, 'OK'))
              facets = call(:compute_facets_for!, 'gen_categorize_email', out, { 'decision' => 'IRRELEVANT' })
              (out['telemetry'] ||= {})['facets'] = facets
              return call(:local_log_attach!, out,
                call(:local_log_entry, :gen_categorize_email, started_at, t0, out, nil, { 'pre_filter' => out['pre_filter'], 'facets' => facets }))
            end
            # REVIEW path: continue to normal classification; you already expose chosen/confidence downstream
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

            emb_resp = call(:predict_embeddings, connection, emb_model_path, [email_inst] + cat_insts, {}, cid)
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
              referee   = call(:llm_referee, connection, input['generative_model'], email_text, shortlist, cats, input['fallback_category'], cid, (input['referee_system_preamble'].presence || nil))
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
            referee = call(:llm_referee, connection, input['generative_model'], email_text, cats.map { |c| c['name'] }, cats, input['fallback_category'], cid, (input['referee_system_preamble'].presence || nil))
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
          result = result.merge(call(:telemetry_envelope, t0, cid, true, 200, 'OK'))

          # Facets (compact metrics block) + local log
          facets = call(:compute_facets_for!, 'gen_categorize_email', result)
          (result['telemetry'] ||= {})['facets'] = facets
          call(:local_log_attach!, result,
            call(:local_log_entry, :gen_categorize_email, started_at, t0, result, nil, {
              'category'   => result['chosen'],
              'confidence' => result['confidence'],
              'facets'     => facets
            }))

          result
        rescue => e
          # Extract Google error
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')

          # Construct telmetry envelope
          env = call(:telemetry_envelope, t0, cid, false, call(:telemetry_parse_error_code, e), msg)

          # Construct and emit debug attachment, as applicable
          if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
            env['debug'] = call(:debug_pack, true, url, req_body, g)
          end

          call(:local_log_attach!, env,
              call(:local_log_entry, :gen_categorize_email, started_at, t0, nil, e, {
                'google_error' => g
              }))

          error(env)
        end
      end,
      sample_output: lambda do
        {
          'mode' => 'embedding',
          'chosen' => 'Billing',
          'confidence' => 0.91,
          'pre_filter' => {
            'score' => 5,
            'matched_signals' => ['mentions_invoice', 'payment_terms'],
            'decision' => 'REVIEW'
          },
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
          'telemetry' => {
            'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr',
            'facets' => {
              'confidence' => 0.91
            }
          }
        }
      end
    },
    rag_answer: {
      title: '[DEPRECATED] RAG Engine: Get grounded response (one-shot)',
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
      display_priority: 1,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      deprecated: true,

      input_fields: lambda do |object_definitions, _connection, _config_fields|
        [
          { name: 'correlation_id', label: 'Correlation ID', optional: true, hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
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
        cid = call(:ensure_correlation_id!, input)
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
                      .headers(call(:request_headers_auth, connection, cid, nil, req_params_re))
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
        # System Prompt
        sys_text = (input['system_preamble'].presence ||
          "Answer using ONLY the retrieved context chunks. If the context is insufficient, reply with “IDK” "\
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
        }.merge(call(:telemetry_envelope, t0, cid, true, call(:telemetry_success_code, gen_resp), 'OK'))

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
                                call(:request_headers_auth, connection, cid, connection['user_project'], req_params_g),
                                gen_req_body))
          end
        end
        facets = call(:compute_facets_for!, 'rag_answer', out)
        (out['telemetry'] ||= {})['facets'] = facets
        call(:local_log_attach!, out,
          call(:local_log_entry, :rag_answer, started_at, t0, out, nil, {
            'confidence'   => (out['confidence'] rescue nil),
            'tokens_total' => out.dig('usage','totalTokenCount') || out.dig('usageMetadata','totalTokenCount'),
            'facets'       => facets
          }))
        out
      rescue => e
        g   = call(:extract_google_error, e)
        msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
        env = call(:telemetry_envelope, t0, cid, false, call(:telemetry_parse_error_code, e), msg)
        if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
          env['debug'] = call(:debug_pack, true, gen_url || retr_url, (gen_req_body || retr_req_body), g)
        end
        # Local logging object
        call(:local_log_attach!, env,
            call(:local_log_entry, :rag_answer, started_at, t0, nil, e, { 'google_error' => g }))
        # Error for retry
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
            'rank' => { 'mode': 'llm', 'model': 'gemini-2.0-flash' },
            'facets' => {
              'retrieval_top_k' => 12,
              'retrieval_filter' => 'similarity',
              'retrieval_filter_val' => 0.8,
              'contexts_returned' => 1,
              'tokens_total' => 454,
              'confidence' => 0.91
            }
          }
        }
      end
    }
  },
  
  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {
    response_templates: lambda do |_connection|
      [
        ['PTO Approved', 'pto_approved'],
        ['PTO Pending Manager', 'pto_pending_manager'],
        ['W-2 Resend', 'w2_resend'],
        ['Escalation to Human', 'escalation_human'],
        ['Benefits Enrollment Confirmation', 'benefits_enrolled'],
        ['Password Reset', 'password_reset'],
        ['Policy Information', 'pto_policy_info'],
        ['Process Information', 'pto_process_info'],
        ['W2 Access Info', 'w2_access_info'],
        ['W2 Timeline Info', 'w2_timeline_info'],
        ['Payroll Process Info', 'payroll_process_info'],
        ['HRIS Portal Access', 'hris_portal_access_info'],
        ['Verification Letters', 'verification_letters_info'],
        ['Generic Policy Info', 'generic_policy_info'],
        ['Custom Template', 'custom']
      ]
    end,
    modes_classification: lambda do |_connection|
      [ ['Embedding (deterministic)', 'embedding'],
        ['Generative (LLM only)',     'generative'],
        ['Hybrid (embed + referee)',  'hybrid'] ]
    end,
    # rules
    rules_modes: lambda do |_connection|
      [ ['None', 'none'], ['Rows (Lookup Table)', 'rows'], ['Compiled JSON', 'json'] ]
    end,
    rerank_emit_shapes: lambda do |_connection|
      # What to emit from rerank for ergonomics/BC:
      # - records_only:     [{id, score, rank}]
      # - enriched_records: [{id, score, rank, content, metadata}]
      # - context_chunks:   enriched records + generator-ready context_chunks
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
    # Sample outputs
    sample_telemetry: lambda do |duration_ms|
      {
        'http_status' => 200,
        'message' => 'OK',
        'duration_ms' => duration_ms || 12,
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
    end,
    sample_ai_policy_filter: lambda do
      policy = {
        'decision' => 'REVIEW',
        'confidence' => 0.62,
        'matched_signals' => ['low_detail', 'missing_context'],
        'reasons' => ['thin context', 'needs review']
      }
      
      business_data = {
        'policy' => policy,
        'short_circuit' => false
      }
      
      facets = {
        'decision' => policy['decision'],
        'confidence' => policy['confidence'],
        'short_circuit' => false,
        'signals_count' => policy['matched_signals'].length,
        'model_used' => 'gemini-2.0-flash',
        'policy_mode' => 'json',
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 20)
      })
    end,
    sample_deterministic_filter: lambda do
      # Business data that would be returned
      pre_filter = { 
        'hit' => false, 
        'score' => 3, 
        'matched_signals' => ['mentions_invoice', 'attachment_pdf'], 
        'decision' => 'REVIEW' 
      }
      
      intent = { 
        'label' => 'transactional',
        'confidence' => 0.7,
        'basis' => 'keywords' 
      }
      
      email_text = "Subject: Invoice #12345 - Payment Due\n\nBody:\nPlease find attached..."
      
      # Build the business data section
      business_data = {
        'pre_filter' => pre_filter,
        'intent' => intent,
        'email_text' => email_text
      }
      
      # Build facets
      facets = {
        'decision_path' => 'soft_eval',
        'final_decision' => 'REVIEW',
        'intent_label' => intent['label'],
        'intent_confidence' => intent['confidence'],
        'intent_basis' => intent['basis'],
        'rules_evaluated' => true,
        'hard_rules_count' => 5,
        'soft_signals_count' => 12,
        'signals_matched' => pre_filter['matched_signals'].length,
        'soft_score' => pre_filter['score'],
        'has_attachments' => true,
        'has_auth_flags' => false,
        'email_length' => email_text.length,
        'special_headers' => true,
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      # Build telemetry
      telemetry = { 
        'http_status' => 200, 
        'message' => 'OK', 
        'duration_ms' => 12, 
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890' 
      }
      
      # Assemble complete output
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => telemetry
      })
    end,
    sample_embed_text_against_categories: lambda do
      scores = [
        { 'category' => 'Billing', 'score' => 0.91, 'cosine' => 0.82 },
        { 'category' => 'Support', 'score' => 0.47, 'cosine' => -0.06 },
        { 'category' => 'Sales', 'score' => 0.35, 'cosine' => -0.30 }
      ]
      
      business_data = {
        'scores' => scores,
        'shortlist' => ['Billing', 'Support', 'Sales']
      }
      
      facets = {
        'categories_count' => scores.length,
        'shortlist_k' => 3,
        'top_score' => scores.first['score'],
        'embedding_model' => 'text-embedding-005',
        'categories_mode' => 'array',
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 11)
      })
    end,
    sample_rerank_shortlist: lambda do
      ranking = [
        { 'category' => 'Billing', 'prob' => 0.86 },
        { 'category' => 'Support', 'prob' => 0.10 },
        { 'category' => 'Sales', 'prob' => 0.04 }
      ]
      
      business_data = {
        'ranking' => ranking,
        'shortlist' => ['Billing', 'Support', 'Sales']
      }
      
      facets = {
        'mode' => 'llm',
        'top_prob' => ranking.first['prob'],
        'categories_ranked' => ranking.length,
        'generative_model' => 'gemini-2.0-flash',
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 18)
      })
    end,
    sample_llm_referee_with_contexts: lambda do
      referee = {
        'category' => 'Billing',
        'confidence' => 0.86,
        'reasoning' => 'Mentions invoice 4411.',
        'distribution' => [
          { 'category' => 'Billing', 'prob' => 0.86 },
          { 'category' => 'Support', 'prob' => 0.10 },
          { 'category' => 'Sales', 'prob' => 0.04 }
        ]
      }
      
      salience = {
        'span' => 'Can you approve the Q4 budget increase by Friday?',
        'reason' => 'Explicit ask with a clear deadline',
        'importance' => 0.92,
        'tags' => ['approval', 'budget', 'deadline'],
        'entities' => [{ 'type' => 'team', 'text' => 'Finance' }],
        'cta' => 'Approve Q4 budget increase',
        'deadline_iso' => '2025-10-24T17:00:00Z',
        'focus_preview' => 'Email (trimmed): ...',
        'responseId' => 'resp-sal-xyz',
        'usage' => { 'promptTokenCount' => 120, 'candidatesTokenCount' => 70, 'totalTokenCount' => 190 },
        'span_source' => 'llm'
      }
      
      business_data = {
        'referee' => referee,
        'chosen' => 'Billing',
        'confidence' => 0.86,
        'salience' => salience
      }
      
      facets = {
        'chosen' => 'Billing',
        'confidence' => 0.86,
        'salience_mode' => 'llm',
        'salience_len' => salience['span'].length,
        'salience_importance' => salience['importance'],
        'has_contexts' => false,
        'shortlist_size' => 3,
        'generative_model' => 'gemini-2.0-flash',
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 19)
      })
    end,
    sample_gen_generate: lambda do
      # Business data
      candidates = [{
        'content' => { 
          'parts' => [{ 'text' => 'Based on the provided context, the invoice total is $1,234.56.' }],
          'role' => 'model'
        },
        'finishReason' => 'STOP',
        'safetyRatings' => [
          { 'category' => 'HARM_CATEGORY_SEXUALLY_EXPLICIT', 'probability' => 'NEGLIGIBLE' },
          { 'category' => 'HARM_CATEGORY_HATE_SPEECH', 'probability' => 'NEGLIGIBLE' }
        ]
      }]
      
      parsed = {
        'answer' => 'Based on the provided context, the invoice total is $1,234.56.',
        'citations' => [
          { 'chunk_id' => 'doc-1#c2', 'source' => 'invoice.pdf', 'uri' => 'gs://bucket/invoice.pdf', 'score' => 0.92 },
          { 'chunk_id' => 'doc-1#c5', 'source' => 'invoice.pdf', 'uri' => 'gs://bucket/invoice.pdf', 'score' => 0.88 }
        ]
      }
      
      usage_metadata = {
        'promptTokenCount' => 245,
        'candidatesTokenCount' => 26,
        'totalTokenCount' => 271
      }
      
      business_data = {
        'responseId' => 'resp-abc123',
        'candidates' => candidates,
        'usageMetadata' => usage_metadata,
        'confidence' => 0.90,
        'parsed' => parsed
      }
      
      # Build facets
      facets = {
        'mode' => 'rag_with_context',
        'model' => 'gemini-2.0-flash',
        'finish_reason' => 'STOP',
        'confidence' => 0.90,
        'has_citations' => true,
        'citations_count' => 2,
        'tokens_prompt' => 245,
        'tokens_candidates' => 26,
        'tokens_total' => 271,
        'confidence_basis' => 'citations_topk_avg',
        'confidence_k' => 3,
        'safety_blocked' => false,
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      # Return with standard envelope
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 150) # Assuming gen calls take ~150ms
      })
    end,
    sample_rank_texts_with_ranking_api: lambda do
      # Business data
      records = [
        {
          'id' => 'doc-1',
          'score' => 0.92,
          'rank' => 1,
          'content' => 'Our PTO policy allows employees to take up to 20 days per year...',
          'metadata' => { 'source' => 'hr-handbook.pdf', 'category' => 'PTO', 'page' => 15 },
          'llm_relevance' => 0.95,
          'category_alignment' => 0.85
        },
        {
          'id' => 'doc-2',
          'score' => 0.84,
          'rank' => 2,
          'content' => 'Vacation requests must be submitted at least 2 weeks in advance...',
          'metadata' => { 'source' => 'hr-handbook.pdf', 'category' => 'PTO', 'page' => 16 },
          'llm_relevance' => 0.86,
          'category_alignment' => 0.78
        }
      ]
      
      context_chunks = [
        {
          'id' => 'doc-1',
          'text' => 'Our PTO policy allows employees to take up to 20 days per year...',
          'score' => 0.92,
          'source' => 'hr-handbook.pdf',
          'uri' => 'gs://bucket/hr-handbook.pdf',
          'metadata' => { 'source' => 'hr-handbook.pdf', 'category' => 'PTO', 'page' => 15 },
          'metadata_kv' => [
            { 'key' => 'source', 'value' => 'hr-handbook.pdf' },
            { 'key' => 'category', 'value' => 'PTO' },
            { 'key' => 'page', 'value' => 15 }
          ],
          'metadata_json' => '{"source":"hr-handbook.pdf","category":"PTO","page":15}',
          'llm_relevance' => 0.95,
          'category_alignment' => 0.85
        },
        {
          'id' => 'doc-2',
          'text' => 'Vacation requests must be submitted at least 2 weeks in advance...',
          'score' => 0.84,
          'source' => 'hr-handbook.pdf',
          'uri' => 'gs://bucket/hr-handbook.pdf',
          'metadata' => { 'source' => 'hr-handbook.pdf', 'category' => 'PTO', 'page' => 16 },
          'metadata_kv' => [
            { 'key' => 'source', 'value' => 'hr-handbook.pdf' },
            { 'key' => 'category', 'value' => 'PTO' },
            { 'key' => 'page', 'value' => 16 }
          ],
          'metadata_json' => '{"source":"hr-handbook.pdf","category":"PTO","page":16}',
          'llm_relevance' => 0.86,
          'category_alignment' => 0.78
        }
      ]
      
      confidence_distribution = [
        { 'id' => 'doc-1', 'probability' => 0.523, 'reasoning' => 'Directly addresses PTO policy limits' },
        { 'id' => 'doc-2', 'probability' => 0.477, 'reasoning' => 'Covers PTO request procedures' }
      ]
      
      ranking_metadata = {
        'category' => 'PTO',
        'llm_model' => 'gemini-2.0-flash',
        'contexts_filtered' => 0,
        'contexts_ranked' => 2
      }
      
      business_data = {
        'records' => records,
        'context_chunks' => context_chunks,
        'confidence_distribution' => confidence_distribution,
        'ranking_metadata' => ranking_metadata
      }
      
      # Build facets
      facets = {
        'ranking_api' => 'llm.category_ranker',
        'category' => 'PTO',
        'llm_model' => 'gemini-2.0-flash',
        'emit_shape' => 'context_chunks',
        'records_input' => 5,
        'records_filtered' => 0,
        'records_ranked' => 2,
        'records_output' => 2,
        'has_distribution' => true,
        'top_score' => 0.92,
        'category_filtered' => false,
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      # Return with standard envelope
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 85)
      })
    end,
    sample_rag_retrieve_contexts_enhanced: lambda do
      contexts = [
        {
          'id' => 'ctx-1',
          'text' => 'Our company policy allows up to 20 days of PTO per year for full-time employees.',
          'score' => 0.92,
          'source' => 'hr-handbook.pdf',
          'uri' => 'gs://bucket/hr-handbook.pdf',
          'metadata' => { 'page' => 15, 'section' => 'Benefits' },
          'metadata_kv' => [
            { 'key' => 'page', 'value' => '15' },
            { 'key' => 'section', 'value' => 'Benefits' }
          ],
          'metadata_json' => '{"page":15,"section":"Benefits"}',
          'is_pdf' => true,
          'processing_error' => false
        },
        {
          'id' => 'ctx-2', 
          'text' => 'Employees must submit PTO requests at least 2 weeks in advance.',
          'score' => 0.87,
          'source' => 'hr-handbook.pdf',
          'uri' => 'gs://bucket/hr-handbook.pdf',
          'metadata' => { 'page' => 16, 'section' => 'Benefits' },
          'metadata_kv' => [
            { 'key' => 'page', 'value' => '16' },
            { 'key' => 'section', 'value' => 'Benefits' }
          ],
          'metadata_json' => '{"page":16,"section":"Benefits"}',
          'is_pdf' => true,
          'processing_error' => false
        }
      ]
      
      business_data = {
        'question' => 'What is our PTO policy?',
        'contexts' => contexts
      }
      
      facets = {
        'top_k' => 20,
        'contexts_count' => 2,
        'success_count' => 2,
        'error_count' => 0,
        'pdf_contexts_count' => 2,
        'partial_failure' => false,
        'filter_type' => 'similarity',
        'filter_value' => 0.7,
        'rank_mode' => 'llmRanker',
        'rank_model' => 'gemini-2.0-flash',
        'network_error' => false,
        'correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 45) # RAG retrieval typically ~45ms
      })
    end,
    # Output
    build_complete_output: lambda do |result, action_id, extras={}|
      return result unless result.is_a?(Hash)
      
      # Extract core business fields (exclude system fields)
      exclude_keys = %w[ok telemetry complete_output]
      business_fields = result.select { |k, _| !exclude_keys.include?(k) }
      
      # Build facets if compute_facets_for! is available
      facets = extras.any? ? call(:compute_facets_for!, action_id, result, extras) : {}
      
      # Add complete_output as a clean package
      result['complete_output'] = business_fields
      result['facets'] = facets if facets.any?
      
      result
    end,
    # Template processing helpers
    process_template: lambda do |template_str, data|
      return template_str if template_str.blank? || data.blank?
      
      result = template_str.dup
      data.each do |key, value|
        next if value.nil? || value.to_s.strip.empty?
        # Handle both {key} and {{key}} formats
        result.gsub!(/\{\{?\s*#{Regexp.escape(key.to_s)}\s*\}?\}/, value.to_s)
      end
      
      # Warn about missing placeholders (optional)
      missing = result.scan(/\{[^}]+\}/)
      if missing.any?
        # Could log or handle missing placeholders
        result += "\n\n[Note: Missing data for: #{missing.join(', ')}]"
      end
      
      result
    end,
    build_template_prompt: lambda do |template, data, additional_context = nil|
      # Build a prompt that instructs the model to use the template
      prompt = "Generate a response using the following template:\n\n"
      
      if template.is_a?(Hash)
        prompt += "Subject: #{template['subject']}\n" if template['subject']
        prompt += "Body: #{template['body']}\n\n" if template['body']
        prompt += "Required data fields: #{template['required_data'].join(', ')}\n" if template['required_data']
      else
        prompt += template.to_s + "\n\n"
      end
      
      prompt += "\nData to use for placeholders:\n"
      data.each do |key, value|
        prompt += "- #{key}: #{value}\n" if value.present?
      end
      
      prompt += "\n#{additional_context}" if additional_context.present?
      prompt += "\n\nPlease fill in the template with the provided data and ensure the response is professional and complete."
      
      prompt
    end,
    parse_template_json: lambda do |json_str|
      return nil if json_str.blank?
      begin
        parsed = JSON.parse(json_str)
        # Validate template structure
        unless parsed.is_a?(Hash) && (parsed['subject'] || parsed['body'])
          error('Invalid template JSON: must contain "subject" and/or "body" fields')
        end
        parsed
      rescue JSON::ParserError => e
        error("Invalid template JSON: #{e.message}")
      end
    end,
    fetch_template_from_library: lambda do |template_id|
      templates = {
        'pto_policy_info' => {
          'id' => 'pto_policy_info',
          'category' => 'PTO',
          'subject' => 'RE: PTO policy information',
          'body' => "For your reference: {policy_snippet}\n\nYou can review additional details here: {policy_link}",
          'required_data' => ['policy_snippet', 'policy_link'],
          'bindings' => {
            'policy_snippet' => ['policy_context.pto_policy', 'information_data.pto_policy'],
            'policy_link' => ['resources_data.pto_policy_link']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['direct_question', 'policy_question', 'how_do_i', 'what_is', 'when_is', 'where_is'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'pto_process_info' => {
          'id' => 'pto_process_info',
          'category' => 'PTO',
          'subject' => 'RE: How PTO works',
          'body' => "Here is the standard approach for PTO: {next_steps}\n\nTypical timing: {timeline}",
          'required_data' => ['next_steps', 'timeline'],
          'bindings' => {
            'next_steps' => ['next_steps_data.pto_process'],
            'timeline' => ['timeline_data.pto_processing']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['direct_question', 'how_do_i', 'policy_question'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'w2_access_info' => {
          'id' => 'w2_access_info',
          'category' => 'W2',
          'subject' => 'RE: How to access W-2 ({tax_year})',
          'body' => "W-2s for {tax_year} are available: {availability_text}\n\nAccess here: {portal_link}",
          'required_data' => ['tax_year', 'availability_text', 'portal_link'],
          'bindings' => {
            'tax_year' => ['extracted_data.tax_year'],
            'availability_text' => ['information_data.w2_availability'],
            'portal_link' => ['resources_data.employee_portal']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['direct_question', 'where_is', 'how_do_i', 'what_is'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'w2_timeline_info' => {
          'id' => 'w2_timeline_info',
          'category' => 'W2',
          'subject' => 'RE: W-2 timeline ({tax_year})',
          'body' => 'Timeline for {tax_year} W-2: {w2_timeline}',
          'required_data' => ['tax_year', 'w2_timeline'],
          'bindings' => {
            'tax_year' => ['extracted_data.tax_year'],
            'w2_timeline' => ['timeline_data.w2_availability']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['when_is', 'direct_question'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'payroll_process_info' => {
          'id' => 'payroll_process_info',
          'category' => 'Payroll',
          'subject' => 'RE: Payroll process information',
          'body' => "For payroll questions, the standard review is: {review_steps}\n\nTypical timing: {review_timeline}\n\nMore info: {resource_link}",
          'required_data' => ['review_steps', 'review_timeline', 'resource_link'],
          'bindings' => {
            'review_steps' => ['next_steps_data.payroll_review_process', 'next_steps_data.payroll_review'],
            'review_timeline' => ['timeline_data.payroll_review'],
            'resource_link' => ['resources_data.payroll_faq']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['how_do_i', 'what_is', 'where_is', 'direct_question'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'hris_portal_access_info' => {
          'id' => 'hris_portal_access_info',
          'category' => 'HRIS/Portal Access',
          'subject' => 'RE: Accessing the HR portal',
          'body' => "Portal: {portal_link}\n\nSystem: {system_name}\n\nIf guidance is needed: {access_help}",
          'required_data' => ['portal_link', 'system_name', 'access_help'],
          'bindings' => {
            'portal_link' => ['resources_data.hris_portal_link', 'resources_data.employee_portal'],
            'system_name' => ['system_data.hris_name'],
            'access_help' => ['information_data.portal_access_help', 'policy_context.portal_access_help']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['how_do_i', 'where_is', 'direct_question'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'verification_letters_info' => {
          'id' => 'verification_letters_info',
          'category' => 'Verification & Letters',
          'subject' => 'RE: Employment verification',
          'body' => "For employment verification, use: {verification_portal}\n\nStandard details included: {verification_details}",
          'required_data' => ['verification_portal', 'verification_details'],
          'bindings' => {
            'verification_portal' => ['resources_data.verification_portal'],
            'verification_details' => ['information_data.verification_details']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['how_do_i', 'where_is', 'what_is', 'direct_question'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        },

        'generic_policy_info' => {
          'id' => 'generic_policy_info',
          'category' => 'Other',
          'subject' => 'RE: Policy information',
          'body' => "Here is the relevant policy information: {policy_snippet}\n\nReference: {policy_link}",
          'required_data' => ['policy_snippet', 'policy_link'],
          'bindings' => {
            'policy_snippet' => ['policy_context.policy_snippet', 'information_data.policy_snippet'],
            'policy_link' => ['resources_data.policy_link']
          },
          'eligibility' => {
            'email_type' => 'direct_request',
            'intent_kind' => 'information_request',
            'intent_signals_any' => ['policy_question', 'what_is', 'how_do_i', 'direct_question'],
            'prohibit_signals_any' => ['first_person_request'],
            'min_confidence' => 0.60
          },
          'null_on_missing_grounding' => true,
          'response_type' => 'information_provided'
        }
      }

      templates[template_id] || error("Template '#{template_id}' not found")
    end,
    # Schema helpers
    build_llm_config_from_schema: lambda do |schema_input|
      return {} unless schema_input.is_a?(Hash)
      
      structure = schema_input['structure'] || {}
      validation = schema_input['validation_rules'] || {}
      
      # Build responseSchema from structure
      response_schema = {
        'type' => 'object',
        'additionalProperties' => false,
        'properties' => {},
        'required' => []
      }
      
      structure.each do |field, spec|
        prop = {
          'type' => spec['type'] || 'string'
        }
        
        # Apply constraints from schema
        prop['enum'] = spec['enum'] if spec['enum'] && validation['strict_enum']
        prop['minimum'] = spec['minimum'] if spec['minimum']
        prop['maximum'] = spec['maximum'] if spec['maximum']
        prop['minItems'] = spec['minItems'] if spec['minItems']
        prop['maxItems'] = spec['maxItems'] if spec['maxItems']
        prop['items'] = spec['items'] if spec['items']
        
        response_schema['properties'][field] = prop
        response_schema['required'] << field if spec['required']
      end
      
      # Build system prompt enhancement
      prompt_addition = ""
      if validation['provide_example']
        example = {}
        structure.each do |field, spec|
          example[field] = spec['example'] || spec['default'] || 
                          case spec['type']
                          when 'string' then spec['enum'] ? spec['enum'].first : ""
                          when 'number' then 0.5
                          when 'array' then []
                          when 'object' then {}
                          else nil
                          end
        end
        prompt_addition = "\n\nExample valid output:\n#{JSON.pretty_generate(example)}"
      end
      
      {
        'response_schema' => response_schema,
        'prompt_addition' => prompt_addition,
        'validation_rules' => validation
      }
    end,
    validate_against_schema: lambda do |response, schema_input, apply_defaults = true|
      structure = schema_input['structure'] || {}
      
      # Validate each field
      structure.each do |field, spec|
        value = response[field]
        
        # Apply defaults if missing
        if value.nil? && apply_defaults && spec['default']
          response[field] = spec['default']
          value = spec['default']
        end
        
        # Validate enums
        if spec['enum'] && !spec['enum'].include?(value)
          response[field] = spec['enum'].first # Coerce to valid value
        end
        
        # Validate ranges
        if spec['type'] == 'number' && value
          value = value.to_f
          value = spec['minimum'] if spec['minimum'] && value < spec['minimum']
          value = spec['maximum'] if spec['maximum'] && value > spec['maximum']
          response[field] = value
        end
      end
      
      response
    end,
    expand_schema_template: lambda do |template, variables = {}|
      # Deep clone the template
      expanded = JSON.parse(template.to_json)
      
      # Replace variables like $CATEGORIES
      json_str = expanded.to_json
      variables.each do |key, value|
        json_str.gsub!("\"$#{key}\"", value.to_json)
      end
      
      JSON.parse(json_str)
    end,
    validate_grounding!: lambda do |category, context_data, categories_config|
      # Find category configuration
      cat_config = categories_config.find { |c| c['name'] == category } || {}
      grounding_req = cat_config.dig('responder_profile', 'grounding', 'any_of') || []
      
      return { 'valid' => true, 'has_grounding' => false } if grounding_req.empty?
      
      # Check if any required grounding is present
      found_fields = []
      grounding_req.each do |path|
        parts = path.split('.')
        value = parts.reduce(context_data) { |data, part| data.is_a?(Hash) ? data[part] : nil }
        found_fields << path if value.present?
      end
      
      if found_fields.empty?
        {
          'valid' => false,
          'has_grounding' => false,
          'missing' => grounding_req,
          'message' => "Missing required grounding for #{category}: need one of #{grounding_req.join(' OR ')}"
        }
      else
        {
          'valid' => true,
          'has_grounding' => true,
          'found' => found_fields,
          'message' => "Found grounding: #{found_fields.join(', ')}"
        }
      end
    end,
    validate_policy_schema!: lambda do |response, schema|
      structure = schema['structure'] || {}
      
      # Check required fields
      required_fields = structure.select { |k,v| v['required'] == true }.keys
      missing = required_fields - response.keys
      error("Policy missing required fields: #{missing.join(', ')}") unless missing.empty?
      
      # Validate decision enum strictly
      if response['decision']
        valid_decisions = structure.dig('decision', 'enum') || ['IRRELEVANT', 'REVIEW', 'KEEP']
        unless valid_decisions.include?(response['decision'])
          response['decision'] = 'REVIEW' # Safe default
          response['_validation_warnings'] ||= []
          response['_validation_warnings'] << "Invalid decision corrected to REVIEW"
        end
      end
      
      # Validate confidence range
      if response['confidence']
        conf = response['confidence'].to_f
        response['confidence'] = [[conf, 0.0].max, 1.0].min
      end
      
      # Validate arrays have correct types
      ['matched_signals', 'reasons', 'escalation_reasons'].each do |field|
        if response[field] && !response[field].is_a?(Array)
          response[field] = [response[field].to_s]
        end
      end
      
      response
    end,
    pre_generation_gate!: lambda do |input|
      # Check all requirements for generation
      checks = {
        'email_type' => input['email_type'] == 'direct_request',
        'intent' => input['intent_kind'] == 'information_request',
        'confidence' => (input['confidence'].to_f || 0) >= 0.60,
        'category_valid' => input['category'].present?,
        'no_chain_detected' => input['chain_detected'] != true,
        'no_safety_flags' => input['safety_blocked'] != true,
        'has_email_text' => input['email_text'].present?
      }
      
      failed = checks.select { |k,v| !v }.keys
      if failed.any?
        return {
          'gate_passed' => false,
          'failed_checks' => failed,
          'should_generate' => false,
          'reason' => case failed.first
            when 'email_type' then 'Not a direct request'
            when 'intent' then 'Not an information request'
            when 'confidence' then 'Confidence below threshold'
            when 'no_chain_detected' then 'Email chain detected'
            when 'no_safety_flags' then 'Safety concerns detected'
            else 'Missing required fields'
          end
        }
      end
      
      { 'gate_passed' => true, 'should_generate' => true }
    end,
    validate_config_alignment!: lambda do |policy_config, semantic_config, categories|
      issues = []
      warnings = []
      
      # Check category alignment
      policy_cats = policy_config.dig('schema', 'category') || []
      defined_cats = categories.map { |c| c['name'] }
      
      missing_in_defs = policy_cats - defined_cats
      missing_in_policy = defined_cats - policy_cats
      
      issues << "Categories in policy but not defined: #{missing_in_defs.join(', ')}" if missing_in_defs.any?
      warnings << "Categories defined but not in policy: #{missing_in_policy.join(', ')}" if missing_in_policy.any?
      
      # Check confidence threshold alignment
      policy_gen_min = policy_config['min_confidence_for_generation'].to_f
      semantic_keep_min = (semantic_config.dig('thresholds', 'keep_min').to_f / 10.0) rescue 0.0
      
      if (policy_gen_min - semantic_keep_min).abs > 0.1
        warnings << "Confidence threshold mismatch: policy=#{policy_gen_min}, semantic≈#{semantic_keep_min}"
      end
      
      # Check email type requirements
      policy_types = policy_config.dig('schema', 'email_type') || []
      semantic_excludes = semantic_config.dig('hard_exclude', 'forwarded_chain') ? ['forwarded_chain'] : []
      
      # Check intent alignment
      policy_intents = policy_config.dig('schema', 'intent_kind') || []
      if !policy_intents.include?('information_request')
        issues << "Policy must include 'information_request' as an intent_kind for generation"
      end
      
      {
        'valid' => issues.empty?,
        'issues' => issues,
        'warnings' => warnings,
        'message' => issues.any? ? "Config issues: #{issues.join('; ')}" : "Configuration aligned"
      }
    end,
    # PDF handling
    sanitize_pdf_text: lambda do |raw_text|
      begin
        return '' if raw_text.nil?
        
        text = raw_text.to_s
        
        # Limit input size upfront to prevent performance issues
        if text.length > 50000
          text = text[0..50000] + '...[truncated]'
        end
        
        # Phase 1: Fix double-escaped sequences (most common PDF artifact)
        text = text
          .gsub(/\\\\n/, "\n")        # Convert \\n to actual newline
          .gsub(/\\\\t/, " ")         # Convert \\t to space (not tab to avoid layout issues)
          .gsub(/\\\\r/, "")          # Remove \\r entirely
          .gsub(/\\\\"/, '"')         # Convert \\" to "
          .gsub(/\\\\'/, "'")         # Convert \\' to '
          .gsub(/\\\\/, "\\")         # Cleanup remaining double backslashes
        
        # Phase 2: Handle single-escaped sequences (if not caught above)
        text = text
          .gsub(/\\n/, "\n")          # Convert \n to newline
          .gsub(/\\t/, " ")           # Convert \t to space
          .gsub(/\\r/, "")            # Remove \r
        
        # Phase 3: Remove control characters except tab, newline, carriage return
        control_chars = (0..31).map { |i| i.chr }.join
        keep_chars = "\t\n\r"
        chars_to_remove = control_chars.delete(keep_chars)
        text = text.delete(chars_to_remove)
        
        # Phase 4: Fix PDF-specific layout artifacts
        text = text
          .gsub(/(\w)-\n(\w)/, '\1\2')    # Rejoin hyphenated words across lines
          .gsub(/\r\n|\r/, "\n")          # Normalize line endings
          .gsub(/\n{3,}/, "\n\n")         # Collapse excessive newlines
          .gsub(/[ \t]+/, ' ')            # Collapse spaces and tabs
          .gsub(/^[ \t]+|[ \t]+$/m, '')   # Trim line starts/ends (multiline mode)
          .gsub(/##(\w)/, '## \1')        # Add space after ## headers
          .gsub(/\.\.+$/, '...')          # Normalize ellipsis at end
        
        # Phase 5: Fix common PDF extraction patterns
        text = text
          .gsub(/\s*\n\s*-\s*/, "\n• ")   # Convert hyphen lists to bullets
          .gsub(/([.!?])\s*\n+([A-Z])/, '\1 \2')  # Join sentences split across lines
          .gsub(/\[\s*\]/, '')            # Remove empty brackets
          .gsub(/\(\s*\)/, '')            # Remove empty parentheses
        
        # Phase 6: Final cleanup and encoding
        text = text
          .strip                          # Remove leading/trailing whitespace
          .gsub(/\s+\n/, "\n")           # Remove trailing spaces on lines
          .gsub(/\n\s+/, "\n")           # Remove leading spaces on lines
        
        # Truncate if still too long after cleaning
        if text.length > 10000
          text = text[0..10000] + '...[truncated]'
        end
        
        # Final encoding cleanup with multiple fallbacks
        begin
          # Try UTF-8 first
          text.encode('UTF-8', 
                      invalid: :replace, 
                      undef: :replace, 
                      replace: ' ')
              .strip
        rescue Encoding::UndefinedConversionError => e
          # Fallback: try Windows-1252 (common in PDFs)
          begin
            text.encode('UTF-8', 'Windows-1252', 
                        invalid: :replace, 
                        undef: :replace, 
                        replace: '?')
                .strip
          rescue
            # Last resort: ASCII-safe version
            text.encode('ASCII', 
                        invalid: :replace, 
                        undef: :replace, 
                        replace: '?')
                .encode('UTF-8')
                .strip
          end
        end
        
      rescue => e
        # Return sanitized error message if all else fails
        "[PDF processing error: #{e.message[0..100]}]"
      end
    end,
    is_pdf_source?: lambda do |source_uri, metadata|
      begin
        # Return false if we have nothing to check
        return false if source_uri.nil? && metadata.nil?
        
        # Check source URI extension (most reliable)
        uri = source_uri.to_s.downcase.strip
        if uri.present?
          # Direct PDF extension check
          return true if uri.end_with?('.pdf')
          
          # Check for URL-encoded PDF extension
          return true if uri.include?('.pdf?') || uri.include?('.pdf#')
          
          # Check for PDF in path even if not at end
          return true if uri.include?('/pdf/') || uri.include?('_pdf_')
        end
        
        # Check metadata for file type indicators
        if metadata.is_a?(Hash)
          # Check explicit file type fields
          file_type = (metadata['file_type'] || 
                      metadata['fileType'] || 
                      metadata['mimeType'] || 
                      metadata['mime_type'] || 
                      metadata['content_type'] || 
                      metadata['contentType'] || '').to_s.downcase
          
          return true if file_type.include?('pdf')
          
          # Check source/filename in metadata
          source = (metadata['source'] || 
                    metadata['filename'] || 
                    metadata['file_name'] || 
                    metadata['fileName'] || 
                    metadata['name'] || '').to_s.downcase
          
          return true if source.end_with?('.pdf')
          
          # Check for PDF processor indicators
          processor = (metadata['processor'] || 
                      metadata['parser'] || '').to_s.downcase
          
          return true if processor.include?('pdf') || 
                        processor.include?('document_ai') || 
                        processor.include?('ocr')
          
          # Check for page numbers (strong PDF indicator)
          return true if metadata.key?('page') || 
                        metadata.key?('pageNumber') || 
                        metadata.key?('page_number')
        end
        
        false
        
      rescue => e
        # Default to false if detection fails
        puts "Error in is_pdf_source?: #{e.message}"
        false
      end
    end,
    safe_extract_contexts: lambda do |response|
      begin
        return [] unless response.is_a?(Hash)
        
        # Primary path: response.contexts.contexts (Vertex AI RAG standard)
        if response.dig('contexts', 'contexts').is_a?(Array)
          return response.dig('contexts', 'contexts')
        end
        
        # Secondary: response.contexts as array
        if response['contexts'].is_a?(Array)
          return response['contexts']
        end
        
        # Tertiary: look for any 'contexts' key recursively (with depth limit)
        find_contexts_array = lambda do |obj, depth = 0|
          return nil if depth > 3  # Prevent infinite recursion
          
          case obj
          when Hash
            # Check if this hash has a 'contexts' key with an array
            if obj['contexts'].is_a?(Array) && !obj['contexts'].empty?
              # Verify it looks like context objects (has expected fields)
              first = obj['contexts'].first
              if first.is_a?(Hash) && 
                (first.key?('text') || first.key?('chunkText') || 
                  first.key?('chunkId') || first.key?('sourceUri'))
                return obj['contexts']
              end
            end
            
            # Recursively search hash values
            obj.each_value do |v|
              result = find_contexts_array.call(v, depth + 1)
              return result if result
            end
          when Array
            # Check if this looks like a contexts array directly
            if !obj.empty? && obj.first.is_a?(Hash) && 
              (obj.first.key?('text') || obj.first.key?('chunkId') || 
                obj.first.key?('chunkText'))
              return obj
            end
            
            # Search array elements
            obj.each do |elem|
              result = find_contexts_array.call(elem, depth + 1)
              return result if result
            end
          end
          nil
        end
        
        # Try to find contexts anywhere in response
        found = find_contexts_array.call(response)
        return found if found
        
        # Last resort: empty array
        []
        
      rescue => e
        # Log the error for debugging but don't fail
        puts "Error in safe_extract_contexts: #{e.message}"
        []
      end
    end,
    llm_category_aware_ranker: lambda do |connection, model, query, category, category_context, contexts, corr=nil|
      model_path = call(:build_model_path_with_global_preview, connection, model)
      
      system_text = <<~SYS
        You are an expert relevance scorer for a knowledge retrieval system.
        Given a query within a specific category, evaluate how well each context answers the query.
        
        Scoring criteria:
        1. Relevance (0-1): How directly the context addresses the query
        2. Category Alignment (0-1): How well the context fits the category domain
        
        Be precise and calibrated in your scoring. Output valid JSON only.
      SYS
      
      # Build context descriptions
      context_list = contexts.map { |c|
        text_preview = (c['content'] || c['text']).to_s[0..400]
        source = (c['metadata'] || {})['source'] || 'unknown'
        
        "ID: #{c['id']}\nSource: #{source}\nContent: #{text_preview}"
      }.join("\n\n---\n\n")
      
      user_prompt = <<~USR
        Category: #{category}
        #{category_context.present? ? "Category Context: #{category_context}" : ""}
        
        Query: #{query}
        
        Contexts to evaluate:
        #{context_list}
        
        Score each context for relevance to the query and alignment with the category.
      USR
      
      payload = {
        'systemInstruction' => { 'role' => 'system', 'parts' => [{ 'text' => system_text }] },
        'contents' => [{ 'role' => 'user', 'parts' => [{ 'text' => user_prompt }] }],
        'generationConfig' => {
          'temperature' => 0,
          'maxOutputTokens' => 1024,
          'responseMimeType' => 'application/json',
          'responseSchema' => {
            'type' => 'object',
            'properties' => {
              'rankings' => {
                'type' => 'array',
                'items' => {
                  'type' => 'object',
                  'properties' => {
                    'id' => { 'type' => 'string' },
                    'relevance' => { 'type' => 'number' },
                    'category_alignment' => { 'type' => 'number' },
                    'reasoning' => { 'type' => 'string' }
                  },
                  'required' => ['id', 'relevance', 'category_alignment']
                }
              }
            },
            'required' => ['rankings']
          }
        }
      }
      
      loc = (model_path[/\/locations\/([^\/]+)/, 1] || 'global').to_s.downcase
      url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
      
      resp = post(url)
              .headers(call(:request_headers_auth, connection, corr || call(:build_correlation_id), 
                            connection['user_project'], "model=#{model_path}"))
              .payload(call(:json_compact, payload))
      
      text = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
      JSON.parse(text) rescue { 'rankings' => [] }
    end,
    coerce_integer: lambda do |v, fallback|
      Integer(v) rescue fallback
    end,
    path_rag_retrieve_contexts: lambda do |connection|
      "/v1/projects/#{call(:ensure_project_id!, connection)}/locations/#{call(:ensure_location!, connection)}:retrieveContexts"
    end,
    safe_parse_json: lambda do |maybe_string|
      return maybe_string unless maybe_string.is_a?(String)
      begin
        parse_json(maybe_string)
      rescue
        # If the server lied about content-type or returned pretty text,
        # keep the original; the caller can handle/log.
        maybe_string
      end
    end,
    ensure_project_id!: lambda do |connection|
      pid = connection['project'].to_s.strip
      error('Project is required') if pid.empty?
      pid
    end,
    ensure_location!: lambda do |connection|
      loc = connection['location'].to_s.strip
      error('Location is required') if loc.empty?
      loc
    end,
    default_headers: lambda do |connection|
      # Deliberately keep these minimal to avoid duplication of headers
      {
      'Content-Type' => 'application/json; charset=utf-8',
      'Accept' => 'application/json'
      }
    end,
    aiplatform_host: lambda do |connection|
      "#{call(:ensure_location!, connection)}-aiplatform.googleapis.com"
    end,
    build_retrieval_opts_from_input!: lambda do |input|
      cfg  = (input['rag_retrieval_config'].is_a?(Hash) ? input['rag_retrieval_config'] : {})
      filt = cfg['filter'].is_a?(Hash) ? cfg['filter'] : {}
      rank = cfg['ranking'].is_a?(Hash) ? cfg['ranking'] : {}

      topk = cfg['top_k'] || input['similarity_top_k']
      dist = filt['vector_distance_threshold']   || input['vector_distance_threshold']
      sim  = filt['vector_similarity_threshold'] || input['vector_similarity_threshold']
      rsm  = rank['rank_service_model']          || input['rank_service_model']
      llm  = rank['llm_ranker_model']            || input['llm_ranker_model']

      # Validate oneof unions
      call(:guard_threshold_union_0!, dist, sim)
      call(:guard_ranker_union_0!, rsm, llm)

      {
        'topK'                      => topk,
        'vectorDistanceThreshold'   => dist,
        'vectorSimilarityThreshold' => sim,
        'rankServiceModel'          => rsm,
        'llmRankerModel'            => llm
      }.delete_if { |_k, v| v.nil? || v == '' }
    end,
    aipl_service_host_0: lambda do |connection, loc=nil|
      l = (loc || connection['location']).to_s.downcase
      (l.blank? || l == 'global') ? 'aiplatform.googleapis.com' : "#{l}-aiplatform.googleapis.com"
    end,
    aipl_v1_url_0: lambda do |connection, loc, path|
      "https://#{call(:aipl_service_host_0, connection, loc)}/v1/#{path}"
    end,
    normalize_rag_corpus_0: lambda do |connection, raw|
      v = raw.to_s.strip
      return '' if v.blank?
      return v if v.start_with?('projects/')
      # Allow short form: just corpus id -> expand using connection project/region
      call(:ensure_project_id_0!, connection)
      loc = (connection['location'] || '').to_s.downcase
      error("RAG corpus requires regional location; got '#{loc}'") if loc.blank? || loc == 'global'
      "projects/#{connection['project_id']}/locations/#{loc}/ragCorpora/#{v}"
    end,
    ensure_project_id_0!: lambda do |connection|
      # Method mutates caller-visible state, but this is a known and desired side effect.
      pid = (connection['project_id'].presence ||
              (JSON.parse(connection['service_account_key_json'].to_s)['project_id'] rescue nil)).to_s
      error('Project ID is required (not found in connection or key)') if pid.blank?
      connection['project_id'] = pid
      pid
    end,
    build_rag_retrieve_payload_0: lambda do |question, rag_corpus, restrict_ids = [], opts = {}|
      # Optional opts:
      #   'topK', 'vectorDistanceThreshold', 'vectorSimilarityThreshold',
      #   'rankServiceModel', 'llmRankerModel'

      rag_res = { 'ragCorpus' => rag_corpus }
      ids     = call(:sanitize_drive_ids_0, restrict_ids, allow_empty: true, label: 'restrict_to_file_ids')
      rag_res['ragFileIds'] = ids if ids.present?

      query = { 'text' => question.to_s }
      rr_cfg = {}
      if opts.is_a?(Hash)
        if opts['topK']
          rr_cfg['topK'] = call(:clamp_int_0, (opts['topK'] || 0), 1, 200)
        end
        # Filter union
        dist = opts['vectorDistanceThreshold']
        sim  = opts['vectorSimilarityThreshold']
        call(:guard_threshold_union_0!, dist, sim)
        filt = {}
        filt['vectorDistanceThreshold']   = call(:safe_float_0, dist) if !dist.nil?
        filt['vectorSimilarityThreshold'] = call(:safe_float_0, sim)  if !sim.nil?
        rr_cfg['filter'] = filt unless filt.empty?

        # Ranking union
        rsm = (opts['rankServiceModel'].to_s.strip)
        llm = (opts['llmRankerModel'].to_s.strip)
        call(:guard_ranker_union_0!, rsm, llm)
        if rsm != ''
          rr_cfg['ranking'] = { 'rankService' => { 'modelName' => rsm } }
        elsif llm != ''
          rr_cfg['ranking'] = { 'llmRanker'   => { 'modelName' => llm } }
        end
      end
      query['ragRetrievalConfig'] = rr_cfg unless rr_cfg.empty?

      {
        'query'          => query,
        'vertexRagStore' => { 'ragResources'  => [rag_res] }
      }
    end,
    sanitize_drive_ids_0: lambda do |raw_list, allow_empty: false, label: 'drive_file_ids'|
      # 1) normalize → 2) drop empties → 3) de-dup
      norm = call(:safe_array_0, raw_list)
              .map { |x| call(:normalize_drive_file_id_0, x) }
              .reject { |x| x.to_s.strip.empty? }
              .uniq
      return [] if norm.empty? && allow_empty
      error("No valid Drive IDs found in #{label}. Remove empty entries or fix links.") if norm.empty?
      bad = norm.find { |id| id !~ /\A[A-Za-z0-9_-]{8,}\z/ }
      error("Invalid Drive ID in #{label}: #{bad}") if bad
      norm
    end,
    safe_array_0: lambda do |v|
      return [] if v.nil? || v == false
      return v  if v.is_a?(Array)
      [v]
    end,
    normalize_drive_file_id_0:   lambda { |raw| call(:normalize_drive_resource_id_0, raw) },
    normalize_drive_resource_id_0: lambda do |raw|
      # Accept strings, datapill Hashes, and common Drive URLs → bare ID
      return '' if raw.nil? || raw == false
      v =
        if raw.is_a?(Hash)
          raw['id'] || raw[:id] ||
          raw['fileId'] || raw[:fileId] ||
          raw['value'] || raw[:value] ||
          raw['name'] || raw[:name] ||
          raw['path'] || raw[:path] ||
          raw.to_s
        else
          raw
        end.to_s.strip
      return '' if v.empty? || %w[null nil none undefined - (blank)].include?(v.downcase)

      if v.start_with?('http://', 'https://')
        if (m = v.match(%r{/file/d/([^/?#]+)}))      then v = m[1]
        elsif (m = v.match(%r{/folders/([^/?#]+)}))  then v = m[1]
        elsif (m = v.match(/[?&]id=([^&#]+)/))       then v = m[1]
        end
      end

      if v.include?('=>') || v.include?('{') || v.include?('}')
        begin
          j = JSON.parse(v) rescue nil
          if j.is_a?(Hash)
            v = j['id'] || j['fileId'] || j['value'] || ''
          end
        rescue; end
      end

      prior = v.dup
      v = v[/[A-Za-z0-9_-]+/].to_s
      v = '' if v.length < 8 && prior.start_with?('http')
      v
    end,
    clamp_int_0: lambda do |n, min, max|
      [[n.to_i, min].max, max].min
    end,
    safe_float_0: lambda do |v|
      return nil if v.nil?; Float(v) rescue v.to_f
    end,
    guard_threshold_union_0!: lambda do |dist, sim|
      return true if dist.nil? || dist.to_s == ''
      return true if sim.nil?  || sim.to_s  == ''
      error('Provide only one of vector_distance_threshold OR vector_similarity_threshold')
    end,
    guard_ranker_union_0!: lambda do |rank_service_model, llm_ranker_model|
      r = rank_service_model.to_s.strip
      l = llm_ranker_model.to_s.strip
      return true if r.empty? || l.empty?
      error('Provide only one of rank_service_model OR llm_ranker_model')
    end,
    normalize_retrieve_contexts_0!: lambda do |raw_resp|
      b = raw_resp.is_a?(Hash) || raw_resp.is_a?(Array) ? raw_resp : {}

      # Fast paths
      if b.is_a?(Hash)
        return Array(b['contexts']) if b['contexts'].is_a?(Array)

        if b['contexts'].is_a?(Hash)
          inner = b['contexts']
          return Array(inner['contexts']) if inner['contexts'].is_a?(Array)
        end
      end

      # Recursive search for a key literally named "contexts" whose value is an Array
      finder = lambda do |obj|
        case obj
        when Hash
          obj.each do |k, v|
            return v if k.to_s == 'contexts' && v.is_a?(Array)
            if v.is_a?(Hash) || v.is_a?(Array)
              found = finder.call(v)
              return found if found
            end
          end
        when Array
          obj.each do |e|
            found = finder.call(e)
            return found if found
          end
        end
        nil
      end

      Array(finder.call(b))
    end,
    map_context_chunks_0: lambda do |raw_contexts, maxn = 20|
      call(:safe_array_0, raw_contexts).first(maxn).each_with_index.map do |c, i|
        h  = c.is_a?(Hash) ? c : {}
        md = (
          h['metadata'] ||
          h['structuredMetadata'] ||
          h['customMetadata'] ||
          {}
        )
        md = md.is_a?(Hash) ? md : {}

        # Prefer explicit keys; fall back to common alternates
        cid = h['chunkId'] || h['id'] || h['name'] || h['chunk_id'] || "ctx-#{i+1}"
        txt = h['text'] || h['content'] || h['chunkText'] || h['chunk_text'] || ''
        scr = (
          h['score'] || h['relevanceScore'] || h['relevance_score'] ||
          h['similarity'] || h['rankScore']
        )

        src = (
          h['sourceDisplayName'] || md['source'] || md['displayName'] || h['source']
        )
        uri = (
          h['sourceUri'] || h['uri'] || md['uri'] || md['gcsUri'] || md['url']
        )

        # Normalize types
        scr = scr.to_f if !scr.nil?

        {
          'id'            => cid.to_s,
          'text'          => txt.to_s,
          'score'         => (scr || 0.0).to_f,
          'source'        => src,
          'uri'           => uri,
          'metadata'      => md,
          'metadata_kv'   => md.map { |k,v| { 'key' => k.to_s, 'value' => v } },
          'metadata_json' => (md.empty? ? nil : md.to_json)
        }
      end
    end,
    telemetry_envelope_0: lambda do |started_at, correlation_id, ok, code, message|
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
    telemetry_success_code_0: lambda do |resp|
      (resp.is_a?(Hash) && (resp['status'] || resp['status_code'])) ? (resp['status'] || resp['status_code']).to_i : 200
    end,
    telemetry_parse_error_code_0: lambda do |err|
      begin
        if err.respond_to?(:[])
          code = err['status'] || err.dig('response', 'status') ||
                err.dig('response', 'status_code') || err.dig('error', 'code')
          return code.to_i if code
        end
      rescue; end
      begin
        body = (err.respond_to?(:[]) && err.dig('response','body')).to_s
        j = JSON.parse(body) rescue nil
        c = j && j.dig('error','code')
        return c.to_i if c
      rescue; end
      m = err.to_s.match(/\b(\d{3})\b/)
      m ? m[1].to_i : 500
    end,
    extract_google_error_0: lambda do |err|
      begin
        body = (err.respond_to?(:[]) && err.dig('response','body')).to_s
        json = JSON.parse(body) rescue nil
        if json
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
          return { 'message' => json['message'], 'raw' => json } if json['message']
        end
      rescue
      end
      {}
    end,
    build_correlation_id_0: lambda do
      SecureRandom.uuid
    end,
    json_compact_0: lambda do |obj|
      case obj
      when Hash
        obj.each_with_object({}) do |(k, v), h|
          next if v.nil?
          cv = call(:json_compact_0, v)
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
        obj.map { |e| call(:json_compact_0, e) }.reject do |cv|
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
    http_body_json_0: lambda do |resp|
      if resp.is_a?(Hash) && resp.key?('body')
        parsed = (JSON.parse(resp['body']) rescue nil)
        parsed.nil? ? {} : parsed
      elsif resp.is_a?(String)
        (JSON.parse(resp) rescue {}) || {}
      elsif resp.is_a?(Hash) || resp.is_a?(Array)
        resp
      else
        {}
      end
    end,

    # Ultra-tolerant unwrappers
    unwrap_json_like!: lambda do |v|
      return v if v.is_a?(Hash) || v.is_a?(Array)
      return (call(:safe_json, v) || v) if v.is_a?(String)
      v
    end,

    find_contexts_array_any!: lambda do |v|
      # Depth-first search for an Array of context objects.
      # Handles:
      #   {contexts:[...]}
      #   {contexts:{contexts:[...]}}
      #   stringified JSON at any level
      #   wrappers like {body|data|response|payload|result: ...}
      v = call(:unwrap_json_like!, v)
      if v.is_a?(Array)
        return v if !v.empty? && v.first.is_a?(Hash) &&
                    (v.first.key?('text') || v.first.key?('chunkText') ||
                    v.first.key?('sourceUri') || v.first.key?('chunkId'))
        v.each do |e|
          r = call(:find_contexts_array_any!, e)
          return r if r
        end
        return nil
      elsif v.is_a?(Hash)
        c = v['contexts']
        c = call(:unwrap_json_like!, c) if c
        return c if c.is_a?(Array)
        return c['contexts'] if c.is_a?(Hash) && c['contexts'].is_a?(Array)
        %w[body data result response payload vertexRagStore].each do |k|
          r = call(:find_contexts_array_any!, v[k])
          return r if r
        end
        v.each_value do |e|
          r = call(:find_contexts_array_any!, e)
          return r if r
        end
      end
      nil
    end,
    coerce_contexts_array!: lambda do |maybe|
      # 1) try as-is
      arr = call(:find_contexts_array_any!, maybe)
      arr = Array(arr)
      # 2) if elements are stringified JSON contexts, parse them
      arr = arr.map { |e| e.is_a?(String) ? (call(:safe_json, e) || e) : e }
      # 3) final filter: keep only Hash elements (context objects)
      arr.select { |e| e.is_a?(Hash) }
    end,
    # --- JSON helpers (gentle, friendly errors) -------------------------------
    json_parse_gently!: lambda do |raw|
      return raw if raw.is_a?(Hash) || raw.is_a?(Array)
      s = raw.to_s.strip
      return nil if s.empty?
      # Common copy/paste mistakes: Ruby hashes or smart quotes
      if s.include?('=>')
        error('Invalid JSON: looks like a Ruby hash (=>). Convert to JSON (":" and double quotes).')
      end
      begin
        JSON.parse(s)
      rescue JSON::ParserError => e
        head = s.gsub(/[\r\n\t]/, ' ')[0, 120]
        error("Invalid JSON: #{e.message.split(':').first}. Starts with: #{head.inspect}")
      end
    end,
    safe_json_obj!: lambda do |raw|
      v = call(:json_parse_gently!, raw)
      error('Invalid JSON for policy: empty input') if v.nil?
      error('Invalid JSON for policy: expected object') unless v.is_a?(Hash)
      v
    end,
    safe_json_arr!: lambda do |raw|
      v = call(:json_parse_gently!, raw)
      
      # Handle object with "categories" key (common wrapper format)
      if v.is_a?(Hash) && v['categories'].is_a?(Array)
        return v['categories']
      end
      
      error('Invalid JSON for categories: expected array or object with "categories" array') unless v.is_a?(Array)
      v
    end,
    # UI assembly helpers (schema-by-config)
    ui_show_advanced_toggle: lambda do |default=false|
      { name: 'show_advanced', label: 'Show advanced options',
        type: 'boolean', control_type: 'checkbox',
        default: default, sticky: true, extends_schema: true,
        hint: 'Toggle to reveal advanced parameters.' }
    end,
    ui_truthy: lambda do |v|
      (v == true) || (v.to_s.downcase == 'true') || (v.to_s == '1')
    end,
    ui_df_inputs: lambda do |object_definitions, cfg|
      [
        { name: 'email', label: 'Email', type: 'object',
          properties: Array(object_definitions['email_envelope']), optional: false },
        { name: 'rules_mode', control_type: 'select', default: 'none', pick_list: 'rules_modes' },
        { name: 'rules_rows', ngIf: 'input.rules_mode == "rows"',
          type: 'array', of: 'object', properties: Array(object_definitions['rule_rows_table']), optional: true },
        { name: 'rules_json', ngIf: 'input.rules_mode == "json"', optional: true, control_type: 'text-area' },
        { name: 'fallback_category', optional: true, default: 'Other' }
      ]
    end,
    ui_policy_inputs: lambda do |object_definitions, cfg|
      adv = call(:ui_truthy, cfg['show_advanced'])
      base = [
        { name: 'email_text', optional: false },
        { name: 'policy_mode', label: 'Policy input mode', control_type: 'select',
          options: [['None','none'], ['JSON','json']], default: 'none', optional: false,
          extends_schema: true, hint: 'Switch to use JSON policy for testing.' }
      ]
      base << { name: 'model', label: 'Generative model', control_type: 'text',
                optional: true, default: 'gemini-2.0-flash' } if adv
      base << { name: 'policy_json', label: 'Policy JSON',
                ngIf: 'input.policy_mode == "json"', optional: true,
                hint: 'Paste policy spec JSON for testing (overrides defaults this run).' } if adv
      base << { name: 'confidence_short_circuit', type: 'number', optional: true, default: 0.8,
                hint: 'Short-circuit only when decision=IRRELEVANT and confidence ≥ this value.' }
      base
    end,
    ui_embed_inputs: lambda do |object_definitions, cfg|
      adv = call(:ui_truthy, cfg['show_advanced'])
      base = [
        { name: 'email_text', optional: false },
        { name: 'categories_mode', label: 'Categories input mode', control_type: 'select',
          options: [['Array (pills)','array'], ['JSON','json']], default: 'array',
          optional: false, extends_schema: true, hint: 'Switch to paste categories as JSON.' },
        { name: 'categories', type: 'array', of: 'object', optional: true,
          ngIf: 'input.categories_mode == "array"',
          properties: Array(object_definitions['category_def']) }
      ]
      if adv
        base += [
          { name: 'embedding_model', control_type: 'text', optional: true, default: 'text-embedding-005' },
          { name: 'shortlist_k', type: 'integer', optional: true, default: 3 },
          { name: 'categories_json', label: 'Categories JSON',
            ngIf: 'input.categories_mode == "json"', optional: true, control_type: 'text-area',
            hint: 'Paste categories array JSON for testing (overrides pills this run).' }
        ]
      end
      base
    end,
    ui_rerank_inputs: lambda do |object_definitions, cfg|
      adv = call(:ui_truthy, cfg['show_advanced'])
      base = [
        { name: 'email_text', optional: false },
        { name: 'categories_mode', label: 'Categories input mode', control_type: 'select',
          options: [['Array (pills)','array'], ['JSON','json']], default: 'array',
          optional: false, extends_schema: true, hint: 'Switch to paste categories as JSON.' },
        { name: 'categories', type: 'array', of: 'object', optional: true,
          ngIf: 'input.mode == "llm" && input.categories_mode == "array"',
          properties: Array(object_definitions['category_def']) },
        { name: 'shortlist', type: 'array', of: 'string', optional: false },
        { name: 'mode', control_type: 'select', optional: false, default: 'none',
          options: [['None','none'], ['LLM','llm']], extends_schema: true }
      ]
      if adv
        base << { name: 'generative_model', control_type: 'text', optional: true, default: 'gemini-2.0-flash',
                  ngIf: 'input.mode == "llm"' }
        base << { name: 'categories_json', label: 'Categories JSON', control_type: 'text-area',
                  ngIf: 'input.mode == "llm" && input.categories_mode == "json"', optional: true,
                  hint: 'Paste categories array JSON for testing (overrides pills this run).' }

      end
      base
    end,
    ui_ref_inputs: lambda do |object_definitions, cfg|
      adv = call(:ui_truthy, cfg['show_advanced'])
      base = [
        { name: 'email_text', optional: false },
        { name: 'categories_mode', label: 'Categories input mode', control_type: 'select',
          options: [['Array (pills)','array'], ['JSON','json']], default: 'array',
          optional: false, extends_schema: true, hint: 'Switch to paste categories as JSON.' },
        { name: 'categories', type: 'array', of: 'object', optional: true,
          ngIf: 'input.categories_mode == "array"',
          properties: Array(object_definitions['category_def']) },
        { name: 'shortlist', type: 'array', of: 'string', optional: true,
          hint: 'If omitted, all categories are allowed.' },
        { name: 'generative_model', control_type: 'text', optional: true, default: 'gemini-2.0-flash' },
        { name: 'min_confidence', type: 'number', optional: true, default: 0.25 },
        { name: 'fallback_category', optional: true, default: 'Other' }
      ]
      if adv
        base << { name: 'categories_json', label: 'Categories JSON', control_type: 'text-area',
                  ngIf: 'input.categories_mode == "json"', optional: true,
                  hint: 'Paste categories array JSON for testing (overrides pills this run).' }
      end
      base
    end,
    ui_retrieve_inputs: lambda do |object_definitions, cfg|
      adv = call(:ui_truthy, cfg['show_advanced'])
      base = [
        { name: 'email_text', optional: false, hint: 'Free text query (use norm_email_envelope upstream if you have subject/body).' },
        { name: 'rag_corpus', label: 'RAG corpus ID', optional: false, hint: 'Vertex RAG corpus name/id.' }
      ]
      if adv
        props = Array(object_definitions['rag_retrieval_config'])
        if props.empty?
          props = [
            { name: 'top_k', type: 'integer', hint: 'Max contexts to return (default 6).' },
            { name: 'filter', type: 'object', properties: [
                { name: 'vector_distance_threshold', type: 'number' },
                { name: 'vector_similarity_threshold', type: 'number' }
            ]},
            { name: 'ranking', type: 'object', properties: [
                { name: 'rank_service_model' },
                { name: 'llm_ranker_model' }
            ]}
          ]
        end
        base << { name: 'rag_retrieval_config', label: 'Retrieval config', type: 'object', optional: true,
                  properties: props }
      end
      base
    end,

    # Steps
    step_begin!: lambda do |action_id, input|
      { 'action'=>action_id.to_s, 'started_at'=>Time.now.utc.iso8601,
        't0'=>Time.now, 'cid'=>call(:ensure_correlation_id!, input) }
    end,
    step_ok!: lambda do |ctx, result, code=200, msg='OK', extras=nil|
      env = call(:telemetry_envelope, ctx['t0'], ctx['cid'], true, code, msg)
      out = (result || {}).merge(env)
      
      # Build complete output
      out = call(:build_complete_output, out, ctx['action'], (extras || {}))

      call(:local_log_attach!, out,
        call(:local_log_entry, ctx['action'], ctx['started_at'], ctx['t0'], out, nil, (extras||{})))
    end,
    step_err!: lambda do |ctx, err|
      g   = call(:extract_google_error, err)
      msg = [err.to_s, (g['message'] || nil)].compact.join(' | ')
      env = call(:telemetry_envelope, ctx['t0'], ctx['cid'], false, call(:telemetry_parse_error_code, err), msg)
      call(:local_log_attach!, env,
        call(:local_log_entry, ctx['action'], ctx['started_at'], ctx['t0'], nil, err, { 'google_error'=>g }))
      error(env)
    end,

    # --- Normalizers (single source of truth) --------------------------------
    norm_email_envelope!: lambda do |h|
      s = (h || {}).to_h
      subj = (s['subject'] || s[:subject]).to_s
      body = (s['body']    || s[:body]).to_s
      error('Provide subject and/or body') if subj.strip.empty? && body.strip.empty?
      {
        'subject'=>subj, 'body'=>body, 'from'=>s['from'] || s[:from],
        'headers'=> (s['headers'].is_a?(Hash) ? s['headers'] : {}),
        'attachments'=> Array(s['attachments']),
        'auth'=> (s['auth'].is_a?(Hash) ? s['auth'] : {}),
        'email_text'=>call(:build_email_text, subj, body)
      }
    end,
    norm_categories!: lambda do |raw|
      cats = call(:safe_array, raw).map { |c|
        c.is_a?(String) ? { 'name'=>c } :
          { 'name'=>c['name'] || c[:name],
            'description'=>c['description'] || c[:description],
            'examples'=>call(:safe_array,(c['examples'] || c[:examples])) }
      }.select { |c| c['name'].to_s.strip != '' }
      error('At least 2 categories are required') if cats.length < 2
      cats
    end,

    # --- Header helper (auth + routing) -----------------------------------------
    # Unified auth+header builder.
    request_headers_auth: lambda do |connection, correlation_id=nil, user_project=nil, request_params=nil, opts=nil|
      # Backward-compatible signature; final arg `opts` is optional:
      #   opts = {
      #     scopes:        Array|String (default: cloud-platform),
      #     force_remint:  true|false   (default: false),
      #     cache_salt:    String       (optional logical namespace for token cache)
      #   }
      opts ||= {}
      begin
        # Resolve scopes and whether to bypass cache for this call
        scopes = opts.key?(:scopes) ? call(:auth_normalize_scopes, opts[:scopes]) : call(:const_default_scopes)
        force  = (opts[:force_remint] == true)
        salt   = opts[:cache_salt].to_s.strip
        on_error = opts[:on_error]

        token =
          if force
            # Explicit per-call remint (does NOT raise here; callers handle errors)
            fresh = call(:auth_issue_token!, connection, scopes)
            connection['access_token'] = fresh['access_token'].to_s
            fresh['access_token'].to_s
          else
            # Normal path: reuse cached token or mint if missing/expired
            t = connection['access_token'].to_s
            if t.empty?
              t = call(:auth_build_access_token!, connection, scopes: scopes)
              connection['access_token'] = t
            end
            t.to_s
          end

        h = {
          'X-Correlation-Id' => correlation_id.to_s,
          'Content-Type'     => 'application/json',
          'Accept'           => 'application/json'
        }
        # FIX: use the actual token variable; only set when non-empty.
        h['Authorization'] = "Bearer #{token}" unless token.to_s.empty?

        up = user_project.to_s.strip
        h['x-goog-user-project']   = up unless up.empty?

        rp = request_params.to_s.strip
        h['x-goog-request-params'] = rp unless rp.empty?

        h
      rescue => e
        # Harden behavior: optionally return nil so callers (e.g., logger) can skip the call.
        # Also drop a breadcrumb for debugging (non-prod).
        begin
          unless call(:normalize_boolean, connection['prod_mode'])
            connection['__last_tail_log_error'] = {
              timestamp: Time.now.utc.iso8601,
              method: 'request_headers_auth',
              message: e.message.to_s[0,512],
              class: e.class.to_s
            }
          end
        rescue; end
        return nil if on_error == :return_nil
        # Fallback: minimal headers (legacy behavior)
        { 'X-Correlation-Id' => correlation_id.to_s,
          'Content-Type'     => 'application/json',
          'Accept'           => 'application/json' }
      end
    end,
    # --- Salience blending (no extraction) --------------------------------------
    maybe_append_salience: lambda do |email_text, salience, importance|
      imp = (importance.to_f rescue 0.0)
      return email_text if !salience.is_a?(Hash) && !salience.is_a?(Array)
      return email_text if imp <= 0.0
      block = salience.is_a?(Array) ? salience.compact.map(&:to_s).join(', ') : call(:json_compact, salience)
      "#{email_text}\n\nSignals (weight=#{imp}):\n#{block}"
    end,
    # Compile Workato-native "single-table" rows into the rulepack JSON
    hr_compile_rulepack_from_rows!: lambda do |rows|
      arr = call(:safe_array, rows)
      # normalize columns -> strings
      rows_n = arr.map { |r| (r || {}).to_h.transform_keys(&:to_s) }
      on = lambda { |r| (r['enabled'].to_s.strip.downcase != 'false') }
      parse_list = lambda do |s|
        str = s.to_s
        if str.include?('|')
          str.split('|').map { |x| x.strip }.reject(&:empty?)
        elsif str.include?(',')
          str.split(',').map { |x| x.strip }.reject(&:empty?)
        else
          str
        end
      end
      sortd = rows_n.select(&on).sort_by { |r| (r['priority'].to_i rescue 999) }
      hard = sortd.select { |r| r['family'] == 'HARD' }
      soft = sortd.select { |r| r['family'] == 'SOFT' }
      thr  = sortd.select { |r| r['family'] == 'THRESHOLD' }
      grd  = sortd.select { |r| r['family'] == 'GUARD' }

      hard_pack = {}
      hard.each do |r|
        fld = r['field'].to_s
        (hard_pack[fld] ||= []) << {
          'operator' => r['operator'],
          'pattern'  => r['pattern'],
          'value'    => parse_list.call(r['pattern']),
          'action'   => r['action']
        }
      end

      soft_pack = soft.map do |r|
        {
          'name'          => (r['rule_id'] || r['notes'] || 'signal'),
          'field'         => r['field'],
          'pattern_type'  => r['operator'],
          'pattern_value' => r['pattern'],
          'weight'        => (r['weight'].to_i rescue 0),
          'cap_per_email' => (r['cap_per_email'].to_s == '' ? nil : r['cap_per_email'].to_i)
        }
      end

      thr_pack = {}
      thr.each do |r|
        key = r['pattern'].to_s # keep | triage_min | triage_max
        val = (r['weight'].to_i rescue 0)
        thr_pack[key] = val if key != ''
      end

      guards = {}
      grd.each do |r|
        cat = r['category'].to_s; next if cat == ''
        g = (guards[cat] ||= { 'required' => [], 'forbidden' => [], 'flags' => {} })
        case r['operator']
        when 'required'  then g['required']  << r['pattern']
        when 'forbidden' then g['forbidden'] << r['pattern']
        when 'value'     then g['flags'][(r['pattern'] || '').to_s] = (r['flag_a'] || r['flag_b'] || true)
        when 'is_true'   then g['flags'][(r['pattern'] || '').to_s] = true
        end
      end

      {
        'hard_exclude' => hard_pack,
        'soft_signals' => soft_pack,
        'thresholds'   => thr_pack,
        'guards'       => guards
      }
    end,
    hr_rx:        lambda { |s| Regexp.new(s) rescue nil },
    hr_list:      lambda { |s| s.to_s.split(/[|,]/).map { |x| x.strip.downcase }.reject(&:empty?) },
    hr_pick:      lambda { |email|
      {
        'subject'     => email['subject'].to_s,
        'body'        => email['body'].to_s,
        'from'        => email['from'].to_s,
        'headers'     => (email['headers'] || {}),
        'attachments' => Array(email['attachments']).map { |a| a['filename'].to_s.downcase },
        'auth'        => (email['auth'] || {})
      }
    },
    hr_eval_hard?: lambda do |email, hard_pack|
      f = call(:hr_pick, email)

      # generic helper
      match = lambda do |field, rule|
        case rule['operator']
        when 'equals'
          k, v = rule['pattern'].to_s.split(':', 2)
          val = (field == 'headers' ? f['headers'][k.to_s] : f[field]).to_s.downcase
          return val.start_with?((v || '').downcase)
        when 'contains'
          f[field].to_s.downcase.include?(rule['pattern'].to_s.downcase)
        when 'regex'
          (rx = call(:hr_rx, rule['pattern'])) && f[field].to_s =~ rx
        when 'header_present'
          f['headers'].key?(rule['pattern'].to_s)
        when 'ext_in'
          exts = call(:hr_list, rule['pattern'])
          f['attachments'].any? { |fn| exts.any? { |e| fn.end_with?(".#{e}") || fn =~ /\.(#{e})$/ } }
        when 'is_true'
          # security flags carried on email['auth']
          f['auth'][rule['pattern'].to_s] == true
        else false
        end
      end

      hard_pack.each do |field, rules|
        Array(rules).each do |r|
          if match.call(field, r)
            return { hit: true, action: r['action'] || 'exclude', reason: "#{field}:#{r['operator']}" }
          end
        end
      end

      { hit: false }
    end,
    hr_eval_soft: lambda do |email, signals|
      f = call(:hr_pick, email)
      score = 0; hits = []

      signals.each do |s|
        field = (s['field'] || 'any')
        text_sources =
          case field
          when 'subject' then [f['subject']]
          when 'body'    then [f['body']]
          when 'from'    then [f['from']]
          when 'headers' then [f['headers'].to_json]
          when 'attachments' then [f['attachments'].join(' ')]
          else [f['subject'], f['body']]
          end

        matched =
          case s['pattern_type']
          when 'regex'
            (rx = call(:hr_rx, s['pattern_value'])) && text_sources.any? { |t| t =~ rx }
          when 'contains'
            needles = call(:hr_list, s['pattern_value'])
            text_sources.any? { |t| nt = t.to_s.downcase; needles.any? { |n| nt.include?(n) } }
          when 'ext_in'
            exts = call(:hr_list, s['pattern_value'])
            f['attachments'].any? { |fn| exts.any? { |e| fn.end_with?(".#{e}") || fn =~ /\.(#{e})$/ } }
          when 'header_present'
            f['headers'].key?(s['pattern_value'].to_s)
          else false
          end

        if matched
          score += s['weight'].to_i
          hits << (s['name'] || 'signal')
        end
      end

      { score: score, matched: hits }
    end,
    hr_eval_decide: lambda do |score, thr|
      keep = (thr['keep'] || 6).to_i
      lo   = (thr['triage_min'] || 4).to_i
      hi   = (thr['triage_max'] || 5).to_i
      if score >= keep then 'HR-REQUEST'
      elsif score >= lo && score <= hi then 'REVIEW'
      else 'IRRELEVANT'
      end
    end,
    hr_eval_guards_ok?: lambda do |category, email, guards|
      g = guards[category] || {}
      f = {
        'subject' => email['subject'].to_s,
        'body'    => email['body'].to_s
      }

      Array(g['required']).each do |rxs|
        rx = call(:hr_rx, rxs); return false unless rx && (f['subject'] =~ rx || f['body'] =~ rx)
      end
      Array(g['forbidden']).each do |rxs|
        rx = call(:hr_rx, rxs); return false if rx && (f['subject'] =~ rx || f['body'] =~ rx)
      end
      true
    end,
    ensure_correlation_id!: lambda do |input|
      cid = input['correlation_id']
      (cid.is_a?(String) && cid.strip != '') ? cid.strip : SecureRandom.uuid
    end,

    # Heuristic: average of top-K citation scores, clamped to [0,1]. Returns nil if no scores.
    overall_confidence_from_citations: lambda do |citations, k=3|
      arr = Array(citations).map { |c| (c.is_a?(Hash) ? c['score'] : nil) }.compact.map(&:to_f)
      return nil if arr.empty?
      topk = arr.sort.reverse.first([[k, 1].max, arr.length].min)
      avg  = topk.sum / topk.length.to_f
      [[avg, 0.0].max, 1.0].min.round(4)
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
  
      # Get telemetry for correlation_id (always needed)
      tel = out['telemetry'].is_a?(Hash) ? out['telemetry'] : {}

      # Action-specific facet extraction
      case action_id.to_s
      when 'rag_retrieve_contexts_enhanced'
        # Extract from extras passed by the action
        retrieval = extras['retrieval'] || {}
        rank = extras['rank'] || {}
        
        h['top_k'] = retrieval['top_k']
        h['contexts_count'] = retrieval['contexts_count']
        h['success_count'] = retrieval['success_count']
        h['error_count'] = retrieval['error_count']
        h['pdf_contexts_count'] = retrieval['pdf_contexts_count']
        h['partial_failure'] = retrieval['partial_failure']
        
        if retrieval['filter']
          h['filter_type'] = retrieval['filter']['type']
          h['filter_value'] = retrieval['filter']['value']
        end
        
        if rank && rank.any?
          h['rank_mode'] = rank['mode']
          h['rank_model'] = rank['model']
        end
        
        h['network_error'] = extras['network_error'].present?
        
      when 'gen_generate'
        # For generation actions, extract from output
        # Context counts
        ctxs = out['contexts'] || out['context_chunks']
        h['contexts_returned'] = Array(ctxs).length if ctxs
        
        # Token usage
        usage = out['usage'] || out['usageMetadata']
        h.merge!(call(:_facet_tokens, usage)) if usage
        
        # Generation outcome fields
        fr = call(:_facet_finish_reason, out)
        h['gen_finish_reason'] = call(:_facet_str, fr) if fr
        
        # Safety blocked
        c0 = Array(out['candidates']).first || {}
        h['safety_blocked'] = call(:_facet_safety_blocked?, c0) if c0.any?
        
        # Confidence
        h['confidence'] = call(:_facet_float, out['confidence']) if out.key?('confidence')
        
        # Abstention detection
        ans = out['answer']
        if ans.is_a?(String)
          h['answered_unknown'] = true if ans.strip =~ /\A(?i:(i\s+don['']?t\s+know|cannot\s+answer|no\s+context|not\s+enough\s+context))/
        end
        
      else
        # For all other actions, just merge the extras directly
        # (deterministic_filter, ai_policy_filter, embed_text_against_categories, etc.)
        # These actions already provide clean extras
        extras_clean = extras.is_a?(Hash) ? extras.dup : {}
        extras_clean.delete_if { |_k,v| v.is_a?(Array) || v.is_a?(Hash) } # avoid bloat
        h.merge!(extras_clean)
      end
      
      # Always surface correlation_id for downstream grouping
      if tel['correlation_id'].to_s.strip != ''
        h['correlation_id'] = tel['correlation_id'].to_s.strip
      end
      
      # Final compaction: drop nils/empties
      h.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
      h
    end,

    # -- Local logging 
    local_log_entry: lambda do |action_id, started_at, t0, result=nil, err=nil, extras=nil|
      now  = Time.now
      beg  = started_at || now.utc.iso8601
      dur  = (t0 ? ((now.to_f - t0.to_f) * 1000).round : nil)
      {
        'ts'           => now.utc.iso8601,
        'action'       => action_id.to_s,
        'started_at'   => beg,
        'ended_at'     => now.utc.iso8601,
        'latency_ms'   => dur,
        'status'       => err ? 'error' : 'ok',
        'correlation'  => (result && result.dig('telemetry','correlation_id')),
        'http_status'  => (result && result.dig('telemetry','http_status')),
        'message'      => (result && result.dig('telemetry','message')),
        'error_class'  => (err && err.class.to_s),
        'error_msg'    => (err && err.message.to_s[0,512]),
        'extras'       => (extras.is_a?(Hash) ? extras : nil)
      }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
    end,
    local_log_attach!: lambda do |container, entry|
      begin
        tel = (container['telemetry'] ||= {})
        arr = (tel['local_logs'] ||= [])
        arr << entry if entry.is_a?(Hash) && !entry.empty?
      rescue
        # don’t raise from logging
      end
      container
    end,

    gen_generate_core!: lambda do |connection, input, corr=nil|
      t0   = Time.now
      corr = (corr.to_s.strip.empty? ? call(:build_correlation_id) : corr.to_s.strip)
      mode = (input['mode'] || 'plain').to_s

      # Pre-generation validation for RAG mode
      if mode == 'rag_with_context'
        gate_check = call(:pre_generation_gate!, input)
        unless gate_check['gate_passed']
          return {
            'status' => 'blocked',
            'gate_check' => gate_check,
            'responseId' => nil,
            'candidates' => [],
            'confidence' => 0.0,
            'telemetry' => call(:telemetry_envelope, t0, corr, false, 200, gate_check['reason'])
          }
        end
      end

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
          <<~SYSPROMPT
            CRITICAL OUTPUT REQUIREMENTS:
            1. Your ENTIRE response must be valid JSON - no text before or after
            2. Use EXACTLY the schema provided - no extra fields
            3. DO NOT include markdown formatting, code blocks, or backticks
            4. If context is insufficient, set status to "insufficient_context"
            
            Answer using ONLY the provided context chunks. Keep answers concise and cite chunk IDs.
            
            Example structure:
            {"answer":"Based on context...", "citations":[{"chunk_id":"c1","source":"doc.pdf"}]}
          SYSPROMPT
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
        h  = c.is_a?(Hash) ? c : {}
        # metadata may arrive as a JSON string
        md_raw = h['metadata']
        md     = md_raw.is_a?(Hash) ? md_raw : (call(:safe_json, md_raw) || {})
        # text/id/score aliases across API variants
        text = h['text'] ||
               h['chunkText'] ||
               h.dig('context','text') ||
               h.dig('documentContext','text') ||
               ''
        src = h['sourceDisplayName'] || md['source']
        uri = h['sourceUri'] || md['uri'] || md['gcsUri'] || md['url']
        {
          'id'            => (h['chunkId'] || h['id'] || "ctx-#{i+1}"),
          'text'          => text.to_s,
          'score'         => (h['score'] || h['relevanceScore'] || 0.0).to_f,
          'source'        => src,
          'uri'           => uri,
          'metadata'      => md,
          'metadata_kv'   => md.map { |k,v| { 'key' => k.to_s, 'value' => v } },
          'metadata_json' => (md.empty? ? nil : md.to_json)
        }
      end
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
    llm_referee: lambda do |connection, model, email_text, shortlist_names, all_cats, fallback_category = nil, corr=nil, system_preamble=nil|
      # Minimal, schema-constrained JSON referee using Gemini
      model_path = call(:build_model_path_with_global_preview, connection, model)
      req_params = "model=#{model_path}"

      cats_norm = call(:safe_array, all_cats).map { |c| c.is_a?(Hash) ? c : { 'name' => c.to_s } }
      allowed   = if shortlist_names.present?
                     call(:safe_array, shortlist_names).map { |x| x.is_a?(Hash) ? (x['name'] || x[:name]).to_s : x.to_s }
                   else
                     cats_norm.map { |c| c['name'] }
                   end
      # System Prompt (overrideable per-run)
      system_text = system_preamble.presence || <<~SYS
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
    extract_salient_span!: lambda do |connection, subject, body, model='gemini-2.0-flash', max_span=500, temperature=0, corr=nil, system_preamble=nil|
      plain = call(:email_minify, subject, body)
      focus = call(:email_focus_trim, plain, 8000)

      # System Prompt (overrideable per-run)
      system_text = system_preamble.presence || (
        "Extract the single most important sentence or short paragraph from an email. " \
        "Return valid JSON only. Keep the extracted span under #{max_span} characters. " \
        "importance is a calibrated score in [0,1]."
      )

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
    end,

    # --- Log-assemble helpers --------------------------------------------
    la_json_parse_safe!: lambda do |text|
      return [] unless text.is_a?(String) && text.strip != ''
      begin
        j = JSON.parse(text)
        j.is_a?(Array) ? j : [j]
      rescue
        text.lines.map(&:strip).reject(&:empty?).map { |ln| JSON.parse(ln) rescue nil }.compact
      end
    end,
    la_parse_ts: lambda do |v|
      case v
      when Integer then Time.at(v / 1000.0).utc
      when Float   then Time.at(v / 1000.0).utc
      when String
        begin Time.parse(v).utc rescue nil end
      else nil
      end
    end,
    la_ms_between: lambda do |t1, t2|
      return nil unless t1 && t2
      ((t2 - t1) * 1000.0).round
    end,
    la_flatten_hash: lambda do |h, prefix=''|
      return {} unless h.is_a?(Hash)
      flat = {}
      h.each do |k,v|
        key = prefix == '' ? k.to_s : "#{prefix}.#{k}"
        if v.is_a?(Hash)
          flat.merge!(call(:la_flatten_hash, v, key))
        else
          flat[key] = v
        end
      end
      flat
    end,
    la_norm_entry!: lambda do |raw|
      h = raw.is_a?(Hash) ? raw.dup : {}
      # Lift common aliases
      h['ts']            ||= h['timestamp'] || h['time'] || h['t']
      h['level']         ||= (h['severity'] || 'INFO').to_s.upcase
      h['action']        ||= h['action_name'] || h['actor'] || 'unknown'
      h['event']         ||= h['stage'] || h['step'] || 'event'
      h['status']        ||= (h['ok'] == false || h['error'] ? 'error' : 'ok')
      h['correlation_id']||= h['correlation'] || h['trace_id'] || h.dig('telemetry','correlation_id')
      # Timestamps
      t = call(:la_parse_ts, h['ts'])
      h['ts'] = (t || Time.now.utc).iso8601(3)
      # Latency
      if h['latency_ms'].is_a?(String) then h['latency_ms'] = h['latency_ms'].to_i end
      if !h['latency_ms'] && (h['t_start'] || h['t_end'])
        t1 = call(:la_parse_ts, h['t_start']); t2 = call(:la_parse_ts, h['t_end'])
        h['latency_ms'] = call(:la_ms_between, t1, t2)
      end
      # Error normalization
      err = h['error']
      if err.is_a?(String)
        h['error'] = { 'message' => err }
      elsif err.is_a?(Hash)
        h['error'] = {
          'message' => err['message'] || err['msg'],
          'code'    => err['code'] || err['status'],
          'where'   => err['where'] || h['action'],
          'raw'     => err
        }
      end
      # Facets: prefer telemetry.facets if present
      h['facets'] ||= h.dig('telemetry','facets') || h['context'] || h['meta'] || {}
      h
    end,
    la_group_by: lambda do |arr, key|
      out = Hash.new { |hh,k| hh[k] = [] }
      Array(arr).each { |e| out[(e[key].to_s rescue '')] << e }
      out
    end,
    la_build_summary: lambda do |cid, events|
      sorted = Array(events).sort_by { |e| e['ts'].to_s }
      t_first = Time.parse(sorted.first['ts']) rescue nil
      t_last  = Time.parse(sorted.last['ts'])  rescue nil
      counts = Hash.new(0)
      by_event = Hash.new(0)
      errs = []
      facets_merged = {}
      sorted.each do |e|
        counts[e['level']] += 1 if e['level']
        by_event[e['event']] += 1 if e['event']
        if e['error'].is_a?(Hash)
          sig = [e['error']['message'], e['error']['where']].compact.join('|')
          errs << e['error'] unless errs.any? { |x| [x['message'], x['where']].compact.join('|') == sig }
        end
        if e['facets'].is_a?(Hash)
          e['facets'].each { |k,v| facets_merged[k] ||= v }
        end
      end
      {
        'correlation_id' => (cid == '' ? nil : cid),
        't_first'        => (t_first&.iso8601(3)),
        't_last'         => (t_last&.iso8601(3)),
        'duration_ms'    => (call(:la_ms_between, t_first, t_last) || 0),
        'counts'         => counts,
        'by_event'       => by_event,
        'errors'         => errs,
        'facets'         => facets_merged
      }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
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
  # Retrieve contexts
    # - host=`https://aiplatform.googleapis.com/v1/{parent}:retreiveContexts`
    # - parent=`projects/project/locations/location`
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
    # 1d. (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations/retrieveContexts)
  # 2. Embedding (https://ai.google.dev/gemini-api/docs/embeddings)
  # 3. Ranking (https://docs.cloud.google.com/generative-ai-app-builder/docs/ranking)
  # 4. Count tokens
    # 4a. publisher model  (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.publishers.models/countTokens)
    # 4b. endpoint/tuned model (https://docs.cloud.google.com/vertex-ai/generative-ai/docs/reference/rest/v1/projects.locations.endpoints/countTokens)
# --------------------------------------------------------------------------
