# frozen_string_literal: true
require 'openssl'
require 'base64'
require 'json'
require 'time'
require 'securerandom'


{
  title: 'Vertex RAG Engine',
  subtitle: 'RAG Engine',
  version: '1.0.3',
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
      # Always visible - Core fields needed to connect
      { name: 'service_account_key_json',  label: 'Service Account Key', optional: false, control_type: 'text-area', hint: 'Paste full JSON key from GCP' },
      { name: 'project_id', label: 'GCP Project ID', optional: false,  control_type: 'text', hint: 'GCP project ID (inferred from key if blank)' },
      { name: 'location', label: 'GCP Region', optional: false,  control_type: 'text', hint: 'e.g., us-central1, us-east4' },  
      { name: 'discovery_api_version', label: 'Discovery API version',  control_type: 'select',  optional: true,  default: 'v1alpha',
        options: [['v1alpha','v1alpha'], ['v1beta','v1beta'], ['v1','v1']] },
      
      # Toggle for advanced options
      { name: 'show_advanced', label: 'Show advanced options', type: 'boolean', control_type: 'checkbox', optional: true,
        extends_schema: true, int: 'Enable production mode settings and monitoring options' }
    ],
    
    extended_fields: lambda do |connection|
      # Only show these fields when user checks "Show advanced options"
      if connection['show_advanced'] == 'true'
        [
          { name: 'prod_mode', label: 'Production mode', optional: true, control_type: 'checkbox', type: 'boolean', default: true,
            hint: 'When enabled, suppresses debug echoes and enforces strict idempotency/retry rules.' },
          { name: 'user_project', label: 'User project for quota/billing', optional: true, control_type: 'text',
            hint: 'Sets x-goog-user-project for billing/quota. Service account must have roles/serviceusage.serviceUsageConsumer on this project.' },
          { name: 'enable_facets_logging', label: 'Enable facets in tail logs', type: 'boolean', control_type: 'checkbox', optional: true, 
            default: true, hint: 'Adds a compact jsonPayload.facets block (retrieval/ranking/generation metrics). No effect on action outputs.' }
        ]
      else
        []
      end
    end,

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
          { name: 'mode', control_type: 'select', pick_list: 'gen_generate_modes', 
            optional: false, default: 'plain' },
          { name: 'model', optional: false, control_type: 'text' },
          
          # Optional signal enrichment fields (always visible for pipeline use)
          { name: 'signals_category', optional: true, sticky: true,
            hint: 'Category context from upstream classification (enhances generation focus)' },
          { name: 'signals_confidence', optional: true, type: 'number', sticky: true,
            hint: 'Upstream confidence score (may adjust generation parameters)' },
          { name: 'signals_intent', optional: true, sticky: true,
            hint: 'Intent classification from upstream (customizes response style)' },
          { name: 'use_signal_enrichment', type: 'boolean', control_type: 'checkbox', 
            optional: true, default: true,
            hint: 'Apply upstream signals to enhance generation quality' },
          
          # Mode-specific fields
          { name: 'contents',
            type: 'array', of: 'object', properties: object_definitions['content'], optional: true,
            ngIf: 'input.mode != "rag_with_context"' },
          
          { name: 'system_preamble', label: 'System Instructions', control_type: 'text-area', 
            extends_schema: false, optional: true, 
            hint: 'Provide system-level instructions to guide the model\'s behavior' },
          
          { name: 'generation_config', type: 'object', 
            properties: object_definitions['generation_config'] },
          { name: 'safetySettings', type: 'array', of: 'object', 
            properties: object_definitions['safety_setting'] },
          { name: 'toolConfig', type: 'object' },
          
          # Correlation and debugging
          { name: 'correlation_id', label: 'Correlation ID', optional: true, sticky: true,
            hint: 'Pass the same ID across actions to stitch logs and metrics.' },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true },
          
          # ---------- Grounding (only when grounded_* modes) ----------
          { name: 'grounding_info', label: 'Grounding via Google Search',
            hint: 'Uses the built-in googleSearch tool.',
            ngIf: 'input.mode == "grounded_google"', optional: true },
          
          # Vertex AI Search parameters
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
              { name: 'id' }, 
              { name: 'text', optional: false }, 
              { name: 'source' }, 
              { name: 'uri' },
              { name: 'score', type: 'number' }, 
              { name: 'metadata', type: 'object' }
            ],
            ngIf: 'input.mode == "rag_with_context"'
          },
          { name: 'max_chunks', type: 'integer', optional: true, default: 20, 
            ngIf: 'input.mode == "rag_with_context"' },
          { name: 'max_prompt_tokens', type: 'integer', optional: true, default: 3000, 
            ngIf: 'input.mode == "rag_with_context"' },
          { name: 'reserve_output_tokens', type: 'integer', optional: true, default: 512, 
            ngIf: 'input.mode == "rag_with_context"' },
          { name: 'count_tokens_model', optional: true, 
            ngIf: 'input.mode == "rag_with_context"' },
          { name: 'trim_strategy', control_type: 'select', pick_list: 'trim_strategies', 
            optional: true, default: 'drop_low_score', 
            ngIf: 'input.mode == "rag_with_context"' },
          { name: 'temperature', type: 'number', optional: true, 
            ngIf: 'input.mode == "rag_with_context"' },
          
          # RAG Store grounding
          { name: 'rag_corpus', optional: true, 
            hint: 'projects/{project}/locations/{region}/ragCorpora/{corpus}', 
            ngIf: 'input.mode == "grounded_rag_store"' },
          { name: 'rag_retrieval_config', label: 'Retrieval config', type: 'object',
            properties: object_definitions['rag_retrieval_config'], 
            ngIf: 'input.mode == "grounded_rag_store"', optional: true }
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
    },
    # NEW 2025-11-14
    pipeline_gate: {
      fields: lambda do |c, cf|
        [
          { name: 'prelim_pass', type: 'boolean' },
          { name: 'hard_block', type: 'boolean' },
          { name: 'hard_reason' },
          { name: 'soft_score', type: 'integer' },
          { name: 'decision' },
          { name: 'generator_hint' },
          { name: 'can_proceed', type: 'boolean' },
          { name: 'has_category', type: 'boolean' },
          { name: 'generation_eligible', type: 'boolean' }
        ]
      end
    },
    scored_category: {
      fields: lambda do |c, cf|
        [
          { name: 'category' },
          { name: 'score', type: 'number' },
          { name: 'cosine', type: 'number', optional: true },
          { name: 'prob', type: 'number', optional: true }
        ]
      end
    },
    policy_decision: {
      fields: lambda do |c, cf|
        [
          { name: 'decision' },
          { name: 'confidence', type: 'number' },
          { name: 'matched_signals', type: 'array', of: 'string' },
          { name: 'reasons', type: 'array', of: 'string' }
        ]
      end
    },
    intent_classification: {
      fields: lambda do |c, cf|
        [
          { name: 'label' },
          { name: 'confidence', type: 'number' },
          { name: 'basis' }
        ]
      end
    },
    context_chunk_standard: {
      fields: lambda do |c, cf|
        [
          { name: 'id' },
          { name: 'text' },
          { name: 'score', type: 'number' },
          { name: 'source' },
          { name: 'uri' },
          { name: 'metadata', type: 'object' },
          { name: 'metadata_kv', type: 'array', of: 'object' },
          { name: 'metadata_json' },
          { name: 'is_pdf', type: 'boolean', optional: true },
          { name: 'processing_error', type: 'boolean', optional: true }
        ]
      end
    },
    # Enhanced intent classification with richer types
    intent_classification_enhanced: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'label', hint: 'Intent type detected' },
          { name: 'confidence', type: 'number' },
          { name: 'basis', hint: 'How intent was determined' },
          { name: 'sub_type', optional: true, hint: 'More specific intent category' }
        ]
      end
    },
    # Combined policy + intent response
    policy_with_intent: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'decision', hint: 'IRRELEVANT/HUMAN/KEEP' },
          { name: 'confidence', type: 'number' },
          { name: 'intent', type: 'object', properties: [
            { name: 'label' },
            { name: 'confidence', type: 'number' },
            { name: 'sub_type', optional: true }
          ]},
          { name: 'matched_signals', type: 'array', of: 'string' },
          { name: 'reasons', type: 'array', of: 'string' }
        ]
      end
    }
  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    deterministic_filter: {
      title: 'Filter: Heuristics (with intent)',
      subtitle: 'Filter: Heuristics',
      description: 'Pre-screen/filter that evaluates an email against rules and heuristically infers intent',
      display_priority: 510,
      help: lambda do |_|
        { body: 'Coarse evaluation of an email against rules.' }
      end,
      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        call(:ui_df_inputs_simplified, object_definitions, config_fields) +
          Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'passed', type: 'boolean', 
            hint: 'True if email passes all hard rules' },
          { name: 'hard_block', type: 'boolean' },
          { name: 'hard_reason', hint: 'e.g., forwarded_chain, safety_block' },
          { name: 'email_type', hint: 'direct_request, forwarded_chain, etc.' },
          { name: 'email_text' },
          { name: 'soft_score', type: 'integer', hint: 'Soft signals score if rules configured' },
          { name: 'gate', type: 'object',
            properties: object_definitions['pipeline_gate'] },
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
      end,
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :deterministic_filter, input)

        env = call(:norm_email_envelope!, (input['email'] || input))
        subj, body, email_text = env['subject'], env['body'], env['email_text']

        # Build/choose rulepack
        rules = nil
        if input['rules_mode'] == 'json' && input['rules_json'].present?
          rules = call(:safe_json, input['rules_json'])
        elsif input['rules_mode'] == 'rows' && Array(input['rules_rows']).any?
          rules = call(:hr_compile_rulepack_from_rows!, input['rules_rows'])
        end

        # Defaults
        email_type = 'direct_request'
        hard_block = false
        hard_reason = nil
        soft_score = 0
        preliminary_decision = 'KEEP'

        if rules.is_a?(Hash)
          # Check hard rules
          hard = call(:hr_eval_hard?, {
            'subject' => subj, 
            'body' => body, 
            'from' => env['from'],
            'headers' => env['headers'], 
            'attachments' => env['attachments'], 
            'auth' => env['auth']
          }, (rules['hard_exclude'] || {}))

          if hard[:hit]
            hard_reason = hard[:reason]
            
            # Map hard reasons to email type
            case hard_reason
            when 'forwarded_chain'
              email_type = 'forwarded_chain'
              hard_block = true
              preliminary_decision = 'HUMAN'
            when 'internal_discussion'
              email_type = 'internal_discussion'
              hard_block = true
              preliminary_decision = 'HUMAN'
            when 'mailing_list', 'bounce'
              email_type = hard_reason
              hard_block = true
              preliminary_decision = 'IRRELEVANT'
            when 'safety_block'
              email_type = 'blocked'
              hard_block = true
              preliminary_decision = 'HUMAN'
            end
          end

          # If not hard blocked, check soft signals
          unless hard_block
            soft = call(:hr_eval_soft, {
              'subject' => subj, 
              'body' => body, 
              'from' => env['from'],
              'headers' => env['headers'], 
              'attachments' => env['attachments']
            }, (rules['soft_signals'] || []))
            
            soft_score = soft[:score] || 0
            preliminary_decision = call(:hr_eval_decide, soft_score, (rules['thresholds'] || {}))
          end
        end

        # Build gate
        gate = {
          'prelim_pass' => !hard_block && email_type == 'direct_request',
          'hard_block' => hard_block,
          'hard_reason' => hard_reason,
          'soft_score' => soft_score,
          'decision' => preliminary_decision,
          'generator_hint' => (hard_block ? 'blocked' : 'check_policy')
        }

        out = {
          'passed' => !hard_block,
          'hard_block' => hard_block,
          'hard_reason' => hard_reason,
          'email_type' => email_type,
          'email_text' => email_text,
          'soft_score' => soft_score,
          'gate' => gate
        }

        call(:step_ok!, ctx, out, 200, 'OK', { 
          'hard_blocked' => hard_block,
          'email_type' => email_type,
          'soft_score' => soft_score,
          'rules_evaluated' => rules.is_a?(Hash)
        })
      end, 
      sample_output: lambda do
        call(:sample_deterministic_filter_simplified)
      end
    },
    ai_triage_filter: {
      title: 'Filter: AI triage',
      subtitle: 'Filter: Triage (IRRELEVANT/HUMAN/KEEP)',
      description: 'LLM-based triage to determine if email should proceed through pipeline',
      display_priority: 501,
      
      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        base = [
          { name: 'email_text', optional: false },
          { name: 'email_type', optional: true, default: 'direct_request',
            hint: 'From deterministic filter' },
          { name: 'system_preamble', label: 'System Instructions', control_type: 'text-area', 
            optional: true, hint: 'Override default system instructions for email triage' },
          
          # Thresholds
          { name: 'min_confidence_for_keep', type: 'number', 
            default: 0.60, hint: 'Minimum confidence to mark as KEEP' },
          { name: 'confidence_short_circuit', type: 'number', 
            default: 0.85, hint: 'High confidence IRRELEVANT bypasses downstream' }
        ]
        
        adv = config_fields['show_advanced'] ? [
          { name: 'model', default: 'gemini-2.0-flash' },
          { name: 'temperature', type: 'number', default: 0 },
          { name: 'custom_policy_json', control_type: 'text-area', optional: true,
            hint: 'Additional triage rules in JSON format' }
        ] : []
        
        base + adv + Array(object_definitions['observability_input_fields'])
      end,     
      output_fields: lambda do |object_definitions, connection|
        [
          # Business outputs
          { name: 'decision', hint: 'IRRELEVANT, HUMAN, or KEEP' },
          { name: 'confidence', type: 'number' },
          { name: 'reasons', type: 'array', of: 'string' },
          { name: 'matched_signals', type: 'array', of: 'string' },
          
          # Pipeline control
          { name: 'should_continue', type: 'boolean',
            hint: 'True if pipeline should continue to next step' },
          { name: 'short_circuit', type: 'boolean',
            hint: 'True if high-confidence IRRELEVANT' },
          
          # Pass-through signals
          { name: 'signals_triage', hint: 'Copy of decision for downstream' },
          { name: 'signals_confidence', type: 'number' },
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
      end,     
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :ai_triage_filter, input)
        
        # Build model path
        model = input['model'] || 'gemini-2.0-flash'
        model_path = call(:build_model_path_with_global_preview, connection, model)
        
        # Enhanced system prompt that recognizes broader employee questions
        default_system_text = <<~SYS
          You are an email triage system for employee inquiries. Classify emails carefully.
          
          DECISIONS:
          - KEEP: Any legitimate employee question or request that could be answered, including:
            * HR policies (PTO, leave, attendance)
            * Benefits questions (401k, health insurance, FSA, retirement)
            * Payroll or compensation inquiries
            * Employment verification or documentation requests
            * Workplace policies or procedures
            * Any question where the employee is seeking information or help
          
          - HUMAN: Requires human judgment:
            * Complaints or grievances
            * Sensitive or legal matters
            * Escalations or threats
            * Complex situations requiring context beyond what could be contained in a knowledge base
            * Potentially discriminatory content
            
          - IRRELEVANT: Not an employee inquiry:
            * Spam or marketing emails
            * Newsletters (unless internal company newsletters with questions)
            * Auto-generated messages with no action needed
            * Personal conversations unrelated to employment
          
          IMPORTANT: If an employee is asking a question about ANY work-related topic
          (even if managed by third parties like 401k providers, insureres), classify as KEEP.
          
          Also detect if the email contains a question that needs answering.
          #{input['include_domain_detection'] ? 'Identify the domain/topic area.' : ''}
          
          Output MUST be valid JSON only. No text before or after.
        SYS

        # Use the user-provided prompt or fall back to default
        system_text = input['system_preamble'].presence || default_system_text
        
        # Add custom policy if provided
        if input['custom_policy_json'].present?
          system_text += "\n\nAdditional rules:\n#{input['custom_policy_json']}"
        end
        
        # Response schema with domain detection
        schema_props = {
          'decision' => {
            'type' => 'string',
            'enum' => ['IRRELEVANT', 'HUMAN', 'KEEP']
          },
          'confidence' => { 'type' => 'number', 'minimum' => 0, 'maximum' => 1 },
          'reasons' => {
            'type' => 'array',
            'items' => { 'type' => 'string' },
            'maxItems' => 3
          },
          'matched_signals' => {
            'type' => 'array',
            'items' => { 'type' => 'string' },
            'maxItems' => 5
          },
          'has_question' => { 'type' => 'boolean' }
        }
        
        required = ['decision', 'confidence', 'has_question']
        
        if input['include_domain_detection'] != false
          schema_props['detected_domain'] = { 'type' => 'string' }
        end
        
        response_schema = {
          'type' => 'object',
          'additionalProperties' => false,
          'properties' => schema_props,
          'required' => required
        }
        
        # Make API call
        payload = {
          'systemInstruction' => { 'role' => 'system', 'parts' => [{'text' => system_text}] },
          'contents' => [{ 'role' => 'user', 'parts' => [{ 'text' => "Email:\n#{input['email_text']}" }] }],
          'generationConfig' => {
            'temperature' => (input['temperature'] || 0).to_f,
            'maxOutputTokens' => 256,
            'responseMimeType' => 'application/json',
            'responseSchema' => response_schema
          }
        }
        
        loc = (model_path[/\/locations\/([^\/]+)/,1] || 'global').to_s.downcase
        url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
        
        resp = post(url)
                .headers(call(:request_headers_auth, connection, ctx['cid'], 
                            connection['user_project'], "model=#{model_path}"))
                .payload(call(:json_compact, payload))
        
        # Parse response
        text = resp.dig('candidates',0,'content','parts',0,'text').to_s
        parsed = call(:safe_json, text) || {}
        
        decision = (parsed['decision'] || 'HUMAN').to_s
        confidence = [[call(:safe_float, parsed['confidence']) || 0.0, 0.0].max, 1.0].min
        
        # Determine pipeline flow
        min_conf = (input['min_confidence_for_keep'] || 0.60).to_f
        should_continue = (decision == 'KEEP' && confidence >= min_conf) || 
                          (decision == 'HUMAN')
        
        short_circuit = decision == 'IRRELEVANT' && 
                      confidence >= (input['confidence_short_circuit'] || 0.85).to_f
        
        out = {
          'decision' => decision,
          'confidence' => confidence,
          'reasons' => call(:safe_array, parsed['reasons']),
          'matched_signals' => call(:safe_array, parsed['matched_signals']),
          'detected_domain' => parsed['detected_domain'],
          'has_question' => parsed['has_question'] || false,
          'should_continue' => should_continue,
          'short_circuit' => short_circuit,
          'signals_triage' => decision,
          'signals_confidence' => confidence,
          'signals_domain' => parsed['detected_domain']
        }
        
        call(:step_ok!, ctx, out, 200, 'OK', {
          'decision' => decision,
          'confidence' => confidence,
          'detected_domain' => parsed['detected_domain'],
          'has_question' => parsed['has_question'],
          'should_continue' => should_continue,
          'short_circuit' => short_circuit
        })
      end,
      sample_output: lambda do
        call(:sample_ai_triage_filter)
      end
    },
    ai_intent_classifier: {
      title: 'Classify: Intent',
      subtitle: 'Classify: User intent',
      description: 'Determine what the user is trying to accomplish',
      display_priority: 500,     
      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        base = [
          { name: 'email_text', optional: false },
          { name: 'system_preamble', label: 'System Instructions', control_type: 'text-area',
            optional: true, hint: 'Override default system instructions for intent classification' },
          # Intent configuration
          { name: 'intent_types', type: 'array', of: 'string',
            default: ['information_request', 'action_request', 'status_inquiry', 
                    'complaint', 'feedback', 'auto_reply', 'unknown'],
            hint: 'Possible intent categories' },
          
          { name: 'actionable_intents', type: 'array', of: 'string',
            default: ['information_request', 'action_request'],
            hint: 'Intents that can be automated' },
          
          # Pass-through from previous steps
          { name: 'signals_triage', optional: true,
            hint: 'Triage decision from previous step' },
          { name: 'email_type', optional: true }
        ]
        
        adv = config_fields['show_advanced'] ? [
          { name: 'model', default: 'gemini-2.0-flash' },
          { name: 'temperature', type: 'number', default: 0 },
          { name: 'extract_entities', type: 'boolean', control_type: 'checkbox',
            default: false, hint: 'Extract named entities' },
          { name: 'detect_sentiment', type: 'boolean', control_type: 'checkbox',
            default: false, hint: 'Detect emotional sentiment' }
        ] : []
        
        base + adv + Array(object_definitions['observability_input_fields'])
      end,      
      output_fields: lambda do |object_definitions, connection|
        [
          # Business outputs
          { name: 'intent', hint: 'Primary user intent' },
          { name: 'confidence', type: 'number' },
          { name: 'is_actionable', type: 'boolean',
            hint: 'True if intent can be automated' },
          
          # Optional enrichments
          { name: 'secondary_intents', type: 'array', of: 'string', optional: true },
          { name: 'entities', type: 'array', of: 'object', optional: true,
            properties: [
              { name: 'type' },
              { name: 'value' },
              { name: 'context' }
            ]
          },
          { name: 'sentiment', optional: true,
            hint: 'positive, negative, or neutral' },
          
          # Pipeline control
          { name: 'requires_context', type: 'boolean',
            hint: 'True if RAG retrieval recommended' },
          { name: 'suggested_category', optional: true,
            hint: 'Hint for category classification' },
          
          # Pass-through signals  
          { name: 'signals_intent' },
          { name: 'signals_intent_confidence', type: 'number' },
          { name: 'signals_triage' },  # Pass through from previous
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
      end,    
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :ai_intent_classifier, input)
        
        # Build model path
        model = input['model'] || 'gemini-2.0-flash'
        model_path = call(:build_model_path_with_global_preview, connection, model)
        
        # System prompt focused on intent only
        system_text = <<~SYS
          You are an intent classification system. Determine what the user wants.
          
          INTENT TYPES:
          - information_request: Asking for information, policies, procedures
          - action_request: Requesting specific action (approval, document)
          - status_inquiry: Checking status of existing request
          - complaint: Expressing dissatisfaction or escalation
          - feedback: Providing feedback or suggestions
          - auto_reply: Out-of-office or automated response
          - unknown: Cannot determine clear intent
          
          Output MUST be valid JSON only.
        SYS
        
        # Use user-provided system prompt or fall back to default
        system_text = input['system_preamble'].presence || default_system_text

        # Append any conditional instructions (existing logic)
        system_text += "\n#{input['extract_entities'] ? 'Extract key entities mentioned.' : ''}"
        system_text += "\n#{input['detect_sentiment'] ? 'Detect overall sentiment.' : ''}"
        
        
        # Build response schema
        schema_props = {
          'intent' => {
            'type' => 'string',
            'enum' => input['intent_types']
          },
          'confidence' => { 'type' => 'number', 'minimum' => 0, 'maximum' => 1 },
          'secondary_intents' => {
            'type' => 'array',
            'items' => { 'type' => 'string', 'enum' => input['intent_types'] }
          },
          'requires_context' => { 'type' => 'boolean' },
          'suggested_category' => { 'type' => 'string' }
        }
        
        required = ['intent', 'confidence']
        
        if input['extract_entities']
          schema_props['entities'] = {
            'type' => 'array',
            'items' => {
              'type' => 'object',
              'properties' => {
                'type' => { 'type' => 'string' },
                'value' => { 'type' => 'string' },
                'context' => { 'type' => 'string' }
              },
              'required' => ['type', 'value']
            }
          }
        end
        
        if input['detect_sentiment']
          schema_props['sentiment'] = {
            'type' => 'string',
            'enum' => ['positive', 'negative', 'neutral']
          }
        end
        
        # Make API call
        payload = {
          'systemInstruction' => { 'role' => 'system', 'parts' => [{'text' => system_text}] },
          'contents' => [{ 'role' => 'user', 'parts' => [{ 'text' => "Email:\n#{input['email_text']}" }] }],
          'generationConfig' => {
            'temperature' => (input['temperature'] || 0).to_f,
            'maxOutputTokens' => 512,
            'responseMimeType' => 'application/json',
            'responseSchema' => {
              'type' => 'object',
              'additionalProperties' => false,
              'properties' => schema_props,
              'required' => required
            }
          }
        }
        
        loc = (model_path[/\/locations\/([^\/]+)/,1] || 'global').to_s.downcase
        url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
        
        resp = post(url)
                .headers(call(:request_headers_auth, connection, ctx['cid'],
                            connection['user_project'], "model=#{model_path}"))
                .payload(call(:json_compact, payload))
        
        # Parse response
        text = resp.dig('candidates',0,'content','parts',0,'text').to_s
        parsed = call(:safe_json, text) || {}
        
        intent = (parsed['intent'] || 'unknown').to_s
        confidence = [[call(:safe_float, parsed['confidence']) || 0.0, 0.0].max, 1.0].min
        actionable_intents = input['actionable_intents'] || ['information_request', 'action_request']
        
        out = {
          'intent' => intent,
          'confidence' => confidence,
          'is_actionable' => actionable_intents.include?(intent),
          'secondary_intents' => parsed['secondary_intents'],
          'entities' => parsed['entities'],
          'sentiment' => parsed['sentiment'],
          'requires_context' => parsed['requires_context'] || false,
          'suggested_category' => parsed['suggested_category'],
          'signals_intent' => intent,
          'signals_intent_confidence' => confidence,
          'signals_triage' => input['signals_triage']  # Pass through
        }
        
        call(:step_ok!, ctx, out, 200, 'OK', {
          'intent' => intent,
          'confidence' => confidence,
          'is_actionable' => out['is_actionable'],
          'has_entities' => parsed['entities'].is_a?(Array) && parsed['entities'].any?,
          'sentiment' => parsed['sentiment']
        })
      end,
      sample_output: lambda do
        call(:sample_ai_intent_classifier)
      end
    },
    ai_policy_filter: {
      title: 'Filter: AI triage',
      subtitle: 'Filter: Triage',
      description: 'Fuzzy triage via LLM under a strict JSON schema',
      display_priority: 501,
      help: lambda do |_|
        { body: 'Constrained LLM decides IRRELEVANT/HUMAN/KEEP. Returns results as JSON.' }
      end,
      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal advanced parameters.' }
      ],  
      input_fields: lambda do |object_definitions, connection, config_fields|
        call(:ui_policy_inputs_enhanced, object_definitions, config_fields) +
          Array(object_definitions['observability_input_fields'])
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          # Triage outputs
          { name: 'policy', type: 'object',
            properties: object_definitions['policy_decision'] },
          
          # Intent outputs (NEW)
          { name: 'intent', type: 'object',
            properties: object_definitions['intent_classification'] },
          
          { name: 'short_circuit', type: 'boolean' },
          { name: 'email_type' },
          
          { name: 'generator_gate', type: 'object', properties: [
            { name: 'pass_to_responder', type: 'boolean' },
            { name: 'reason' },
            { name: 'generator_hint' }
          ]},
          
          # Pass-through signals for downstream
          { name: 'signals_intent' },
          { name: 'signals_intent_confidence', type: 'number' },
          
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
      end,
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :ai_policy_filter, input)
        
        begin
          # Build model path
          model = input['model'] || 'gemini-2.0-flash'
          model_path = call(:build_model_path_with_global_preview, connection, model)
          loc = (model_path[/\/locations\/([^\/]+)/,1] || 'global').to_s.downcase
          url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
          req_params = "model=#{model_path}"

          # Get intent types configuration
          intent_types = input['intent_types'] || 
                        ['information_request', 'action_request', 'status_inquiry', 
                        'complaint', 'auto_reply', 'unknown']
          
          actionable_intents = input['actionable_intents'] || 
                              ['information_request', 'action_request']

          # Build enhanced system text with intent
          system_text = <<~SYS
            You are a strict email classification system. Your ENTIRE response must be valid JSON.
            
            Classify the email with TWO tasks:
            1. TRIAGE decision: Is this email relevant and how should it be handled?
            2. INTENT detection: What is the user trying to accomplish?
            
            TRIAGE DECISIONS:
            - IRRELEVANT: Not relevant to HR, spam, newsletters, auto-generated
            - HUMAN: Needs human review (complaints, complex, sensitive)
            - KEEP: Valid request that can be handled automatically
            
            INTENT TYPES:
            - information_request: Asking for information, policies, or procedures
            - action_request: Requesting a specific action (approval, document generation)
            - status_inquiry: Checking status of existing request
            - complaint: Expressing dissatisfaction or escalation
            - auto_reply: Out-of-office, bounce, or automated response
            - unknown: Cannot determine clear intent
            
            CRITICAL: Output ONLY valid JSON. No text before or after.
            
            Example valid response:
            {"decision":"KEEP","confidence":0.85,"intent":"information_request","intent_confidence":0.9,"matched_signals":["policy_question"],"reasons":["Clear PTO policy inquiry"]}
          SYS

          # Add custom policy if provided
          if input['policy_json'].present? && input['policy_mode'] == 'json'
            policy_spec = call(:safe_json_obj!, input['policy_json'])
            if policy_spec.is_a?(Hash)
              system_text += "\n\nAdditional policy rules:\n#{call(:json_compact, policy_spec)}"
            end
          end

          # Build response schema with intent
          response_schema = {
            'type' => 'object',
            'additionalProperties' => false,
            'properties' => {
              'decision' => {
                'type' => 'string',
                'enum' => ['IRRELEVANT', 'HUMAN', 'KEEP']
              },
              'confidence' => { 
                'type' => 'number', 
                'minimum' => 0, 
                'maximum' => 1 
              },
              'intent' => { 
                'type' => 'string', 
                'enum' => intent_types 
              },
              'intent_confidence' => { 
                'type' => 'number', 
                'minimum' => 0, 
                'maximum' => 1 
              },
              'matched_signals' => {
                'type' => 'array',
                'items' => { 'type' => 'string' },
                'maxItems' => 10
              },
              'reasons' => {
                'type' => 'array',
                'items' => { 'type' => 'string' },
                'maxItems' => 5
              }
            },
            'required' => ['decision', 'confidence', 'intent']
          }

          # Build generation config
          gen_config = {
            'temperature' => (input['temperature'] || 0).to_f,
            'maxOutputTokens' => 512,
            'responseMimeType' => 'application/json',
            'responseSchema' => response_schema
          }

          # Build request payload
          payload = {
            'systemInstruction' => { 
              'role' => 'system', 
              'parts' => [{'text' => system_text}] 
            },
            'contents' => [
              { 
                'role' => 'user', 
                'parts' => [{ 'text' => "Email:\n#{input['email_text']}" }] 
              }
            ],
            'generationConfig' => gen_config
          }

          # Make API request
          resp = post(url)
                  .headers(call(:request_headers_auth, connection, ctx['cid'], 
                              connection['user_project'], req_params))
                  .payload(call(:json_compact, payload))
          
          # Parse response
          text, meta = call(:extract_candidate_text, resp)
          
          # Parse JSON with defaults
          parsed = {}
          if text.is_a?(String) && text.length > 0
            clean_text = text.gsub(/```json\n?/, '').gsub(/```\n?/, '').strip
            parsed = call(:safe_json, clean_text) || {}
          end
          
          # Extract and validate fields
          decision = (parsed['decision'] || 'HUMAN').to_s
          unless ['IRRELEVANT', 'HUMAN', 'KEEP'].include?(decision)
            decision = 'HUMAN'
          end
          
          confidence = [[call(:safe_float, parsed['confidence']) || 0.0, 0.0].max, 1.0].min
          
          intent = (parsed['intent'] || 'unknown').to_s
          unless intent_types.include?(intent)
            intent = 'unknown'
          end
          
          intent_confidence = [[call(:safe_float, parsed['intent_confidence']) || confidence, 0.0].max, 1.0].min
          
          # Build policy and intent objects
          policy = {
            'decision' => decision,
            'confidence' => confidence,
            'matched_signals' => call(:safe_array, parsed['matched_signals']),
            'reasons' => call(:safe_array, parsed['reasons'])
          }
          
          intent_obj = {
            'label' => intent,
            'confidence' => intent_confidence,
            'basis' => 'llm_classification'
          }
          
          # Calculate short circuit
          short_circuit = (decision == 'IRRELEVANT' && 
                          confidence >= (input['confidence_short_circuit'] || 0.8).to_f)

          # Generator gate logic with intent check
          email_type = input['email_type'] || 'direct_request'
          min_conf = (input['min_confidence_for_generation'] || 0.60).to_f
          
          block_reasons = []
          block_reasons << 'non_direct_request' if email_type != 'direct_request'
          block_reasons << 'policy_irrelevant' if decision == 'IRRELEVANT'
          block_reasons << 'low_confidence' if confidence < min_conf
          
          # Check intent gate (NEW)
          if input['require_actionable_intent'] && !actionable_intents.include?(intent)
            block_reasons << "non_actionable_intent:#{intent}"
          end
          
          pass_to_responder = block_reasons.empty?
          generator_hint = pass_to_responder ? 'pass' : 'blocked'

          # Build output
          out = {
            'policy' => policy,
            'intent' => intent_obj,
            'short_circuit' => short_circuit,
            'email_type' => email_type,
            'generator_gate' => {
              'pass_to_responder' => pass_to_responder,
              'reason' => block_reasons.any? ? block_reasons.join(',') : 'meets_requirements',
              'generator_hint' => generator_hint
            },
            'signals_intent' => intent,
            'signals_intent_confidence' => intent_confidence
          }
          
          call(:step_ok!, ctx, out, call(:telemetry_success_code, resp), 'OK', {
            'decision' => decision,
            'confidence' => confidence,
            'intent' => intent,
            'intent_confidence' => intent_confidence,
            'short_circuit' => short_circuit,
            'generator_hint' => generator_hint
          })
          
        rescue => e
          call(:step_err!, ctx, e)
        end
      end,
      sample_output: lambda do
        call(:sample_ai_policy_filter_enhanced)
      end
    },
    embed_text_against_categories: {
      title: 'Categorize: Semantic similarity',
      subtitle: 'Categorize: Semantic similarity (email against categories)',
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
          # Business outputs
          { name: 'scores', type: 'array', of: 'object',
            properties: object_definitions['scored_category'] },  # <-- Using object def
          { name: 'shortlist', type: 'array', of: 'string' },
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs) 
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
        # Pass through signals
        out['signals_category'] = input['signals_category']
        out['signals_confidence'] = input['signals_confidence']
        out['signals_intent'] = input['signals_intent']

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
      subtitle: 'Categorize: Re-rank',
      description: 'Listwise re-ordering of categories using an LLM. Emits probability distribution over the shortlist.',
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
          # Business outputs
          { name: 'ranking', type: 'array', of: 'object',
            properties: object_definitions['scored_category'] },  # Uses category + prob
          { name: 'shortlist', type: 'array', of: 'string' },
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
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
        # Pass through signals
        out['signals_category'] = input['signals_category']
        out['signals_confidence'] = input['signals_confidence']
        out['signals_intent'] = input['signals_intent']

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
    llm_referee_with_contexts: {
      title: 'Categorize: LLM as referee',
      subtitle: 'Adjudicate among shortlist; accepts ranked categories',
      display_priority: 497,
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
          # Critical business outputs
          { name: 'chosen', hint: 'CRITICAL: Used by steps 6-8' },
          { name: 'confidence', type: 'number' },
          
          { name: 'referee', type: 'object', properties: [
            { name: 'category' },
            { name: 'confidence', type: 'number' },
            { name: 'reasoning' },
            { name: 'distribution', type: 'array', of: 'object',
              properties: object_definitions['scored_category'] }  # Reuse for distribution
          ]},
          
          # Optional salience
          { name: 'salience', type: 'object', properties: [
            { name: 'span' },
            { name: 'reason' },
            { name: 'importance', type: 'number' },
            { name: 'tags', type: 'array', of: 'string' },
            { name: 'entities', type: 'array', of: 'object' },
            { name: 'cta' },
            { name: 'deadline_iso' },
            { name: 'focus_preview' },
            { name: 'responseId' },
            { name: 'usage', type: 'object' },
            { name: 'span_source' }
          ]},
          
          # Signal for downstream
          { name: 'signals_category', hint: 'Copy of chosen for downstream' },
          { name: 'signals_confidence', hint: 'Confidence for downstream use' },
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
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
        out['signals_category'] = chosen
        out['signals_confidence'] = out['confidence']
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
    rag_retrieve_contexts_enhanced: {
      title: 'Context: Retrieve contexts (enhanced)',
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
          { name: 'rag_corpus', optional: true, hint: 'Full or short ID. Example: my-corpus. Will auto-expand using connection project/location.' },
          { name: 'rag_file_ids', type: 'array', of: 'string', optional: true,
            hint: 'Optional: limit to these file IDs (must belong to the same corpus).' },
          { name: 'top_k', type: 'integer', optional: true, default: 20, hint: 'Max contexts to return.' },
          { name: 'correlation_id', optional: true, hint: 'For tracking related requests.' },
          { name: 'signals_category', optional: true, hint: 'Category from step 5 (enhances retrieval)' },
          { name: 'sanitize_pdf_content', type: 'boolean', control_type: 'checkbox',default: true,
            hint: 'Clean PDF extraction artifacts when detected (recommended)' },
          { name: 'on_error_behavior', control_type: 'select', default: 'skip',
            pick_list: [
              ['Skip failed contexts', 'skip'],
              ['Include error placeholders', 'include'],
              ['Fail entire request', 'fail']
            ],
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
          # Business outputs
          { name: 'question' },
          { name: 'contexts', type: 'array', of: 'object',
            properties: object_definitions['context_chunk_standard'] },  # All context fields
          
          # Standard fields (but with enhanced telemetry)
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
      title: 'Context: Rerank contexts',
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
      config_fields: [
        { name: 'show_advanced', label: 'Show advanced options',
          type: 'boolean', control_type: 'checkbox',
          default: false, sticky: true, extends_schema: true,
          hint: 'Toggle to reveal filtering and distribution options.' }
      ],
      input_fields: lambda do |object_definitions, connection, config_fields|
        show_adv = (config_fields['show_advanced'] == true)
        
        base = [
          { name: 'query_text', optional: false, hint: 'The user query to rank contexts against' },
          { name: 'records', type: 'array', of: 'object', optional: false, properties: [
            { name: 'id', optional: false }, 
            { name: 'content', optional: false }, 
            { name: 'score', type: 'number', optional: true, hint: 'Original retrieval score' },
            { name: 'source', optional: true, hint: 'Source document name' },
            { name: 'uri', optional: true, hint: 'Source document URI/path' },
            { name: 'metadata', type: 'object', optional: true }
            ], hint: 'Retrieved contexts to rank (id + content required)' },
          { name: 'category', optional: true, 
            hint: 'Pre-determined category (e.g., PTO, Billing, Support) to inform ranking' },
          { name: 'signals_category', optional: true, 
            hint: 'Falls back to this if category not provided directly' },
          { name: 'correlation_id', label: 'Correlation ID', optional: true, 
            hint: 'Pass the same ID across actions to stitch logs and metrics.', sticky: true },
          { name: 'llm_model', optional: true, default: 'gemini-2.0-flash',
            hint: 'LLM model for semantic ranking' },
          { name: 'top_n', type: 'integer', optional: true, 
            hint: 'Max contexts to return (default: all)' }
        ]
        
        if show_adv
          base + [
            # Category-specific options
            { name: 'category_context', optional: true,
              hint: 'Additional context about the category to guide ranking' },
            { name: 'include_category_in_query', type: 'boolean', control_type: 'checkbox', 
              optional: true, default: true,
              hint: 'Include category context in ranking query for better relevance' },
            
            # Filtering options
            { name: 'filter_by_category_metadata', type: 'boolean', control_type: 'checkbox', 
              optional: true, default: false,
              hint: 'Pre-filter contexts by category match in metadata before ranking' },
            { name: 'category_metadata_key', optional: true, default: 'category',
              ngIf: 'input.filter_by_category_metadata',
              hint: 'Metadata field containing category tags' },
            
            # LLM processing limits
            { name: 'llm_max_contexts', type: 'integer', optional: true, default: 50,
              hint: 'Maximum number of contexts to process with LLM (for cost/performance)' },
            { name: 'include_confidence_distribution', type: 'boolean', control_type: 'checkbox', 
              optional: true, default: false,
              hint: 'Return probability distribution across contexts' },
            
            # Output options
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
        else
          base
        end
      end,
      output_fields: lambda do |object_definitions, _connection|
        [
          # Business outputs - records always present
          { name: 'records', type: 'array', of: 'object', properties: [
            { name: 'id' },
            { name: 'score', type: 'number' },
            { name: 'rank', type: 'integer' },
            { name: 'content' },
            { name: 'source' },
            { name: 'uri' },
            { name: 'metadata', type: 'object' },
            { name: 'llm_relevance', type: 'number' },
            { name: 'category_alignment', type: 'number' }
          ]},
          
          # Context chunks (when emit_shape = context_chunks)
          { name: 'context_chunks', type: 'array', of: 'object', 
            properties: object_definitions['context_chunk_standard'] + [
              # Additional fields specific to ranking
              { name: 'llm_relevance', type: 'number' },
              { name: 'category_alignment', type: 'number' }
            ]
          },
          
          # Optional confidence distribution
          { name: 'confidence_distribution', type: 'array', of: 'object', properties: [
            { name: 'id' },
            { name: 'probability', type: 'number' },
            { name: 'reasoning' }
          ]},
          
          # Metadata about the ranking operation
          { name: 'ranking_metadata', type: 'object', properties: [
            { name: 'category' },
            { name: 'llm_model' },
            { name: 'contexts_filtered', type: 'integer' },
            { name: 'contexts_ranked', type: 'integer' }
          ]},
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
      end,
      execute: lambda do |connection, input|
        ctx = call(:step_begin!, :rank_texts_with_ranking_api, input)
        
        begin
          call(:ensure_project_id!, connection)
          loc = call(:aiapps_loc_resolve, connection, input['ai_apps_location'])
          
          # Extract category and prepare query
          category = input['category'].to_s.strip || input['signals_category'] || ''
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
              'score' => orig['score'] || 0.0,
              'rank' => 999,
              'source' => orig['source'],       # Preserve source
              'uri' => orig['uri']  
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
              { 
                'id' => r['id'], 
                'score' => r['score'], 
                'rank' => r['rank'],
                'uri' => r['uri'],      # Include URI
                'source' => r['source']  # Include source
              }
            }
          when 'enriched_records'
            result['records'] = enriched
          else  # context_chunks
            chunks = enriched.map { |r|
              # Try to get from direct fields first
              md = r['metadata'] || {}
              source_key = input['source_key'] || 'source'
              uri_key = input['uri_key'] || 'uri'
              
              chunk = {
                'id' => r['id'],
                'text' => r['content'].to_s,
                'score' => r['score'].to_f,
                'source' => r['source'] || md[source_key],
                'uri' => r['uri'] || md[uri_key],
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
    gen_generate: {
      title: 'Generative: Grounded query',
      description: 'Query a generative endpoint (configurable to allow grounding)',
      help: lambda do |_|
        {
          body: "Select an option from the `Mode` field, then fill only the fields rendered by the recipe builder. "\
                "Required fields per mode: `RAG-LITE`: [question, context_chunks]. " \
                '`VERTEX-SEARCH ONLY`: [vertex_ai_search_datastore OR vertex_ai_search_serving_config]. ' \
                '`RAG-STORE ONLY`: [rag_corpus]. ' \
                '`GOOGLE SEARCH`: No additional fields required. ' \
                '`PLAIN`: Just model and contents.',
          learn_more_url: 'https://ai.google.dev/gemini-api/docs/models',
          learn_more_text: 'Find a current list of available Gemini models'
        }
      end,
      display_priority: 470,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      
      input_fields: lambda { |od, c, cf| od['gen_generate_input'] },
      
      output_fields: lambda do |od, c|
        [
          # Gemini API response structure
          { name: 'responseId' },
          { name: 'modelVersion' },
          { name: 'usageMetadata', type: 'object', properties: [
            { name: 'promptTokenCount', type: 'integer' },
            { name: 'candidatesTokenCount', type: 'integer' },
            { name: 'totalTokenCount', type: 'integer' }
          ]},
          
          { name: 'candidates', type: 'array', of: 'object', properties: [
            { name: 'finishReason' },
            { name: 'safetyRatings', type: 'array', of: 'object' },
            { name: 'groundingMetadata', type: 'object' },
            { name: 'content', type: 'object', properties: [
              { name: 'role' },
              { name: 'parts', type: 'array', of: 'object' }
            ]}
          ]},
          
          # RAG-specific outputs
          { name: 'parsed', type: 'object', properties: [
            { name: 'answer' },
            { name: 'citations', type: 'array', of: 'object', properties: [
              { name: 'chunk_id' },
              { name: 'source' },
              { name: 'uri' },
              { name: 'score', type: 'number' }
            ]}
          ]},
          
          { name: 'confidence', type: 'number' },
          
          # Signal tracking
          { name: 'applied_signals', type: 'array', of: 'string',
            hint: 'Which signals were used to enhance generation' },
          
          # Standard fields
          { name: 'complete_output', type: 'object' },
          { name: 'facets', type: 'object', optional: true }
        ] + call(:standard_operational_outputs)
      end,
      
      execute: lambda do |connection, raw_input|
        ctx = call(:step_begin!, :gen_generate, raw_input)
        
        begin
          input = call(:normalize_input_keys, raw_input)
          
          # Prepare the request
          request_info = call(:gen_generate_prepare_request!, connection, input, ctx['cid'])
          
          # Make the HTTP request
          resp = post(request_info['url'])
                  .headers(request_info['headers'])
                  .payload(request_info['payload'])
          
          # Process the response
          result = call(:gen_generate_process_response!, resp, input, request_info, connection)
          
          # Add applied signals tracking
          result['applied_signals'] = request_info['applied_signals'] || []
          
          # Extract key metrics for facets
          mode = (input['mode'] || 'plain').to_s
          model = input['model']
          finish_reason = call(:_facet_finish_reason, result) rescue result.dig('candidates', 0, 'finishReason')
          
          # Build facets
          facets = {
            'mode' => mode,
            'model' => model,
            'finish_reason' => finish_reason,
            'confidence' => result['confidence'],
            'has_citations' => result.dig('parsed', 'citations').is_a?(Array) && result.dig('parsed', 'citations').any?,
            'signals_used' => result['applied_signals'].any?
          }.compact
          
          call(:step_ok!, ctx, result, 200, 'OK', facets)
          
        rescue => e
          call(:step_err!, ctx, e)
        end
      end,
      
      sample_output: lambda do
        call(:sample_gen_generate)
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
    sample_deterministic_filter_simplified: lambda do
      {
        'passed' => true,
        'hard_block' => false,
        'hard_reason' => nil,
        'email_type' => 'direct_request',
        'email_text' => "Subject: PTO Request\n\nBody: I'd like to request...",
        'soft_score' => 3,
        'gate' => {
          'prelim_pass' => true,
          'hard_block' => false,
          'hard_reason' => nil,
          'soft_score' => 3,
          'decision' => 'KEEP',
          'generator_hint' => 'check_policy'
        },
        'ok' => true,
        'op_telemetry' => call(:sample_telemetry, 8)
      }
    end,
    sample_ai_policy_filter_enhanced: lambda do
      {
        'policy' => {
          'decision' => 'KEEP',
          'confidence' => 0.85,
          'matched_signals' => ['direct_question', 'policy_reference'],
          'reasons' => ['Clear PTO policy question']
        },
        'intent' => {
          'label' => 'information_request',
          'confidence' => 0.92,
          'basis' => 'llm_classification'
        },
        'short_circuit' => false,
        'email_type' => 'direct_request',
        'generator_gate' => {
          'pass_to_responder' => true,
          'reason' => 'meets_requirements',
          'generator_hint' => 'pass'
        },
        'signals_intent' => 'information_request',
        'signals_intent_confidence' => 0.92,
        'ok' => true,
        'op_telemetry' => call(:sample_telemetry, 45)
      }
    end,
    sample_ai_triage_filter: lambda do
      {
        'decision' => 'KEEP',
        'confidence' => 0.88,
        'reasons' => [
          'Employee asking about benefits transfer',
          'Clear question requiring information',
          'Legitimate work-related inquiry'
        ],
        'matched_signals' => ['question', 'benefits', '401k', 'transfer_request'],
        'detected_domain' => 'benefits',
        'has_question' => true,
        'should_continue' => true,
        'short_circuit' => false,
        'signals_triage' => 'KEEP',
        'signals_confidence' => 0.88,
        'signals_domain' => 'benefits',
        'ok' => true,
        'op_telemetry' => call(:sample_telemetry, 35)
      }
    end,
    sample_ai_intent_classifier: lambda do
      {
        'intent' => 'information_request',
        'confidence' => 0.88,
        'is_actionable' => true,
        'secondary_intents' => ['status_inquiry'],
        'entities' => [
          {
            'type' => 'policy_type',
            'value' => 'PTO',
            'context' => 'vacation time policy'
          },
          {
            'type' => 'date_reference',
            'value' => 'next month',
            'context' => 'planning vacation next month'
          }
        ],
        'sentiment' => 'neutral',
        'requires_context' => true,
        'suggested_category' => 'PTO',
        'signals_intent' => 'information_request',
        'signals_intent_confidence' => 0.88,
        'signals_triage' => 'KEEP',
        'ok' => true,
        'op_telemetry' => call(:sample_telemetry, 38)
      }
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
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => {
          'top_score' => 0.91,
          'categories_count' => 3
        },
        'ok' => true,
        'op_correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
        'op_telemetry' => call(:sample_telemetry, 11)
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
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => {
          'mode' => 'llm',
          'top_prob' => 0.86
        },
        'ok' => true,
        'op_correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
        'op_telemetry' => call(:sample_telemetry, 18)
      })
    end,
    sample_llm_referee_with_contexts: lambda do
      referee = {
        'category' => 'Billing',
        'confidence' => 0.86,
        'reasoning' => 'Invoice reference detected',
        'distribution' => [
          { 'category' => 'Billing', 'prob' => 0.86 },
          { 'category' => 'Support', 'prob' => 0.10 },
          { 'category' => 'Sales', 'prob' => 0.04 }
        ]
      }
      
      salience = {
        'span' => 'Can you approve the Q4 budget increase?',
        'reason' => 'Clear action request',
        'importance' => 0.92,
        'tags' => ['approval', 'budget'],
        'entities' => [{ 'type' => 'department', 'text' => 'Finance' }],
        'cta' => 'Approve budget',
        'deadline_iso' => '2025-10-24T17:00:00Z',
        'focus_preview' => 'Email preview...',
        'span_source' => 'llm'
      }
      
      business_data = {
        'chosen' => 'Billing',  # CRITICAL field
        'confidence' => 0.86,
        'referee' => referee,
        'salience' => salience,
        'signals_category' => 'Billing'  # For downstream
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => {
          'chosen' => 'Billing',
          'confidence' => 0.86
        },
        'ok' => true,
        'op_correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
        'op_telemetry' => call(:sample_telemetry, 19)
      })
    end,
    sample_gen_generate: lambda do
      candidates = [{
        'content' => { 
          'parts' => [{ 'text' => 'Based on the provided context, the invoice total is $1,234.56.' }],
          'role' => 'model'
        },
        'finishReason' => 'STOP',
        'safetyRatings' => [
          { 'category' => 'HARM_CATEGORY_SEXUALLY_EXPLICIT', 'probability' => 'NEGLIGIBLE' }
        ]
      }]
      
      parsed = {
        'answer' => 'Based on the provided context, the invoice total is $1,234.56.',
        'citations' => [
          { 'chunk_id' => 'doc-1#c2', 'source' => 'invoice.pdf', 'uri' => 'gs://bucket/invoice.pdf', 'score' => 0.92 }
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
        'parsed' => parsed,
        'applied_signals' => ['category', 'intent']
      }
      
      facets = {
        'mode' => 'rag_with_context',
        'model' => 'gemini-2.0-flash',
        'finish_reason' => 'STOP',
        'confidence' => 0.90,
        'has_citations' => true,
        'signals_used' => true
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => facets,
        'ok' => true,
        'telemetry' => call(:sample_telemetry, 150)
      })
    end,
    sample_rank_texts_with_ranking_api: lambda do
      records = [
        {
          'id' => 'doc-1',
          'score' => 0.92,
          'rank' => 1,
          'content' => 'PTO policy: 20 days per year...',
          'metadata' => { 'source' => 'hr-handbook.pdf' },
          'llm_relevance' => 0.95,
          'category_alignment' => 0.85
        }
      ]
      
      context_chunks = [
        {
          'id' => 'doc-1',
          'text' => 'PTO policy: 20 days per year...',
          'score' => 0.92,
          'source' => 'hr-handbook.pdf',
          'uri' => 'gs://bucket/hr-handbook.pdf',
          'metadata' => { 'page' => 15 },
          'metadata_kv' => [{ 'key' => 'page', 'value' => '15' }],
          'metadata_json' => '{"page":15}',
          'llm_relevance' => 0.95,
          'category_alignment' => 0.85
        }
      ]
      
      business_data = {
        'records' => records,
        'context_chunks' => context_chunks,
        'ranking_metadata' => {
          'category' => 'PTO',
          'llm_model' => 'gemini-2.0-flash',
          'contexts_filtered' => 0,
          'contexts_ranked' => 1
        }
      }
      
      business_data.merge({
        'complete_output' => business_data.dup,
        'facets' => {
          'category' => 'PTO',
          'top_score' => 0.92
        },
        'ok' => true,
        'op_correlation_id' => 'a1b2c3d4-e5f6-7890-abcd-ef1234567890',
        'op_telemetry' => call(:sample_telemetry, 85)
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
    ui_df_inputs_simplified: lambda do |object_definitions, cfg|
      show_adv = call(:ui_truthy, cfg['show_advanced'])
      
      # Simplified base - no intent-related fields
      base = [
        { name: 'email', type: 'object', optional: false, properties: [
          { name: 'subject', optional: false, hint: 'Email subject line' },
          { name: 'body', optional: false, control_type: 'text-area', hint: 'Email body content' },
          { name: 'from', optional: true, hint: 'Sender email address' }
        ]},
        
        { name: 'rules_mode', label: 'Rules Mode', control_type: 'select', 
          default: 'none', pick_list: 'rules_modes', extends_schema: true,
          hint: 'Choose how to provide filtering rules' },
          
        { name: 'rules_rows', label: 'Rule Definitions',
          ngIf: 'input.rules_mode == "rows"', type: 'array', of: 'object',
          properties: [
            { name: 'family', control_type: 'select',
              options: [['HARD','HARD'], ['SOFT','SOFT'], ['THRESHOLD','THRESHOLD']] },
            { name: 'field', control_type: 'select',
              options: [['subject','subject'], ['body','body'], ['from','from']] },
            { name: 'operator', control_type: 'select',
              options: [['contains','contains'], ['regex','regex']] },
            { name: 'pattern', hint: 'Pattern to match' },
            { name: 'weight', type: 'integer', default: 1 },
            { name: 'action', optional: true },
            { name: 'enabled', type: 'boolean', control_type: 'checkbox', default: true }
          ],
          optional: true, hint: 'Define rules for filtering' },
          
        { name: 'rules_json', label: 'Rules JSON',
          ngIf: 'input.rules_mode == "json"', optional: true, control_type: 'text-area',
          hint: 'Paste complete rulepack JSON' }
      ]
      
      if show_adv
        base + [
          { name: 'attachments', type: 'array', of: 'object', optional: true,
            properties: [
              { name: 'filename' },
              { name: 'mimeType' },
              { name: 'size', type: 'integer' }
            ],
            hint: 'Attachments for rule evaluation' },
          { name: 'auth', type: 'object', optional: true,
            hint: 'Authentication flags (SPF, DKIM)' }
        ]
      else
        base
      end
    end,
    ui_policy_inputs_enhanced: lambda do |object_definitions, cfg|
      adv = call(:ui_truthy, cfg['show_advanced'])
      
      base = [
        { name: 'email_text', optional: false },
        { name: 'email_type', optional: true, default: 'direct_request',
          hint: 'From deterministic filter (direct_request, forwarded_chain, etc.)' },
        
        # Intent configuration
        { name: 'intent_types', label: 'Intent Types', type: 'array', of: 'string',
          default: ['information_request', 'action_request', 'status_inquiry', 
                  'complaint', 'auto_reply', 'unknown'],
          hint: 'Intent categories to detect' },
        
        { name: 'actionable_intents', label: 'Actionable Intents', 
          type: 'array', of: 'string',
          default: ['information_request', 'action_request'],
          hint: 'Intents that can proceed to generation' },
        
        { name: 'require_actionable_intent', type: 'boolean', 
          control_type: 'checkbox', default: true,
          hint: 'Block generation for non-actionable intents' },
        
        # Confidence thresholds
        { name: 'min_confidence_for_generation', type: 'number', 
          default: 0.60, hint: 'Minimum confidence to pass to generator' },
        
        { name: 'confidence_short_circuit', type: 'number', 
          default: 0.85, hint: 'Confidence to skip downstream when IRRELEVANT' }
      ]
      
      if adv
        base + [
          { name: 'model', default: 'gemini-2.0-flash' },
          { name: 'temperature', type: 'number', default: 0 },
          
          { name: 'policy_mode', control_type: 'select',
            options: [['None','none'], ['JSON','json']], 
            default: 'none', extends_schema: true },
            
          { name: 'policy_json', label: 'Policy JSON',
            ngIf: 'input.policy_mode == "json"', optional: true,
            control_type: 'text-area',
            hint: 'Additional policy rules in JSON format' }
        ]
      else
        base
      end
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
      show_adv = call(:ui_truthy, cfg['show_advanced'])
      
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
      
      if show_adv
        base + [
          { name: 'categories_json', label: 'Categories JSON', control_type: 'text-area',
            ngIf: 'input.categories_mode == "json"', optional: true,
            hint: 'Paste categories array JSON for testing (overrides pills this run).' },
          { name: 'contexts', type: 'array', of: 'object', optional: true,
            properties: [
              { name: 'id' },
              { name: 'text' },
              { name: 'score', type: 'number' },
              { name: 'source' }
            ],
            hint: 'Pre-provide contexts to append to email for better categorization' },
          { name: 'intent_kind', optional: true,
            hint: 'Can gate processing based on intent type from earlier steps' }
        ]
      else
        base
      end
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
      elsif score >= lo && score <= hi then 'HUMAN'
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

    # Generative query
    gen_generate_prepare_request!: lambda do |connection, input, corr=nil|
      corr = (corr.to_s.strip.empty? ? call(:build_correlation_id) : corr.to_s.strip)
      mode = (input['mode'] || 'plain').to_s
      
      # Track which signals we're using
      applied_signals = []

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
        corpus = call(:normalize_rag_corpus, connection, input['rag_corpus'])
        error('rag_corpus is required for grounded_rag_store') if corpus.blank?
        
        call(:guard_threshold_union!, input['vector_distance_threshold'], input['vector_similarity_threshold'])
        call(:guard_ranker_union!, input['rank_service_model'], input['llm_ranker_model'])

        vr = { 'ragResources' => [ { 'ragCorpus' => corpus } ] }
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
        # Technical validation: contexts are required for this mode
        chunks = call(:safe_array, input['context_chunks'])
        error('context_chunks are required for rag_with_context mode') if chunks.empty?
        
        # Build RAG prompt
        q        = input['question'].to_s
        error('question is required for rag_with_context mode') if q.blank?
        
        maxn     = call(:clamp_int, (input['max_chunks'] || 20), 1, 100)
        chunks   = chunks.first(maxn)
        
        items = chunks.map { |c| 
          c.merge('text' => call(:truncate_chunk_text, c['text'], 800)) 
        }
        
        target_total  = (input['max_prompt_tokens'].presence || 3000).to_i
        reserve_out   = (input['reserve_output_tokens'].presence || 512).to_i
        budget_prompt = [target_total - reserve_out, 400].max
        model_for_cnt = (input['count_tokens_model'].presence || input['model']).to_s
        strategy      = (input['trim_strategy'].presence || 'drop_low_score').to_s

        ordered = case strategy
                  when 'diverse_mmr' then call(:mmr_diverse_order, items.sort_by { |c| [-(c['score']||0.0).to_f, c['id'].to_s] }, alpha: 0.7, per_source_cap: 3)
                  when 'drop_low_score' then items.sort_by { |c| [-(c['score']||0.0).to_f, c['id'].to_s] }
                  else items
                  end
        ordered = call(:drop_near_duplicates, ordered, 0.9)

        # Build system prompt with optional signal enrichment
        sys_text = input['system_preamble'].presence ||
          <<~SYSPROMPT
            You are a helpful HR assistant providing guidance to employees. Your tone should be:
            - Warm and supportive, not procedural or cold
            - Use "you can" or "you may want to" instead of "you must" or "you are responsible for"
            - Offer helpful next steps and additional context when available
            - Acknowledge that transitions can be challenging
            - Include practical tips from the context when relevant
            
            CRITICAL OUTPUT REQUIREMENTS:
            1. Your ENTIRE response must be valid JSON - no text before or after
            2. Use EXACTLY the schema provided - no extra fields
            3. Answer using ONLY the provided context chunks
            4. Maintain a conversational, helpful tone while being accurate
          SYSPROMPT
        
        # Apply signal enrichment if enabled
        if input['use_signal_enrichment'] != false
          if input['signals_category'].present?
            sys_text += "\n\nDomain context: This is a #{input['signals_category']} inquiry."
            applied_signals << 'category'
          end
          
          if input['signals_intent'].present?
            intent_guidance = case input['signals_intent']
            when 'information_request' then 'Provide clear, factual information.'
            when 'action_request' then 'Focus on actionable steps or procedures.'
            when 'status_inquiry' then 'Provide current status and any relevant updates.'
            else nil
            end
            if intent_guidance
              sys_text += "\nResponse style: #{intent_guidance}"
              applied_signals << 'intent'
            end
          end
          
          # Adjust temperature based on confidence signal
          if input['signals_confidence'].present? && gen_cfg.nil?
            conf = input['signals_confidence'].to_f
            # Higher upstream confidence = lower temperature
            temp_adjustment = conf > 0.8 ? 0.0 : (conf > 0.6 ? 0.3 : 0.5)
            gen_cfg = { 'temperature' => temp_adjustment }
            applied_signals << 'confidence'
          end
        end
        
        kept = call(:select_prefix_by_budget, connection, ordered, q, sys_text, budget_prompt, model_for_cnt)
        blob = call(:format_context_chunks, kept)
        
        gen_cfg ||= {}
        gen_cfg['temperature'] = (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0)
        gen_cfg['maxOutputTokens'] = reserve_out
        gen_cfg['responseMimeType'] = 'application/json'
        gen_cfg['responseSchema'] = {
          'type'=>'object','additionalProperties'=>false,
          'properties'=>{
            'answer'=>{'type'=>'string'},
            'citations'=>{'type'=>'array','items'=>{'type'=>'object','additionalProperties'=>false,
              'properties'=>{'chunk_id'=>{'type'=>'string'},'source'=>{'type'=>'string'},'uri'=>{'type'=>'string'},'score'=>{'type'=>'number'}}}}
          },
          'required'=>['answer']
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

      {
        'url' => url,
        'headers' => call(:request_headers_auth, connection, corr, connection['user_project'], req_params),
        'payload' => call(:json_compact, payload),
        'mode' => mode,
        'correlation_id' => corr,
        'started_at' => Time.now,
        'applied_signals' => applied_signals
      }
    end,
    gen_generate_process_response!: lambda do |resp, input, request_info, connection|
      t0 = request_info['started_at']
      corr = request_info['correlation_id']
      mode = request_info['mode']
      
      code = call(:telemetry_success_code, resp)
      
      out = resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
      
      if mode == 'rag_with_context'
        text   = resp.dig('candidates',0,'content','parts',0,'text').to_s
        parsed = call(:safe_parse_json, text)
        
        # DEBUG: Log what we're working with
        debug_info = {}
        
        # Check what chunks we received
        debug_info['chunks_received'] = input['context_chunks'].is_a?(Array) ? input['context_chunks'].length : 0
        debug_info['chunks_sample'] = input['context_chunks']&.first&.keys if input['context_chunks'].is_a?(Array)
        
        # Check what citations the LLM gave us
        debug_info['citations_from_llm'] = parsed['citations']&.length || 0
        debug_info['citation_ids'] = parsed['citations']&.map { |c| c['chunk_id'] } if parsed['citations'].is_a?(Array)
        
        # ENHANCEMENT: Build chunk map with multiple ID formats
        chunk_map = {}
        if input['context_chunks'].is_a?(Array)
          input['context_chunks'].each_with_index do |chunk, idx|
            # Try multiple ID fields
            chunk_id = chunk['id'] || chunk['chunk_id'] || "ctx-#{idx+1}"
            
            # Also map without brackets in case LLM returns [ctx-1] as ctx-1
            cleaned_id = chunk_id.to_s.gsub(/^\[|\]$/, '')
            
            # Map both the original and cleaned IDs
            [chunk_id, cleaned_id, "[#{chunk_id}]", "[#{cleaned_id}]"].each do |id_variant|
              chunk_map[id_variant] = {
                'source' => chunk['source'] || chunk.dig('metadata', 'source') || chunk.dig('metadata', 'sourceDisplayName'),
                'uri' => chunk['uri'] || chunk.dig('metadata', 'uri') || chunk.dig('metadata', 'sourceUri') || chunk.dig('metadata', 'url'),
                'score' => chunk['score'] || chunk['relevanceScore'] || 0.0,
                'original_id' => chunk_id
              }
            end
          end
          
          debug_info['chunk_map_keys'] = chunk_map.keys.take(5)  # First 5 keys for debugging
        end
        
        # Enrich citations
        if parsed.is_a?(Hash) && parsed['citations'].is_a?(Array)
          parsed['citations'] = parsed['citations'].map do |citation|
            cited_id = citation['chunk_id']
            
            # Try to find chunk data with various ID formats
            chunk_data = chunk_map[cited_id] || 
                        chunk_map["[#{cited_id}]"] || 
                        chunk_map[cited_id.to_s.gsub(/^\[|\]$/, '')]
            
            if chunk_data
              enriched = {
                'chunk_id' => citation['chunk_id'],
                'source' => citation['source'].present? ? citation['source'] : chunk_data['source'],
                'uri' => citation['uri'].present? ? citation['uri'] : chunk_data['uri'],
                'score' => citation['score'] || chunk_data['score']
              }
              
              debug_info["enriched_#{cited_id}"] = {
                'had_uri_before' => citation['uri'].present?,
                'has_uri_after' => enriched['uri'].present?,
                'uri_value' => enriched['uri']
              }
              
              enriched.compact
            else
              debug_info["missing_#{cited_id}"] = 'not_found_in_chunk_map'
              citation
            end
          end
        end
        
        # Add debug info to output if in debug mode
        if !call(:normalize_boolean, connection['prod_mode']) || input['debug']
          (out['debug'] ||= {})['citation_enrichment'] = debug_info
        end
        
        out['parsed'] = { 
          'answer' => parsed['answer'] || text, 
          'citations' => parsed['citations'] || [] 
        }
        
        # Compute overall confidence from cited chunk scores
        conf = call(:overall_confidence_from_citations, out['parsed']['citations'])
        out['confidence'] = conf if conf
        
        (out['telemetry'] ||= {})['confidence'] = { 
          'basis' => 'citations_topk_avg', 
          'k' => 3,
          'n' => Array(out.dig('parsed','citations')).length 
        }
      end
      
      out
    end,
    gen_generate_handle_error!: lambda do |err, request_info, connection, input|
      t0 = request_info['started_at']
      corr = request_info['correlation_id']
      
      g   = call(:extract_google_error, err)
      msg = [err.to_s, (g['message'] || nil)].compact.join(' | ')
      env = call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, err), msg)
      
      if !call(:normalize_boolean, connection['prod_mode']) && call(:normalize_boolean, input['debug'])
        env['debug'] = call(:debug_pack, true, request_info['url'], request_info['payload'], g)
      end
      
      error(env)
    end,

    # Preview pack
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
    extract_uri_from_context: lambda do |context|
      # Try multiple possible URI field locations
      uri = context['uri'] || 
            context.dig('metadata', 'uri') ||
            context.dig('metadata', 'sourceUri') ||
            context.dig('metadata', 'source_uri') ||
            context.dig('metadata', 'gcsUri') ||
            context.dig('metadata', 'url') ||
            context['sourceUri'] ||
            context['source_uri']
      
      # Clean up the URI if needed
      uri = uri.to_s.strip if uri
      uri.present? ? uri : nil
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
    end,

    # Standard field groups
    standard_operational_outputs: lambda do
      [
        { name: 'op_correlation_id' },
        { name: 'op_telemetry', type: 'object', properties: [
          { name: 'http_status', type: 'integer' },
          { name: 'message' },
          { name: 'duration_ms', type: 'integer' },
          { name: 'correlation_id' }
        ]}
      ]
    end,
    standard_operational_inputs: lambda do
      [
        { name: 'op_correlation_id', optional: true, sticky: true,
          hint: 'Tracking ID across all stages' },
        { name: 'op_debug', type: 'boolean', optional: true }
      ]
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
    # - host=`https://aiplatform.googleapis.com``/v1/{model}:generateContent` 
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
