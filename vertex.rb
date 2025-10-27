# frozen_string_literal: true
require 'openssl'
require 'base64'
require 'json'
require 'time'
require 'securerandom'


{
  title: 'Vertex RAG Engine',
  subtitle: 'RAG Engine',
  version: '0.9.5',
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
      { name: 'location',                   optional: false,  control_type: 'text',
        hint: 'e.g., global, us-central1, us-east4' },
      { name: 'project_id',                 optional: false,   control_type: 'text',
        hint: 'GCP project ID (inferred from key if blank)' },
      { name: 'user_project',               optional: true,   control_type: 'text',      label: 'User project for quota/billing',
        extends_schema: true, hint: 'Sets x-goog-user-project for billing/quota. Service account must have roles/serviceusage.serviceUsageConsumer on this project.' },
      { name: 'discovery_api_version', label: 'Discovery API version', control_type: 'select', optional: true, default: 'v1alpha',
        pick_list: 'discovery_versions', hint: 'v1alpha for AI Applications; switch to v1beta/v1 if/when you migrate.' }
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
        # Keep headers minimal; envelope/correlation lives in actions
        h = { 'Authorization' => "Bearer #{connection['access_token']}" }
        up = connection['user_project'].to_s.strip
        h['x-goog-user-project'] = up unless up.empty?
        headers(h)
      end,

      token_url: 'https://oauth2.googleapis.com/token',

      # Let Workato trigger re-acquire on auth errors
      refresh_on: [401],
      detect_on:  [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    },
  
    base_uri: lambda do |connection|
      # This block cannot call a method, the context WithGlobalDSL does not expose 'call'
      loc  = (connection['location'].to_s.strip.downcase)
      loc  = 'global' if loc.empty?
      host = (loc == 'global') ? 'aiplatform.googleapis.com' : "#{loc}-aiplatform.googleapis.com"
      "https://#{host}/v1/"
    end,

  },

  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |connection|
    proj = connection['project_id']
    loc  = connection['location']
    base = (connection['base_url'].presence || "https://#{loc}-aiplatform.googleapis.com").gsub(%r{/+$}, '')
    get("#{base}/v1/projects/#{proj}/locations/#{loc}")
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
      fields: lambda do
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
      fields: lambda do
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
      fields: lambda do
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
      fields: lambda do
        [
          { name: 'predictions', type: 'array', of: 'object' },
          { name: 'deployedModelId' }
        ]
      end
    },

    batch_job: {
      fields: lambda do |_|
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
      fields: lambda do |_|
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
      fields: lambda do
        [
          { name: 'category'  },   # e.g., HARM_CATEGORY_*
          { name: 'threshold' }    # e.g., BLOCK_LOW_AND_ABOVE
        ]
      end
    },

    kv_pair: {
      fields: lambda do
        [
          { name: 'key' },
          { name: 'value' }
        ]
      end
    }

  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {

    # 1) Email categorization (100)
    gen_categorize_email: {
      title: 'Email: Categorize email',
      subtitle: 'Classify an email into a category',
      help: lambda do |_|
        { body: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.'}
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
        t0   = Time.now
        corr = call(:build_correlation_id)
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

            emb_resp = call(:predict_embeddings, connection, emb_model_path, [email_inst] + cat_insts)
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
              'mode'       => mode,
              'chosen'     => chosen,
              'confidence' => referee['confidence'],
              'referee'    => referee
            }

          else
            error("Unknown mode: #{mode}")
          end

          # --- post-processing / salience blend & attachments ---
          if preproc && preproc['importance']
            blend = [[(input['confidence_blend'] || 0.15).to_f, 0.0].max, 0.5].min
            result['confidence'] = [[result['confidence'].to_f + blend * (preproc['importance'].to_f - 0.5), 0.0].max, 1.0].min
          end
          result['preproc'] = preproc if preproc

          # Attach telemetry and RETURN the hash
          result = result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
          result
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
          },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },
    email_extract_salient_span: {
      title: 'Email: Extract salient span',
      subtitle: 'Pull the most important sentence/paragraph from an email',
      display_priority: 100,
      help: lambda do |_|
        { body: 'Heuristically trims boilerplate/quotes, then asks the model for the single most important span (<= 500 chars), with rationale, tags, and optional call-to-action metadata.' }
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
        t0   = Time.now
        corr = call(:build_correlation_id)
        url = nil; req_body = nil
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
          req_body = call(:json_compact, payload)
          resp = post(url).headers(call(:request_headers, corr)).payload(req_body)

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
          out
        rescue => e
          g   = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          out = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
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

    # 2)  Generate content (90)
    gen_generate_content: {
      title: 'Generative: Generate content (Gemini)',
      subtitle: 'Generate content from a prompt',
      help: lambda do |_|
        { body: 'Provide a prompt to generate content from an LLM. Uses "POST :generateContent".'}
      end,
      display_priority: 90,
      retry_on_request: ['GET','HEAD'], # removed "POST" to preserve idempotency, prevent duplication of jobs
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, config_fields|
        object_definitions['gen_generate_content_input']
      end,

      output_fields: lambda do |object_definitions, connection|
         Array(object_definitions['generate_content_output']) + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        t0   = Time.now
        corr = call(:build_correlation_id)

        begin
          # Model path and region must agree
          model_path = call(:build_model_path_with_global_preview, connection, input['model'])
          loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase

          # Build payload (hardened)
          contents = call(:sanitize_contents!, input['contents'])
          error('At least one non-system message with non-empty parts is required in contents') if contents.blank?

          sys_inst = call(:system_instruction_from_text, input['system_preamble'])

          gen_cfg  = call(:sanitize_generation_config, input['generation_config'])

          tools    = call(:sanitize_tools!, input['tools'])
          tool_cfg = call(:safe_obj, input['toolConfig'])
          safety   = call(:sanitize_safety!, input['safetySettings'])

          payload = {
            'contents'          => contents,
            'systemInstruction' => sys_inst,
            'tools'             => tools,
            'toolConfig'        => tool_cfg,
            'safetySettings'    => safety,
            'generationConfig'  => gen_cfg
          }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

          url  = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
          resp = post(url)
                  .headers(call(:request_headers, corr))
                  .payload(call(:json_compact, payload))

          # Guard: empty candidates (safety block or other)
          cands = call(:safe_array, resp['candidates'])
          if cands.empty?
            # Surface a friendlier error but keep telemetry envelope
            err_msg = (resp.dig('promptFeedback','blockReason') || 'No candidates returned').to_s
            return {}.merge(call(:telemetry_envelope, t0, corr, false, 200, err_msg))
          end

          code = call(:telemetry_success_code, resp)
          resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))

        rescue => e
          g = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
        end
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
          ],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },
    gen_generate_grounded: {
      title: 'Generative: Generate (grounded)',
      subtitle: 'Generate with grounding via Google Search or Vertex AI Search',
      display_priority: 90,
      retry_on_request: [ 'GET', 'HEAD' ],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, connection, _config_fields|
        [
          { name: 'contents', type: 'array', of: 'object', properties: object_definitions['content'], optional: false },
          { name: 'model', label: 'Model', optional: false, control_type: 'text' },
          { name: 'grounding', control_type: 'select', pick_list: 'modes_grounding', optional: false },
          { name: 'system_preamble', optional: true, hint: 'Optional guardrails/system text.' },
          { name: 'vertex_ai_search_datastore', optional: true,
            hint: 'projects/.../locations/.../collections/default_collection/dataStores/...' },
          { name: 'vertex_ai_search_serving_config', optional: true,
            hint: 'projects/.../locations/.../collections/.../engines/.../servingConfigs/default_config' }
        ]
      end,
      output_fields: lambda do |object_definitions, connection|
        Array(object_definitions['generate_content_output']) + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = call(:build_correlation_id)

        # Compute model path
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        # Build payload
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        tools =
          if input['grounding'] == 'google_search'
            [ { 'googleSearch' => {} } ]
          else
            ds   = input['vertex_ai_search_datastore'].to_s
            scfg = input['vertex_ai_search_serving_config'].to_s
            # Back-compat: allow legacy 'engine' by mapping -> servingConfig
            legacy_engine = input['vertex_ai_search_engine'].to_s
            scfg = legacy_engine if scfg.blank? && legacy_engine.present?

            # Enforce XOR
            error('Provide exactly one of vertex_ai_search_datastore OR vertex_ai_search_serving_config') \
              if (ds.blank? && scfg.blank?) || (ds.present? && scfg.present?)

            vas = {}
            vas['datastore']     = ds unless ds.blank?
            vas['servingConfig'] = scfg unless scfg.blank?
            [ { 'retrieval' => { 'vertexAiSearch' => vas } } ]
          end

        gen_cfg = call(:sanitize_generation_config, input['generation_config'])

        payload = {
          'contents'          => contents,
          'systemInstruction' => sys_inst,
          'tools'             => tools,
          'toolConfig'        => input['toolConfig'],
          'safetySettings'    => input['safetySettings'],
          'generationConfig'  => gen_cfg
        }.delete_if { |_k, v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

        # Call endpoint
        loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
        url = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
        begin
          resp = post(url)
                  .headers(call(:request_headers, corr))
                  .payload(call(:json_compact, payload))
          code = call(:telemetry_success_code, resp)
          resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        {
          'responseId' => 'resp-456',
          'modelVersion' => 'gemini-2.5-pro',
          'candidates' => [
            { 'content' => { 'role' => 'model', 'parts' => [ { 'text' => 'Grounded answer...' } ] },
              'groundingMetadata' => { 'citationSources' => [ { 'uri' => 'https://en.wikipedia.org/wiki/...' } ] } }
          ],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },
    gen_answer_with_context: {
      title: 'Generative: Generate (use provided context chunks)',
      subtitle: '',
      help: lambda do |_|
        { body: 'Answer a question using caller-supplied context chunks (RAG-lite). Returns structured JSON with citations.' }
      end,
      display_priority: 90,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'text' },
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

          { name: 'temperature', type: 'number', optional: true, hint: 'Override temperature (default 0).' },

          { name: 'salience_text', label: 'Salience (from prior step)', optional: true,
            hint: 'Short span to prioritize (e.g., email_extract_salient_span.salient_span)' },
          { name: 'salience_id', optional: true, hint: 'Optional ID to show in citations (e.g., "salience")' },
          { name: 'salience_score', type: 'number', optional: true, hint: 'Optional pseudo-score to order with chunks' },

          # Prompt budgeting (optional but recommended)
          { name: 'max_prompt_tokens', type: 'integer', optional: true, default: 3000,
            hint: 'Hard cap on prompt tokens (excludes output). If exceeded, chunks are dropped.' },
          { name: 'reserve_output_tokens', type: 'integer', optional: true, default: 512,
            hint: 'Reserve this many tokens for the model’s answer.' },
          { name: 'count_tokens_model', optional: true,
            hint: 'If set, use this model for countTokens (defaults to `model`).' },
          { name: 'trim_strategy', control_type: 'select', optional: true, default: 'drop_low_score',
            pick_list: 'trim_strategies',
            hint: 'How to shrink when over budget: drop_low_score, diverse_mmr, or truncate_chars' }
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
        url = nil; req_body = nil

        begin
          model_path = call(:build_model_path_with_global_preview, connection, input['model'])

          max_chunks = call(:clamp_int, (input['max_chunks'] || 20), 1, 100)
          chunks     = call(:safe_array, input['context_chunks']).first(max_chunks)
          error('context_chunks must be a non-empty array') if chunks.blank?

          # 2a) Build a unified list with salience (if provided) at the top
          sal_text = (input['salience_text'].to_s.strip)
          sal_id   = (input['salience_id'].presence || 'salience').to_s
          sal_scr  = (input['salience_score'].presence || 1.0).to_f
          items = []
          if sal_text.present?
            items << { 'id' => sal_id, 'text' => sal_text, 'score' => sal_scr, 'source' => 'salience' }
          end
          items.concat(chunks)

          # 2b) Token budgeting targets
          target_total  = (input['max_prompt_tokens'].presence || 3000).to_i
          reserve_out   = (input['reserve_output_tokens'].presence || 512).to_i
          budget_prompt = [target_total - reserve_out, 400].max # never go below 400 for the prompt
          model_for_count = (input['count_tokens_model'].presence || input['model']).to_s

          # 2b-new) Selection pipeline: truncate -> (order w/ strategy) -> drop dupes -> O(log n) token-count selection
          strategy = (input['trim_strategy'].presence || 'drop_low_score').to_s

          # Pin salience if present
          base = []
          base << items.shift if items.first && items.first['source'] == 'salience'

          # Early truncation to tame token growth
          items = items.map { |c| c.merge('text' => call(:truncate_chunk_text, c['text'], 800)) }

          ordered =
            case strategy
            when 'diverse_mmr'
              seed = items.sort_by { |c| [-(c['score'] || 0.0).to_f, c['id'].to_s] }
              call(:mmr_diverse_order, seed, alpha: 0.7, per_source_cap: 3)
            when 'drop_low_score'
              items.sort_by { |c| [-(c['score'] || 0.0).to_f, c['id'].to_s] }
            when 'truncate_chars'
              items # keep incoming order (after truncation)
            else
              items
            end

          ordered = call(:drop_near_duplicates, ordered, 0.9)
          pool = base + ordered

          sys_text = input['system_preamble'].presence ||
            'Answer using ONLY the provided context chunks. If the context is insufficient, reply with “I don’t know.” Keep answers concise and cite chunk IDs.'

          kept = call(:select_prefix_by_budget, connection, pool, input['question'], sys_text, budget_prompt, model_for_count)
          items = kept

          # 2c) Build the final blob; if empty, try to keep a trimmed salience
          ctx_blob = call(:format_context_chunks, items)
          if ctx_blob.to_s.strip.empty? && sal_text.present?
            ctx_blob = call(:format_context_chunks, [
              { 'id' => sal_id, 'text' => sal_text[0, 4000], 'score' => 1.0, 'source' => 'salience' }
            ])
          end

          # 3) Request
          gen_cfg = {
            'temperature'       => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
            'maxOutputTokens'   => reserve_out,
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

          sys_inst = call(:system_instruction_from_text, sys_text)
          contents = [
            { 'role' => 'user', 'parts' => [ { 'text' => "Question:\n#{input['question']}\n\nContext:\n#{ctx_blob}" } ] }
          ]
          payload = { 'contents' => contents, 'systemInstruction' => sys_inst, 'generationConfig' => gen_cfg }

          # Derive location from model_path to avoid region/global mismatch
          loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
          url  = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
          req_body = call(:json_compact, payload)

          resp = post(url).headers(call(:request_headers, corr)).payload(req_body)

          text = resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s

          # One controlled retry if nothing came back (e.g., safety or schema hiccup)
          if text.to_s.strip.empty?
            retry_payload = payload.dup
            retry_payload['generationConfig'] = {
              'temperature' => 0,
              'maxOutputTokens' => reserve_out,
              'responseMimeType' => 'text/plain'
            }
            resp2 = post(url).headers(call(:request_headers, corr)).payload(call(:json_compact, retry_payload))
            text = resp2.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
            resp = resp2 if text.present?
          end

          parsed = call(:safe_parse_json, text)

          {
            'answer'     => parsed['answer'] || text,
            'citations'  => parsed['citations'] || [],
            'responseId' => resp['responseId'],
            'usage'      => resp['usageMetadata']
          }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

        rescue => e
          out = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end
          out
        end
      end,

      sample_output: lambda do
        {
          'answer' => 'The outage began at 09:12 UTC due to a misconfigured firewall rule.',
          'citations' => [
            { 'chunk_id' => 'doc-42#p3', 'source' => 'postmortem', 'uri' => 'https://kb/acme/pm-42#p3', 'score' => 0.89 }
          ],
          'responseId' => 'resp-789',
          'usage' => { 'promptTokenCount' => 311, 'candidatesTokenCount' => 187, 'totalTokenCount' => 498 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },

    # 3)  RAG store engine (80-86)
    rag_retrieve_contexts: {
      title: 'RAG (Serving): Retrieve contexts',
      subtitle: 'projects.locations:retrieveContexts (Vertex RAG Store)',
      display_priority: 86,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do
        [
          { name: 'rag_corpus', optional: false,
            hint: 'Accepts either full resource name (e.g., "projects/{project}/locations/{region}/ragCorpora/{corpus}") or the "corpus"' },
          { name: 'question', optional: false },
          { name: 'restrict_to_file_ids', type: 'array', of: 'string', optional: true },
          { name: 'max_contexts', type: 'integer', optional: true, default: 20 }
        ]
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
              { name: 'metadata_kv', label: 'metadata (KV)', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
              { name: 'metadata_json', label: 'metadata (JSON)', }
            ]
          }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message' }, { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        # Build correlation ID, now (for traceability)
        t0 = Time.now
        corr = call(:build_correlation_id)
        
        proj = connection['project_id']
        loc  = connection['location']
        raise 'Connection is missing project_id' if proj.blank?
        raise 'Connection is missing location'   if loc.blank?

        corpus = call(:normalize_rag_corpus, connection, input['rag_corpus'])
        error('rag_corpus is required') if corpus.blank?

        loc = (connection['location'] || '').downcase
        parent = "projects/#{connection['project_id']}/locations/#{loc}"

        payload = call(:build_rag_retrieve_payload, input['question'], corpus, input['restrict_to_file_ids'])

        url  = call(:aipl_v1_url, connection, loc, "#{parent}:retrieveContexts")
        resp = post(url)
                .headers(call(:request_headers, corr))
                .payload(call(:json_compact, payload))

        raw   = call(:normalize_retrieve_contexts!, resp)
        maxn  = call(:clamp_int, (input['max_contexts'] || 20), 1, 200)
        mapped = call(:map_context_chunks, raw, maxn)

        {
          'question' => input['question'],
          'contexts' => mapped
        }.merge(call(:telemetry_envelope, t0, corr, true, call(:telemetry_success_code, raw), 'OK'))
        rescue => e
          g = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
        end
      end,
      sample_output: lambda do
        {
          'question' => 'What is the PTO carryover policy?',
          'contexts' => [
            { 'id' => 'doc-42#c3', 'text' => 'Employees may carry over up to 40 hours...', 'score' => 0.91,
            'source' => 'handbook', 'uri' => 'https://drive.google.com/file/d/abc...', 'metadata' => { 'page' => 7 },
            'metadata_kv' => [ { 'key' => 'page', 'value' => 7 } ],
            'metadata_json' => '{"page":7}' }
          ],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 22, 'correlation_id' => 'sample' }
        }
      end
    },
    rag_answer: {
      title: 'RAG (Serving): Retrieve + answer (one-shot)',
      subtitle: 'Retrieve contexts from a corpus and generate a cited answer',
      display_priority: 86,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'text' },
          { name: 'rag_corpus', optional: false,
            hint: 'projects/{project}/locations/{region}/ragCorpora/{corpus}' },
          { name: 'question', optional: false },

          { name: 'restrict_to_file_ids', type: 'array', of: 'string', optional: true },
          { name: 'max_contexts', type: 'integer', optional: true, default: 12 },

          { name: 'system_preamble', optional: true,
            hint: 'e.g., Only answer from retrieved contexts; say “I don’t know” otherwise.' },
          { name: 'temperature', type: 'number', optional: true, hint: 'Default 0' }
        ]
      end,
      output_fields: lambda do |_|
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
          },
          { name: 'request_preview', type: 'object' },
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message' }, { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        # Build correlation id and now (logging)
        t0 = Time.now
        corr = call(:build_correlation_id)
        begin
          # Validate inputs
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)

          # 1) Retrieve contexts (inline call to same API used by rag_retrieve_contexts)
          corpus = call(:normalize_rag_corpus, connection, input['rag_corpus'])
          error('rag_corpus is required') if corpus.blank?

          loc    = (connection['location'] || '').downcase
          parent = "projects/#{connection['project_id']}/locations/#{loc}"

          retrieve_payload = call(:build_rag_retrieve_payload, input['question'], corpus, input['restrict_to_file_ids'])

          retr_url  = call(:aipl_v1_url, connection, loc, "#{parent}:retrieveContexts")
          retr_req_body = call(:json_compact, retrieve_payload)

          # Precompute model path & generation scaffold used by both real and validate-only flows
          model_path = call(:build_model_path_with_global_preview, connection, input['model'])
          gen_cfg = {
            'temperature'       => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
            'maxOutputTokens'   => 1024,
            'responseMimeType'  => 'application/json',
            'responseSchema'    => {
              'type'=>'object','additionalProperties'=>false,
              'properties'=>{'answer'=>{'type'=>'string'},'citations'=>{'type'=>'array','items'=>{'type'=>'object','additionalProperties'=>false,'properties'=>{'chunk_id'=>{'type'=>'string'},'source'=>{'type'=>'string'},'uri'=>{'type'=>'string'},'score'=>{'type'=>'number'}}}}},
              'required'=>['answer']
            }
          }
          sys_text = (input['system_preamble'].presence ||
            'Answer using ONLY the retrieved context chunks. If the context is insufficient, reply with “I don’t know.” Keep answers concise and include citations with chunk_id, source, uri, and score.')
          sys_inst = { 'role'=>'system','parts'=>[{'text'=>sys_text}] }

          # Dry-run: return previews of both calls
          if call(:normalize_boolean, input['validate_only'])
            preview_retr = call(:request_preview_pack, retr_url, 'POST', call(:request_headers, corr), retr_req_body)
            loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
            gen_url        = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
            preview_gen    = call(:request_preview_pack, gen_url, 'POST', call(:request_headers, corr), call(:json_compact, {
              'contents' => [{ 'role'=>'user','parts'=>[{ 'text' => "Question:\n#{input['question']}\n\nContext:\n<trimmed in runtime>" }]}],
              'systemInstruction' => sys_inst,
              'generationConfig'  => gen_cfg
            }))
            return { 'ok'=>true, 'request_preview'=> { 'retrieve'=>preview_retr['request_preview'], 'generate'=>preview_gen['request_preview'] } }
                   .merge(call(:telemetry_envelope_ex, t0, corr, true, 200, 'DRY_RUN', { 'action' => 'rag_answer' }))
          end

          retr_resp = call(:http_call!, 'POST', retr_url)
                        .headers(call(:request_headers, corr))
                        .payload(retr_req_body)
          raw_ctxs = call(:normalize_retrieve_contexts!, retr_resp)

          maxn  = call(:clamp_int, (input['max_contexts'] || 12), 1, 100)
          chunks = call(:map_context_chunks, raw_ctxs, maxn)
          error('No contexts retrieved; check corpus/permissions/region') if chunks.empty?

          # 2) Generate structured answer with parsed context
          model_path = call(:build_model_path_with_global_preview, connection, input['model'])

          gen_cfg = {
            'temperature'       => (input['temperature'].present? ? call(:safe_float, input['temperature']) : 0),
            'maxOutputTokens'   => 1024,
            'responseMimeType'  => 'application/json',
            'responseSchema'    => {
              'type'        => 'object', 'additionalProperties' => false,
              'properties'  => {
                'answer'    => { 'type' => 'string' },
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

          sys_text = (input['system_preamble'].presence ||
            'Answer using ONLY the retrieved context chunks. If the context is insufficient, reply with “I don’t know.” '\
            'Keep answers concise and include citations with chunk_id, source, uri, and score.')
          sys_inst = { 'role' => 'system', 'parts' => [ { 'text' => sys_text } ] }

          ctx_blob = call(:format_context_chunks, chunks)
          contents = [
            { 'role' => 'user', 'parts' => [
                { 'text' => "Question:\n#{input['question']}\n\nContext:\n#{ctx_blob}" }
              ]
            }
          ]

          gen_payload = {
            'contents'          => contents,
            'systemInstruction' => sys_inst,
            'generationConfig'  => gen_cfg
          }

          # Derive location from model path to avoid region/global mismatch
          loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
          gen_url  = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:generateContent")
          gen_req_body = call(:json_compact, gen_payload)
          gen_raw   = call(:http_call!, 'POST', gen_url)
                        .headers(call(:request_headers, corr))
                        .payload(gen_req_body)


          gen_body  = call(:http_body_json, gen_raw)
          text      = gen_body.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
          parsed = call(:safe_parse_json, text)

          {
            'answer'     => (parsed['answer'] || text),
            'citations'  => (parsed['citations'] || []),
            'responseId' => gen_body['responseId'],
            'usage'      => gen_body['usageMetadata']
          }.merge(call(:telemetry_envelope, t0, corr, true, call(:telemetry_success_code, gen_raw), 'OK'))
        rescue => e
          g = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
        end
      end,

      sample_output: lambda do
        {
          'answer' => 'Employees may carry over up to 40 hours of PTO.',
          'citations' => [
            { 'chunk_id' => 'doc-42#c3', 'source' => 'handbook', 'uri' => 'https://drive.google.com/file/d/abc...', 'score' => 0.91 }
          ],
          'responseId' => 'resp-123',
          'usage' => { 'promptTokenCount' => 298, 'candidatesTokenCount' => 156, 'totalTokenCount' => 454 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 44, 'correlation_id' => 'sample' }
        }
      end
    },

    rag_corpora_create: {
      title: 'RAG (Corpus): Create corpus',
      subtitle: 'projects.locations.ragCorpora.create',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [
          { name: 'corpusId', optional: false, hint: 'Short ID for the new corpus' },
          { name: 'displayName', optional: true },
          { name: 'description', optional: true },
          { name: 'labels', type: 'object', optional: true },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'name' }, { name: 'displayName' }, { name: 'description' },
          { name: 'labels', type: 'object' }, { name: 'createTime' }, { name: 'updateTime' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]},
          { name: 'debug', type: 'object' }
        ]
      end,
      execute: lambda do |connection, input|
        t0 = Time.now; corr = call(:build_correlation_id); url=nil; req_body=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          loc  = connection['location'].to_s.downcase
          parent = "projects/#{connection['project_id']}/locations/#{loc}"
          url  = call(:aipl_v1_url, connection, loc, "#{parent}/ragCorpora")
          body = {
            'displayName' => input['displayName'],
            'description' => input['description'],
            'labels'      => input['labels']
          }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
          req_body = call(:json_compact, body)
          raw  = call(:http_call!, 'POST', url).params(corpusId: input['corpusId'].to_s).headers(call(:request_headers, corr)).payload(req_body)
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          out  = body.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, body) if call(:normalize_boolean, input['debug'])
          end
          out
        rescue => e
          g = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          out = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end
          out
        end
      end,
      sample_output: lambda do
        {
          'name' => 'projects/p/locations/us-central1/ragCorpora/hr-kb',
          'displayName' => 'HR KB',
          'labels' => { 'env' => 'prod' },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample' }
        }
      end
    },
    rag_corpora_get: {
      title: 'RAG (Corpus): Get corpus',
      subtitle: 'projects.locations.ragCorpora.get',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [ { name: 'rag_corpus', optional: false, hint: 'Short id or full resource' } ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'name' }, { name: 'displayName' }, { name: 'description' },
          { name: 'labels', type: 'object' }, { name: 'createTime' }, { name: 'updateTime' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        t0 = Time.now; corr = call(:build_correlation_id); url=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          path = call(:build_rag_corpus_path, connection, input['rag_corpus'])
          loc  = connection['location'].to_s.downcase
          url  = call(:aipl_v1_url, connection, loc, path)
          raw  = call(:http_call!, 'GET', url).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          body.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end
    },
    rag_corpora_list: {
      title: 'RAG (Corpus): List corpora',
      subtitle: 'projects.locations.ragCorpora.list',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [
          { name: 'page_size', type: 'integer', optional: true },
          { name: 'page_token', optional: true },
          { name: 'validate_only', label: 'Validate only (no call)', type: 'boolean', control_type: 'checkbox', optional: true },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'items', type: 'array', of: 'object', properties: [
            { name: 'name' }, { name: 'displayName' }, { name: 'description' },
            { name: 'labels', type: 'object' }, { name: 'createTime' }, { name: 'updateTime' }
          ]},
          { name: 'next_page_token' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]},
          { name: 'request_preview', type: 'object' },
          { name: 'debug', type: 'object' }
        ]
      end,
      execute: lambda do |connection, input|
        t0=Time.now; corr=call(:build_correlation_id); url=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          loc  = connection['location'].to_s.downcase
          parent = "projects/#{connection['project_id']}/locations/#{loc}"
          url  = call(:aipl_v1_url, connection, loc, "#{parent}/ragCorpora")
          qs = {}
          qs['pageSize']  = input['page_size'].to_i if input['page_size'].to_i > 0
          qs['pageToken'] = input['page_token'] if input['page_token'].present?
          if call(:normalize_boolean, input['validate_only'])
            qstr   = (qs && qs.any?) ? ('?' + qs.map { |k,v| "#{k}=#{v}" }.join('&')) : ''
            preview = call(:request_preview_pack, "#{url}#{qstr}", 'GET', call(:request_headers, corr), nil)
            return { 'ok' => true }.merge(preview).merge(call(:telemetry_envelope_ex, t0, corr, true, 200, 'DRY_RUN', { 'action' => 'rag_corpora_list' }))
          end
          raw  = call(:http_call!, 'GET', url).params(qs).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          out = {
            'items' => call(:safe_array, body['ragCorpora']),
            'next_page_token' => body['nextPageToken'],
            'ok' => true
          }.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          unless call(:normalize_boolean, connection['prod_mode'])
            qstr = (qs && qs.any?) ? ('?' + qs.map { |k,v| "#{k}=#{v}" }.join('&')) : ''
            out['debug'] = call(:debug_pack, input['debug'], "#{url}#{qstr}", nil, body) if call(:normalize_boolean, input['debug'])
          end
          out
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end
    },
    rag_corpora_delete: {
      title: 'RAG (Corpus): Delete corpus',
      subtitle: 'projects.locations.ragCorpora.delete',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [ { name: 'rag_corpus', optional: false } ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'name' }, { name: 'done', type: 'boolean' }, { name: 'error', type: 'object' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        t0=Time.now; corr=call(:build_correlation_id); url=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          path = call(:build_rag_corpus_path, connection, input['rag_corpus'])
          loc  = connection['location'].to_s.downcase
          url  = call(:aipl_v1_url, connection, loc, path)
          raw  = call(:http_call!, 'DELETE', url).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          body.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end
    },
    rag_files_import: {
      title: 'RAG (Files): Import files to corpus',
      subtitle: 'projects.locations.ragCorpora.ragFiles:import',
      display_priority: 85,
      retry_on_request: ['GET','HEAD'], # removed "POST" to preserve idempotency, prevent duplication of jobs
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do
        [
          { name: 'rag_corpus_resource_name', optional: false, hint: 'Accepts either full resource name (e.g., "projects/{project}/locations/{region}/ragCorpora/{corpus}") or the "corpus"' },

          # Exactly one source family:
          { name: 'source_family', label: 'Source family', optional: false, control_type: 'select', extends_schema: true,
            pick_list: 'rag_source_families', hint: 'Choose which source you are importing from (purely a UI gate; validation still enforced at runtime).' },
          { name: 'gcs_uris',         optional: true, ngIf: 'input.source_family == "gcs"',   type: 'array', of: 'string', 
            hint: 'Pass files or directory prefixes (e.g., gs://bucket/dir). Wildcards (*, **) are NOT supported.' },
          { name: 'folder_or_files', label: 'Drive input type', optional: true, ngIf: 'input.source_family == "drive"', control_type: 'select', extends_schema: true,
            pick_list: 'drive_input_type' },
          { name: 'drive_folder_id',  optional: true, ngIf: 'input.folder_or_files == "folder"', 
            hint: 'Google Drive folder ID (share with Vertex RAG service agent)' },
          { name: 'drive_file_ids',   optional: true, ngIf: 'input.folder_or_files == "files"', type: 'array', of: 'string', 
            hint: 'Optional explicit file IDs if not using folder' },

          # Tuning / ops
          { name: 'maxEmbeddingRequestsPerMin', type: 'integer', optional: true },
          { name: 'rebuildAnnIndex', type: 'boolean', optional: true, default: false, hint: 'Set true after first large import to build ANN index' },
          { name: 'importResultGcsSink', type: 'object', optional: true, properties: [
              { name: 'outputUriPrefix', optional: false, hint: 'gs://bucket/prefix/' }
            ]},
          # Debug
          { name: 'show_debug', label: 'Show debug options', type: 'boolean', control_type: 'checkbox', optional: true, 
            extends_schema: true, default: false },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true, ngIf: 'input.show_debug == "true"',
            hint: 'Echo request URL/body and Google error body for troubleshooting' }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'name' },           # LRO: projects/.../operations/...
          { name: 'done', type: 'boolean' },
          { name: 'metadata', type: 'object' },
          { name: 'error', type: 'object' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message' }, { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        # Build correlation ID, now (for traceability)
        t0 = Time.now
        corr = call(:build_correlation_id)
        begin
          url = nil
          req_body = nil

          # Validate inputs
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)

          corpus = call(:normalize_rag_corpus, connection, input['rag_corpus_resource_name'])
          error('rag_corpus_resource_name is required') if corpus.blank?

          # Build payload
          payload  = call(:build_rag_import_payload!, input)

          loc = (connection['location'] || '').downcase
          url = call(:aipl_v1_url, connection, loc, "#{corpus}/ragFiles:import")
          req_body = call(:json_compact, payload)
          raw  = call(:http_call!, 'POST', url)
                   .headers(call(:request_headers, corr))
                   .payload(req_body)
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          out  = body.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          if call(:normalize_boolean, input['debug'])
            ops_root = "https://#{call(:aipl_service_host, connection, loc)}/v1/projects/#{connection['project_id']}/locations/#{loc}/operations"
            dbg = call(:debug_pack, true, url, req_body, body) || {}
            dbg['ops_list_url'] = ops_root
            out['debug'] = dbg
          end
          out

        rescue => e
          g = call(:extract_google_error, e)
          vio = (g['violations'] || []).map { |x| "#{x['field']}: #{x['reason']}" }.join(' ; ')
          msg = [e.to_s, (g['message'] || nil), (vio.presence)].compact.join(' | ')
          out = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
          # Assess environment (dev/prod)
          unless call(:normalize_boolean, connection['prod_mode'])
            out['debug'] = call(:debug_pack, input['debug'], url, req_body, nil) if call(:normalize_boolean, input['debug'])
          end
          # Return
          out
        end
      end,

      sample_output: lambda do
        {
          'name' => 'projects/p/locations/us-central1/operations/1234567890',
          'done' => false,
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 18, 'correlation_id' => 'sample' }
        }
      end
    },
    rag_files_list: {
      title: 'RAG (Files): List files in corpus',
      subtitle: 'projects.locations.ragCorpora.ragFiles.list',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [
          { name: 'rag_corpus', optional: false, hint: 'Short id or full resource' },
          { name: 'page_size', type: 'integer', optional: true },
          { name: 'page_token', optional: true },
          { name: 'validate_only', label: 'Validate only (no call)', type: 'boolean', control_type: 'checkbox', optional: true },
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'items', type: 'array', of: 'object', properties: [
            { name: 'name' }, { name: 'displayName' }, { name: 'sourceUri' },
            { name: 'createTime' }, { name: 'updateTime' },
            { name: 'mimeType' }, { name: 'sizeBytes', type: 'integer' },
            { name: 'labels', type: 'object' },
            { name: 'labels_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
            { name: 'labels_json' },
            { name: 'metadata', type: 'object' },
            { name: 'metadata_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
            { name: 'metadata_json' }
          ]},
          { name: 'next_page_token' },
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]},
          { name: 'debug', type: 'object' }
        ]
      end,
      execute: lambda do |connection, input|
        t0=Time.now; corr=call(:build_correlation_id); url=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          corpus = call(:build_rag_corpus_path, connection, input['rag_corpus'])
          loc = connection['location'].to_s.downcase
          url = call(:aipl_v1_url, connection, loc, "#{corpus}/ragFiles")
          qs = {}
          qs['pageSize']  = input['page_size'].to_i if input['page_size'].to_i > 0
          qs['pageToken'] = input['page_token'] if input['page_token'].present?
          if call(:normalize_boolean, input['validate_only'])
            qstr   = (qs && qs.any?) ? ('?' + qs.map { |k,v| "#{k}=#{v}" }.join('&')) : ''
            preview = call(:request_preview_pack, "#{url}#{qstr}", 'GET', call(:request_headers, corr), nil)
            return { 'ok' => true }.merge(preview).merge(call(:telemetry_envelope_ex, t0, corr, true, 200, 'DRY_RUN', { 'action' => 'rag_files_list' }))
          end
          raw  = call(:http_call!, 'GET', url).params(qs).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          items = call(:safe_array, body['ragFiles']).map do |it|
            h  = (it || {}).to_h
            lbl = (h['labels'] || {}).to_h
            md  = (h['metadata'] || {}).to_h
            h.merge(
              'labels_kv'   => lbl.map { |k,v| { 'key' => k.to_s, 'value' => v } },
              'labels_json' => (lbl.empty? ? nil : lbl.to_json),
              'metadata_kv' => md.map  { |k,v| { 'key' => k.to_s, 'value' => v } },
              'metadata_json'=> (md.empty? ? nil : md.to_json)
            )
          end
          out = {
            'items' => items,
            'next_page_token' => body['nextPageToken'],
            'ok' => true
          }.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          unless call(:normalize_boolean, connection['prod_mode'])
            qstr = (qs && qs.any?) ? ('?' + qs.map { |k,v| "#{k}=#{v}" }.join('&')) : ''
            # Keep full wrapper in debug for inspecting status/headers/body
            out['debug'] = call(:debug_pack, input['debug'], "#{url}#{qstr}", nil, body) if call(:normalize_boolean, input['debug'])
          end
          out
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end
    },
    rag_files_get: {
      title: 'RAG (Files): Get file',
      subtitle: 'projects.locations.ragCorpora.ragFiles.get',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [ { name: 'rag_file', optional: false, hint: 'Full name: projects/.../ragCorpora/{id}/ragFiles/{fileId}' } ]
      end,
      output_fields: lambda do |object_definitions, connection|
        [
          { name: 'name' }, { name: 'displayName' }, { name: 'sourceUri' },
          { name: 'mimeType' }, { name: 'sizeBytes', type: 'integer' },
          { name: 'labels', type: 'object' },
          { name: 'labels_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
          { name: 'labels_json' },
          { name: 'metadata', type: 'object' },
          { name: 'metadata_kv', type: 'array', of: 'object', properties: object_definitions['kv_pair'] },
          { name: 'metadata_json' },
          { name: 'createTime' }, { name: 'updateTime' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        t0=Time.now; corr=call(:build_correlation_id); url=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          name = call(:build_rag_file_path, connection, input['rag_file'])
          loc  = connection['location'].to_s.downcase
          url  = call(:aipl_v1_url, connection, loc, name)
          raw  = call(:http_call!, 'GET', url).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          lbl = (body['labels'] || {}).to_h
          md  = (body['metadata'] || {}).to_h
          out = body.merge(
            'labels_kv'    => lbl.map { |k,v| { 'key' => k.to_s, 'value' => v } },
            'labels_json'  => (lbl.empty? ? nil : lbl.to_json),
            'metadata_kv'  => md.map  { |k,v| { 'key' => k.to_s, 'value' => v } },
            'metadata_json'=> (md.empty? ? nil : md.to_json)
          ).merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
          out
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end
    },
    rag_files_delete: {
      title: 'RAG (Files): Delete file',
      subtitle: 'projects.locations.ragCorpora.ragFiles.delete',
      display_priority: 85,
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      input_fields: lambda do |_|
        [ { name: 'rag_file', optional: false, hint: 'Full name: projects/.../ragCorpora/{id}/ragFiles/{fileId}' } ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'name' }, { name: 'done', type: 'boolean' }, { name: 'error', type: 'object' }
        ] + [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' }, { name: 'message' },
            { name: 'duration_ms', type: 'integer' }, { name: 'correlation_id' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        t0=Time.now; corr=call(:build_correlation_id); url=nil
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)
          name = call(:build_rag_file_path, connection, input['rag_file'])
          loc  = connection['location'].to_s.downcase
          url  = call(:aipl_v1_url, connection, loc, name)
          raw  = call(:http_call!, 'DELETE', url).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, raw)
          body = call(:http_body_json, raw)
          body.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end
    },

    # 4)  Utility (5-10)
    embed_text: {
      title: 'Embeddings: Embed text',
      subtitle: 'Get embeddings from a publisher embedding model',
      help: lambda do |_|
        { body: 'POST :predict on a publisher embedding model' }
      end,
      display_priority: 7,
      retry_on_request: ['GET', 'HEAD'],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', label: 'Embedding model', optional: false, control_type: 'text', default: 'text-embedding-005' },
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
        corr = call(:build_correlation_id)

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
          'metadata' => { 'billableCharacterCount' => 230 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },
    count_tokens: {
      title: 'Utility: Count tokens',
      description: 'POST :countTokens on a publisher model',
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
        t0 = Time.now
        corr = call(:build_correlation_id)

        # Compute model path
        model_path = call(:build_model_path_with_global_preview, connection, input['model'])

        # Build payload
        contents   = call(:sanitize_contents_roles, input['contents'])
        error('At least one non-system message is required in contents') if contents.blank?
        sys_inst   = call(:system_instruction_from_text, input['system_preamble'])

        loc_from_model = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
        url = call(:aipl_v1_url, connection, loc_from_model, "#{model_path}:countTokens")

        begin
          resp = post(url)
                    .headers(call(:request_headers, corr))
                    .payload(call(:json_compact, {
                      'contents'          => contents,
                      'systemInstruction' => sys_inst
                    }))
          code = call(:telemetry_success_code, resp)
          resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        rescue => e
          g = call(:extract_google_error, e)
          msg = [e.to_s, (g['message'] || nil)].compact.join(' | ')
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
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
      title: 'Operations: Get (poll LRO)',
      subtitle: 'google.longrunning.operations.get',
      display_priority: 5,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do
        [
          { name: 'operation', optional: false,
            hint: 'Operation name or full path, e.g., projects/{p}/locations/{l}/operations/{id}' }
        ]
      end,

      output_fields: lambda do |_|
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
        t0 = Time.now
        corr = call(:build_correlation_id)
        begin
          call(:ensure_project_id!, connection)
          # Accept either full /v1/... or name-only
          op = input['operation'].to_s.sub(%r{^/v1/}, '')
          loc = (connection['location'].presence || 'us-central1').to_s.downcase
          url = call(:aipl_v1_url, connection, loc, op.start_with?('projects/') ? op : "projects/#{connection['project_id']}/locations/#{loc}/operations/#{op}")
          resp = get(url).headers(call(:request_headers, corr))
          code = call(:telemetry_success_code, resp)
          resp.merge(call(:telemetry_envelope, t0, corr, true, code, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        { 'name' => 'projects/p/locations/us-central1/operations/123', 'done' => false,
          'ok' => true, 'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 8, 'correlation_id' => 'sample' } }
      end
    }

  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {
    trim_strategies: lambda do
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
    modes_classification: lambda do
      [%w[Embedding embedding], %w[Generative generative], %w[Hybrid hybrid]]
    end,

    modes_grounding: lambda do
      [%w[Google\ Search google_search], %w[Vertex\ AI\ Search vertex_ai_search]]
    end,

    roles: lambda do
      # Contract-conformant roles (system handled via system_preamble)
      [['user','user'], ['model','model']]
    end,

    rag_source_families: lambda do
      [
        ['Google Cloud Storage', 'gcs'],
        ['Google Drive', 'drive']
      ]
    end,

    drive_input_type: lambda do
      [
        ['Drive Files', 'files'],
        ['Drive Folder', 'folder']
      ]
    end,

    distance_measures: lambda do
      [
        ['Cosine distance', 'COSINE_DISTANCE'],
        ['Dot product',     'DOT_PRODUCT'],
        ['Euclidean',       'EUCLIDEAN_DISTANCE']
      ]
    end,

    iam_services: lambda do
      [['Vertex AI','vertex'], ['AI Applications (Discovery)','discovery']]
    end,

    discovery_versions: lambda do
      [
        ['v1alpha', 'v1alpha'],
        ['v1beta',  'v1beta'],
        ['v1',      'v1']
      ]
    end

  },

  # --------- METHODS ------------------------------------------------------
  methods: {

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
    redact_json: lambda do |obj|
      # Shallow redaction of obvious secrets in request bodies; extend as needed
      begin
        j = obj.is_a?(String) ? JSON.parse(obj) : obj
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
    const_default_scopes: lambda do
       [ 'https://www.googleapis.com/auth/cloud-platform' ]
    end,
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

    # --- URL and resource building ----------------------------------------
    aipl_service_host: lambda do |connection, loc=nil|
      l = (loc || connection['location']).to_s.downcase
      (l.blank? || l == 'global') ? 'aiplatform.googleapis.com' : "#{l}-aiplatform.googleapis.com"
    end,
    aipl_v1_url: lambda do |connection, loc, path|
      "https://#{call(:aipl_service_host, connection, loc)}/v1/#{path}"
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
      l = (loc || connection['location']).to_s.downcase
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
    build_rag_retrieve_payload: lambda do |question, rag_corpus, restrict_ids = []|
      rag_res = { 'ragCorpus' => rag_corpus }
      ids     = call(:sanitize_drive_ids, restrict_ids, allow_empty: true, label: 'restrict_to_file_ids')
      rag_res['ragFileIds'] = ids if ids.present?
      {
        'query'          => { 'text'          => question.to_s },
        # NOTE: union member is supplied at top-level (not wrapped in "dataSource")
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
    predict_embeddings: lambda do |connection, model_path, instances, params={}|
      max  = call(:embedding_max_instances, model_path)
      preds = []
      billable = 0
      # Derive location from the model path (projects/.../locations/{loc}/...)
      loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase

      (instances || []).each_slice(max) do |slice|
        url  = call(:aipl_v1_url, connection, loc, "#{model_path}:predict")
        resp = post(url)
                .headers(call(:request_headers, call(:build_correlation_id)))
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
    llm_referee: lambda do |connection, model, email_text, shortlist_names, all_cats, fallback_category = nil|
      # Minimal, schema-constrained JSON referee using Gemini
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
      resp = post(url)
               .headers(call(:request_headers, call(:build_correlation_id)))
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

    # --- Hints ------------------------------------------------------------
    model_id_hint: lambda do
      'Free-text model id. Short: "gemini-2.5-pro" or "text-embedding-005". ' +
      'Publisher form: "publishers/google/models/{id}". ' +
      'Full: "projects/{project}/locations/{region}/publishers/google/models/{id}". ' +
      'Tip: find ids in Vertex Model Garden or via REST GET v1/projects/{project}/locations/{region}/publishers/google/models/*.'
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
    request_headers: lambda do |correlation_id, extra=nil|
      base = {
        'X-Correlation-Id' => correlation_id.to_s,
        'Content-Type'     => 'application/json',
        'Accept'           => 'application/json'
      }
      extra.is_a?(Hash) ? base.merge(extra) : base
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
    extract_salient_span!: lambda do |connection, subject, body, model='gemini-2.0-flash', max_span=500, temperature=0|
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
      resp = post(url).headers(call(:request_headers, call(:build_correlation_id))).payload({
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
    count_tokens_quick!: lambda do |connection, model_id, contents, system_text=nil|
      # Build path for whichever model we’re counting
      model_path = call(:build_model_path_with_global_preview, connection, model_id)
      loc = (model_path[/\/locations\/([^\/]+)/, 1] || (connection['location'].presence || 'global')).to_s.downcase
      url = call(:aipl_v1_url, connection, loc, "#{model_path}:countTokens")

      payload = {
        'contents' => contents,
        'systemInstruction' => call(:system_instruction_from_text, system_text)
      }.delete_if { |_k,v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }

      begin
        post(url).headers(call(:request_headers, call(:build_correlation_id))).payload(call(:json_compact, payload))
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
    body: "For actions calling host 'aiplatform.googleapis.com/v1', use relative paths. For actions calling other endpoints (e.g. discovery engine), provide the absolute URL."
  }
}
