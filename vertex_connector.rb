# frozen_string_literal: true

{
  title: 'Vertex AI Adapter',
  version: '0.9.1',
  description: 'Vertex AI (Gemini + Text Embeddings + Endpoints) via service account JWT',

  # --------- CONNECTION ---------------------------------------------------

  connection: {
    fields: [
      { name: 'service_account_key_json', control_type: 'text-area',  optional: false, hint: 'Paste full JSON key' },
      { name: 'location',                                             optional: false, hint: 'e.g., global, us-central1, us-east4' },
      { name: 'project_id', optional: true, hint: 'GCP project ID (inferred from key if blank)' },
      { name: 'user_project', label: 'User project for quota/billing', optional: true, extends_schema: true,
        hint: 'Sets x-goog-user-project for billing/quota. Service account must have roles/serviceusage.serviceUsageConsumer on this project.' },

      # Defaults for test probe
      { name: 'set_defaults_for_probe', type: 'boolean', control_type: 'checkbox', extends_schema: true, optional: false, 
        default: false, hint: 'Optionally set default model(s) for connection test' },
      { name: 'default_probe_gen_model', optional: true, ngIf: 'input.set_defaults_for_probe == "true"',
        hint: 'e.g., gemini-2.0-flash' },
      { name: 'default_probe_embed_model', optional: true, ngIf: 'input.set_defaults_for_probe == "true"',
        hint: 'e.g., text-embedding-005' },

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
        # Keep headers minimal; envelope/correlation lives in actions
        h   = { 'Authorization': "Bearer #{connection['access_token']}"}
        up  = connection['user_project'].to_s.strip
        h['x-goog-user-project'] = up if up.present?
        headers(h)
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

  test: lambda do |connection|
    # Fast path: if defaults provided, exercise a real, auth’d call to surface IAM/billing issues early.
    if %w[true 1 yes].include?(connection['set_defaults_for_probe'].to_s.downcase) && connection['default_probe_gen_model'].present?
      # Evaluate connection fields
      call(:ensure_project_id!, connection)
      # Build model path (global preview)
      model_path = call(:build_model_path_with_global_preview, connection, connection['default_probe_gen_model'])
      # Define location (prefer connection, fallback to global)
      loc = (connection['location'].presence || 'global').to_s.downcase
      # POST to endpoint
      url = call(:aipl_v1_url, connection, loc, "#{model_path}:countTokens")
      post(url)
        .headers(call(:request_headers, call(:build_correlation_id)))
        .payload({
          'contents' => [{ 'role' => 'user', 'parts' => [{ 'text' => 'ping' }] }],
          'systemInstruction' => call(:system_instruction_from_text, 'Connection probe')
        })
    else
      get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
        .headers(call(:request_headers, call(:build_correlation_id)))
        .params(pageSize: 1)
    end
  end,

  # --------- OBJECT DEFINITIONS -------------------------------------------

  object_definitions: {
    content_part: {
      fields: lambda do
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
      end
    },

    # Per contract: role ∈ {user, model}
    content: {
      fields: lambda do |object_definitions|
        [
          { name: 'role', control_type: 'select', pick_list: 'roles', optional: false },
          { name: 'parts', type: 'array', of: 'object',
            properties: object_definitions['content_part'], optional: false }
        ]
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

    safety_setting: {
      fields: lambda do
        [
          { name: 'category'  },   # e.g., HARM_CATEGORY_*
          { name: 'threshold' }    # e.g., BLOCK_LOW_AND_ABOVE
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

    # Align to contract: embeddings object, not array
    embed_output: {
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
    }

  },

  # --------- ACTIONS ------------------------------------------------------

  actions: {

    # 1) Email categorization
    gen_categorize_email: {
      title: 'Categorize email',
      subtitle: 'Classify an email into a category',
      help: lambda do |_|
        { body: 'Classify an email into one of the provided categories using embeddings (default) or a generative referee.'}
      end,
      display_priority: 10,
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
        # Build correlation ID, now (for traceability)
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
          },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },

    # 2) RAG engine (Vertex AI)
    rag_files_import: {
      title: 'RAG: Import files to corpus',
      subtitle: 'projects.locations.ragCorpora.ragFiles:import',
      display_priority: 9,
      retry_on_request: ['GET','HEAD'], # removed "POST" to preserve idempotency, prevent duplication of jobs
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,
      config_fields: [
        {
          name: 'source_family',
          label: 'Source family',
          optional: false,
          control_type: 'select',
          pick_list: 'rag_source_families',
          hint: 'Choose which source you are importing from (purely a UI gate; validation still enforced at runtime).'
        }
      ],

      input_fields: lambda do
        [
          { name: 'rag_corpus_resource_name', optional: false,
            hint: 'Accepts either full resource name (e.g., "projects/{project}/locations/{region}/ragCorpora/{corpus}") or the "corpus"' },

          # Exactly one source family:
          { name: 'gcs_uris', type: 'array', of: 'string', optional: true, ngIf: 'config.source_family == "gcs"',
            hint: 'Pass files or directory prefixes (e.g., gs://bucket/dir). Wildcards (*, **) are NOT supported.' },
          { name: 'drive_folder_id', optional: true, ngIf: 'config.source_family == "drive"',
            hint: 'Google Drive folder ID (share with Vertex RAG service agent)' },
          { name: 'drive_file_ids', type: 'array', of: 'string', optional: true, ngIf: 'config.source_family == "drive"',
            hint: 'Optional explicit file IDs if not using folder' },

          # Tuning / ops
          { name: 'maxEmbeddingRequestsPerMin', type: 'integer', optional: true },
          { name: 'rebuildAnnIndex', type: 'boolean', optional: true, default: false,
            hint: 'Set true after first large import to build ANN index' },
          { name: 'importResultGcsSink', type: 'object', optional: true, properties: [
              { name: 'outputUriPrefix', optional: false, hint: 'gs://bucket/prefix/' }
            ]},
          { name: 'debug', type: 'boolean', control_type: 'checkbox', optional: true,
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
          resp = post(url)
                   .headers(call(:request_headers, corr))
                   .payload(req_body)
          out = resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
          if call(:normalize_boolean, input['debug'])
            ops_root = "https://#{call(:aipl_service_host, connection, loc)}/v1/projects/#{connection['project_id']}/locations/#{loc}/operations"
            dbg = call(:debug_pack, true, url, req_body, nil) || {}
            dbg['ops_list_url'] = ops_root
            out['debug'] = dbg
          end
          out

        rescue => e
          g = call(:extract_google_error, e)
          vio = (g['violations'] || []).map { |x| "#{x['field']}: #{x['reason']}" }.join(' ; ')
          msg = [e.to_s, (g['message'] || nil), (vio.presence)].compact.join(' | ')
          out = {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), msg))
          out['debug'] = call(:debug_pack, input['debug'], url, req_body, g)
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

    rag_retrieve_contexts: {
      title: 'RAG: Retrieve contexts',
      subtitle: 'projects.locations:retrieveContexts (Vertex RAG Store)',
      display_priority: 9,
      retry_on_request: ['GET','HEAD','POST'],
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

      output_fields: lambda do |_|
        [
          { name: 'question' },
          { name: 'contexts', type: 'array', of: 'object', properties: [
              { name: 'id' },
              { name: 'text' },
              { name: 'score', type: 'number' },
              { name: 'source' },
              { name: 'uri' },
              { name: 'metadata', type: 'object' }
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
        begin
          call(:ensure_project_id!, connection)
          call(:ensure_regional_location!, connection)

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
          }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
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
              'source' => 'handbook', 'uri' => 'https://drive.google.com/file/d/abc...', 'metadata' => { 'page' => 7 } }
          ],
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 22, 'correlation_id' => 'sample' }
        }
      end
    },

    rag_answer: {
      title: 'RAG: Retrieve + answer (one-shot)',
      subtitle: 'Retrieve contexts from a corpus and generate a cited answer',
      display_priority: 9,
      retry_on_request: ['GET','HEAD','POST'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
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
          retr_resp = post(retr_url)
                        .headers(call(:request_headers, corr))
                        .payload(call(:json_compact, retrieve_payload))
          raw_ctxs = call(:normalize_retrieve_contexts!, retr_resp)

          maxn  = call(:clamp_int, (input['max_contexts'] || 12), 1, 100)
          chunks = call(:map_context_chunks, raw_ctxs, maxn)
          error('No contexts retrieved; check corpus/permissions/region') if chunks.empty?

          # 2) Generate structured answer with your existing schema pattern
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

          gen_url   = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
          gen_resp  = post(gen_url)
                       .headers(call(:request_headers, corr))
                       .payload(call(:json_compact, gen_payload))

          text = gen_resp.dig('candidates', 0, 'content', 'parts', 0, 'text').to_s
          parsed = call(:safe_parse_json, text)

          {
            'answer'     => (parsed['answer'] || text),
            'citations'  => (parsed['citations'] || []),
            'responseId' => gen_resp['responseId'],
            'usage'      => gen_resp['usageMetadata']
          }.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
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

    # 3) Generate content (Gemini)
    gen_generate_content: {
      title: 'Generative: Generate content (Gemini)',
      subtitle: 'Generate content from a prompt',
      help: lambda do |_|
        { body: 'Provide a prompt to generate content from an LLM. Uses "POST :generateContent".'}
      end,
      display_priority: 8,
      retry_on_request: ['GET','HEAD'], # removed "POST" to preserve idempotency, prevent duplication of jobs
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
         Array(object_definitions['generate_content_output']) + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = call(:build_correlation_id)

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
        url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
        begin
          resp = post(url)
                  .headers(call(:request_headers, corr))
                  .payload(call(:json_compact, payload))
          resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
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
      display_priority: 8,
      retry_on_request: [ 'GET', 'HEAD' ],
      retry_on_response: [408, 429, 500, 502, 503, 504],
      max_retries: 3,

      input_fields: lambda do |object_definitions|
        [
          { name: 'contents', type: 'array', of: 'object', properties: object_definitions['content'], optional: false },
          { name: 'model', label: 'Model', optional: false, control_type: 'select', pick_list: 'models_generative',
            toggle_hint: 'Use custom value', toggle_field: { name: 'model', label: 'Model', type: 'string', control_type: 'text' } },
          { name: 'grounding', control_type: 'select', pick_list: 'modes_grounding', optional: false },

          { name: 'vertex_ai_search_datastore', optional: true,
            hint: 'projects/.../locations/.../collections/default_collection/dataStores/...' },
          { name: 'vertex_ai_search_serving_config', optional: true,
            hint: 'projects/.../locations/.../collections/.../engines/.../servingConfigs/default_config' },
          # Back-compat (deprecated):
          { name: 'vertex_ai_search_engine', optional: true, hint: '(deprecated) use .../servingConfigs/... instead' },
          { name: 'system_preamble', label: 'System preamble (text)', optional: true },
          { name: 'toolConfig', type: 'object' },
          { name: 'generationConfig', type: 'object', properties: object_definitions['generation_config'] },
          { name: 'safetySettings',  type: 'array', of: 'object', properties: object_definitions['safety_setting'] }
        ]
      end,

      output_fields: lambda do |object_definitions|
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
            tools = [ { 'googleSearch' => {} } ]
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
        url = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")
        begin
          resp = post(url)
                  .headers(call(:request_headers, corr))
                  .payload(call(:json_compact, payload))
          resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
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
      display_priority: 8,
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
          url  = call(:aipl_v1_url, connection, loc, "#{model_path}:generateContent")

          resp = post(url)
                   .headers(call(:request_headers, corr))
                   .payload(call(:json_compact, payload))

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
          'usage' => { 'promptTokenCount' => 311, 'candidatesTokenCount' => 187, 'totalTokenCount' => 498 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' }
        }
      end
    },

    # 3) Embeddings
    embed_text: {
      title: 'Embeddings: Embed text',
      subtitle: 'Get embeddings from a publisher embedding model',
      help: lambda do |_|
        { body: 'POST :predict on a publisher embedding model' }
      end,
      display_priority: 7,
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

    # 4) Predict
    endpoint_predict: {
      title: 'Prediction: Endpoint predict (custom model)',
      subtitle: 'POST :predict to a Vertex AI Endpoint',
      display_priority: 6,
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
        Array(object_definitions['predict_output']) + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = call(:build_correlation_id)

        # Evaluate connection fields
        call(:ensure_project_id!, connection)
        call(:ensure_regional_location!, connection) # require non-global

        # Build URL, payload
        url     = call(:endpoint_predict_url, connection, input['endpoint'])
        payload = { 'instances' => input['instances'], 'parameters' => input['parameters'] }

        # Call EP
        begin
          resp = post(url)
                   .headers(call(:request_headers, corr))
                   .payload(call(:json_compact, payload))
          resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        { 'predictions' => [ { 'score' => 0.92, 'label' => 'positive' } ],
          'deployedModelId' => '1234567890',
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' } }
      end
    },

    batch_prediction_create: {
      title: 'Batch: Create prediction job',
      subtitle: 'Create projects.locations.batchPredictionJobs',
      batch: true,
      display_priority: 6,

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
        Array(object_definitions['batch_job']) + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = call(:build_correlation_id)

        # Evaluate connection fields
        call(:ensure_project_id!, connection)
        call(:ensure_regional_location!, connection)

        # Build the payload
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

        # Build the endpoint URL
        loc         = connection['location']
        path        = "projects/#{connection['project_id']}/locations/#{loc}/batchPredictionJobs"
        url         = call(:aipl_v1_url, connection, loc, path)

        # Call ep
        begin
          resp = post(url)
                   .headers(call(:request_headers, corr))
                   .payload(call(:json_compact, payload))
          resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        { 'name'        => 'projects/p/locations/us-central1/batchPredictionJobs/123',
          'displayName' => 'batch-2025-10-06',
          'state'       => 'JOB_STATE_PENDING',
          'model'       => 'projects/p/locations/us-central1/models/456',
          'ok'          => true,
          'telemetry'   => { 
            'http_status'     => 200,
            'message'         => 'OK',
            'duration_ms'     => 12,
            'correlation_id'  => 'sample-corr'
          }
        }
      end
    },

    batch_prediction_get: {
      title: 'Batch: Fetch prediction job (get)',
      subtitle: 'Get a batch prediction job by ID',
      batch: true,
      display_priority: 6,

      input_fields: lambda do
        [ { name: 'job_id', optional: false } ]
      end,

      output_fields: lambda do |object_definitions|
        Array(object_definitions['batch_job']) + Array(object_definitions['envelope_fields'])
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = call(:build_correlation_id)

        # Evaluate connection fields
        call(:ensure_project_id!, connection)
        call(:ensure_regional_location!, connection)

        # Build endpoint URL
        name = input['job_id'].to_s.start_with?('projects/') ?
          input['job_id'] :
          "projects/#{connection['project_id']}/locations/#{connection['location']}/batchPredictionJobs/#{input['job_id']}"
        loc = connection['location']
        url  = call(:aipl_v1_url, connection, loc, name.sub(%r{^/v1/}, ''))

        # Call EP
        begin
          resp = get(url).headers(call(:request_headers, corr))
          resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        rescue => e
          {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
        end
      end,

      sample_output: lambda do
        { 'name' => 'projects/p/locations/us-central1/batchPredictionJobs/123',
          'displayName' => 'batch-2025-10-06',
          'state' => 'JOB_STATE_SUCCEEDED',
          'outputInfo' => { 'gcsOutputDirectory' => 'gs://my-bucket/prediction-...' },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 12, 'correlation_id' => 'sample-corr' } }
      end
    },

    # 6) Utility
    count_tokens: {
      title: 'Utility: Count tokens',
      description: 'POST :countTokens on a publisher model',
      display_priority: 5,
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

      output_fields: lambda do |object_definitions|
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

        loc = (connection['location'].presence || 'global').to_s.downcase
        url = call(:aipl_v1_url, connection, loc, "#{model_path}:countTokens")

        begin
          resp = post(url)
                    .headers(call(:request_headers, corr))
                    .payload(call(:json_compact, {
                      'contents'          => contents,
                      'systemInstruction' => sys_inst
                    }))
          resp.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
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
    }

  },

  # --------- PICK LISTS ---------------------------------------------------

  pick_lists: {
    modes_classification: lambda do
      [%w[Embedding embedding], %w[Generative generative], %w[Hybrid hybrid]]
    end,

    modes_grounding: lambda do
      [%w[Google\ Search google_search], %w[Vertex\ AI\ Search vertex_ai_search]]
    end,

    models_embedding: lambda do |connection|
      begin
        # v1beta1 publishers.models.list — global; no project/location.
        resp = get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
                 .headers(call(:request_headers, call(:build_correlation_id)))
                 .params(pageSize: 200)

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
    end,

    models_generative: lambda do |connection|
      begin
        resp = get('https://aiplatform.googleapis.com/v1beta1/publishers/google/models')
                 .headers(call(:request_headers, call(:build_correlation_id)))
                 .params(pageSize: 200)
        items = call(:safe_array, resp['publisherModels'])
                  .map { |m| m['name'].to_s.split('/').last }
                  .select { |id| id.start_with?('gemini-') }
                  .uniq.sort
      rescue => _e
        items = []
      end
      items.map { |id| [id, id] }
    end,

    # Contract-conformant roles (system handled via system_preamble)
    roles: lambda do
      [['user','user'], ['model','model']]
    end,

    rag_source_families: lambda do
      [
        ['Google Cloud Storage', 'gcs'],
        ['Google Drive', 'drive']
      ]
    end
  },

  # --------- METHODS ------------------------------------------------------
  methods: {

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
        %w[access_token authorization api_key bearer token].each do |k|
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
      v = '' if v.length < 8 && prior.start_with?('http') # common bogus scrape from share links
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

    build_rag_retrieve_payload: lambda do |question, rag_corpus, restrict_ids = []|
      rag_res = { 'ragCorpus' => rag_corpus }
      ids     = call(:safe_array, restrict_ids).map { |x| call(:normalize_drive_file_id, x) }.reject(&:blank?)
      rag_res['ragFileIds'] = ids if ids.present?
      {
        'query'          => { 'text'          => question.to_s },
        # NOTE: union member is supplied at top-level (not wrapped in "dataSource")
        'vertexRagStore' => { 'ragResources'  => [rag_res] }
      }
    end,

    map_context_chunks: lambda do |raw_contexts, maxn = 20|
      call(:safe_array, raw_contexts).first(maxn).each_with_index.map do |c, i|
        {
          'id'       => (c['chunkId'] || "ctx-#{i+1}"),
          'text'     => c['text'].to_s,
          'score'    => (c['score'] || c['relevanceScore'] || 0.0).to_f,
          'source'   => (c['sourceDisplayName'] || c.dig('metadata','source')),
          'uri'      => (c['sourceUri']        || c.dig('metadata','uri')),
          'metadata' => c['metadata']
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
        call(:safe_array, input['drive_file_ids']).each do |fid|
          file_id = call(:normalize_drive_file_id, fid)
          error('drive_file_ids contains an invalid Drive ID') if file_id.blank?
          res_ids << { 'resourceId' => file_id, 'resourceType' => 'RESOURCE_TYPE_FILE' }
        end
        error('Provide drive_folder_id or drive_file_ids (and share with Vertex RAG Data Service Agent)') if res_ids.empty?
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

    # --- Sanitizers and conversion ----------------------------------------

    safe_array: lambda do |v|
      return [] if v.nil? || v == false
      return v  if v.is_a?(Array)
      [v]
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

    # --- RAG helpers ------------------------------------------------------

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

    request_headers: lambda do |correlation_id, extra=nil|
      base = {
        'X-Correlation-Id': correlation_id.to_s,
        'Content-Type':      'application/json',
        'Accept':            'application/json'
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
    end

  },

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {},

  # --------- CUSTOM ACTION SUPPORT ----------------------------------------
  custom_action: true,
  custom_action_help: {
    body: 'Create custom Vertex AI operations using the established connection'
  }
}
