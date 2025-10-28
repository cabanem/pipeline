# frozen_string_literal: true
require 'openssl'
require 'base64'
require 'json'
require 'time'
require 'securerandom'


{
  title: 'Vertex RAG Engine',
  subtitle: 'RAG Engine',
  version: '0.9.7',
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
      { name: 'service_account_key_json',   optional: false,  control_type: 'text-area', hint: 'Paste full JSON key' },
      { name: 'location',                   optional: false,  control_type: 'text', hint: 'e.g., global, us-central1, us-east4' },
      { name: 'project_id',                 optional: false,   control_type: 'text', hint: 'GCP project ID (inferred from key if blank)' },
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
    # A) Connection-scoped
    connection_config: {
      fields: [
        {
          name: 'project',
          label: 'Project',
          type: :string,
          optional: false,
          hint: 'Google Cloud project ID or number used in all resource names (projects/{project}).'
        },
        {
          name: 'location',
          label: 'Location (region)',
          type: :string,
          optional: false,
          control_type: 'select',
          options: %w[
            us-central1 us-east1 us-east4 us-west1
            europe-west1 europe-west4
            asia-east1 asia-northeast1
          ],
          hint: 'Regional location for Vertex AI and RAG resources. All request paths and any regional base URL must use this same region.'
        },
        {
          name: 'publisher',
          label: 'Publisher (for model path)',
          type: :string,
          optional: true,
          default: 'google',
          hint: 'Used when addressing publisher models (…/publishers/{publisher}/models/{model}). Default "google".'
        }
      ],
      additional_properties: false
    },
    headers_allowlist: {
      # Doc-only helper used in comments/examples. Do not bind these to action inputs.
      fields: [
        {
          name: 'headers',
          type: :array,
          of: :string,
          optional: false,
          hint: 'Connector adds Authorization automatically from auth. Only the following headers should ever be set by the connector: Authorization, Content-Type. Per-action inputs must not affect headers.'
        }
      ],
      sample_output: {
        headers: ['Authorization', 'Content-Type']
      },
      additional_properties: false
    },
    error_google_json: {
      fields: [
        {
          name: 'error',
          type: :object,
          optional: false,
          properties: [
            { name: 'code', type: :integer, optional: false },
            { name: 'message', type: :string, optional: false },
            { name: 'status', type: :string, optional: true },
            { name: 'details', type: :array, of: :object, optional: true }
          ]
        }
      ],
      sample_output: {
        error: {
          code: 429,
          message: 'Rate limited. Retry after 3 seconds.',
          status: 'RESOURCE_EXHAUSTED',
          details: [
            { '@type': 'type.googleapis.com/google.rpc.ErrorInfo', reason: 'RATE_LIMITED' },
            { '@type': 'type.googleapis.com/google.rpc.RetryInfo', retryDelay: '3s' }
          ]
        }
      },
      additional_properties: false
    },

    # I) Small utilities
    enum_location: {
      fields: [
        {
          name: 'location',
          type: :string,
          control_type: 'select',
          options: %w[
            us-central1 us-east1 us-east4 us-west1
            europe-west1 europe-west4
            asia-east1 asia-northeast1
          ],
          optional: false
        }
      ],
      additional_properties: false
    },
    model_path: {
      fields: [
        {
          name: 'model',
          type: :string,
          optional: false,
          hint: "One of:\n- projects/{project}/locations/{location}/publishers/{publisher}/models/{model}\n- projects/{project}/locations/{location}/endpoints/{endpoint}\n\nThe {location} segment must match the connector’s selected location; when using a regional base URL, it must match the same region."
        }
      ],
      additional_properties: false
    },
    text_record: {
      fields: [
        { name: 'id', type: :string },
        { name: 'title', type: :string, optional: true },
        { name: 'content', type: :string, control_type: 'text-area' },
        { name: 'metadata', type: :object, optional: true }
      ],
      additional_properties: false,
      sample_output: { id: 'doc-1', title: 'Benefits', content: '...', metadata: { source: 'intranet' } }
    },
    context_chunk: {
      fields: [
        { name: 'id' },
        { name: 'uri', optional: true },
        { name: 'content', control_type: 'text-area' },
        { name: 'score', type: :number, optional: true },
        { name: 'metadata', type: :object, optional: true }
      ],
      sample_output: { id: 'c1', uri: 'gs://bucket/a.txt', content: '...', score: 0.83 }
    },

    # B) Generative (LLM) objects
    gen_content_part: {
      fields: [
        { name: 'text', type: :string, optional: true },
        {
          name: 'inlineData',
          type: :object,
          optional: true,
          properties: [
            { name: 'mimeType', type: :string, optional: true },
            { name: 'data', type: :string, optional: true }
          ]
        },
        {
          name: 'fileData',
          type: :object,
          optional: true,
          properties: [
            { name: 'mimeType', type: :string, optional: true },
            { name: 'fileUri', type: :string, control_type: 'url', optional: true }
          ]
        }
      ],
      additional_properties: true
    },
    gen_content: {
      fields: [
        {
          name: 'role',
          type: :string,
          control_type: 'select',
          options: %w[user model system],
          optional: false
        },
        {
          name: 'parts',
          type: :array,
          of: :object,
          optional: false,
          properties: [
            { name: 'text', type: :string, optional: true },
            {
              name: 'inlineData',
              type: :object,
              optional: true,
              properties: [
                { name: 'mimeType', type: :string, optional: true },
                { name: 'data', type: :string, optional: true }
              ]
            },
            {
              name: 'fileData',
              type: :object,
              optional: true,
              properties: [
                { name: 'mimeType', type: :string, optional: true },
                { name: 'fileUri', type: :string, control_type: 'url', optional: true }
              ]
            }
          ]
        }
      ],
      additional_properties: true
    },
    gen_generation_config: {
      fields: [
        { name: 'temperature', type: :number, optional: true },
        { name: 'topP', type: :number, optional: true },
        { name: 'topK', type: :integer, optional: true },
        { name: 'maxOutputTokens', type: :integer, optional: true },
        { name: 'candidateCount', type: :integer, optional: true },
        { name: 'stopSequences', type: :array, of: :string, optional: true },
        { name: 'responseMimeType', type: :string, optional: true },
        {
          name: 'responseSchema',
          type: :object,
          optional: true,
          properties: [],
          hint: 'Schema for structured outputs. Rule: if responseSchema is present, responseMimeType is required.'
        }
      ],
      additional_properties: true
    },
    gen_generate_content_request: {
      fields: [
        {
          name: 'contents',
          type: :array,
          of: :object,
          optional: false,
          properties: [
            { name: 'role', type: :string, control_type: 'select', options: %w[user model system], optional: false },
            {
              name: 'parts',
              type: :array,
              of: :object,
              optional: false,
              properties: [
                { name: 'text', type: :string, optional: true },
                { name: 'inlineData', type: :object, properties: [{ name: 'mimeType', type: :string }, { name: 'data', type: :string }], optional: true },
                { name: 'fileData', type: :object, properties: [{ name: 'mimeType', type: :string }, { name: 'fileUri', type: :string, control_type: 'url' }], optional: true }
              ]
            }
          ]
        },
        {
          name: 'systemInstruction',
          type: :object,
          properties: [
            { name: 'role', type: :string, control_type: 'select', options: %w[user model system] },
            { name: 'parts', type: :array, of: :object }
          ],
          optional: true
        },
        { name: 'tools', type: :array, of: :object, optional: true },
        { name: 'toolConfig', type: :object, optional: true },
        { name: 'groundingConfig', type: :object, optional: true },
        {
          name: 'labels',
          type: :object,
          optional: true,
          hint: 'String map of labels (key/value).'
        },
        {
          name: 'safetySettings',
          type: :array,
          of: :object,
          optional: true,
          properties: [
            { name: 'category', type: :string, optional: true },
            { name: 'threshold', type: :string, optional: true }
          ]
        },
        {
          name: 'generationConfig',
          type: :object,
          properties: [
            { name: 'temperature', type: :number, optional: true },
            { name: 'topP', type: :number, optional: true },
            { name: 'topK', type: :integer, optional: true },
            { name: 'maxOutputTokens', type: :integer, optional: true },
            { name: 'candidateCount', type: :integer, optional: true },
            { name: 'stopSequences', type: :array, of: :string, optional: true },
            { name: 'responseMimeType', type: :string, optional: true },
            { name: 'responseSchema', type: :object, properties: [], optional: true }
          ],
          optional: true,
          hint: 'If responseSchema is provided, responseMimeType must also be provided.'
        },
        { name: 'cachedContent', type: :string, optional: true },
        { name: 'modelArmorConfig', type: :object, optional: true }
      ],
      additional_properties: false
    },
    gen_generate_content_response: {
      fields: [
        { name: 'responseId', type: :string, optional: true },
        { name: 'modelVersion', type: :string, optional: true },
        { name: 'usageMetadata', type: :object, optional: true },
        { name: 'candidates', type: :array, of: :object, optional: true },
        { name: 'promptFeedback', type: :object, optional: true }
      ],
      sample_output: {
        responseId: 'r-9c7d1e7f',
        modelVersion: 'publishers/google/models/gemini-1.5-pro-002',
        usageMetadata: { promptTokenCount: 52, candidatesTokenCount: 37, totalTokenCount: 89 },
        candidates: [
          {
            index: 0,
            content: {
              role: 'model',
              parts: [
                { text: "Category: HR.Policy\nRationale: Mentions benefits enrollment window and due date." }
              ]
            },
            finishReason: 'STOP'
          }
        ],
        promptFeedback: { safetyRatings: [] }
      },
      additional_properties: true
    },

    # C) Count tokens
    gen_count_tokens_request: {
      fields: [
        {
          name: 'contents',
          type: :array,
          of: :object,
          optional: true,
          properties: [
            { name: 'role', type: :string, control_type: 'select', options: %w[user model system], optional: false },
            {
              name: 'parts',
              type: :array,
              of: :object,
              optional: false,
              properties: [
                { name: 'text', type: :string, optional: true },
                { name: 'inlineData', type: :object, properties: [{ name: 'mimeType', type: :string }, { name: 'data', type: :string }], optional: true },
                { name: 'fileData', type: :object, properties: [{ name: 'mimeType', type: :string }, { name: 'fileUri', type: :string, control_type: 'url' }], optional: true }
              ]
            }
          ],
          hint: 'Use this OR instances (oneOf).'
        },
        {
          name: 'instances',
          type: :array,
          of: :object,
          optional: true,
          properties: [],
          hint: 'Use this OR contents (oneOf).'
        },
        { name: 'tools', type: :array, of: :object, optional: true },
        { name: 'systemInstruction', type: :object, optional: true },
        {
          name: 'generationConfig',
          type: :object,
          optional: true,
          properties: [
            { name: 'temperature', type: :number, optional: true },
            { name: 'topP', type: :number, optional: true },
            { name: 'topK', type: :integer, optional: true },
            { name: 'maxOutputTokens', type: :integer, optional: true },
            { name: 'candidateCount', type: :integer, optional: true },
            { name: 'stopSequences', type: :array, of: :string, optional: true }
          ]
        }
      ],
      additional_properties: false
    },
    gen_count_tokens_response: {
      fields: [
        { name: 'totalTokens', type: :integer, optional: false },
        { name: 'totalBillableCharacters', type: :integer, optional: true },
        {
          name: 'promptTokensDetails',
          type: :array,
          of: :object,
          optional: true,
          properties: [
            { name: 'modality', type: :string, optional: true },
            { name: 'tokenCount', type: :integer, optional: true }
          ]
        }
      ],
      sample_output: {
        totalTokens: 128,
        totalBillableCharacters: 512,
        promptTokensDetails: [
          { modality: 'TEXT', tokenCount: 128 }
        ]
      },
      additional_properties: true
    },

    # D) Embeddings
    emb_predict_request: {
      fields: [
        {
          name: 'instances',
          type: :array,
          of: :object,
          optional: false,
          properties: [
            { name: 'content', type: :string, optional: false },
            { name: 'title', type: :string, optional: true },
            {
              name: 'task_type',
              type: :string,
              control_type: 'select',
              options: %w[
                RETRIEVAL_QUERY RETRIEVAL_DOCUMENT SEMANTIC_SIMILARITY
                CLASSIFICATION CLUSTERING QUESTION_ANSWERING FACT_VERIFICATION
                CODE_RETRIEVAL_QUERY
              ],
              optional: true
            }
          ]
        },
        {
          name: 'parameters',
          type: :object,
          optional: true,
          properties: [
            { name: 'autoTruncate', type: :boolean, optional: true },
            { name: 'outputDimensionality', type: :integer, optional: true }
          ]
        }
      ],
      additional_properties: false
    },
    emb_predict_response: {
      fields: [
        {
          name: 'predictions',
          type: :array,
          of: :object,
          optional: false,
          properties: [
            {
              name: 'embeddings',
              type: :object,
              optional: false,
              properties: [
                { name: 'values', type: :array, of: :number, optional: false },
                {
                  name: 'statistics',
                  type: :object,
                  optional: true,
                  properties: [
                    { name: 'truncated', type: :boolean, optional: true },
                    { name: 'token_count', type: :integer, optional: true }
                  ],
                  additional_properties: false
                }
              ],
              additional_properties: false
            }
          ]
        }
      ],
      sample_output: {
        predictions: [
          {
            embeddings: {
              values: [0.012, -0.034, 0.998, 0.121, -0.220],
              statistics: { truncated: false, token_count: 12 }
            }
          }
        ]
      },
      additional_properties: true
    },

    # E) Ranking (Discovery Engine)
    rank_ranking_record: {
      fields: [
        { name: 'id', type: :string, optional: false },
        { name: 'title', type: :string, optional: true },
        { name: 'content', type: :string, optional: true, control_type: 'text-area' },
        { name: 'metadata', type: :object, optional: true }
      ],
      additional_properties: false,

      # Validation note:
      # Each record must include id and at least one of title or content.

      sample_output: {
        id: 'a',
        title: 'Benefits overview',
        content: 'Doc A…',
        metadata: { source: 'intranet' }
      },
      hint: 'Each record must include id and at least one of title or content.'
    },
    rank_request: {
      fields: [
        {
          name: 'query',
          type: :object,
          optional: false,
          properties: [
            {
              name: 'text',
              type: :string,
              optional: false,
              hint: 'Free-text query. You may also supply the API’s alternate form where query is a plain string; this object mirrors the RAG {text} shape.'
            }
          ]
        },
        {
          name: 'records',
          type: :array,
          of: :object,
          optional: false,
          properties: [
            { name: 'id', type: :string, optional: false },
            { name: 'title', type: :string, optional: true },
            { name: 'content', type: :string, optional: true },
            { name: 'metadata', type: :object, optional: true }
          ],
          hint: 'At least one of title or content is required per record.'
        },
        {
          name: 'topN',
          type: :integer,
          optional: true,
          hint: 'Optional cap on number of records to return.'
        },
        {
          name: 'model',
          type: :string,
          optional: true,
          default: 'semantic-ranker-512@latest',
          hint: 'Defaults to semantic-ranker-512@latest if omitted.'
        }
      ],
      additional_properties: false
    },
    rank_response: {
      fields: [
        {
          name: 'records',
          type: :array,
          of: :object,
          optional: true,
          properties: [
            { name: 'id', type: :string, optional: false },
            { name: 'title', type: :string, optional: true },
            { name: 'content', type: :string, optional: true },
            { name: 'score', type: :number, optional: false },
            { name: 'metadata', type: :object, optional: true }
          ]
        }
      ],
      sample_output: {
        records: [
          {
            id: 'b',
            title: 'Open enrollment dates',
            content: 'Open enrollment runs Nov 1–15.',
            score: 0.92,
            metadata: { source: 'hr_portal' }
          },
          {
            id: 'a',
            title: 'Benefits overview',
            score: 0.81,
            metadata: { source: 'handbook' }
          }
        ]
      },
      additional_properties: true
    },

    # F) RAG — retrieve contexts
    rag_vertex_store_resource: {
      fields: [
        {
          name: 'ragCorpus',
          type: :string,
          optional: false,
          hint: 'Resource name pattern: projects/{project}/locations/{location}/ragCorpora/{corpus}. The {location} must match the connector’s selected region.'
        },
        { name: 'ragFileIds', type: :array, of: :string, optional: true }
      ],
      additional_properties: false
    },
    rag_vertex_store: {
      fields: [
        {
          name: 'ragResources',
          type: :array,
          of: :object,
          optional: false,
          properties: [
            { name: 'ragCorpus', type: :string, optional: false },
            { name: 'ragFileIds', type: :array, of: :string, optional: true }
          ]
        }
      ],
      additional_properties: false
    },
    rag_retrieve_contexts_request: {
      fields: [
        {
          name: 'query',
          type: :object,
          optional: false,
          properties: [
            { name: 'text', type: :string, optional: false }
          ]
        },
        {
          name: 'dataSource',
          type: :object,
          optional: false,
          properties: [
            {
              name: 'vertexRagStore',
              type: :object,
              optional: false,
              properties: [
                {
                  name: 'ragResources',
                  type: :array,
                  of: :object,
                  optional: false,
                  properties: [
                    { name: 'ragCorpus', type: :string, optional: false, hint: 'projects/{project}/locations/{location}/ragCorpora/{corpus}' },
                    { name: 'ragFileIds', type: :array, of: :string, optional: true }
                  ]
                }
              ]
            }
          ]
        }
      ],
      additional_properties: false
    },
    rag_retrieved_context: {
      fields: [
        { name: 'chunkId', type: :string, optional: true },
        { name: 'text', type: :string, control_type: 'text-area', optional: true },
        { name: 'score', type: :number, optional: true },
        { name: 'sourceUri', type: :string, control_type: 'url', optional: true },
        { name: 'sourceDisplayName', type: :string, optional: true },
        { name: 'chunk', type: :object, optional: true },
        { name: 'metadata', type: :object, optional: true }
      ],
      additional_properties: true,
      sample_output: {
        chunkId: 'hr:123#c5',
        text: 'Open enrollment closes November 15.',
        score: 0.83,
        sourceUri: 'gs://corp/hr/benefits.pdf#page=3',
        sourceDisplayName: 'Benefits Guide 2024',
        metadata: { page: 3, corpus: 'hr' }
      }
    },
    rag_retrieve_contexts_response: {
      fields: [
        {
          name: 'contexts',
          type: :object,
          optional: true,
          properties: [
            {
              name: 'contexts',
              type: :array,
              of: :object,
              optional: true,
              properties: [
                { name: 'chunkId', type: :string, optional: true },
                { name: 'text', type: :string, optional: true },
                { name: 'score', type: :number, optional: true },
                { name: 'sourceUri', type: :string, control_type: 'url', optional: true },
                { name: 'sourceDisplayName', type: :string, optional: true },
                { name: 'chunk', type: :object, optional: true },
                { name: 'metadata', type: :object, optional: true }
              ]
            }
          ]
        }
      ],
      sample_output: {
        contexts: {
          contexts: [
            {
              chunkId: 'hr:123#c5',
              text: 'Open enrollment closes November 15.',
              score: 0.83,
              sourceUri: 'gs://corp/hr/benefits.pdf#page=3',
              sourceDisplayName: 'Benefits Guide 2024',
              metadata: { page: 3, corpus: 'hr' }
            },
            {
              chunkId: 'policy:88#c2',
              text: 'Employees must complete elections by Nov 15.',
              score: 0.79,
              sourceUri: 'https://intranet/policies/benefits',
              sourceDisplayName: 'Intranet Benefits Policy',
              metadata: { department: 'HR' }
            }
          ]
        }
      },
      additional_properties: true
    },

    # G) Operations (LRO)
    ops_operation: {
      fields: [
        { name: 'name', type: :string, optional: true, hint: 'Full LRO name: projects/{project}/locations/{location}/operations/{operationId}' },
        { name: 'done', type: :boolean, optional: true },
        { name: 'error', type: :object, optional: true },
        { name: 'metadata', type: :object, optional: true },
        { name: 'response', type: :object, optional: true }
      ],
      sample_output: {
        name: 'projects/demo/locations/us-east4/operations/op-123',
        done: false,
        metadata: {
          '@type': 'type.googleapis.com/google.cloud.aiplatform.v1.DeleteOperationMetadata',
          target: 'projects/demo/locations/us-east4/ragCorpora/hr'
        }
      },
      sample_output_done: {
        name: 'projects/demo/locations/us-east4/operations/op-123',
        done: true,
        response: { '@type': 'type.googleapis.com/google.protobuf.Empty' }
      },
      additional_properties: false
    },

    # H) Pagination helpers
    pg_page_request: {
      fields: [
        {
          name: 'pageSize',
          type: :integer,
          optional: true,
          hint: 'Number of items to return (1–1000). Requests above 1000 are rejected by the API.',
          control_type: 'number'
        },
        { name: 'pageToken', type: :string, optional: true }
      ],
      additional_properties: false
    },
    pg_page_response: {
      fields: [
        { name: 'nextPageToken', type: :string, optional: true }
      ],
      sample_output: { nextPageToken: 'gAAAAABlP...' },
      additional_properties: false
    }
  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    embed_text: {
      title: 'Embeddings: Predict',
      subtitle: '',
      description: '',
      display_priority: 95,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', optional: false, hint: 'e.g., text-embedding-004' },
          { name: 'texts', type: :array, of: :string, optional: false },
          { name: 'task_type', optional: true }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'predictions', type: :array, of: :object, properties: [
            { name: 'embeddings', type: :object, properties: [
              { name: 'values', type: :array, of: :number }
            ] }
          ]},
          { name: '_meta', type: :object }
        ]
      end,
      execute: lambda do |connection, input|
        model = call(:normalize_model!, input['model'])
        host  = call(:aiplatform_host, connection)
        path  = call(:path_embeddings_predict, connection, model)

        post("https://#{host}#{path}")
          .headers(call(:default_headers, connection))
          .payload({
            instances: input['texts'].map { |t| { content: t } },
            parameters: { task_type: input['task_type'] }.compact
          })
          .after_response      { |code, body, headers, _msg| call(:normalize_response!, code, body, headers) }
          .after_error_response{ |code, body, headers, _msg| call(:normalize_error!,   code, body, headers) }
      end,
      sample_output: lambda do
        #
      end
    },
    gen_content: {
      title: 'Generative: Generate content',
      subtitle: '',
      description: '',
      help: lambda do |_input, _picklist_label|
        { body: '' }
      end,
      display_priority: 94,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', optional: false, hint: 'e.g., gemini-1.5-pro' },
          { name: 'prompt', optional: false, control_type: 'text-area' },
          { name: 'system_instruction', optional: true, control_type: 'text-area' },
          { name: 'temperature', type: :number, optional: true },
          { name: 'top_p', type: :number, optional: true }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'candidates', type: :array, of: :object, properties: [
            { name: 'content', type: :object }
          ]},
          { name: '_meta', type: :object }
        ]
      end,
      execute: lambda do |connection, input|
        model = call(:normalize_model!, input['model'])
        host  = call(:aiplatform_host, connection)
        path  = call(:path_generate_content, connection, model)

        body = {
          systemInstruction: input['system_instruction'].present? ? { role: 'system', parts: [{ text: input['system_instruction'] }]} : nil,
          contents: [{ role: 'user', parts: [{ text: input['prompt'] }]}],
          generationConfig: { temperature: input['temperature'], topP: input['top_p'] }.compact
        }.compact

        post("https://#{host}#{path}")
          .headers(call(:default_headers, connection))
          .payload(body)
          .after_response      { |code, body, headers, _msg| call(:normalize_response!, code, body, headers) }
          .after_error_response{ |code, body, headers, _msg| call(:normalize_error!,   code, body, headers) }
      end,
      sample_output: lambda do
        #
      end

    },
    rag_retrieve_contexts: {
      title: 'RAG (Serving): Retrieve contexts',
      subtitle: '',
      description: '',
      display_priority: 1
      help: lambda do |_input, _picklist_label|
        { body: '' }
      end,
      display_priority: 86,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'rag_corpus', optional: false, hint: 'projects/{p}/locations/{l}/ragCorpora/{corpus}' },
          { name: 'query', optional: false },
          { name: 'max_contexts', type: :integer, optional: true, default: 12 }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'contexts', type: :array, of: :object, properties: object_definitions['context_chunk'][:fields] },
          { name: '_meta', type: :object }
        ]
      end,
      execute: lambda do |connection, input|
        host = call(:aiplatform_host, connection)
        path = call(:path_rag_retrieve_contexts, connection, input['rag_corpus'])
        body = {
          query: input['query'],
          retrievalConfig: { maxContexts: call(:coerce_integer, input['max_contexts'], 12) }
        }
        post("https://#{host}#{path}")
          .headers(call(:default_headers, connection))
          .payload(body)
          .after_response { |code, body, headers, msg| call(:normalize_response!, code, body, headers) }
          .after_error_response { |code, body, headers, msg| call(:normalize_error!, code, body, headers) }
      end,
      sample_output: lambda do
        {}
      end
    },
    rag_answer: {
      title: 'RAG: Retrieve + Answer',
      subtitle: '',
      description: '',
      help: lambda do |input, picklist_label|
        { body: 'Answer a question using caller-supplied context chunks (RAG-lite). Returns structured JSON with citations.' }
      end,
      display_priority: 92,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', optional: false },
          { name: 'rag_corpus', optional: false },
          { name: 'question', optional: false },
          { name: 'max_contexts', type: :integer, optional: true, default: 12 }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'answer', type: :string },
          { name: 'citations', type: :array, of: :object },
          { name: '_meta', type: :object }
        ]
      end,
      execute: lambda do |connection, input|
        host = call(:aiplatform_host, connection)
        path = call(:path_rag_answer, connection, input['rag_corpus'])
        body = { question: input['question'], answerGenerationConfig: { model: input['model'], maxContexts: call(:coerce_integer, input['max_contexts'], 12) } }

        post("https://#{host}#{path}")
          .headers(call(:default_headers, connection))
          .payload(body)
          .after_response      { |code, body, headers, _msg| call(:normalize_response!, code, body, headers) }
          .after_error_response{ |code, body, headers, _msg| call(:normalize_error!,   code, body, headers) }
      end,
      sample_output: lambda do
        #
      end

    },
    rank_texts: {
      title: 'Ranking API: Rank records',
      subtitle: '',
      description: '',
      help: lambda do |input, picklist_label|
        { body: '' }
      end,
      display_priority: 89,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |object_definitions, _connection, _config|
        [
          { name: 'ranking_config', optional: false, hint: 'projects/{p}/locations/{l}/rankingConfigs/{name}' },
          { name: 'query_text', optional: false },
          { name: 'records', type: 'array', of: 'object', optional: false,
            properties: [
              { name: 'id', optional: false },
              { name: 'title', optional: true },
              { name: 'content', optional: false, control_type: 'text-area' },
              { name: 'metadata', type: 'object', optional: true }
            ]
          }
        ]
      end,
      output_fields: lambda do |_|
        [
          { name: 'ranked_records', type: :array, of: :object },
          { name: '_meta', type: :object }
        ]
      end,
      execute: lambda do |connection, input|
        host = call(:aiplatform_host, connection)
        path = call(:path_ranking_rank, connection, input['ranking_config'])
        body = { query: { text: input['query_text'] }, records: input['records'] }

        post("https://#{host}#{path}")
          .headers(call(:default_headers, connection))
          .payload(body)
          .after_response { |code, body, headers, msg| call(:normalize_response!, code, body, headers) }
          .after_error_response { |code, body, headers, msg| call(:normalize_error!, code, body, headers) }
      end,
      sample_output: lambda do
        {}
      end
    }
  },

  # --------- METHODS ------------------------------------------------------
  methods: {
    coerce_integer: lambda do |v, fallback|
      Integer(v) rescue fallback
    end,
    normalize_error!: lambda do |code, body, _headers|
      parsed = begin
        body.is_a?(String) && !body.empty? ? JSON.parse(body) : (body || {})
      rescue
        {}
      end
      err = parsed['error'].is_a?(Hash) ? parsed['error'] : { 'message' => body.to_s }
      error({ 'code' => code, 'message' => err['message'], 'details' => err['details'] })
    end,
    normalize_response!: lambda do |code, body, headers|
      parsed = begin
        body.is_a?(String) && !body.empty? ? JSON.parse(body) : (body || {})
      rescue
        {}
      end
      meta = (parsed['_meta'] ||= {})
      meta['http_status']    = code
      meta['request_id']     = headers['x-request-id'] || headers['X-Request-Id']
      meta['retry_after']    = headers['Retry-After']
      meta['etag']           = headers['ETag']
      meta['last_modified']  = headers['Last-Modified']
      meta['model_version']  = headers['x-goog-model-id'] || headers['X-Model-Version']
      parsed
    end,
    normalize_model!: lambda do |m|
      mm = m.to_s.strip
      error('Model id required') if mm.empty?
      mm
    end,
    # --- REQUIRED ENV HELPERS (SDK-compliant signatures) -----------------
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
    aiplatform_host: lambda do |connection|
      "#{call(:ensure_location!, connection)}-aiplatform.googleapis.com"
    end,
    default_headers: lambda do |connection|
      h = { 'Content-Type' => 'application/json; charset=utf-8' }
      if (qp = connection['quota_project_id'].to_s.strip).present?
        h['x-goog-user-project'] = qp
      end
      h
    end,
    # --- PATH BUILDERS ----------------------------------------------------
    path_generate_content: lambda do |connection, model|
      "/v1/projects/#{call(:ensure_project_id!, connection)}/locations/#{call(:ensure_location!, connection)}/publishers/google/models/#{model}:generateContent"
    end,
    path_embeddings_predict: lambda do |connection, model|
      "/v1/projects/#{call(:ensure_project_id!, connection)}/locations/#{call(:ensure_location!, connection)}/publishers/google/models/#{model}:predict"
    end,
    path_rag_retrieve_contexts: lambda do |_connection, rag_corpus|
      "/v1/#{rag_corpus}:retrieveContexts"
    end,
    path_rag_answer: lambda do |_connection, rag_corpus|
      "/v1/#{rag_corpus}:answer"
    end,
    path_ranking_rank: lambda do |_connection, ranking_config|
      "/v1/#{ranking_config}:rank"
    end

  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {},

  # --------- TRIGGERS -----------------------------------------------------
  triggers: {},

  # --------- CUSTOM ACTION SUPPORT ----------------------------------------
  custom_action: true,
  custom_action_help: {
    body: "For actions calling host 'aiplatform.googleapis.com/v1', use relative paths. " \
          "For actions calling other endpoints (e.g. discovery engine), provide the absolute URL."
  }
}