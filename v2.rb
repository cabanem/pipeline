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
       # Single source of truth for Vertex resource addressing
       { name: 'project',  label: 'Project',  control_type: 'text',  optional: false, hint: 'Google Cloud project ID or number used in resource names (projects/{project}).' },
       { name: 'location', label: 'Location (region)', control_type: 'select', optional: false,
         options: %w[ us-central1 us-east1 us-east4 us-west1 europe-west1 europe-west4 asia-east1 asia-northeast1 ],
         hint: 'Regional location for Vertex AI and RAG resources. The same region must be used in paths and base host.' },
       { name: 'publisher', label: 'Publisher (for model path)', control_type: 'text', optional: true, default: 'google',
         hint: 'Used for publisher model paths: …/publishers/{publisher}/models/{model}. Default "google".' },

        # Requestor-pays / quota attribution
        { name: 'user_project', label: 'User project (x-goog-user-project)', control_type: 'text', optional: true,
          hint: 'Optional. If set, adds x-goog-user-project for requestor-pays or quota attribution.' },

        # Service account credential (JWT source)
        { name: 'service_account_json', label: 'Service account JSON', control_type: 'text-area', optional: false,
          hint: 'Paste the full service-account key JSON (fields like client_email, private_key). Used to mint a JWT and exchange for an access token.' }
    ],

    authorization: {
      # JWT-bearer → OAuth access token (custom)
      type: 'custom',

      acquire: lambda do |connection|
        scopes     = call(:const_default_scopes)   # → ['https://www.googleapis.com/auth/cloud-platform']
        access_tok = call(:auth_build_access_token!, connection, scopes: scopes)
        scope_key  = scopes.join(' ')
        cached     = call(:auth_token_cache_get, connection, scope_key)

        {
          access_token: access_tok,
          token_type:   'Bearer',
          expires_in:   (cached && cached['expires_in']) || 3600,
          expires_at:   (cached && cached['expires_at']) || (Time.now.utc + 3600 - 60).iso8601
        }
      end,

      apply: lambda do |connection|
        h = { 'Authorization' => "Bearer #{connection['access_token']}" }
        up = connection['user_project'].to_s.strip
        h['x-goog-user-project'] = up unless up.empty?
        headers(h)
      end,

      token_url: 'https://oauth2.googleapis.com/token',
      refresh_on: [401],
      detect_on:  [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    }

  },

  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |connection|
    project = connection['project'].to_s.strip
    location = connection['location'].to_s.strip
    error('Project is required for connection test') if project.empty?
    error('Location is required for connection test') if location.empty?

    base = "https://#{location}-aiplatform.googleapis.com"
    get("#{base}/v1/projects/#{project}/locations/#{location}")
  end,

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {
    # A) Connection-scoped
    connection_config: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    headers_allowlist: {
      # Doc-only helper used in comments/examples. Do not bind these to action inputs.
      fields: lambda do |connection, config_fields|
        [
          {
            name: 'headers',
            type: :array,
            of: :string,
            optional: false,
            hint: 'Connector adds Authorization automatically from auth. Only the following headers should ever be set by the connector: Authorization, Content-Type. Per-action inputs must not affect headers.'
          }
        ]
      end,
      sample_output: { headers: ['Authorization', 'Content-Type'] },
      additional_properties: false
    },
    error_google_json: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    model_path: {
      fields: lambda do |connection, config_fields|
        [
          {
            name: 'model',
            type: :string,
            optional: false,
            hint: "One of:\n- projects/{project}/locations/{location}/publishers/{publisher}/models/{model}\n- projects/{project}/locations/{location}/endpoints/{endpoint}\n\nThe {location} segment must match the connector’s selected location; when using a regional base URL, it must match the same region."
          }
        ]
      end,
      additional_properties: false
    },
    text_record: {
      fields: lambda do |connection, config_fields|
        [
          { name: 'id', type: :string },
          { name: 'title', type: :string, optional: true },
          { name: 'content', type: :string, control_type: 'text-area' },
          { name: 'metadata', type: :object, optional: true }
        ]
      end,
      additional_properties: false,
      sample_output: { id: 'doc-1', title: 'Benefits', content: '...', metadata: { source: 'intranet' } }
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

    # B) Generative (LLM) objects
    gen_content_part: {
      fields: lambda do |connection, config_fields|
        [
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
      end,
      additional_properties: true
    },
    gen_content: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: true
    },
    gen_generation_config: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: true
    },
    gen_generate_content_request: {
      fields: lambda do |connection, config_fields|
        [
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
          { name: 'labels', type: :object, optional: true, hint: 'String map of labels (key/value).' },
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
        ]
      end,
      additional_properties: false
    },
    gen_generate_content_response: {
      fields: lambda do |connection, config_fields|
        [
          { name: 'responseId', type: :string, optional: true },
          { name: 'modelVersion', type: :string, optional: true },
          { name: 'usageMetadata', type: :object, optional: true },
          { name: 'candidates', type: :array, of: :object, optional: true },
          { name: 'promptFeedback', type: :object, optional: true }
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    gen_count_tokens_response: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    emb_predict_response: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
          { name: 'id', type: :string, optional: false },
          { name: 'title', type: :string, optional: true },
          { name: 'content', type: :string, optional: true, control_type: 'text-area' },
          { name: 'metadata', type: :object, optional: true }
        ]
      end,
      additional_properties: false,
      sample_output: { id: 'a', title: 'Benefits overview', content: 'Doc A…', metadata: { source: 'intranet' } },
      hint: 'Each record must include id and at least one of title or content.'
    },
    rank_request: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    rank_response: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
          {
            name: 'ragCorpus',
            type: :string,
            optional: false,
            hint: 'Resource name pattern: projects/{project}/locations/{location}/ragCorpora/{corpus}. The {location} must match the connector’s selected region.'
          },
          { name: 'ragFileIds', type: :array, of: :string, optional: true }
        ]
      end,
      additional_properties: false
    },
    rag_vertex_store: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    rag_retrieve_contexts_request: {
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
      additional_properties: false
    },
    rag_retrieved_context: {
      fields: lambda do |connection, config_fields|
        [
          { name: 'chunkId', type: :string, optional: true },
          { name: 'text', type: :string, control_type: 'text-area', optional: true },
          { name: 'score', type: :number, optional: true },
          { name: 'sourceUri', type: :string, control_type: 'url', optional: true },
          { name: 'sourceDisplayName', type: :string, optional: true },
          { name: 'chunk', type: :object, optional: true },
          { name: 'metadata', type: :object, optional: true }
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
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
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
          { name: 'name', type: :string, optional: true, hint: 'Full LRO name: projects/{project}/locations/{location}/operations/{operationId}' },
          { name: 'done', type: :boolean, optional: true },
          { name: 'error', type: :object, optional: true },
          { name: 'metadata', type: :object, optional: true },
          { name: 'response', type: :object, optional: true }
        ]
      end,
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
      fields: lambda do |connection, config_fields|
        [
          {
            name: 'pageSize',
            type: :integer,
            optional: true,
            hint: 'Number of items to return (1–1000). Requests above 1000 are rejected by the API.',
            control_type: 'number'
          },
          { name: 'pageToken', type: :string, optional: true }
        ]
      end,
      additional_properties: false
    },
    pg_page_response: {
      fields: lambda do |connection, config_fields|
        [
          { name: 'nextPageToken', type: :string, optional: true }
        ]
      end,
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
      # Idempotent POSTs are retried via retry_on_response only
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'model', optional: false, hint: 'e.g., text-embedding-004' },
          { name: 'texts', type: :array, of: :string, optional: false },
          { name: 'task_type', label: 'Task type', type: :string, optional: true,
            hint: 'e.g., RETRIEVAL_QUERY or RETRIEVAL_DOCUMENT' },
          { name: 'auto_truncate', label: 'Auto truncate', type: :boolean, optional: true },
          { name: 'output_dimensionality', label: 'Output dimensionality', type: :integer, optional: true }
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
      display_priority: 86,
      retry_on_request: ['GET','HEAD'],
      retry_on_response: [408,429,500,502,503,504],
      max_retries: 3,

      input_fields: lambda do |_|
        [
          { name: 'rag_corpus', optional: true, hint: 'projects/{p}/locations/{l}/ragCorpora/{corpus}' },
          { name: 'query', optional: false, hint: 'Plain-text query string.' },
          { name: 'max_contexts', label: 'Top K', type: :integer, optional: true, default: 12, hint: 'Maps to query.ragRetrievalConfig.topK.' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'contexts', type: :array, of: :object, properties: object_definitions['context_chunk'] },
          { name: '_meta', type: :object }
        ]
      end,
      execute: lambda do |connection, input|
        host = call(:aiplatform_host, connection)
        path = call(:path_rag_retrieve_contexts, connection)

        body = {
          'query' => {
            'text' => input['query'].to_s,
            'ragRetrievalConfig' => {
              'topK' => call(:coerce_integer, input['max_contexts'], 12)
            }
          }
        }
        rc = input['rag_corpus'].to_s.strip
        unless rc.empty?
          # 'data_source' is a union; include only the chosen member at top level.
          body['vertexRagStore'] = {
            'ragResources' => [{ 'ragCorpus' => rc }]
          }
        end

        result = post("https://#{host}#{path}")
          .headers(call(:default_headers, connection))
          .payload(body)
          .after_response do |code, body, headers, _|
            parsed = call(:safe_parse_json, body)
            call(:normalize_response!, code, parsed, headers)
          end
          .after_error_response do |code, body, headers, _|
            parsed = call(:safe_parse_json, body)
            call(:normalize_error!, code, parsed, headers)
          end

        # API returns: { contexts: { contexts: [ ... ] } }
        arr = []
        # result is guaranteed Hash-ish after safe_parse_json, but be defensive anyway
        if result.respond_to?(:dig)
          arr = result.dig('contexts', 'contexts') || []
        end

        mapped = arr.map do |c|
          {
            'id' => c.dig('chunk', 'id') || c.dig('chunk', 'chunkId'),
            'uri' => c['sourceUri'],
            'content' => c['text'],
            'score' => c['score'],
            'metadata' => {
              'sourceDisplayName' => c['sourceDisplayName'],
              'chunk' => c['chunk']
            }.compact
          }.compact
        end

        { 'contexts' => mapped, '_meta' => { 'raw_count' => mapped.length } }
      end,
      sample_output: lambda do
        {
          'contexts' => [
            {
              'id' => 'chunk-123',
              'uri' => 'gs://bucket/doc.txt',
              'content' => '...',
              'score' => 0.83,
              'metadata' => { 'sourceDisplayName' => 'doc.txt' }
            }
          ],
          '_meta' => { 'raw_count' => 1 }
        }
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
        host = 'discoveryengine.googleapis.com'
        path = "/v1/#{input['ranking_config']}:rank"
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
    # -------- Utilities ---------------------------------------------------
    log_debug: lambda do |msg, data = {}|
      call(:telemetry_debug, msg, data)
    end,
    telemetry_debug: lambda do |msg, data = {}|
      # Simple, safe debug logger (no-op friendly)
      call(:workato_logger).call("DEBUG: #{msg} #{data.to_json}")
    end,
    validate_location_coherence!: lambda do |connection, resource|
      return true unless resource.is_a?(String) && resource.include?('/locations/')
      loc = resource[%r{/locations/([^/]+)}, 1]
      if loc && loc != connection['location']
        error("Region mismatch: resource location '#{loc}' != connection.location '#{connection['location']}'")
      end
      true
    end,

    # --- Auth (JWT → OAuth) ---------------------------------------------------
    const_default_scopes: -> { ['https://www.googleapis.com/auth/cloud-platform'] },
    auth_normalize_scopes: lambda do |scopes|
      arr = case scopes
            when nil    then call(:const_default_scopes)
            when String then scopes.split(/\s+/)
            when Array  then scopes
            else             call(:const_default_scopes)
            end
      arr.map(&:to_s).reject(&:empty?).uniq
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
    auth_get_sa_key!: lambda do |connection|
      raw = connection['service_account_key_json'].presence ||
            connection['service_account_json'].presence ||
            connection['service_account_key'].presence ||
            ''
      error('Service account key JSON is required') if raw.to_s.strip.empty?
      key = JSON.parse(raw) rescue nil
      error('Invalid service account key JSON') unless key.is_a?(Hash)
      error('Invalid service account key: missing client_email') if key['client_email'].to_s.strip.empty?
      error('Invalid service account key: missing private_key')  if key['private_key'].to_s.strip.empty?
      key
    end,
    auth_resolve_token_url!: lambda do |connection, key|
      # Prefer explicit connection override, then key.token_uri, else Google default
      url = connection['token_url'].presence || key['token_uri'].presence || 'https://oauth2.googleapis.com/token'
      u = url.to_s.strip
      error('Invalid token URL for JWT bearer exchange') if u.empty?
      u
    end,
    auth_token_cache_get: lambda do |connection, scope_key|
      cache = (connection['__token_cache'] ||= {})
      tok   = cache[scope_key]
      return nil unless tok.is_a?(Hash) && tok['access_token'].to_s != '' && tok['expires_at'].to_s != ''
      exp = (Time.parse(tok['expires_at']) rescue nil)
      return nil unless exp && Time.now < (exp - 60) # 60-sec skew buffer
      tok
    end,
    auth_token_cache_put: lambda do |connection, scope_key, token_hash|
      # Don’t cache broken tokens
      error('Auth error: missing access_token in token response') if token_hash['access_token'].to_s.empty?
      error('Auth error: missing expires_at in token response')   if token_hash['expires_at'].to_s.empty?

      cache = (connection['__token_cache'] ||= {})
      cache[scope_key] = token_hash
      token_hash
    end,
    auth_issue_token!: lambda do |connection, scopes|
      key       = call(:auth_get_sa_key!, connection)
      token_url = call(:auth_resolve_token_url!, connection, key)

      # Normalize private key newlines for OpenSSL
      pk = key['private_key'].to_s.gsub("\\n", "\n")

      # Normalize scopes → space-separated string (exactly like old)
      set       = Array(scopes).compact.map(&:to_s).reject(&:empty?)
      scope_str = set.join(' ')
      error('Scopes are required for JWT bearer exchange') if scope_str.empty?

      # JWT claim: aud MUST equal the POST target
      now = Time.now.to_i
      payload = {
        iss:   key['client_email'],
        scope: scope_str,
        aud:   token_url,
        iat:   now,
        exp:   now + 3600
      }

      assertion = call(:jwt_sign_rs256, payload, pk)

      # Exchange (form-encoded) – let Workato parse JSON into a Hash
      res = post(token_url)
              .payload(
                grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                assertion:  assertion
              )
              .request_format_www_form_urlencoded

      # Be tolerant: if Workato already parsed, these keys are at top level.
      access_token = res['access_token'].to_s
      token_type   = (res['token_type'].presence || 'Bearer').to_s
      expires_in   = res['expires_in'].to_i

      # If still empty, surface Google’s error directly (old vs new UX)
      if access_token.empty?
        # Try common Google shapes
        body_err  = (res['error_description'] || res.dig('error', 'message') || res['error'] || res['message']).to_s
        status    = (res['status'] || res['code']).to_s
        hint = [("status=#{status}" unless status.empty?), ("msg=#{body_err}" unless body_err.empty?)].compact.join(', ')
        error("Token exchange failed (no access_token). #{hint}")
      end

      ttl = (expires_in.zero? ? 3600 : expires_in)
      {
        'access_token' => access_token,
        'token_type'   => token_type,
        'expires_in'   => ttl,
        'expires_at'   => (Time.now.utc + ttl).iso8601,
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

    # --- Local JWT sanity check (no network) ---------------------------------
    b64url_decode: lambda do |str|
      # URL-safe base64 decode with padding fix
      s = str.to_s
      s += '=' * ((4 - s.length % 4) % 4)
      Base64.urlsafe_decode64(s)
    end,
    auth_jwt_selfcheck!: lambda do |connection|
      raw = connection['service_account_json'].to_s
      error('Service account JSON is required') if raw.empty?

      key = JSON.parse(raw) rescue nil
      error('Invalid service account JSON') unless key.is_a?(Hash)

      client_email = key['client_email'].to_s.strip
      private_key  = key['private_key'].to_s
      error('Invalid service account key: missing client_email') if client_email.empty?
      error('Invalid service account key: missing private_key')  if private_key.empty?

      # Normalize PEM newlines
      pk_pem = private_key.gsub(/\\n/, "\n")
      rsa    = OpenSSL::PKey::RSA.new(pk_pem) # will raise on malformed PEM

      # Build a tiny, throwaway JWT
      now = Time.now.to_i
      claims = {
        iss: client_email,
        sub: client_email,
        iat: now,
        exp: now + 300,
        aud: 'selfcheck' # arbitrary audience; we’re not sending this anywhere
      }

      jwt = call(:jwt_sign_rs256, claims, pk_pem)

      # Verify the signature locally using the public key
      parts = jwt.split('.')
      error('JWT selfcheck failed: malformed token') unless parts.length == 3

      signed_input = [parts[0], parts[1]].join('.')
      sig          = call(:b64url_decode, parts[2])

      ok = rsa.public_key.verify(OpenSSL::Digest::SHA256.new, sig, signed_input)
      error('JWT selfcheck failed: signature verify false (bad key?)') unless ok

      # Also ensure header/payload parse cleanly (sanity)
      header  = JSON.parse(call(:b64url_decode, parts[0])) rescue {}
      payload = JSON.parse(call(:b64url_decode, parts[1])) rescue {}
      error('JWT selfcheck failed: bad header')  unless header['alg'] == 'RS256' && header['typ'] == 'JWT'
      error('JWT selfcheck failed: bad payload') unless payload['iss'].to_s == client_email

      # Return something minimally useful for logs
      { ok: true, kid: header['kid'], iss: payload['iss'] }
    end,
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
      # Workato passes parsed JSON as body; ensure a Hash
      parsed = body.is_a?(Hash) ? body : (body.present? ? JSON.parse(body) : {})
      parsed['_http'] = { 'status' => code, 'headers' => headers }
      parsed
    end,
    normalize_model!: lambda do |m|
      mm = m.to_s.strip
      error('Model id required') if mm.empty?
      mm
    end,
    normalize_debug_blob: lambda do |_connection, blob|
      # Always return the original blob (no redaction)
      blob
    end,
    build_request_opts: lambda do |connection, base_opts|
      # Unconditional behavior (no prod/dev adjustment)
      base_opts
    end,
    # --- REQUIRED ENV HELPERS (SDK-compliant signatures) -----------------
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
    aiplatform_host: lambda do |connection|
      "#{call(:ensure_location!, connection)}-aiplatform.googleapis.com"
    end,
    default_headers: lambda do |connection|
      # Deliberately keep these minimal to avoid duplication of headers
      {
      'Content-Type' => 'application/json; charset=utf-8',
      'Accept' => 'application/json'
      }
    end,
    path_generate_content: lambda do |connection, model|
      pub = (connection['publisher'].presence || 'google')
      "/v1/projects/#{call(:ensure_project_id!, connection)}/locations/#{call(:ensure_location!, connection)}/publishers/#{pub}/models/#{model}:generateContent"
    end,
    path_embeddings_predict: lambda do |connection, model|
      pub = (connection['publisher'].presence || 'google')
      "/v1/projects/#{call(:ensure_project_id!, connection)}/locations/#{call(:ensure_location!, connection)}/publishers/#{pub}/models/#{model}:predict"
    end,
    path_rag_retrieve_contexts: lambda do |connection|
      "/v1/projects/#{call(:ensure_project_id!, connection)}/locations/#{call(:ensure_location!, connection)}:retrieveContexts"
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
    body: "Provide the absolute URL for API calls."
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
