require 'digest'
require 'time'
require 'json'
require 'csv'
require 'securerandom'

{
  title: "RAG Utilities",
  description: "Custom utility functions for RAG email response system",
  version: "0.3.0",
  help: lambda do 
    { body: "Provides text processing, chunking, similarity, prompt building, and validation utilities for retrieval-augmented generation (RAG) systems." }
  end,
  author: "",

  # --------- CONNECTION ---------------------------------------------------
  connection: {
    help: lambda do
      { body: "Configure default settings for text processing. These can be overriden in individual actions. Environment selection determines logging verbosity and processing limits." }
    end,
    fields: [
      # Group: Developer API
      { name: "developer_api_host",     label: "Workato region",        control_type: "select",   optional: true,   group: "Developer API",
        default: "app.eu", sticky: true,  support_pills: false, hint: "Only required when using custom rules from Data Tables. Defaults to EU. See Workato data centers.",
        options: [ # <- use options (static) on connection
          ["US (www.workato.com)",    "www"],
          ["EU (app.eu.workato.com)", "app.eu"],
          ["JP (app.jp.workato.com)", "app.jp"],
          ["SG (app.sg.workato.com)", "app.sg"],
          ["AU (app.au.workato.com)", "app.au"],
          ["IL (app.il.workato.com)", "app.il"],
          ["Developer sandbox (app.trial.workato.com)", "app.trial"] ] },
      { name: "api_token",              label: "API token (Bearer)",    control_type: "password", optional: true,   group: "Developer API", 
        sticky: true,  hint: "Workspace admin → API clients → API keys", },
      # Group: Labeling
      { name: "environment",            label: "Environment",           control_type: "select",   optional: false,  group: "Labeling",
        default: "development", sticky: true, support_pills: false, hint: "Select the environment for the connector (for your own routing/labeling).",  
        options: [
          ["Development", "development"],
          ["Staging", "staging"],
          ["Production", "production"]
        ]},
      # Group: RAG defaults
      { name: "chunk_size_default",     label: "Default Chunk Size",    control_type: "number",   optional: true,   group: "RAG defaults",
        default: 1000, type: "integer", convert_input: "integer_conversion", hint: "Default token size for text chunks" },
      { name: "chunk_overlap_default",  label: "Default Chunk Overlap", control_type: "number",   optional: true,   group: "RAG defaults",
        default: 100, type: "integer", convert_input: "integer_conversion", hint: "Default token overlap between chunks" },
      { name: "similarity_threshold",   label: "Similarity Threshold",  control_type: "number",   optional: true,   group: "Similarity defaults",
        default: 0.7, type: "number", convert_input: "float_conversion", hint: "Minimum similarity score (0-1) for cosine/euclidean; used as default gate." }
    ],
    authorization: {
      type: "custom_auth",
      apply: lambda do |connection|
        if connection['api_token'].present?
          headers(
            'Authorization' => "Bearer #{connection['api_token']}",
            'Accept'        => 'application/json'
          )
        end
      end
    },
    base_uri: lambda do |connection|
      host = (connection['developer_api_host'].presence || 'app.eu').to_s
      "https://#{host}.workato.com"
    end
  },

  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |connection|
    result = {
      environment: (connection["environment"] || "development"),
      region: (connection['developer_api_host'] || 'app.eu')
    }

    if connection['api_token'].present?
      begin
        whoami = call(:execute_with_retry, connection, -> { get('/api/users/me') })
        result[:account] = whoami["name"] || whoami["id"]
        result[:status]  = "connected"
      rescue RestClient::ExceptionWithResponse => e
        result[:status] = "failed (#{e.http_code})"
        result[:hint]   = "Likely region/token mismatch. Ensure developer_api_host='app.eu' and the API client was created in the EU workspace."
        return result
      end

      # Management probes (optional)
      begin
        call(:execute_with_retry, connection, -> { get('/api/data_tables').params(page: 1, per_page: 1) })
        result[:data_tables] = "reachable"
      rescue RestClient::ExceptionWithResponse => e
        result[:data_tables] = "not reachable (#{e.http_code})"
      end
      begin
        call(:execute_with_retry, connection, -> { get('/api/projects').params(page: 1, per_page: 1) })
        result[:projects] = "reachable"
      rescue RestClient::ExceptionWithResponse => e
        result[:projects] = "not reachable (#{e.http_code})"
      end
      begin
        call(:execute_with_retry, connection, -> { get('/api/folders').params(page: 1, per_page: 1) })
        result[:folders] = "reachable"
      rescue RestClient::ExceptionWithResponse => e
        result[:folders] = "not reachable (#{e.http_code})"
      end

      # Records v1 smoke test (decisive for rules/templates)
      begin
        # Try to discover 1 table id to query
        tables_resp = call(:execute_with_retry, connection, -> { get('/api/data_tables').params(page: 1, per_page: 1) })
        arr = tables_resp.is_a?(Array) ? tables_resp : (tables_resp['data'] || [])
        if arr.any?
          tid = arr.first['id']
          post("#{call(:dt_records_base, connection)}/api/v1/tables/#{tid}/query")
            .headers('Authorization' => "Bearer #{connection['api_token']}")
            .payload(select: ['$record_id'], limit: 1)
          result[:data_table_records_v1] = "reachable"
        else
          result[:data_table_records_v1] = "no tables to test"
        end
      rescue RestClient::ExceptionWithResponse => e
        code = e.http_code.to_i
        result[:data_table_records_v1] =
          (code == 401 || code == 403) ? "not reachable (#{code}) – add role: Data table records (v1)" : "not reachable (#{code})"
      end
    else
      result[:status] = "connected (no API token)"
    end

    result
  end,

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    
    # --- 1.  Smart chunk text ----------------------------------------------
    smart_chunk_text: {
      title: "Smart Chunk Text",
      subtitle: "Intelligently chunk text preserving context",
      description: "Split text into chunks with smart boundaries and overlap.",
      help: lambda do
        {
          body: "Splits text into token‑approximate chunks using sentence/paragraph boundaries and overlap. Use connection defaults to avoid per‑step config."
        }
      end,

      config_fields: [
        {
          name: "use_custom_settings",
          label: "Configuration mode",
          control_type: "select",
          pick_list: [
            ["Use connection defaults", "defaults"],
            ["Custom settings", "custom"]
          ],
          default: "defaults",
          sticky: true,
          hint: "Select 'Custom' to override connection defaults."
        }
      ],

      input_fields: lambda do |object_definitions, connection, config|
        fields = [
          { name: "text", label: "Input text", type: "string",
            optional: false, control_type: "text-area",
            hint: "Raw text to be chunked" },
          { name: "document_metadata", label: "Document metadata", type: "object",
            optional: true,
            properties: [
              { name: "document_id", label: "Document ID", type: "string", optional: true },
              { name: "file_name", label: "File name", type: "string", optional: true },
              { name: "file_id", label: "File ID", type: "string", optional: true }
            ],
            hint: "Optional document metadata to include with each chunk" }
        ]
        if config["use_custom_settings"] == "custom"
          fields.concat(object_definitions["chunking_config"])
        end
        fields
      end,

      output_fields: lambda do |object_definitions|
        object_definitions["chunking_result"]
      end,

      sample_output: lambda do
        {
          "chunks" => [
            { "chunk_id" => "chunk_0", "chunk_index" => 0, "text" => "Lorem ipsum…", "token_count" => 120, "start_char" => 0, "end_char" => 480, "metadata" => { "has_overlap" => false, "is_final" => false } }
          ],
          "total_chunks" => 1,
          "total_tokens" => 120
        }
      end,

      execute: lambda do |connection, input, _eis, _eos, config|
        local = call(:deep_copy, input)
        if config["use_custom_settings"] != "custom"
          local['chunk_size']        ||= (connection['chunk_size_default'] || 1000)
          local['chunk_overlap']     ||= (connection['chunk_overlap_default'] || 100)
          local['preserve_sentences']  = true  if local['preserve_sentences'].nil?
          local['preserve_paragraphs'] = false if local['preserve_paragraphs'].nil?
        end

        # Guards for pathological inputs
        cs = (local['chunk_size'] || 1000).to_i
        co = (local['chunk_overlap'] || 100).to_i
        error("Chunk size must be > 0")        if cs <= 0
        error("Chunk overlap must be >= 0")    if co < 0

        result = call(:chunk_text_with_overlap, local)

        # Add document metadata to chunks if provided
        if local['document_metadata'].present?
          result['chunks'].each_with_index do |chunk, idx|
            chunk['metadata'] ||= {}
            chunk['metadata'].merge!({
              'document_id'   => local['document_metadata']['document_id'],
              'file_name'     => local['document_metadata']['file_name'],
              'file_id'       => local['document_metadata']['file_id'],
              'total_chunks'  => result['total_chunks']
            })
          end
        end

        call(:validate_contract, connection, result, 'chunking_result')
      end
    },
    # --- 2.  Clean email text ----------------------------------------------
    clean_email_text: {
      title: "Clean Email Text",
      subtitle: "Preprocess email content for RAG",
      description: "Clean and preprocess email body text",
      help: lambda do
        {
          body: "Removes signatures, quoted text, disclaimers; normalizes whitespace; optional URL extraction."
        }
      end,

      input_fields: lambda do |object_definitions|
        [
          {
            name: "email_body", label: "Email body", type: "string", optional: false,
            control_type: "text-area", hint: "Raw email body text to be cleaned"
          }
        ] + object_definitions["email_cleaning_options"]
      end,

      output_fields: lambda do |object_definitions|
        object_definitions["email_cleaning_result"]
      end,

      sample_output: lambda do
        {
          "cleaned_text" => "Hello team, …",
          "extracted_query" => "Hello team, …",
          "removed_sections" => ["--\nJohn\n"],
          "extracted_urls" => ["https://example.com"],
          "original_length" => 1024,
          "cleaned_length" => 680,
          "reduction_percentage" => 33.59
        }
      end,

      execute: lambda do |connection, input|
        local = call(:deep_copy, input)
        result = call(:process_email_text, local)
        call(:validate_contract, connection, result, 'email_cleaning_result')
      end

    },
    # --- 3.  Calculate similarity ------------------------------------------
    calculate_similarity: {
      title: "Calculate Vector Similarity",
      subtitle: "Compute similarity scores for vectors",
      description: "Compute similarity between embedding vectors",
      help: lambda do
        { body: "Supports cosine, euclidean, and dot product. Dot product without normalization requires a model‑appropriate absolute threshold." }
      end,

      config_fields: [
        {
          name: "similarity_method",
          label: "Similarity method",
          control_type: "select",
          pick_list: "similarity_types",
          default: "cosine",
          sticky: true,
          hint: "Controls which inputs are shown below."
        }
      ],

      input_fields: lambda do |_object_definitions, _connection, config|
        fields = [
          {
            name: "vectors", label: "Vectors to compare", type: "object",
            properties: [
              { name: "vector_a", label: "First vector",  type: "array", of: "number", list_mode_toggle: true, optional: false },
              { name: "vector_b", label: "Second vector", type: "array", of: "number", list_mode_toggle: true, optional: false }
            ],
            group: "Vectors"
          }
        ]
        method = (config['similarity_method'] || 'cosine').to_s
        unless method == 'dot_product'
          fields << {
            name: "normalize", label: "Normalize vectors", control_type: "checkbox",
            type: "boolean", default: true, optional: true,
            hint: "Ignored for dot product.", group: "Options"
          }
        end
        fields
      end,

      output_fields: lambda do |object_definitions|
        object_definitions["similarity_result"]
      end,

      sample_output: lambda do
        {
          "similarity_score" => 0.873421,
          "similarity_percentage" => 87.34,
          "is_similar" => true,
          "similarity_type" => "cosine",
          "computation_time_ms" => 2,
          "threshold_used" => 0.7,
          "vectors_normalized" => true
        }
      end,

      execute: lambda do |connection, input, _eis, _eos, config|
        local = {
          'vector_a'       => Array(input.dig('vectors', 'vector_a')),
          'vector_b'       => Array(input.dig('vectors', 'vector_b')),
          'similarity_type'=> (config['similarity_method'] || 'cosine'),
          'normalize'      => input.key?('normalize') ? input['normalize'] : true
        }
        error("Vectors cannot be empty") if local['vector_a'].empty? || local['vector_b'].empty?
        result = call(:compute_similarity, local, connection)
        call(:validate_contract, connection, result, 'similarity_result')
      end
    },
    # --- 4.  Prepare embedding batch ---------------------------------------
    prepare_embedding_batch: {
      title: "Prepare Embedding Batch",
      subtitle: "Prepare text content for embedding processing",
      description: "Process text array into embedding request format with batch management",
      help: lambda do
        { body: "Accepts array of text objects with id, content, title, and metadata. Generates batch IDs and formats data according to embedding_request contract for inter-connector compatibility." }
      end,

      input_fields: lambda do |object_definitions|
        [
          {
            name: "texts", label: "Text objects", type: "array", of: "object",
            optional: false, list_mode_toggle: true,
            properties: [
              { name: "id", label: "Unique ID", type: "string", optional: false },
              { name: "content", label: "Text content", type: "string", optional: false },
              { name: "title", label: "Title", type: "string", optional: true },
              { name: "metadata", label: "Metadata", type: "object", optional: true }
            ]
          },
          {
            name: "task_type", label: "Task type", control_type: "select",
            pick_list: [
              ["Retrieval Document", "RETRIEVAL_DOCUMENT"],
              ["Query", "QUERY"],
              ["Semantic Similarity", "SEMANTIC_SIMILARITY"]
            ],
            optional: false, sticky: true, support_pills: false,
            hint: "Type of embedding task for Vertex AI processing"
          },
          {
            name: "batch_size", label: "Batch size", type: "integer",
            optional: true, default: 25, hint: "Number of texts per batch"
          },
          {
            name: "include_title_in_text", label: "Include title in text",
            type: "boolean", control_type: "checkbox", default: true,
            hint: "Prepend title to content for embedding"
          },
          {
            name: "batch_prefix", label: "Batch ID prefix", type: "string",
            optional: true, default: "emb_batch",
            hint: "Prefix for generated batch IDs"
          }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: "batches", type: "array", of: "object", properties: [
            { name: "batch_id", type: "string" },
            { name: "batch_number", type: "integer" },
            { name: "requests", type: "array", of: "object", properties: [
              { name: "text", type: "string" },
              { name: "metadata", type: "object" }
            ]},
            { name: "size", type: "integer" }
          ]},
          { name: "total_batches", type: "integer" },
          { name: "total_texts", type: "integer" },
          { name: "task_type", type: "string" },
          { name: "batch_generation_timestamp", type: "string" }
        ]
      end,

      sample_output: lambda do
        {
          "batches" => [
            {
              "batch_id" => "emb_batch_0_20240115103000",
              "batch_number" => 0,
              "requests" => [
                {
                  "text" => "Product Overview: This is a sample product description...",
                  "metadata" => {
                    "id" => "doc_123",
                    "title" => "Product Overview",
                    "task_type" => "RETRIEVAL_DOCUMENT",
                    "batch_id" => "emb_batch_0_20240115103000"
                  }
                }
              ],
              "size" => 1
            }
          ],
          "total_batches" => 1,
          "total_texts" => 1,
          "task_type" => "RETRIEVAL_DOCUMENT",
          "batch_generation_timestamp" => "2024-01-15T10:30:00Z"
        }
      end,

      execute: lambda do |connection, input, _eis, _eos, _config|
        call(:prepare_embedding_batch_exec, connection, input)
      end
    },
    # --- 5.  Build RAG prompt ----------------------------------------------
    build_rag_prompt: {
      title: "Build RAG Prompt",
      subtitle: "Construct optimized RAG prompt",
      description: "Build retrieval‑augmented generation prompt",
      help: lambda do
        { body: "Use a built‑in template or select a custom template from Data Tables. Custom selection requires API token & table access." }
      end,

      config_fields: [
        { name: "prompt_mode",            label: "Prompt configuration",          control_type: "select", support_pills: false, default: "template",
          sticky: true, pick_list: [["Template-based", "template"], ["Custom instructions", "custom"]] },
        { name: "template_source",        label: "Template source",               control_type: "select", support_pills: false, default: "builtin",
          pick_list: [["Built-in", "builtin"], ["Custom (Data Tables)", "custom"]], sticky: true },
        { name: "templates_table_id",     label: "Templates table (Data Tables)", control_type: "select", support_pills: false,
          pick_list: "tables", ngIf: 'input.template_source == "custom"', hint: "Required when Template source = Custom" },
        { name: "template_display_field", label: "Display field name",            control_type: 'text',   support_pills: false, default: "name",
          type: "string",  optional: true, sticky: true, ngIf: 'input.template_source == "custom"', hint: "Column shown in the dropdown" },
        { name: "template_value_field",   label: "Value field name",              control_type: 'text',   support_pills: false, default: "",
          type: "string",  optional: true, sticky: true, ngIf: 'input.template_source == "custom"', hint: "Stored value for the selection. Leave blank to use the Record ID." },
        { name: "template_content_field", label: "Content field name",            control_type: 'text',   support_pills: false, default: "content",
          type: "string",  optional: true, sticky: true, ngIf: 'input.template_source == "custom"', hint: "Column containing the prompt text" }
      ],

      input_fields: lambda do |object_definitions, _connection, config|
        fields = [
          { name: "query", label: "User query", type: "string", optional: false, control_type: "text-area", group: "Query" },
          {
            name: "context_documents", label: "Context documents",
            type: "array", of: "object",
            properties: object_definitions["context_document"],
            list_mode_toggle: true, optional: false, group: "Context"
          }
        ]
        if config["prompt_mode"] == "template"
          fields << {
            name: "prompt_template", label: "Prompt template",
            type: "string", group: "Template settings",
            control_type: "select", pick_list: "prompt_templates",
            pick_list_params: {
              template_source: (config['template_source'] || 'builtin'),
              templates_table_id: config['templates_table_id'],
              template_display_field: (config['template_display_field'] || 'name'),
              template_value_field: (config['template_value_field'] || ''),
              template_content_field: (config['template_content_field'] || 'content')
            },
            optional: true,
            toggle_hint: "Select",
            toggle_field: { name: "prompt_template", label: "Template (custom text)", type: "string", control_type: "text", toggle_hint: "Use text" }
          }
        else
          fields << {
            name: "system_instructions", label: "System instructions",
            type: "string", control_type: "text-area", optional: true,
            hint: "Custom system instructions for the prompt", group: "Custom settings"
          }
        end

        fields += [
          {
            name: "advanced_settings", label: "Advanced settings", type: "object", optional: true,
            group: "Advanced", hint: "Optional configuration",
            properties: [
              { name: "max_context_length", label: "Max context length (tokens)", type: "integer", default: 3000, convert_input: "integer_conversion", hint: "Maximum tokens for context" },
              { name: "include_metadata", label: "Include metadata", type: "boolean", control_type: "checkbox", default: false, convert_input: "boolean_conversion", hint: "Include document metadata in prompt" }
            ]
          }
        ]
        fields
      end,

      output_fields: lambda do
        [
          { name: "formatted_prompt", type: "string" },
          { name: "token_count", type: "integer" },
          { name: "context_used", type: "integer" },
          { name: "truncated", type: "boolean", control_type: "checkbox" },
          { name: "prompt_metadata", type: "object" }
        ]
      end,

      sample_output: lambda do
        {
          "formatted_prompt" => "Context:\n…\n\nQuery: …\n\nAnswer:",
          "token_count" => 512, "context_used" => 3, "truncated" => false,
          "prompt_metadata" => { "template" => "standard", "using_template_content" => false }
        }
      end,

      execute: lambda do |connection, input, _eis, _eos, config|
        local = call(:deep_copy, input)
        if local['advanced_settings']
          local.merge!(local['advanced_settings'])
          local.delete('advanced_settings')
        end

        if config["prompt_mode"] == "template"
          source = (config["template_source"] || "builtin").to_s
          sel    = (local["prompt_template"] || "").to_s

          if source == "custom" && config["templates_table_id"].present? && sel.present?
            inline = (sel.include?("\n") || sel.length > 200)

            unless inline
              resolved = call(:resolve_template_selection, connection, config, sel)
              if resolved && resolved["content"].to_s.strip.length.positive?
                local["template_content"] = resolved["content"].to_s
                local["prompt_metadata"] = {
                  template_source: "custom",
                  templates_table_id: config["templates_table_id"],
                  template_value: sel,
                  template_display: resolved["display"]
                }.compact
              end
            else
              local["template_content"] = sel
              local["prompt_metadata"] = { template_source: "inline" }
            end
          elsif sel.present? && (sel.include?("\n") || sel.length > 200)
            local["template_content"] = sel
            local["prompt_metadata"] = { template_source: "inline" }
          end
        end

        call(:construct_rag_prompt, local)
      end
    },
    # --- 6.  Validate LLM response -----------------------------------------
    validate_llm_response: {
      title: "Validate LLM Response",
      subtitle: "Validate and score LLM output",
      description: "Check response quality and relevance",
      help: lambda do
        { body: "Lightweight heuristics: query overlap, length, rule checks, and confidence score." }
      end,

      input_fields: lambda do |object_definitions|
        [
          { name: "response_text", label: "LLM response", type: "string", optional: false, control_type: "text-area" },
          { name: "original_query", label: "Original query", type: "string", optional: false },
          { name: "context_provided", label: "Context documents", type: "array", of: "string", optional: true, list_mode_toggle: true },
          { name: "validation_rules", label: "Validation rules", type: "array", of: "object", properties: object_definitions["validation_rule"], optional: true },
          { name: "min_confidence", label: "Minimum confidence", type: "number", convert_input: "float_conversion", optional: true, default: 0.7, sticky: true }
        ]
      end,

      output_fields: lambda do
        [
          { name: "is_valid", type: "boolean", control_type: "checkbox" },
          { name: "confidence_score", type: "number" },
          { name: "pass_fail", type: "boolean", label: "Validation passed",
            hint: "True if response passes all validation checks" },
          { name: "action_required", type: "string", label: "Action required",
            hint: "Next recommended action based on validation results" },
          { name: "validation_results", type: "object" },
          { name: "issues_count", type: "integer", label: "Number of issues",
            hint: "Total number of validation issues found" },
          { name: "issues_found", type: "array", of: "string" },
          { name: "requires_human_review", type: "boolean", control_type: "checkbox" },
          { name: "suggestions_count", type: "integer", label: "Number of suggestions",
            hint: "Total number of improvement suggestions" },
          { name: "suggested_improvements", type: "array", of: "string" },
          { name: "confidence_level", type: "string", label: "Confidence level",
            hint: "high, medium, or low based on confidence score" }
        ]
      end,

      sample_output: lambda do
        {
          "is_valid" => true, "confidence_score" => 0.84,
          "validation_results" => { "query_overlap" => 0.33, "response_length" => 1100, "word_count" => 230 },
          "issues_found" => [], "requires_human_review" => false, "suggested_improvements" => []
        }
      end,

      execute: lambda do |_connection, input|
        call(:validate_response, input)
      end
    },
    # --- 7.  Generate document metadata ------------------------------------
    generate_document_metadata: {
      title: "Generate Document Metadata",
      subtitle: "Extract metadata from documents",
      description: "Generate metadata for document indexing",
      help: lambda do
        { body: "Token estimate uses 4 chars/token heuristic; key topics via naive frequency analysis." }
      end,

      input_fields: lambda do
        [
          { name: "document_content", label: "Document content", type: "string", optional: false, control_type: "text-area" },
          { name: "file_path", label: "File path", type: "string", optional: false },
          { name: "file_type", label: "File type", type: "string", optional: true, control_type: "select", pick_list: "file_types" },
          { name: "extract_entities", label: "Extract entities", type: "boolean", optional: true, default: true, control_type: "checkbox" },
          { name: "generate_summary", label: "Generate summary", type: "boolean", optional: true, default: true, control_type: "checkbox" }
        ]
      end,

      output_fields: lambda do
        [
          { name: "document_id", type: "string" },
          { name: "file_hash", type: "string" },
          { name: "word_count", type: "integer" },
          { name: "character_count", type: "integer" },
          { name: "estimated_tokens", type: "integer" },
          { name: "language", type: "string" },
          { name: "summary", type: "string" },
          { name: "key_topics", type: "array", of: "string" },
          { name: "entities", type: "object" },
          { name: "created_at", type: "timestamp" },
          { name: "processing_time_ms", type: "integer" }
        ]
      end,

      sample_output: lambda do
        {
          "document_id" => "abc123", "file_hash" => "…sha256…", "word_count" => 2500, "character_count" => 14000,
          "estimated_tokens" => 3500, "language" => "english", "summary" => "…", "key_topics" => %w[rules r ag email],
          "entities" => { "people" => [], "organizations" => [], "locations" => [] },
          "created_at" => Time.now.iso8601, "processing_time_ms" => 12
        }
      end,

      execute: lambda do |connection, input|
        result = call(:extract_metadata, input)
        call(:validate_contract, connection, result, 'document_metadata')
      end
    },
    # --- 8.  Check document changes ----------------------------------------
    check_document_changes: {
      title: "Check Document Changes",
      subtitle: "Detect changes in documents",
      description: "Compare document versions to detect modifications",
      help: lambda do
        { body: "Choose Hash only (fast), Content diff (line‑based), or Smart diff (tokens + structure)." }
      end,

      input_fields: lambda do
        [
          { name: "current_hash", label: "Current document hash", type: "string", optional: false },
          { name: "current_content", label: "Current content", type: "string", optional: true, control_type: "text-area" },
          { name: "previous_hash", label: "Previous document hash", type: "string", optional: false },
          { name: "previous_content", label: "Previous content", type: "string", optional: true, control_type: "text-area" },
          { name: "check_type", label: "Check type", type: "string", optional: true, default: "hash", control_type: "select", pick_list: "check_types" }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: "has_changed", type: "boolean", control_type: "checkbox" },
          { name: "change_type", type: "string" },
          { name: "change_percentage", type: "number" },
          { name: "added_content", type: "array", of: "string" },
          { name: "removed_content", type: "array", of: "string" },
          { name: "modified_sections", type: "array", of: "object", properties: object_definitions["diff_section"] },
          { name: "requires_reindexing", type: "boolean", control_type: "checkbox" }
        ]
      end,

      sample_output: lambda do
        {
          "has_changed" => true, "change_type" => "content_changed", "change_percentage" => 12.5,
          "added_content" => ["new line"], "removed_content" => ["old line"],
          "modified_sections" => [{ "type" => "modified", "current_range" => [10,10], "previous_range" => [10,10], "current_lines" => ["A"], "previous_lines" => ["B"] }],
          "requires_reindexing" => true
        }
      end,

      execute: lambda do |connection, input|
        result = call(:detect_changes, input)
        call(:validate_contract, connection, result, 'change_detection')
      end
    },
    # --- 9.  Calculate metrics ---------------------------------------------
    calculate_metrics: {
      title: "Calculate Performance Metrics",
      subtitle: "Calculate system performance metrics",
      description: "Calculate averages, percentiles, trend and anomalies from time‑series data",
      help: lambda do
        { body: "Computes avg/median/min/max/stddev, P95/P99, simple trend and 2σ anomalies." }
      end,

      input_fields: lambda do |object_definitions|
        [
          { name: "metric_type", label: "Metric type", type: "string", optional: false, control_type: "select", pick_list: "metric_types" },
          { name: "data_points", label: "Data points", list_mode_toggle: true, type: "array", of: "object", optional: false, properties: object_definitions["metric_datapoint"] },
          { name: "aggregation_period", label: "Aggregation period", type: "string", optional: true, default: "hour", control_type: "select", pick_list: "time_periods" },
          { name: "include_percentiles", label: "Include percentiles", type: "boolean", control_type: "checkbox", convert_input: "boolean_conversion", optional: true, default: true }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: "average", type: "number" },
          { name: "median", type: "number" },
          { name: "min", type: "number" },
          { name: "max", type: "number" },
          { name: "std_deviation", type: "number" },
          { name: "percentile_95", type: "number" },
          { name: "percentile_99", type: "number" },
          { name: "total_count", type: "integer" },
          { name: "trend", type: "string" },
          { name: "anomalies_detected", type: "array", of: "object", properties: object_definitions["anomaly"] }
        ]
      end,

      sample_output: lambda do
        {
          "average" => 12.3, "median" => 11.8, "min" => 4.2, "max" => 60.0,
          "std_deviation" => 5.1, "percentile_95" => 22.0, "percentile_99" => 29.5,
          "total_count" => 1440, "trend" => "increasing",
          "anomalies_detected" => [{ "timestamp" => Time.now.iso8601, "value" => 42.0 }]
        }
      end,

      execute: lambda do |connection, input|
        result = call(:compute_metrics, input)
        call(:validate_contract, connection, result, 'metrics_result')
      end
    },
    # --- 10. Optimize batch size -------------------------------------------
    optimize_batch_size: {
      title: "Optimize Batch Size",
      subtitle: "Calculate optimal batch size for processing",
      description: "Recommend an optimal batch size based on historical performance",
      help: lambda do
        { body: "Heuristic scoring by target (throughput/latency/cost/accuracy)." }
      end,

      input_fields: lambda do
        [
          { name: "total_items", label: "Total items to process", type: "integer", optional: false },
          {
            name: "processing_history", label: "Processing history",
            type: "array", of: "object", optional: true,
            properties: [
              { name: "batch_size", type: "integer" },
              { name: "processing_time", type: "number" },
              { name: "success_rate", type: "number" },
              { name: "memory_usage", type: "number" }
            ]
          },
          { name: "optimization_target", label: "Optimization target", type: "string", optional: true, default: "throughput", control_type: "select", pick_list: "optimization_targets" },
          { name: "max_batch_size", label: "Maximum batch size", type: "integer", optional: true, default: 100 },
          { name: "min_batch_size", label: "Minimum batch size", type: "integer", optional: true, default: 10 }
        ]
      end,

      output_fields: lambda do
        [
          { name: "optimal_batch_size", type: "integer" },
          { name: "estimated_batches", type: "integer" },
          { name: "estimated_processing_time", type: "number" },
          { name: "throughput_estimate", type: "number" },
          { name: "confidence_score", type: "number" },
          { name: "recommendation_reason", type: "string" }
        ]
      end,

      sample_output: lambda do
        {
          "optimal_batch_size" => 50, "estimated_batches" => 20, "estimated_processing_time" => 120.5,
          "throughput_estimate" => 41.5, "confidence_score" => 0.8, "recommendation_reason" => "Based on historical performance data"
        }
      end,

      execute: lambda do |_connection, input|
        call(:calculate_optimal_batch, input)
      end
    },
    # --- 11. Classify by pattern -------------------------------------------
    classify_by_pattern: {
      title: "Classify by pattern matching",
      subtitle: "Pattern-based classification without AI",
      description: "Evaluate text against pattern rules from standard library or Data Tables",
      help: lambda do
        { body: "Use standard patterns or supply a Data Table of rules {rule_id, rule_type, rule_pattern, action, priority, active}. Requires API token to read Data Tables." }
      end,
      # CONFIG
      config_fields: [ # Remember -- config drives inputs
        {
          name: "rules_source", label: "Rules source", control_type: "select",
          pick_list: [["Standard", "standard"], ["Custom (Data Tables)", "custom"]],
          default: "standard", sticky: true, support_pills: false,
          hint: "Use 'Custom' to evaluate against a data table of rules."
        },
        {
          name: "custom_rules_table_id", label: "Rules table (Data Tables)",
          control_type: "select", pick_list: "tables", support_pills: false,
          ngIf: 'input.rules_source == "custom"', sticky: true,
          hint: "Required when rules_source is custom"
        },
        # Optional column mapping when teams use different column names
        {
          name: "enable_column_mapping",
          label: "Custom column names?",
          type: "boolean", control_type: "checkbox", default: false,
          ngIf: 'input.rules_source == "custom"',
          sticky: true, support_pills: false
        },
        # Mapped columns – only shown when mapping is enabled
        { name: "col_rule_id",      label: "Rule ID column",      control_type: "select", pick_list: "table_columns",
          ngIf: 'input.rules_source == "custom" && input.enable_column_mapping == true',
          optional: true, sticky: true, support_pills: false },
        { name: "col_rule_type",    label: "Rule type column",    control_type: "select", pick_list: "table_columns",
          ngIf: 'input.rules_source == "custom" && input.enable_column_mapping == true',
          optional: true, sticky: true, support_pills: false },
        { name: "col_rule_pattern", label: "Rule pattern column", control_type: "select", pick_list: "table_columns",
          ngIf: 'input.rules_source == "custom" && input.enable_column_mapping == true',
          optional: true, sticky: true, support_pills: false },
        { name: "col_action",       label: "Action column",       control_type: "select", pick_list: "table_columns",
          ngIf: 'input.rules_source == "custom" && input.enable_column_mapping == true',
          optional: true, sticky: true, support_pills: false },
        { name: "col_priority",     label: "Priority column",     control_type: "select", pick_list: "table_columns",
          ngIf: 'input.rules_source == "custom" && input.enable_column_mapping == true',
          optional: true, sticky: true, support_pills: false },
        { name: "col_active",       label: "Active column",       control_type: "select", pick_list: "table_columns",
          ngIf: 'input.rules_source == "custom" && input.enable_column_mapping == true',
          optional: true, sticky: true, support_pills: false }

      ],
      # INPUT
      input_fields: lambda do |object_definitions, _connection, config|
        fields = [
          {
            name: "email", label: "Email", type: "object", optional: false,
            properties: object_definitions["email_envelope"], group: "Email"
          },
          {
            name: "stop_on_first_match", label: "Stop on first match", control_type: "checkbox",
            type: "boolean", default: true, optional: true, sticky: true,
            hint: "When true, returns as soon as a rule matches.", group: "Execution"
          },
          {
            name: "fallback_to_standard", label: "Fallback to standard patterns",
            type: "boolean", default: true, optional: true, sticky: true, control_type: "checkbox",
            hint: "If custom rules have no match, also evaluate built‑in standard patterns.", group: "Execution"
          },
          {
            name: "max_rules_to_apply", label: "Max rules to apply",
            type: "integer", default: 100, optional: true,
            hint: "Guardrail for pathological rule sets.", group: "Advanced"
          }
        ]

        # Show selected table id as read-only context when relevant
        if (config["rules_source"] || "standard").to_s == "custom"
          fields << {
            name: "selected_rules_table_id",
            label: "Selected rules table",
            type: "string", optional: true, sticky: true,
            hint: "From configuration above.",
            default: config["custom_rules_table_id"],
            control_type: "plain-text", # documented read-only
            support_pills: false,
            group: "Advanced"
          }
        end

        fields
      end,
      # OUTPUT
      output_fields: lambda do |object_definitions|
        [
          { name: "pattern_match", type: "boolean", control_type: "checkbox" },
          { name: "rule_source", type: "string" }, # "custom", "standard", or "none"
          { name: "selected_action", type: "string" },
          { name: "top_match", type: "object", properties: object_definitions["rules_row"] },
          { name: "matches", type: "array", of: "object", properties: object_definitions["rules_row"] },
          { name: "standard_signals", type: "object", properties: object_definitions["standard_signals"] },
          {
            name: "debug", type: "object", properties: [
              { name: "evaluated_rules_count", type: "integer" },
              { name: "schema_validated", type: "boolean", control_type: "checkbox" },
              { name: "errors", type: "array", of: "string" }
            ]
          }
        ]
      end,
      # SAMPLE
      sample_output: lambda do
        {
          "pattern_match" => true,
          "rule_source" => "custom",
          "selected_action" => "archive",
          "top_match" => { "rule_id" => "R-1", "rule_type" => "subject", "rule_pattern" => "receipt", "action" => "archive", "priority" => 10, "field_matched" => "subject", "sample" => "Receipt #12345" },
          "matches" => [],
          "standard_signals" => { "sender_flags" => ["no[-_.]?reply"], "subject_flags" => ["\\breceipt\\b"], "body_flags" => [] },
          "debug" => { "evaluated_rules_count" => 25, "schema_validated" => true, "errors" => [] }
        }
      end,
      # EXECUTE
      execute: lambda do |connection, input, _eis, _eos, config|
        local = call(:deep_copy, input)
        result = call(:evaluate_email_by_rules_exec, connection, local, config)
        call(:validate_contract, connection, result, 'classification_result')
      end

    },
    # --- 12. Prepare for AI ------------------------------------------------
    prepare_for_ai: {
      title: "Prepare text for AI processing",
      subtitle: "Clean and format text with metadata for AI workflows",
      description: "Process text based on source type, apply cleaning rules, and return contract-compliant output",
      help: lambda do
        { body: "Prepares text for AI processing by applying source-specific cleaning rules. Supports email, document, chat, and general text sources. Returns data in cleaned_text contract format for inter-connector compatibility." }
      end,

      input_fields: lambda do |object_definitions|
        [
          {
            name: "text", label: "Text to process", type: "string", control_type: "text-area",
            optional: false, hint: "Raw text content to be processed for AI"
          },
          {
            name: "source_type", label: "Source type", control_type: "select",
            pick_list: [
              ["Email", "email"],
              ["Document", "document"],
              ["Chat", "chat"],
              ["General", "general"]
            ],
            optional: false, sticky: true, support_pills: false,
            hint: "Type of content being processed"
          },
          {
            name: "task_type", label: "Task type", control_type: "select",
            pick_list: [
              ["Classification", "classification"],
              ["Generation", "generation"],
              ["Analysis", "analysis"],
              ["Embedding", "embedding"]
            ],
            optional: false, sticky: true, support_pills: false,
            hint: "AI task the text will be used for"
          },
          {
            name: "options", label: "Processing options", type: "object", optional: true,
            properties: [
              { name: "remove_pii", label: "Remove PII", type: "boolean", control_type: "checkbox", default: false },
              { name: "max_length", label: "Max length", type: "integer", hint: "Maximum characters to retain" },
              { name: "remove_quotes", label: "Remove email quotes", type: "boolean", control_type: "checkbox", default: true },
              { name: "remove_signatures", label: "Remove signatures", type: "boolean", control_type: "checkbox", default: true },
              { name: "remove_disclaimers", label: "Remove disclaimers", type: "boolean", control_type: "checkbox", default: true },
              { name: "extract_urls", label: "Extract URLs", type: "boolean", control_type: "checkbox", default: false },
              { name: "normalize_whitespace", label: "Normalize whitespace", type: "boolean", control_type: "checkbox", default: true }
            ]
          }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: "text", type: "string" },
          { name: "removed_sections", type: "array", of: "string" },
          { name: "word_count", type: "integer" },
          { name: "cleaning_applied", type: "object", properties: [
            { name: "source_type", type: "string" },
            { name: "task_type", type: "string" },
            { name: "operations", type: "array", of: "string" },
            { name: "original_length", type: "integer" },
            { name: "final_length", type: "integer" },
            { name: "reduction_percentage", type: "number" }
          ]},
          { name: "metadata", type: "object", properties: [
            { name: "source_type", type: "string" },
            { name: "task_type", type: "string" },
            { name: "processing_timestamp", type: "string" },
            { name: "extracted_urls", type: "array", of: "string" }
          ]}
        ]
      end,

      sample_output: lambda do
        {
          "text" => "Hello team, I need help with the project analysis...",
          "removed_sections" => ["--\nJohn Doe\nSenior Analyst"],
          "word_count" => 12,
          "cleaning_applied" => {
            "source_type" => "email",
            "task_type" => "classification",
            "operations" => ["remove_signatures", "normalize_whitespace"],
            "original_length" => 150,
            "final_length" => 65,
            "reduction_percentage" => 56.67
          },
          "metadata" => {
            "source_type" => "email",
            "task_type" => "classification",
            "processing_timestamp" => "2024-01-15T10:30:00Z",
            "extracted_urls" => []
          }
        }
      end,

      execute: lambda do |connection, input, _eis, _eos, _config|
        call(:prepare_text_for_ai_exec, connection, input)
      end
    },
    # --- 13. Adapt chunks for Vertex ---------------------------------------
    adapt_chunks_for_vertex: {
      title: "Adapt chunks for Vertex",
      subtitle: "Map chunk objects → {id, text, metadata}",
      description: "Generate stable IDs and merge base metadata.",
      input_fields: lambda do |object_definitions|
        [
          { name: "chunks", type: "array", of: "object", list_mode_toggle: true, optional: false,
            properties: object_definitions["chunk_object"] },
          { name: "id_strategy", control_type: "select", default: "docid_index", sticky: true,
            pick_list: [[ "Use chunk_id", "pass_through" ],
                        [ "Prefix + index", "prefix_index" ],
                        [ "Document ID/Hash + index", "docid_index" ]] },
          { name: "id_prefix", hint: "Used when Prefix + index" },
          { name: "document_id", hint: "Used when Document ID/Hash + index (e.g. from Generate document metadata)" },
          { name: "base_metadata", type: "object", label: "Base metadata merged into each chunk" }
        ]
      end,
      output_fields: lambda do
        [
          { name: "records", type: "array", of: "object", properties: [
            { name: "id" }, { name: "text" }, { name: "metadata", type: "object" }
          ]},
          { name: "count", type: "integer" }
        ]
      end,
      execute: lambda do |_connection, input|
        chunks    = Array(input['chunks'])
        strategy  = (input['id_strategy'] || 'docid_index').to_s
        base_meta = input['base_metadata'] || {}

        recs = chunks.each_with_index.map do |c, idx|
          id =
            case strategy
            when 'pass_through' then (c['chunk_id'].presence || "chunk_#{idx}")
            when 'prefix_index' then [input['id_prefix'], idx].compact.join('_')
            else                      [input['document_id'], idx].compact.join('_')
            end

          meta = (c['metadata'] || {}).merge(base_meta)
                  .merge('chunk_index' => c['chunk_index'], 'chunk_id' => (c['chunk_id'] || id))

          { 'id' => id, 'text' => c['text'].to_s, 'metadata' => meta }
        end

        { 'records' => recs, 'count' => recs.length }
      end
    },
    # --- 14. Serialize chunks to JSONL -------------------------------------
    serialize_chunks_to_jsonl: {
      title: "Serialize chunks (JSONL)",
      subtitle: "Emit JSONL with id/text/metadata",
      input_fields: lambda do |object_definitions|
        [
          { name: "chunks", type: "array", of: "object", list_mode_toggle: true, optional: false,
            properties: object_definitions["chunk_object"] },
          { name: "include_metadata", type: "boolean", control_type: "checkbox", default: true },
          { name: "id_field_name", default: "id" },
          { name: "id_strategy", control_type: "select", default: "docid_index", sticky: true,
            pick_list: [[ "Use chunk_id", "pass_through" ],
                        [ "Prefix + index", "prefix_index" ],
                        [ "Document ID/Hash + index", "docid_index" ]] },
          { name: "id_prefix" }, { name: "document_id" }
        ]
      end,
      output_fields: lambda do
        [ { name: "jsonl", type: "string" }, { name: "lines", type: "integer" } ]
      end,
      execute: lambda do |_connection, input|
        chunks = Array(input['chunks'])
        strat  = (input['id_strategy'] || 'docid_index').to_s
        name   = (input['id_field_name'] || 'id').to_s

        lines = chunks.each_with_index.map do |c, idx|
          id =
            case strat
            when 'pass_through' then (c['chunk_id'].presence || "chunk_#{idx}")
            when 'prefix_index' then [input['id_prefix'], idx].compact.join('_')
            else                      [input['document_id'], idx].compact.join('_')
            end
          row = { name => id, 'text' => c['text'].to_s }
          row['metadata'] = c['metadata'] if input['include_metadata']
          JSON.generate(row)
        end

        { 'jsonl' => lines.join("\n"), 'lines' => lines.length }
      end
    },
    # --- 15. Resolve project context for a recipe --------------------------
    resolve_project_context: {
      title: "Resolve project context from recipe",
      subtitle: "Recipe → folder → owning project (environment-aware)",
      description: "Given a Recipe ID, returns its folder and the project it belongs to.",
      input_fields: lambda do
        [
          { name: "recipe_id", optional: false,
            hint: "Map the Recipe ID datapill or enter an ID manually" }
        ]
      end,
      output_fields: lambda do
        [
          { name: "recipe_id" },
          { name: "folder_id" },
          { name: "folder_name" },
          { name: "is_project_folder", type: "boolean" },
          { name: "project_id" },
          { name: "project_name" },
          { name: "project_folder_id" },
          { name: "environment_host", label: "Environment host" }
        ]
      end,
      execute: lambda do |connection, input|
        error("API token is required for Developer API calls") if connection['api_token'].blank?
        call(:resolve_project_from_recipe, connection, input["recipe_id"])
      end
    },
    # --- 16. Document Processing for RAG Pipeline --------------------------
    process_document_for_rag: {
      title: 'Process document for RAG',
      subtitle: 'Complete document processing pipeline for RAG indexing',
      description: lambda do |input|
        file_name = input.dig('file_metadata', 'file_name') || 'document'
        chunk_size = input['chunk_size'] || 1000
        "Process #{file_name} into RAG-ready chunks (#{chunk_size} chars each)"
      end,

      help: {
        body: 'This action provides a complete document processing pipeline for RAG (Retrieval-Augmented Generation). ' \
              'It takes raw document content and metadata, generates a stable document ID, chunks the text with smart boundaries, ' \
              'and produces enhanced chunks with merged metadata ready for embedding and indexing.'
      },

      input_fields: lambda do |object_definitions|
        [
          { name: 'document_content',     label: 'Document content',    type: 'string',   optional: false,
            hint: 'Raw text content of the document to be processed' },
          { name: 'file_metadata',        label: 'File metadata',       type: 'object',   optional: false,
            properties: [
              { name: 'file_id', label: 'File ID', type: 'string', optional: false },
              { name: 'file_name', label: 'File name', type: 'string', optional: false },
              { name: 'checksum', label: 'Checksum', type: 'string', optional: true },
              { name: 'mime_type', label: 'MIME type', type: 'string', optional: true },
              { name: 'size', label: 'File size', type: 'integer', optional: true },
              { name: 'modified_time', label: 'Modified time', type: 'date_time', optional: true } ],
            hint: 'File metadata object containing file_id, file_name, and optional checksum/mime_type' },
          { name: 'chunk_size',           label: 'Chunk size',          type: 'integer',  optional: true,
            default: 1000, hint: 'Target size for each text chunk in characters (default: 1000)' },
          { name: 'chunk_overlap',        label: 'Chunk overlap',       type: 'integer',  optional: true,
            default: 100, hint: 'Number of characters to overlap between chunks (default: 100)' },
          { name: 'additional_metadata',  label: 'Additional metadata', type: 'object',   optional: true,
            hint: 'Additional metadata to include with each chunk' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'document_id',          label: 'Document ID',         type: 'string',
            hint: 'Unique document identifier generated from file path and checksum' },
          { name: 'chunks',               label: 'Enhanced chunks',     type: 'array', of: 'object',
            properties: [
              { name: 'chunk_id', label: 'Chunk ID', type: 'string' },
              { name: 'text', label: 'Chunk text', type: 'string' },
              { name: 'chunk_index', label: 'Chunk index', type: 'integer' },
              { name: 'start_position', label: 'Start position', type: 'integer' },
              { name: 'end_position', label: 'End position', type: 'integer' },
              { name: 'character_count', label: 'Character count', type: 'integer' },
              { name: 'word_count', label: 'Word count', type: 'integer' },
              { name: 'document_id', label: 'Document ID', type: 'string' },
              { name: 'file_name', label: 'File name', type: 'string' },
              { name: 'file_id', label: 'File ID', type: 'string' },
              { name: 'source', label: 'Source', type: 'string' },
              { name: 'indexed_at', label: 'Indexed at', type: 'string' } ],
            hint: 'Array of chunks with enhanced metadata ready for embedding' },
          { name: 'document_metadata',    label: 'Document metadata',   type: 'object',
            properties: [
              { name: 'total_chunks', label: 'Total chunks', type: 'integer' },
              { name: 'total_characters', label: 'Total characters', type: 'integer' },
              { name: 'total_words', label: 'Total words', type: 'integer' },
              { name: 'processing_timestamp', label: 'Processing timestamp', type: 'string' },
              { name: 'chunk_size_used', label: 'Chunk size used', type: 'integer' },
              { name: 'overlap_used', label: 'Overlap used', type: 'integer' } ],
            hint: 'Summary metadata about the document processing' },
          { name: 'ready_for_embedding',  label: 'Ready for embedding', type: 'boolean',
            hint: 'True when document processing is complete and ready for embedding generation' } ]
      end,

      execute: lambda do |connection, input|
        # Step 1: Extract and validate inputs
        document_content = input['document_content'].to_s
        file_metadata = input['file_metadata'] || {}
        chunk_size = [input.fetch('chunk_size', 1000), 100].max  # Minimum 100 chars
        chunk_overlap = [input.fetch('chunk_overlap', 100), 0].max
        additional_metadata = input['additional_metadata'] || {}

        # Validate required fields
        error('Document content is required') if document_content.empty?
        error('file_metadata.file_id is required') if file_metadata['file_id'].blank?
        error('file_metadata.file_name is required') if file_metadata['file_name'].blank?

        # Step 2: Generate document ID using helper
        file_path = file_metadata['file_name']
        checksum = file_metadata['checksum'] || 'no_checksum'
        document_id = call('generate_document_id', file_path, checksum)

        # Step 3: Call chunk_text_with_overlap action
        chunk_input = {
          'text' => document_content,
          'chunk_size' => chunk_size,
          'chunk_overlap' => (chunk_overlap / 4.0).ceil  # Convert chars to approximate tokens
        }

        chunk_result = call('chunk_text_with_overlap', chunk_input)
        base_chunks = chunk_result['chunks'] || []

        # Step 4: Process each chunk and enhance with metadata
        enhanced_chunks = []
        base_chunks.each_with_index do |chunk, index|
          # Generate chunk ID
          chunk_id = "#{document_id}_chunk_#{index}"

          # Prepare chunk metadata
          chunk_metadata = {
            'chunk_id' => chunk_id,
            'chunk_index' => index,
            'start_position' => chunk['start_position'],
            'end_position' => chunk['end_position'],
            'character_count' => chunk['character_count'],
            'word_count' => chunk['word_count']
          }

          # Merge with additional metadata if provided
          chunk_metadata.merge!(additional_metadata) if additional_metadata.is_a?(Hash)

          # Use helper to merge document metadata
          merge_options = {
            document_id: document_id,
            file_name: file_metadata['file_name'],
            file_id: file_metadata['file_id']
          }

          enhanced_metadata = call('merge_document_metadata', chunk_metadata, file_metadata, merge_options)

          # Build enhanced chunk
          enhanced_chunk = {
            'chunk_id' => chunk_id,
            'text' => chunk['text'],
            'chunk_index' => index,
            'start_position' => chunk['start_position'],
            'end_position' => chunk['end_position'],
            'character_count' => chunk['character_count'],
            'word_count' => chunk['word_count'],
            'document_id' => document_id,
            'file_name' => file_metadata['file_name'],
            'file_id' => file_metadata['file_id'],
            'source' => enhanced_metadata['source'],
            'indexed_at' => enhanced_metadata['indexed_at']
          }

          # Add any additional metadata fields
          enhanced_metadata.each do |key, value|
            unless enhanced_chunk.key?(key)
              enhanced_chunk[key] = value
            end
          end

          enhanced_chunks << enhanced_chunk
        end

        # Step 5: Generate document metadata
        processing_timestamp = Time.now.iso8601
        total_characters = document_content.length
        total_words = document_content.split(/\s+/).length

        document_metadata = {
          'total_chunks' => enhanced_chunks.length,
          'total_characters' => total_characters,
          'total_words' => total_words,
          'processing_timestamp' => processing_timestamp,
          'chunk_size_used' => chunk_size,
          'overlap_used' => chunk_overlap
        }

        # Step 6: Build final response
        {
          'document_id' => document_id,
          'chunks' => enhanced_chunks,
          'document_metadata' => document_metadata,
          'ready_for_embedding' => true
        }
      end
    },
    # --- 17. Prepare document batch ----------------------------------------
    prepare_document_batch: {
      title: 'Prepare document batch for RAG',
      subtitle: 'Process multiple documents and group chunks into batches',
      description: lambda do |input|
        documents = input['documents'] || []
        batch_size = input['batch_size'] || 25
        count = documents.length
        if count > 0
          "Process #{count} documents and group chunks into batches of #{batch_size}"
        else
          'Process multiple documents for RAG indexing'
        end
      end,

      help: {
        body: 'This action processes multiple documents through the RAG pipeline and groups their chunks into batches. ' \
              'Each document is processed using process_document_for_rag, then all chunks are organized into ' \
              'manageable batches for embedding generation. Provides comprehensive metrics and batch tracking.'
      },

      input_fields: lambda do |object_definitions|
        [
          { name: 'documents',              label: 'Documents',             type: 'array', of: 'object',
            properties: [
              { name: 'document_content',     label: 'Document content',  type: 'string', optional: false },
              { name: 'file_metadata',        label: 'File metadata',     type: 'object', optional: false,
                properties: [
                  { name: 'file_id', label: 'File ID', type: 'string', optional: false },
                  { name: 'file_name', label: 'File name', type: 'string', optional: false },
                  { name: 'checksum', label: 'Checksum', type: 'string', optional: true },
                  { name: 'mime_type', label: 'MIME type', type: 'string', optional: true },
                  { name: 'size', label: 'File size', type: 'integer', optional: true },
                  { name: 'modified_time', label: 'Modified time', type: 'date_time', optional: true } ] },
              { name: 'chunk_size',           label: 'Chunk size', type: 'integer', optional: true },
              { name: 'chunk_overlap',        label: 'Chunk overlap',       type: 'integer', optional: true },
              { name: 'additional_metadata',  label: 'Additional metadata', type: 'object', optional: true } ],
             optional: false, hint: 'Array of documents to process, each with content and metadata' },
          { name: 'batch_size',             label: 'Batch size',            type: 'integer', 
            optional: true, default: 25, hint: 'Number of chunks per batch (default: 25, max: 100)' },
          { name: 'default_chunk_size',     label: 'Default chunk size',    type: 'integer', 
            optional: true, default: 1000, hint: 'Default chunk size for documents that don\'t specify one' },
          { name: 'default_chunk_overlap',  label: 'Default chunk overlap', type: 'integer',
            optional: true, default: 100, hint: 'Default chunk overlap for documents that don\'t specify one' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'batches',          label: 'Chunk batches',       type: 'array', of: 'object',
            properties: [
              { name: 'batch_id',       label: 'Batch ID',        type: 'string' },
              { name: 'chunks',         label: 'Chunks',          type: 'array', of: 'object',
                properties: [
                  { name: 'chunk_id',     label: 'Chunk ID',    type: 'string' },
                  { name: 'text',         label: 'Chunk text',  type: 'string' },
                  { name: 'chunk_index',  label: 'Chunk index', type: 'integer' },
                  { name: 'document_id',  label: 'Document ID', type: 'string' },
                  { name: 'file_name',    label: 'File name',   type: 'string' },
                  { name: 'file_id',      label: 'File ID',     type: 'string' },
                  { name: 'source',       label: 'Source',      type: 'string' },
                  { name: 'indexed_at',   label: 'Indexed at',  type: 'string' } ]
              },
              { name: 'document_count', label: 'Document count',  type: 'integer' },
              { name: 'chunk_count',    label: 'Chunk count',     type: 'integer' },
              { name: 'batch_index',    label: 'Batch index',     type: 'integer' } ],
            hint: 'Array of batches, each containing chunks grouped for processing' },
          { name: 'summary',          label: 'Processing summary',  type: 'object',
            properties: [
              { name: 'total_documents',      label: 'Total documents',       type: 'integer' },
              { name: 'total_chunks',         label: 'Total chunks',          type: 'integer' },
              { name: 'total_batches',        label: 'Total batches',         type: 'integer' },
              { name: 'processing_timestamp', label: 'Processing timestamp',  type: 'string' },
              { name: 'successful_documents', label: 'Successful documents',  type: 'integer' },
              { name: 'failed_documents',     label: 'Failed documents',      type: 'integer' } ],
            hint: 'Summary statistics about the batch processing operation' },
          { name: 'failed_documents', label: 'Failed documents',    type: 'array', of: 'object',
            properties: [
              { name: 'file_name',      label: 'File name',     type: 'string' },
              { name: 'file_id',        label: 'File ID',       type: 'string' },
              { name: 'error_message',  label: 'Error message', type: 'string' } ],
            hint: 'Array of documents that failed to process with error details' }
        ]
      end,

      execute: lambda do |connection, input|
        start_time = Time.now
        timestamp = start_time.strftime('%Y%m%d_%H%M%S')

        # Step 1: Extract and validate inputs
        documents = input['documents'] || []
        batch_size = [input.fetch('batch_size', 25), 100].min  # Max 100 chunks per batch
        default_chunk_size = input.fetch('default_chunk_size', 1000)
        default_chunk_overlap = input.fetch('default_chunk_overlap', 100)

        return {
          'batches' => [],
          'summary' => {
            'total_documents' => 0,
            'total_chunks' => 0,
            'total_batches' => 0,
            'processing_timestamp' => start_time.iso8601,
            'successful_documents' => 0,
            'failed_documents' => 0
          },
          'failed_documents' => []
        } if documents.empty?

        # Step 2: Process each document and collect all chunks
        all_chunks = []
        successful_documents = 0
        failed_documents = []

        documents.each do |document|
          begin
            # Prepare input for process_document_for_rag
            process_input = {
              'document_content'    => document['document_content'],
              'file_metadata'       => document['file_metadata'],
              'chunk_size'          => document['chunk_size'] || default_chunk_size,
              'chunk_overlap'       => document['chunk_overlap'] || default_chunk_overlap,
              'additional_metadata' => document['additional_metadata']
            }

            # Process document using the same logic as process_document_for_rag
            document_content    = process_input['document_content'].to_s
            file_metadata       = process_input['file_metadata'] || {}
            chunk_size          = [process_input.fetch('chunk_size', 1000), 100].max
            chunk_overlap       = [process_input.fetch('chunk_overlap', 100), 0].max
            additional_metadata = process_input['additional_metadata'] || {}

            # Validate required fields
            next if document_content.empty? || file_metadata['file_id'].blank? || file_metadata['file_name'].blank?

            # Generate document ID
            file_path   = file_metadata['file_name']
            checksum    = file_metadata['checksum'] || 'no_checksum'
            document_id = call('generate_document_id', file_path, checksum)

            # Chunk the text
            chunk_input = {
              'text'          => document_content,
              'chunk_size'    => chunk_size,
              'chunk_overlap' => (chunk_overlap / 4.0).ceil
            }

            chunk_result  = call('chunk_text_with_overlap', chunk_input)
            base_chunks   = chunk_result['chunks'] || []

            # Process each chunk
            enhanced_chunks = []
            base_chunks.each_with_index do |chunk, index|
              chunk_id = "#{document_id}_chunk_#{index}"

              chunk_metadata = {
                'chunk_id'        => chunk_id,
                'chunk_index'     => index,
                'start_position'  => chunk['start_position'],
                'end_position'    => chunk['end_position'],
                'character_count' => chunk['character_count'],
                'word_count'      => chunk['word_count']
              }

              chunk_metadata.merge!(additional_metadata) if additional_metadata.is_a?(Hash)

              merge_options = {
                document_id:  document_id,
                file_name:    file_metadata['file_name'],
                file_id:      file_metadata['file_id']
              }

              enhanced_metadata = call('merge_document_metadata', chunk_metadata, file_metadata, merge_options)

              enhanced_chunk = {
                'chunk_id'        => chunk_id,
                'text'            => chunk['text'],
                'chunk_index'     => index,
                'start_position'  => chunk['start_position'],
                'end_position'    => chunk['end_position'],
                'character_count' => chunk['character_count'],
                'word_count'      => chunk['word_count'],
                'document_id'     => document_id,
                'file_name'       => file_metadata['file_name'],
                'file_id'         => file_metadata['file_id'],
                'source'          => enhanced_metadata['source'],
                'indexed_at'      => enhanced_metadata['indexed_at']
              }

              enhanced_metadata.each do |key, value|
                unless enhanced_chunk.key?(key)
                  enhanced_chunk[key] = value
                end
              end

              enhanced_chunks << enhanced_chunk
            end

            result = { 'chunks' => enhanced_chunks }

            # Collect chunks from this document
            document_chunks = result['chunks'] || []
            all_chunks.concat(document_chunks)
            successful_documents += 1

          rescue => e
            # Track failed documents
            file_metadata = document['file_metadata'] || {}
            failed_documents << {
              'file_name'     => file_metadata['file_name'] || 'unknown',
              'file_id'       => file_metadata['file_id'] || 'unknown',
              'error_message' => e.message
            }
          end
        end

        # Step 3: Group chunks into batches
        batches = []
        batch_index = 0

        all_chunks.each_slice(batch_size) do |chunk_group|
          # Generate batch ID with timestamp and index
          batch_id = "batch_#{timestamp}_#{batch_index}"

          # Count unique document IDs in this batch
          document_ids = chunk_group.map { |chunk| chunk['document_id'] }.uniq
          document_count = document_ids.length

          # Create batch object
          batch = {
            'batch_id'        => batch_id,
            'chunks'          => chunk_group,
            'document_count'  => document_count,
            'chunk_count'     => chunk_group.length,
            'batch_index'     => batch_index
          }

          batches << batch
          batch_index += 1
        end

        # Step 4: Generate summary
        processing_timestamp = Time.now.iso8601
        summary = {
          'total_documents'       => documents.length,
          'total_chunks'          => all_chunks.length,
          'total_batches'         => batches.length,
          'processing_timestamp'  => processing_timestamp,
          'successful_documents'  => successful_documents,
          'failed_documents'      => failed_documents.length
        }

        # Step 5: Build final response
        {
          'batches'           => batches,
          'summary'           => summary,
          'failed_documents'  => failed_documents
        }
      end
    },
    # --- 18. Chunk GCS batch for embedding ---------------------------------
    chunk_gcs_batch_for_embedding: {
      title: "Chunk GCS batch for embedding",
      subtitle: "Input = gcs_batch_fetch_objects.successful_objects → chunks + (optional) embedding batches",
      description: "Aligns chunking with GCS batch fetch output. Produces chunk objects and optionally ready-to-send embedding request batches.",
      help: lambda do
        { body: "Map the “successful_objects” array from Drive Utilities → GCS: Batch fetch objects. " \
                "This action chunks each object’s text_content using your smart chunker and (optionally) emits embedding batches."}
      end,

      # Let the builder choose chunking mode and whether to emit embedding batches
      config_fields: [
        {
          name: "configuration_mode",
          label: "Configuration mode",
          control_type: "select",
          default: "defaults",
          sticky: true,
          options: [
            ["Use connection defaults (chunk size/overlap)", "defaults"],
            ["Custom (override chunking settings)", "custom"]
          ],
          hint: "Defaults come from RAG Utilities connection."
        },
        {
          name: "produce_embedding_batches",
          label: "Also build embedding batches",
          type: "boolean",
          control_type: "checkbox",
          default: true,
          sticky: true,
          hint: "When checked, builds {batches → requests[]} using your Prepare Embedding Batch contract."
        }
      ],

      input_fields: lambda do |object_definitions, connection, config|
        fields = [
          {
            name: "objects",
            label: "GCS successful objects",
            type: "array", of: "object", list_mode_toggle: true, optional: false,
            group: "Source: GCS",
            hint: "Map Drive Utilities → GCS: Batch fetch objects → successful_objects",
            properties: [
              { name: "bucket" }, { name: "name" }, { name: "size", type: "integer" },
              { name: "content_type" }, { name: "updated" }, { name: "generation" },
              { name: "md5_hash" }, { name: "crc32c" }, { name: "metadata", type: "object" },
              { name: "text_content", type: "string" }, { name: "needs_processing", type: "boolean" },
              { name: "fetch_method" }
            ]
          },
          { name: "skip_empty_text", label: "Skip objects with empty text_content", type: "boolean", control_type: "checkbox", default: true, group: "Source: GCS" },
          { name: "base_metadata", label: "Base metadata to merge into each chunk", type: "object", optional: true, group: "Source: GCS" },

          # Chunking
          { name: "preserve_sentences", label: "Preserve sentences", type: "boolean", control_type: "checkbox", default: true, group: "Chunking", ngIf: 'input.configuration_mode == "custom"' },
          { name: "preserve_paragraphs", label: "Preserve paragraphs", type: "boolean", control_type: "checkbox", default: false, group: "Chunking", ngIf: 'input.configuration_mode == "custom"' },
          { name: "chunk_size", label: "Chunk size (tokens)", type: "integer", default: (connection["chunk_size_default"] || 1000), convert_input: "integer_conversion", group: "Chunking", ngIf: 'input.configuration_mode == "custom"' },
          { name: "chunk_overlap", label: "Chunk overlap (tokens)", type: "integer", default: (connection["chunk_overlap_default"] || 100), convert_input: "integer_conversion", group: "Chunking", ngIf: 'input.configuration_mode == "custom"' },

          # Embedding batches (only used when produce_embedding_batches = true)
          { name: "task_type", label: "Embedding task type", control_type: "select",
            pick_list: [
              ["Retrieval Document", "RETRIEVAL_DOCUMENT"],
              ["Query", "QUERY"],
              ["Semantic Similarity", "SEMANTIC_SIMILARITY"]
            ],
            default: "RETRIEVAL_DOCUMENT", sticky: true, group: "Embedding batches", ngIf: 'input.produce_embedding_batches == true' },
          { name: "batch_size", label: "Embedding batch size", type: "integer", default: 25, convert_input: "integer_conversion", group: "Embedding batches", ngIf: 'input.produce_embedding_batches == true' },
          { name: "include_title_in_text", label: "Include object name as title", type: "boolean", control_type: "checkbox", default: true, group: "Embedding batches", ngIf: 'input.produce_embedding_batches == true' },
          { name: "batch_prefix", label: "Batch ID prefix", type: "string", default: "emb_batch", group: "Embedding batches", ngIf: 'input.produce_embedding_batches == true' }
        ]
        fields
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: "chunks", type: "array", of: "object", properties: object_definitions["chunk_object"] },
          { name: "chunk_count", type: "integer" },
          { name: "objects_processed", type: "integer" },
          { name: "skipped_objects", type: "array", of: "object", properties: [
              { name: "bucket" }, { name: "name" }, { name: "reason" }
          ]},
          # Embedding batches (when produced)
          { name: "embedding", type: "object", properties: [
              { name: "batches", type: "array", of: "object", properties: [
                  { name: "batch_id" }, { name: "batch_number", type: "integer" },
                  { name: "requests", type: "array", of: "object", properties: [
                      { name: "text", type: "string" }, { name: "metadata", type: "object" }
                  ]},
                  { name: "size", type: "integer" }
              ]},
              { name: "total_batches", type: "integer" },
              { name: "total_texts", type: "integer" },
              { name: "task_type" },
              { name: "batch_generation_timestamp" }
          ]},
          { name: "telemetry", type: "object", properties: [
              { name: "success", type: "boolean" }, { name: "timestamp" },
              { name: "metadata", type: "object" },
              { name: "trace", type: "object", properties: [
                  { name: "correlation_id" }, { name: "duration_ms", type: "integer" }
              ]}
          ]}
        ]
      end,

      sample_output: lambda do
        {
          "chunks" => [
            { "chunk_id" => "doc_abc_0", "chunk_index" => 0, "text" => "…", "token_count" => 250,
              "start_char" => 0, "end_char" => 1000,
              "metadata" => { "document_id" => "doc_abc", "bucket" => "my-bucket", "object_name" => "path/file.txt", "source" => "gcs" }
            }
          ],
          "chunk_count" => 1,
          "objects_processed" => 1,
          "skipped_objects" => [],
          "embedding" => {
            "batches" => [
              { "batch_id" => "emb_batch_0_20250101000000", "batch_number" => 0, "requests" => [
                  { "text" => "file.txt: …", "metadata" => { "id" => "doc_abc_0", "title" => "file.txt", "task_type" => "RETRIEVAL_DOCUMENT", "batch_id" => "emb_batch_0_20250101000000" } }
              ], "size" => 1 }
            ],
            "total_batches" => 1, "total_texts" => 1, "task_type" => "RETRIEVAL_DOCUMENT",
            "batch_generation_timestamp" => "2025-01-01T00:00:00Z"
          },
          "telemetry" => { "success" => true, "timestamp" => "2025-01-01T00:00:00Z", "metadata" => {}, "trace" => { "correlation_id" => "cid", "duration_ms" => 10 } }
        }
      end,

      execute: lambda do |connection, input, _eis, _eos, config|
        action_cid = call(:gen_correlation_id)
        started_at = Time.now
        local = call(:deep_copy, input)

        objs = Array(local['objects'] || [])
        error("No GCS objects provided (map gcs_batch_fetch_objects.successful_objects). cid=#{action_cid}") if objs.empty?

        # Chunking settings
        if (config['configuration_mode'] || 'defaults') != 'custom'
          local['chunk_size']        ||= (connection['chunk_size_default'] || 1000)
          local['chunk_overlap']     ||= (connection['chunk_overlap_default'] || 100)
          local['preserve_sentences'] = true  if local['preserve_sentences'].nil?
          local['preserve_paragraphs']= false if local['preserve_paragraphs'].nil?
        end

        cs = (local['chunk_size'] || 1000).to_i
        co = (local['chunk_overlap'] || 100).to_i
        error("Chunk size must be > 0. cid=#{action_cid}") if cs <= 0
        error("Chunk overlap must be >= 0. cid=#{action_cid}") if co < 0

        base_meta = local['base_metadata'].is_a?(Hash) ? local['base_metadata'] : {}
        skip_empty = local['skip_empty_text'] != false

        all_chunks = []
        skipped = []
        texts_for_embedding = []
        processed = 0

        objs.each do |o|
          text = (o['text_content'] || '').to_s
          if skip_empty && text.strip == ''
            skipped << { 'bucket' => o['bucket'], 'name' => o['name'], 'reason' => 'empty_text_content' }
            next
          end

          processed += 1
          # Stable document id using bucket/name + md5 (or generation)
          path = "#{o['bucket']}/#{o['name']}"
          checksum = o['md5_hash'].to_s == '' ? (o['generation'] || 'no_checksum') : o['md5_hash']
          doc_id = call(:generate_document_id, path, checksum)

          chunk_result = call(:chunk_text_with_overlap, {
            'text' => text,
            'chunk_size' => cs,
            'chunk_overlap' => co,
            'preserve_sentences' => !!local['preserve_sentences'],
            'preserve_paragraphs'=> !!local['preserve_paragraphs']
          })

          Array(chunk_result['chunks']).each do |c|
            idx  = c[:chunk_index] || c['chunk_index']
            cid  = "#{doc_id}_#{idx}"
            meta = (c[:metadata] || c['metadata'] || {}).dup
            meta.merge!({
              'document_id'  => doc_id,
              'bucket'       => o['bucket'],
              'object_name'  => o['name'],
              'content_type' => o['content_type'],
              'updated'      => o['updated'],
              'md5_hash'     => o['md5_hash'],
              'generation'   => o['generation'],
              'source'       => 'gcs'
            })
            base_meta.each { |k, v| meta[k.to_s] = v } unless base_meta.empty?

            chunk = {
              'chunk_id'    => cid,
              'chunk_index' => idx,
              'text'        => (c[:text] || c['text']).to_s,
              'token_count' => c[:token_count] || c['token_count'],
              'start_char'  => c[:start_char]  || c['start_char'],
              'end_char'    => c[:end_char]    || c['end_char'],
              'metadata'    => meta
            }
            all_chunks << chunk

            # Build embedding text object
            texts_for_embedding << {
              'id'       => cid,
              'content'  => chunk['text'],
              'title'    => o['name'].to_s,
              'metadata' => {
                'document_id' => doc_id,
                'chunk_index' => idx,
                'bucket'      => o['bucket'],
                'object_name' => o['name'],
                'source'      => 'gcs'
              }
            }
          end
        end

        embedding_out = nil
        if config['produce_embedding_batches'] != false
          embedding_out = call(:prepare_embedding_batch_exec, connection, {
            'texts' => texts_for_embedding,
            'task_type' => (local['task_type'] || 'RETRIEVAL_DOCUMENT'),
            'batch_size' => (local['batch_size'] || 25),
            'include_title_in_text' => (local['include_title_in_text'] != false),
            'batch_prefix' => (local['batch_prefix'] || 'emb_batch')
          })
        end

        {
          'chunks'            => all_chunks,
          'chunk_count'       => all_chunks.length,
          'objects_processed' => processed,
          'skipped_objects'   => skipped,
          'embedding'         => embedding_out,
          'telemetry'         => call(:telemetry_envelope,
                                      true,
                                      { action: 'chunk_gcs_batch_for_embedding',
                                        object_count: objs.length,
                                        processed: processed,
                                        chunks: all_chunks.length,
                                        embedding_batches: embedding_out ? embedding_out['total_batches'] : 0 },
                                      started_at,
                                      action_cid,
                                      true)
        }
      end
    },
    # --- 19. Compare tabular data and emit deltas --------------------------
    delta_compare_sheets_vs_datatable: {
      title: 'Delta: Sheets → Data Table',
      subtitle: 'Compare two lists of records by key and compute creates/updates/deletes',
      help: lambda do |_|
        { body: 'Provide a primary key and two arrays of records (from Google Sheets and from Data Tables). Returns creates, updates (with diffs), deletes, unchanged, and a summary.' }
      end,
      display_priority: 2,

      config_fields: [
        { name: 'primary_key', optional: false, hint: 'Field name used as the unique key (e.g., "id" or "email")' },
        { name: 'ignore_fields', optional: true, type: 'array', of: 'string',
          hint: 'Fields to ignore during comparison (e.g., ["updated_at","_rowNumber"])' },
        { name: 'case_insensitive_keys', type: 'boolean', control_type: 'checkbox', optional: true,
          hint: 'Normalize primary key compare using String#downcase' },
        { name: 'blank_as_nil', type: 'boolean', control_type: 'checkbox', optional: true,
          hint: 'Treat "", "  ", and nil as equivalent (nil) when comparing values' },
        { name: 'include_unchanged', type: 'boolean', control_type: 'checkbox', optional: true,
          hint: 'Include unchanged records in the output' }
      ],

      input_fields: lambda do |_|
        [
          { name: 'source_rows', label: 'Source rows (Google Sheets)', type: 'array', of: 'object', optional: false,
            hint: 'Typically map from Google Sheets → "List rows" or "Read range" output' },
          { name: 'target_rows', label: 'Target rows (Data Table)', type: 'array', of: 'object', optional: false,
            hint: 'Map from Data Tables → "List rows" or HTTP Data Tables API call' }
        ]
      end,

      output_fields: lambda do |_|
        [
          { name: 'summary', type: 'object', properties: [
            { name: 'primary_key' }, { name: 'source_count', type: 'integer' },
            { name: 'target_count', type: 'integer' }, { name: 'creates', type: 'integer' },
            { name: 'updates', type: 'integer' }, { name: 'deletes', type: 'integer' },
            { name: 'unchanged', type: 'integer' }
          ]},
          { name: 'creates', type: 'array', of: 'object' },
          { name: 'updates', type: 'array', of: 'object', properties: [
            { name: 'key' },
            { name: 'before', type: 'object' },
            { name: 'after',  type: 'object' },
            { name: 'diff',   type: 'array', of: 'object', properties: [
              { name: 'field' }, { name: 'before' }, { name: 'after' }
            ]}
          ]},
          { name: 'deletes', type: 'array', of: 'object' },
          { name: 'unchanged', type: 'array', of: 'object' }
        ]
      end,

      execute: lambda do |connection, input|
        # -------- helpers (pure functions) -----------------------------------
        deep_copy = lambda { |obj| JSON.parse(JSON.dump(obj)) }

        normalize_key = lambda do |v, case_insensitive|
          return nil if v.nil?
          return v.to_s.strip.downcase if case_insensitive && v.is_a?(String)
          v.is_a?(String) ? v.strip : v
        end

        blank_to_nil = lambda do |v|
          return nil if v.nil?
          return nil if v.is_a?(String) && v.strip == ''
          v
        end

        normalize_value = lambda do |v, blank_as_nil|
          v = blank_to_nil.call(v) if blank_as_nil
          # Coerce numbers that are stringified (common from Sheets)
          if v.is_a?(String)
            if v =~ /\A-?\d+\z/
              v = v.to_i
            elsif v =~ /\A-?\d+\.\d+\z/
              v = v.to_f
            end
          end
          v
        end

        # Compare two records excluding ignore_fields; returns diff list
        record_diff = lambda do |before, after, ignore_fields, opts|
          diffs = []
          all_fields = (before.keys + after.keys).uniq - ignore_fields
          all_fields.each do |f|
            b = normalize_value.call(before[f], opts[:blank_as_nil])
            a = normalize_value.call(after[f],  opts[:blank_as_nil])
            diffs << { 'field' => f, 'before' => b, 'after' => a } unless b == a
          end
          diffs
        end

        # -------- inputs & normalization -------------------------------------
        pk              = input['primary_key'] || raise('primary_key is required')
        ignore_fields   = Array(input['ignore_fields']).map(&:to_s)
        case_ins_keys   = !!input['case_insensitive_keys']
        blank_as_nil    = !!input['blank_as_nil']
        include_unch    = !!input['include_unchanged']

        src_rows = deep_copy.call(input['source_rows'] || [])
        tgt_rows = deep_copy.call(input['target_rows'] || [])

        # Hash index by primary key
        index_by = lambda do |rows|
          rows.each_with_object({}) do |r, h|
            key = normalize_key.call(r[pk], case_ins_keys)
            next if key.nil? # skip rows without key
            h[key] ||= r
          end
        end

        src_index = index_by.call(src_rows)
        tgt_index = index_by.call(tgt_rows)

        # -------- delta computation ------------------------------------------
        creates   = []
        updates   = []
        deletes   = []
        unchanged = []

        # Creates/Updates/Unchanged (iterate source of truth: Sheets)
        src_index.each do |k, src|
          if !tgt_index.key?(k)
            creates << src
          else
            tgt = tgt_index[k]
            diffs = record_diff.call(tgt, src, ignore_fields, { blank_as_nil: blank_as_nil })
            if diffs.empty?
              unchanged << src if include_unch
            else
              updates << { 'key' => k, 'before' => tgt, 'after' => src, 'diff' => diffs }
            end
          end
        end

        # Deletes (in target but not in source)
        (tgt_index.keys - src_index.keys).each do |k|
          deletes << tgt_index[k]
        end

        {
          summary: {
            primary_key: pk,
            source_count: src_rows.length,
            target_count: tgt_rows.length,
            creates: creates.length,
            updates: updates.length,
            deletes: deletes.length,
            unchanged: unchanged.length
          },
          creates: creates,
          updates: updates,
          deletes: deletes,
          unchanged: unchanged
        }
      end
    }

  },

  # --------- METHODS ------------------------------------------------------
  methods: {
    # ── Minimal observability/safety helpers (no Rails)
    gen_correlation_id: lambda do
      begin
        SecureRandom.uuid
      rescue NameError
        t = (Time.now.to_f * 1000).to_i.to_s(36)
        r = rand(36**8).to_s(36).rjust(8, '0')
        "#{t}-#{r}"
      end
    end,

    deep_copy: lambda do |obj|
      if obj.is_a?(Hash)
        obj.each_with_object({}) { |(k, v), h| h[k] = call(:deep_copy, v) }
      elsif obj.is_a?(Array)
        obj.map { |v| call(:deep_copy, v) }
      else
        obj
      end
    end,

    telemetry_envelope: lambda do |success, metadata, started_at, correlation_id, include_trace|
      duration_ms = ((Time.now - started_at) * 1000).round
      base = {
        'success'   => !!success,
        'timestamp' => Time.now.utc.iso8601,
        'metadata'  => metadata || {}
      }
      if include_trace != false
        base['trace'] = { 'correlation_id' => correlation_id, 'duration_ms' => duration_ms }
      end
      base
    end,

    chunk_text_with_overlap: lambda do |input|
      text = input['text'].to_s
      chunk_size = (input['chunk_size'] || 1000).to_i
      overlap    = (input['chunk_overlap'] || 100).to_i
      preserve_sentences  = !!input['preserve_sentences']
      preserve_paragraphs = !!input['preserve_paragraphs']

      chunk_size = 1 if chunk_size <= 0
      overlap = [[overlap, 0].max, [chunk_size - 1, 0].max].min

      # rough token->char estimate
      chars_per_chunk = [chunk_size, 1].max * 4
      char_overlap    = overlap * 4

      chunks = []
      chunk_index = 0
      position = 0
      text_len = text.length

      while position < text_len
        tentative_end = [position + chars_per_chunk, text_len].min
        chunk_end = tentative_end
        segment = text[position...tentative_end]

        if preserve_paragraphs && tentative_end < text_len
          rel_end = call(:util_last_boundary_end, segment, /\n{2,}/)
          chunk_end = position + rel_end if rel_end
        end

        if preserve_sentences && chunk_end == tentative_end && tentative_end < text_len
          rel_end = call(:util_last_boundary_end, segment, /[.!?]["')\]]?\s/)
          chunk_end = position + rel_end if rel_end
        end

        chunk_end = [position + [chars_per_chunk, 1].max, text_len].min if chunk_end <= position

        chunk_text = text[position...chunk_end]
        token_count = (chunk_text.length / 4.0).ceil

        chunks << {
          chunk_id:    "chunk_#{chunk_index}",
          chunk_index: chunk_index,
          text:        chunk_text,
          token_count: token_count,
          start_char:  position,
          end_char:    chunk_end,
          metadata:    { has_overlap: chunk_index.positive?, is_final: chunk_end >= text_len }
        }

        break if chunk_end >= text_len

        next_position = chunk_end - char_overlap
        position = next_position > position ? next_position : chunk_end
        chunk_index += 1
      end

      # Recipe-friendly enhancements
      first_chunk = chunks.first || {}
      total_tokens = chunks.sum { |c| c[:token_count] }
      average_chunk_size = chunks.any? ? (total_tokens.to_f / chunks.length).round : 0

      {
        chunks_count: chunks.length,
        chunks: chunks,
        first_chunk: first_chunk,
        chunks_json: chunks.to_json,
        total_chunks: chunks.length,
        total_tokens: total_tokens,
        average_chunk_size: average_chunk_size,
        pass_fail: chunks.any?,
        action_required: chunks.any? ? "ready_for_embedding" : "check_input_text"
      }
    end,

    process_email_text: lambda do |input|
      cleaned = (input['email_body'] || '').dup
      original_length = cleaned.length
      removed_sections = []
      extracted_urls = []

      cleaned.gsub!("\r\n", "\n")

      if input['remove_quotes']
        lines = cleaned.lines
        quoted = lines.select { |l| l.lstrip.start_with?('>') }
        removed_sections << quoted.join unless quoted.empty?
        lines.reject! { |l| l.lstrip.start_with?('>') }
        cleaned = lines.join
      end

      if input['remove_signatures']
        lines = cleaned.lines
        sig_idx = lines.rindex { |l| l =~ /^\s*(--\s*$|Best regards,|Regards,|Sincerely,|Thanks,|Sent from my)/i }
        if sig_idx
          removed_sections << lines[sig_idx..-1].join
          cleaned = lines[0...sig_idx].join
        end
      end

      if input['remove_disclaimers']
        lines = cleaned.lines
        disc_idx = lines.rindex { |l| l =~ /(This (e-)?mail|This message).*(confidential|intended only)/i }
        if disc_idx && disc_idx >= lines.length - 25
          removed_sections << lines[disc_idx..-1].join
          cleaned = lines[0...disc_idx].join
        end
      end

      if input['extract_urls']
        extracted_urls = cleaned.scan(%r{https?://[^\s<>"'()]+})
      end

      if input['normalize_whitespace']
        cleaned.gsub!(/[ \t]+/, ' ')
        cleaned.gsub!(/\n{3,}/, "\n\n")
        cleaned.strip!
      end

      extracted_query = cleaned.split(/\n{2,}/).find { |p| p.strip.length.positive? } || cleaned[0, 200].to_s

      # Recipe-friendly enhancements
      has_content = cleaned.strip.length > 10 # Meaningful content threshold
      cleaning_successful = cleaned.length > 0

      {
        cleaned_text: cleaned,
        extracted_query: extracted_query,
        removed_sections_count: removed_sections.length,
        removed_sections: removed_sections,
        urls_count: extracted_urls.length,
        extracted_urls: extracted_urls,
        original_length: original_length,
        cleaned_length: cleaned.length,
        reduction_percentage: (original_length.zero? ? 0 : ((1 - cleaned.length.to_f / original_length) * 100)).round(2),
        pass_fail: cleaning_successful,
        action_required: has_content ? "ready_for_chunking" : "check_email_content",
        has_content: has_content
      }
    end,

    compute_similarity: lambda do |input, connection|
      start_time = Time.now

      a = call(:util_coerce_numeric_vector, input['vector_a'])
      b = call(:util_coerce_numeric_vector, input['vector_b'])
      error('Vectors must be the same length.') unless a.length == b.length
      error('Vectors cannot be empty') if a.empty?

      normalize = input.key?('normalize') ? !!input['normalize'] : true
      type      = (input['similarity_type'] || 'cosine').to_s
      threshold = (connection['similarity_threshold'] || 0.7).to_f

      if normalize
        norm = ->(v) { mag = Math.sqrt(v.sum { |x| x * x }); mag.zero? ? v : v.map { |x| x / mag } }
        a = norm.call(a)
        b = norm.call(b)
      end

      dot   = a.zip(b).sum { |x, y| x * y }
      mag_a = Math.sqrt(a.sum { |x| x * x })
      mag_b = Math.sqrt(b.sum { |x| x * x })

      score =
        case type
        when 'cosine'
          (mag_a > 0 && mag_b > 0) ? dot / (mag_a * mag_b) : 0.0
        when 'euclidean'
          dist = Math.sqrt(a.zip(b).sum { |x, y| (x - y)**2 })
          1.0 / (1.0 + dist)
        when 'dot_product'
          dot
        else
          (mag_a > 0 && mag_b > 0) ? dot / (mag_a * mag_b) : 0.0
        end

      percent = %w[cosine euclidean].include?(type) ? (score * 100).round(2) : nil

      similar =
        case type
        when 'cosine', 'euclidean'
          score >= threshold
        when 'dot_product'
          if normalize
            score >= threshold
          else
            error('For dot_product without normalization, provide an absolute threshold appropriate to your embedding scale.')
          end
        end

      # Recipe-friendly enhancements
      confidence_level = case score
                        when 0.8..1.0 then 'high'
                        when 0.6..0.8 then 'medium'
                        else 'low'
                        end

      {
        similarity_score: score.round(6),
        similarity_percentage: percent,
        is_similar: similar,
        pass_fail: similar,
        action_required: similar ? "vectors_are_similar" : "vectors_are_different",
        confidence_level: confidence_level,
        similarity_type: type,
        computation_time_ms: ((Time.now - start_time) * 1000).round,
        threshold_used: threshold,
        vectors_normalized: normalize
      }
    end,

    prepare_embedding_batch_exec: lambda do |connection, input|
      texts = Array(input['texts'] || [])
      task_type = (input['task_type'] || 'RETRIEVAL_DOCUMENT').to_s
      batch_size = (input['batch_size'] || 25).to_i
      include_title = input['include_title_in_text'] != false
      batch_prefix = (input['batch_prefix'] || 'emb_batch').to_s

      # Generate timestamp for unique batch IDs
      timestamp = Time.now.utc.strftime('%Y%m%d%H%M%S')

      batches = []
      texts.each_slice(batch_size).with_index do |batch, index|
        batch_id = "#{batch_prefix}_#{index}_#{timestamp}"

        requests = batch.map do |text_obj|
          # Build the text content
          content = (text_obj['content'] || '').to_s
          title = (text_obj['title'] || '').to_s

          final_text = if include_title && !title.empty?
                         "#{title}: #{content}"
                       else
                         content
                       end

          # Build metadata according to embedding_request contract
          metadata = (text_obj['metadata'] || {}).dup
          metadata.merge!({
            'id' => text_obj['id'],
            'title' => title,
            'task_type' => task_type,
            'batch_id' => batch_id
          })

          # Create embedding request object
          request = {
            'text' => final_text,
            'metadata' => metadata
          }

          # Validate each request against embedding_request contract
          call(:validate_contract, connection, request, 'embedding_request')

          request
        end

        batch_info = {
          'batch_id' => batch_id,
          'batch_number' => index,
          'requests' => requests,
          'size' => requests.length
        }

        batches << batch_info
      end

      result = {
        'batches' => batches,
        'total_batches' => batches.length,
        'total_texts' => texts.length,
        'task_type' => task_type,
        'batch_generation_timestamp' => Time.now.utc.iso8601
      }

      result
    end,

    construct_rag_prompt: lambda do |input|
      query = input['query'].to_s
      context_docs = Array(input['context_documents'] || [])
      template_key = (input['prompt_template'] || 'standard').to_s
      max_length = (input['max_context_length'] || 3000).to_i
      include_metadata = !!input['include_metadata']
      system_instructions = input['system_instructions'].to_s
      template_content = input['template_content'].to_s

      sorted_context = context_docs.sort_by { |doc| (doc['relevance_score'] || 0) }.reverse

      context_parts = []
      total_tokens = 0
      sorted_context.each do |doc|
        content = doc['content'].to_s
        doc_tokens = (content.length / 4.0).ceil
        break if doc_tokens > max_length && context_parts.empty?
        next if total_tokens + doc_tokens > max_length

        part = content.dup
        part << "\nMetadata: #{JSON.generate(doc['metadata'])}" if include_metadata && doc['metadata']
        context_parts << part
        total_tokens += doc_tokens
      end
      context_text = context_parts.join("\n\n---\n\n")

      base =
        if template_content.strip.length.positive?
          template_content
        else
          case template_key
          when 'standard'
            "Context:\n{{context}}\n\nQuery: {{query}}\n\nAnswer:"
          when 'customer_service'
            "You are a customer service assistant.\n\nContext:\n{{context}}\n\nCustomer Question: {{query}}\n\nResponse:"
          when 'technical'
            "You are a technical support specialist.\n\nContext:\n{{context}}\n\nTechnical Issue: {{query}}\n\nSolution:"
          when 'sales'
            "You are a sales representative.\n\nContext:\n{{context}}\n\nSales Inquiry: {{query}}\n\nResponse:"
          else
            header = system_instructions.strip
            header = "Instructions:\n#{header}\n\n" if header.length.positive?
            "#{header}Context:\n{{context}}\n\nQuery: {{query}}\n\nAnswer:"
          end
        end

      compiled = base.dup
      compiled.gsub!(/{{\s*context\s*}}/i, context_text)
      compiled.gsub!(/{{\s*query\s*}}/i,   query)
      unless base.match?(/{{\s*context\s*}}/i) || base.match?(/{{\s*query\s*}}/i)
        compiled << "\n\nContext:\n#{context_text}\n\nQuery: #{query}\n\nAnswer:"
      end

      {
        formatted_prompt: compiled,
        token_count: (compiled.length / 4.0).ceil,
        context_used: context_parts.length,
        truncated: context_parts.length < sorted_context.length,
        prompt_metadata: (input['prompt_metadata'] || {}).merge(
          template: template_key,
          using_template_content: template_content.strip.length.positive?
        )
      }
    end,

    validate_response: lambda do |input, _connection|
      response = (input['response_text'] || '').to_s
      query    = (input['original_query'] || '').to_s
      rules    = Array(input['validation_rules'] || [])
      min_confidence = (input['min_confidence'] || 0.7).to_f

      issues = []
      confidence = 1.0

      if response.strip.empty?
        issues << 'Response is empty'
        confidence -= 0.5
      elsif response.length < 10
        issues << 'Response is too short'
        confidence -= 0.3
      end

      query_words = query.downcase.split(/\W+/).reject(&:empty?)
      response_words = response.downcase.split(/\W+/).reject(&:empty?)
      overlap = query_words.empty? ? 0.0 : ((query_words & response_words).length.to_f / query_words.length)

      if overlap < 0.1
        issues << 'Response may not address the query'
        confidence -= 0.4
      end

      if response.include?('...') || response.downcase.include?('incomplete')
        issues << 'Response appears incomplete'
        confidence -= 0.2
      end

      rules.each do |rule|
        case rule['rule_type']
        when 'contains'
          unless response.include?(rule['rule_value'].to_s)
            issues << "Response does not contain required text: #{rule['rule_value']}"
            confidence -= 0.3
          end
        when 'not_contains'
          if response.include?(rule['rule_value'].to_s)
            issues << "Response contains prohibited text: #{rule['rule_value']}"
            confidence -= 0.3
          end
        end
      end

      confidence = [[confidence, 0.0].max, 1.0].min

      # Recipe-friendly enhancements
      is_valid = confidence >= min_confidence
      confidence_level = case confidence
                        when 0.8..1.0 then 'high'
                        when 0.6..0.8 then 'medium'
                        else 'low'
                        end

      suggestions = issues.empty? ? [] : ['Review and improve response quality']

      {
        is_valid: is_valid,
        confidence_score: confidence.round(2),
        pass_fail: is_valid,
        action_required: is_valid ? "response_approved" : "response_needs_review",
        validation_results: { query_overlap: overlap.round(2), response_length: response.length, word_count: response_words.length },
        issues_count: issues.length,
        issues_found: issues,
        requires_human_review: confidence < 0.5,
        suggestions_count: suggestions.length,
        suggested_improvements: suggestions,
        confidence_level: confidence_level
      }
    end,

    extract_metadata: lambda do |input|
      content = input['document_content'].to_s
      file_path = input['file_path'].to_s
      extract_entities = input.key?('extract_entities') ? !!input['extract_entities'] : true
      generate_summary = input.key?('generate_summary') ? !!input['generate_summary'] : true

      start_time = Time.now

      word_count = content.split(/\s+/).reject(&:empty?).length
      char_count = content.length
      estimated_tokens = (content.length / 4.0).ceil

      language = content.match?(/[àáâãäåæçèéêëìíîïðñòóôõöøùúûüýþÿ]/i) ? 'non-english' : 'english'
      summary = generate_summary ? (content[0, 200].to_s + (content.length > 200 ? '...' : '')) : ''

      key_topics = []
      if extract_entities
        common = %w[the a an and or but in on at to for of with by from is are was were be been being this that these those there here then than into out over under after before about as it its it's their them they our we you your he she his her him not will would can could should may might must also just more most other some such]
        words = content.downcase.scan(/[a-z0-9\-]+/).reject { |w| w.length < 4 || common.include?(w) }
        freq = words.each_with_object(Hash.new(0)) { |w, h| h[w] += 1 }
        key_topics = freq.sort_by { |_, c| -c }.first(5).map(&:first)
      end

      file_hash = Digest::SHA256.hexdigest(content)
      document_id = Digest::SHA1.hexdigest("#{file_path}|#{file_hash}")

      {
        document_id: document_id,
        file_hash: file_hash,
        word_count: word_count,
        character_count: char_count,
        estimated_tokens: estimated_tokens,
        language: language,
        summary: summary,
        key_topics: key_topics,
        entities: { people: [], organizations: [], locations: [] },
        created_at: Time.now.iso8601,
        processing_time_ms: ((Time.now - start_time) * 1000).round
      }
    end,

    detect_changes: lambda do |input|
      current_hash     = input['current_hash']
      current_content  = input['current_content']
      previous_hash    = input['previous_hash']
      previous_content = input['previous_content']
      check_type       = (input['check_type'] || 'hash').to_s

      if check_type == 'smart' && current_content && previous_content
        diff = call(:util_diff_lines, current_content.to_s, previous_content.to_s)

        tokens_cur  = current_content.to_s.split(/\s+/)
        tokens_prev = previous_content.to_s.split(/\s+/)
        union = (tokens_cur | tokens_prev).length
        intersection = (tokens_cur & tokens_prev).length
        smart_change = union.zero? ? 0.0 : ((1.0 - intersection.to_f / union) * 100).round(2)

        changed = smart_change > 0.0 || diff[:added].any? || diff[:removed].any? || diff[:modified_sections].any?

        return {
          has_changed: changed,
          change_type: changed ? 'smart_changed' : 'none',
          change_percentage: smart_change,
          added_content: diff[:added],
          removed_content: diff[:removed],
          modified_sections: diff[:modified_sections],
          requires_reindexing: changed
        }
      end

      has_changed = current_hash != previous_hash
      change_type = 'none'
      change_percentage = 0.0
      added = []
      removed = []
      modified_sections = []

      if has_changed
        change_type = 'hash_changed'

        if check_type == 'content' && current_content && previous_content
          diff = call(:util_diff_lines, current_content.to_s, previous_content.to_s)
          added = diff[:added]
          removed = diff[:removed]
          modified_sections = diff[:modified_sections]
          change_percentage = diff[:line_change_percentage]
          change_type = 'content_changed'
        end
      end

      {
        has_changed: has_changed,
        change_type: change_type,
        change_percentage: change_percentage,
        added_content: added,
        removed_content: removed,
        modified_sections: modified_sections,
        requires_reindexing: has_changed
      }
    end,

    compute_metrics: lambda do |input|
      data_points = Array(input['data_points'] || [])
      values = data_points.map { |dp| dp['value'].to_f }.sort

      return {
        average: 0, median: 0, min: 0, max: 0,
        std_deviation: 0, percentile_95: 0, percentile_99: 0,
        total_count: 0, trend: 'stable', anomalies_detected: []
      } if values.empty?

      avg = values.sum / values.length.to_f
      median = values.length.odd? ? values[values.length / 2] :
               (values[values.length / 2 - 1] + values[values.length / 2]) / 2.0
      min_v = values.first
      max_v = values.last

      variance = values.map { |v| (v - avg)**2 }.sum / values.length
      std_dev = Math.sqrt(variance)

      pct = lambda do |arr, p|
        return 0 if arr.empty?
        r = (p/100.0) * (arr.length - 1)
        lo = r.floor
        hi = r.ceil
        lo == hi ? arr[lo] : arr[lo] + (r - lo) * (arr[hi] - arr[lo])
      end
      p95 = pct.call(values, 95)
      p99 = pct.call(values, 99)

      half = values.length / 2
      first_half_avg = half.zero? ? avg : values[0...half].sum / half.to_f
      second_half_avg = (values.length - half).zero? ? avg : values[half..-1].sum / (values.length - half).to_f
      trend =
        if second_half_avg > first_half_avg * 1.1 then 'increasing'
        elsif second_half_avg < first_half_avg * 0.9 then 'decreasing'
        else 'stable'
        end

      anomalies = data_points.select { |dp| (dp['value'].to_f - avg).abs > 2 * std_dev }
                             .map { |dp| { timestamp: dp['timestamp'], value: dp['value'] } }

      {
        average: avg.round(2),
        median: median.round(2),
        min: min_v,
        max: max_v,
        std_deviation: std_dev.round(2),
        percentile_95: p95.round(2),
        percentile_99: p99.round(2),
        total_count: values.length,
        trend: trend,
        anomalies_detected: anomalies
      }
    end,

    calculate_optimal_batch: lambda do |input|
      total_items = (input['total_items'] || 0).to_i
      history = Array(input['processing_history'] || [])
      target = (input['optimization_target'] || 'throughput').to_s
      max_batch = (input['max_batch_size'] || 100).to_i
      min_batch = (input['min_batch_size'] || 10).to_i

      if history.empty?
        optimal = [[(total_items / 10.0).ceil, max_batch].min, min_batch].max
        return {
          optimal_batch_size: optimal,
          estimated_batches: (optimal.zero? ? 0 : (total_items.to_f / optimal).ceil),
          estimated_processing_time: 0.0,
          throughput_estimate: 0.0,
          confidence_score: 0.5,
          recommendation_reason: 'No history available, using default calculation'
        }
      end

      optimal =
        case target
        when 'throughput'
          best = history.max_by { |h| h['batch_size'].to_f / [h['processing_time'].to_f, 0.0001].max }
          best['batch_size'].to_i
        when 'latency'
          best = history.min_by { |h| h['processing_time'].to_f / [h['batch_size'].to_f, 1].max }
          best['batch_size'].to_i
        when 'cost'
          best = history.min_by { |h| (h['memory_usage'].to_f * 0.7) - (h['batch_size'].to_f / [h['processing_time'].to_f, 0.0001].max) * 0.3 }
          best['batch_size'].to_i
        when 'accuracy'
          best = history.max_by { |h| (h['success_rate'].to_f * 1000) + (h['batch_size'].to_f / [h['processing_time'].to_f, 0.0001].max) }
          best['batch_size'].to_i
        else
          (history.sum { |h| h['batch_size'].to_i } / [history.length, 1].max)
        end

      optimal = [[optimal, max_batch].min, min_batch].max
      estimated_batches = (optimal.zero? ? 0 : (total_items.to_f / optimal).ceil)
      avg_time = history.sum { |h| h['processing_time'].to_f } / [history.length, 1].max
      estimated_time = avg_time * estimated_batches
      throughput = estimated_time.zero? ? 0.0 : (total_items.to_f / estimated_time)

      {
        optimal_batch_size: optimal,
        estimated_batches: estimated_batches,
        estimated_processing_time: estimated_time.round(2),
        throughput_estimate: throughput.round(2),
        confidence_score: 0.8,
        recommendation_reason: 'Based on historical performance data'
      }
    end,

    # ---------- Helpers ----------

    util_last_boundary_end: lambda do |segment, regex|
      matches = segment.to_enum(:scan, regex).map { Regexp.last_match }
      return nil if matches.empty?
      matches.last.end(0)
    end,

    util_coerce_numeric_vector: lambda do |arr|
      Array(arr).map do |x|
        begin
          Float(x)
        rescue
          error 'Vectors must contain only numerics.'
        end
      end
    end,

    util_diff_lines: lambda do |current_content, previous_content|
      cur = current_content.to_s.split("\n")
      prev = previous_content.to_s.split("\n")
      i = 0
      j = 0
      window = 20
      added = []
      removed = []
      modified_sections = []

      while i < cur.length && j < prev.length
        if cur[i] == prev[j]
          i += 1
          j += 1
          next
        end

        idx_in_cur = ((i + 1)..[i + window, cur.length - 1].min).find { |k| cur[k] == prev[j] }
        idx_in_prev = ((j + 1)..[j + window, prev.length - 1].min).find { |k| prev[k] == cur[i] }

        if idx_in_cur
          block = cur[i...idx_in_cur]
          added.concat(block)
          modified_sections << { type: 'added', current_range: [i, idx_in_cur - 1], previous_range: [j - 1, j - 1], current_lines: block }
          i = idx_in_cur
        elsif idx_in_prev
          block = prev[j...idx_in_prev]
          removed.concat(block)
          modified_sections << { type: 'removed', current_range: [i - 1, i - 1], previous_range: [j, idx_in_prev - 1], previous_lines: block }
          j = idx_in_prev
        else
          modified_sections << { type: 'modified', current_range: [i, i], previous_range: [j, j], current_lines: [cur[i]], previous_lines: [prev[j]] }
          added << cur[i]
          removed << prev[j]
          i += 1
          j += 1
        end
      end

      if i < cur.length
        block = cur[i..-1]
        added.concat(block)
        modified_sections << { type: 'added', current_range: [i, cur.length - 1], previous_range: [j - 1, j - 1], current_lines: block }
      elsif j < prev.length
        block = prev[j..-1]
        removed.concat(block)
        modified_sections << { type: 'removed', current_range: [i - 1, i - 1], previous_range: [j, prev.length - 1], previous_lines: block }
      end

      total_lines = [cur.length, prev.length].max
      line_change_percentage = total_lines.zero? ? 0.0 : (((added.length + removed.length).to_f / total_lines) * 100).round(2)

      { added: added, removed: removed, modified_sections: modified_sections, line_change_percentage: line_change_percentage }
    end,
    
    generate_document_id: lambda do |file_path, checksum|
      # Create stable document ID using SHA256 hash of "path|checksum"
      require 'digest'

      path_str = file_path.to_s.strip
      checksum_str = checksum.to_s.strip

      # Combine path and checksum with pipe separator
      combined = "#{path_str}|#{checksum_str}"

      # Generate SHA256 hash and return as hex string
      Digest::SHA256.hexdigest(combined)
    end,

    calculate_chunk_boundaries: lambda do |text, chunk_size, overlap_tokens = 0|
      # Convert text to string and ensure we have content
      text_str = text.to_s
      return [] if text_str.empty?

      # Calculate overlap in characters (tokens * 4 approximation)
      overlap_chars = overlap_tokens * 4

      # Start with basic chunk positions
      boundaries = []
      start_pos = 0

      while start_pos < text_str.length
        # Calculate end position for this chunk
        end_pos = start_pos + chunk_size

        # If this is the last chunk, take everything remaining
        if end_pos >= text_str.length
          boundaries << { start: start_pos, end: text_str.length }
          break
        end

        # Smart boundary detection - look for sentence endings
        # Search backward from end_pos for sentence boundary
        search_start = [end_pos - 200, start_pos].max  # Don't search too far back
        chunk_text = text_str[search_start...end_pos + 100] || ""

        # Look for sentence endings: period, exclamation, question mark followed by whitespace
        sentence_matches = chunk_text.scan(/[.!?]\s+/).map.with_index do |match, idx|
          match_pos = search_start + chunk_text.index(match, idx * 2)
          match_pos + match.length
        end

        # Find the best sentence boundary near our target end position
        best_boundary = sentence_matches.select { |pos| pos <= end_pos + 50 && pos > start_pos + chunk_size / 2 }.last

        if best_boundary
          # Use sentence boundary
          actual_end = best_boundary
        else
          # Fallback to word boundary
          # Look for last word boundary before end_pos
          word_boundary_text = text_str[start_pos...end_pos + 50] || ""
          word_matches = word_boundary_text.scan(/\s+/).map.with_index do |match, idx|
            match_pos = start_pos + word_boundary_text.index(match, idx * 2)
            match_pos
          end

          word_boundary = word_matches.select { |pos| pos <= end_pos }.last
          actual_end = word_boundary ? word_boundary : end_pos
        end

        # Ensure we don't go past the text
        actual_end = [actual_end, text_str.length].min

        boundaries << { start: start_pos, end: actual_end }

        # Calculate next start position with overlap
        start_pos = [actual_end - overlap_chars, actual_end].min

        # Ensure we make progress
        start_pos = actual_end if start_pos <= boundaries.last[:start]
      end

      boundaries
    end,

    merge_document_metadata: lambda do |chunk_metadata, document_metadata, options = {}|
      # Extract required information
      document_id = options[:document_id] || document_metadata[:id] || document_metadata['id']
      file_name = options[:file_name] || document_metadata[:name] || document_metadata['name']
      file_id = options[:file_id] || document_metadata[:file_id] || document_metadata['file_id']

      # Start with chunk metadata
      merged = chunk_metadata.is_a?(Hash) ? chunk_metadata.dup : {}

      # Add document metadata
      if document_metadata.is_a?(Hash)
        merged.merge!(document_metadata)
      end

      # Add required fields
      merged[:document_id] = document_id if document_id
      merged[:file_name] = file_name if file_name
      merged[:file_id] = file_id if file_id

      # Add source and timestamp
      merged[:source] = 'google_drive'
      merged[:indexed_at] = Time.now.iso8601

      # Convert symbol keys to strings for consistency
      result = {}
      merged.each do |key, value|
        result[key.to_s] = value
      end

      result
    end,

    # ---------- Project resolution helpers ----------
    get_recipe_details: lambda do |connection, recipe_id|
      path = "/api/recipes/#{recipe_id}"
      call(:execute_with_retry, connection, -> { get(path) })
    rescue RestClient::ExceptionWithResponse => e
      hdrs = e.response&.headers || {}
      cid  = hdrs["x-correlation-id"] || hdrs[:x_correlation_id]
      error("Failed to load recipe #{recipe_id} (#{e.http_code}). cid=#{cid} body=#{e.response&.body}")
    end,

    folders_index_cached: lambda do |connection|
      return @__folders_by_id if defined?(@__folders_by_id) && @__folders_by_id.present?
      page = 1
      acc  = {}
      loop do
        resp = call(:execute_with_retry, connection, -> { get('/api/folders').params(page: page, per_page: 100) })
        rows = resp.is_a?(Array) ? resp : (resp["data"] || [])
        rows.each { |f| acc[f["id"]] = f }
        break if rows.size < 100
        page += 1
      end
      @__folders_by_id = acc
    end,

    projects_index_cached: lambda do |connection|
      return @__projects_by_id if defined?(@__projects_by_id) && @__projects_by_id.present?
      page = 1
      acc  = {}
      loop do
        resp = call(:execute_with_retry, connection, -> { get('/api/projects').params(page: page, per_page: 100) })
        rows = resp.is_a?(Array) ? resp : (resp["data"] || [])
        rows.each { |p| acc[p["id"]] = p }
        break if rows.size < 100
        page += 1
      end
      @__projects_by_id = acc
    end,

    resolve_project_from_recipe: lambda do |connection, recipe_id|
      rec = call(:get_recipe_details, connection, recipe_id)
      f_id = rec["folder_id"]
      error("Recipe #{recipe_id} did not include folder_id") if f_id.blank?

      folders = call(:folders_index_cached, connection)
      folder  = folders[f_id]
      error("Folder #{f_id} not found or not visible to this token") if folder.blank?

      result = {
        recipe_id: recipe_id,
        folder_id: f_id,
        folder_name: folder["name"],
        is_project_folder: !!folder["is_project"],
        project_id: nil,
        project_name: nil,
        project_folder_id: nil,
        environment_host: connection["developer_api_host"]
      }

      if folder["is_project"]
        projects = call(:projects_index_cached, connection)
        proj = projects.values.find { |p| p["folder_id"] == f_id }
        result[:project_id]        = proj&.dig("id")
        result[:project_name]      = proj&.dig("name")
        result[:project_folder_id] = f_id
      else
        result[:project_id] = folder["project_id"]
        if result[:project_id].present?
          projects = call(:projects_index_cached, connection)
          proj = projects[result[:project_id]]
          result[:project_name]      = proj&.dig("name")
          result[:project_folder_id] = proj&.dig("folder_id")
        end
      end
      result
    end,

    # ---------- HTTP helpers & endpoints ----------
    devapi_base: lambda do |connection|
      host = (connection['developer_api_host'].presence || 'app.eu').to_s
      "https://#{host}.workato.com"
    end,

    dt_records_base: lambda do |_connection|
      "https://data-tables.workato.com"
    end,

    execute_with_retry: lambda do |connection, operation = nil, &block|
      retries     = 0
      max_retries = 3

      begin
        op = block || operation
        error('Internal error: execute_with_retry called without an operation') unless op
        op.call
      rescue RestClient::ExceptionWithResponse => e
        code = e.http_code.to_i
        if ([429] + (500..599).to_a).include?(code) && retries < max_retries
          hdrs  = e.response&.headers || {}
          ra    = hdrs["Retry-After"] || hdrs[:retry_after]
          delay = if ra.to_s =~ /^\d+$/ then ra.to_i
                  elsif ra.present?
                    begin
                      [(Time.httpdate(ra) - Time.now).ceil, 1].max
                    rescue
                      60
                    end
                  else
                    2 ** retries
                  end
          sleep([delay, 30].min + rand(0..3))
          retries += 1
          retry
        end
        raise
      rescue RestClient::Exceptions::OpenTimeout, RestClient::Exceptions::ReadTimeout => e
        if retries < max_retries
          sleep((2 ** retries) + rand(0..2))
          retries += 1
          retry
        end
        raise e
      end
    end,

    validate_table_id: lambda do |table_id|
      error("Table ID is required") if table_id.blank?
      uuid = /\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/i
      error("Table ID must be a UUID") unless table_id.to_s.match?(uuid)
    end,

    infer_rules_column_mapping: lambda do |_connection, schema, existing_mapping = {}|
      names = Array(schema).map { |c| (c['name'] || '').to_s }
      # start with any explicit mapping; fill gaps heuristically
      mapping = (existing_mapping || {}).dup

      synonyms = {
        'rule_id'      => %w[rule_id id key],
        'rule_type'    => %w[rule_type type field target applies_to scope],
        'rule_pattern' => %w[rule_pattern pattern regex regexp matcher match expression expr contains],
        'action'       => %w[action category label outcome route bucket folder],
        'priority'     => %w[priority prio rank order weight score],
        'active'       => %w[active enabled is_active status state]
      }

      synonyms.each do |key, alts|
        next if mapping[key].to_s.strip != ''
        found = names.find { |n| alts.any? { |alt| n.casecmp(alt).zero? } }
        mapping[key] = found if found
      end

      mapping
    end,

    # Return [ [name, name], ... ] for picklists
    dt_table_columns: lambda do |connection, table_id|
      table = call(:devapi_get_table, connection, table_id)
      schema = table['schema'] || table.dig('data', 'schema') || []
      cols = Array(schema).map { |c| n = (c['name'] || '').to_s; [n, n] }.reject { |a| a[0].empty? }
      cols.presence || [[ "No columns found", nil ]]
    end,

    pick_tables: lambda do |connection|
      page = 1
      acc  = []
      loop do
        resp = call(:execute_with_retry, connection, -> { get('/api/data_tables').params(page: page, per_page: 100) })
        arr = resp.is_a?(Array) ? resp : (resp['data'] || [])
        acc.concat(arr)
        break if arr.size < 100
        page += 1
      end
      acc.map { |t| [t['name'] || t['id'].to_s, t['id']] }
    rescue RestClient::ExceptionWithResponse => e
      hdrs = e.response&.headers || {}
      cid  = hdrs["x-correlation-id"] || hdrs[:x_correlation_id]
      msg  = e.response&.body || e.message
      hint = "Check API token and Developer API host (#{connection['developer_api_host'] || 'app.eu'})."
      error("Failed to load tables (#{e.http_code}) cid=#{cid} #{hint} #{msg}")
    end,

    devapi_get_table: lambda do |connection, table_id|
      call(:execute_with_retry, connection, lambda { get("/api/data_tables/#{table_id}") })
    end,

    validate_rules_schema!: lambda do |_connection, schema, required_names, mapping = {}|
      names = Array(schema).map { |c| (c['name'] || '').to_s }
      expected = required_names.map { |n| (mapping[n] || n).to_s }
      missing = expected.reject { |n| names.include?(n) }
      if missing.any?
        cid = call(:gen_correlation_id)
        error("Rules table missing required fields: #{missing.join(', ')}. " \
              "Columns found=#{names.join(', ')}. " \
              "Tip: enable 'Custom column names?' or rename columns to match. cid=#{cid}")
      end
    end,

    schema_field_id_map: lambda do |_connection, schema|
      Hash[Array(schema).map { |c| [(c['name'] || '').to_s, (c['field_id'] || c['id'] || '').to_s] }]
    end,

    # Pull and normalize rules with optional column mapping
    dt_query_rules_all: lambda do |connection, table_id, required_fields, max_rules, schema = nil, mapping = {}|
      base = call(:dt_records_base, connection)
      url  = "#{base}/api/v1/tables/#{table_id}/query"

      schema ||= begin
        table = call(:devapi_get_table, connection, table_id)
        table['schema'] || table.dig('data', 'schema') || []
      end

      name_to_uuid = call(:schema_field_id_map, connection, schema)
      names = Array(schema).map { |c| (c['name'] || '').to_s }

      # Column resolver honoring mapping
      col = ->(key) { (mapping[key] || key).to_s }

      # Only select columns that actually exist
      base_select = required_fields.map { |k| col.call(k) }
      select_fields = (base_select + ['$record_id', '$created_at', '$updated_at']).uniq
      select_fields = select_fields.select { |n| n.start_with?('$') || names.include?(n) }

      # Optional columns
      active_col = col.call('active');   has_active   = names.include?(active_col)
      prio_col   = col.call('priority'); has_priority = names.include?(prio_col)

      where = has_active ? { active_col => { '$eq' => true } } : nil
      order = has_priority ? { by: prio_col, order: 'asc', case_sensitive: false } : nil

      records = []
      cont = nil
      loop do
        body = { select: select_fields, where: where, order: order, limit: 200, continuation_token: cont }.compact
        # ADD AUTHORIZATION HEADER HERE
        resp = call(:execute_with_retry, connection, lambda { 
          post(url)
            .headers('Authorization' => "Bearer #{connection['api_token']}")
            .payload(body) 
        })
        recs = resp['records'] || resp['data'] || []
        records.concat(recs)
        cont = resp['continuation_token']
        break if cont.blank? || records.length >= max_rules
      end

      # Decode each record to a name->value row using schema
      decoded = records.map do |r|
        doc = r['document'] || []
        row = {}
        doc.each do |cell|
          fid  = (cell['field_id'] || '').to_s
          name = name_to_uuid.key(fid) || cell['name']
          row[name.to_s] = cell['value']
        end
        row['$record_id']  = r['record_id']  if r['record_id']
        row['$created_at'] = r['created_at'] if r['created_at']
        row['$updated_at'] = r['updated_at'] if r['updated_at']
        row
      end

      # Normalize → canonical keys; default active=true if missing, priority=1000 if missing
      normalized = decoded.map do |row|
        {
          'rule_id'      => (row[col.call('rule_id')]      || '').to_s,
          'rule_type'    => (row[col.call('rule_type')]    || '').to_s.downcase,
          'rule_pattern' => (row[col.call('rule_pattern')] || '').to_s,
          'action'       => (row[col.call('action')]       || '').to_s,
          'priority'     => names.include?(col.call('priority')) ? call(:coerce_int, connection, row[col.call('priority')], 1000) : 1000,
          'active'       => names.include?(col.call('active'))   ? call(:coerce_bool, connection, row[col.call('active')])    : true,
          'created_at'   => row['$created_at']
        }
      end

      normalized
        .select { |r| r['active'] == true }
        .sort_by { |r| r['priority'] || 1000 }
        .first(max_rules)
    end,

    coerce_bool: lambda do |_connection, v|
      return true  if v == true || v.to_s.strip.downcase == 'true' || v.to_s == '1'
      return false if v == false || v.to_s.strip.downcase == 'false' || v.to_s == '0'
      !!v
    end,

    coerce_int: lambda do |_connection, v, default|
      Integer(v)
    rescue
      default.to_i
    end,

    safe_regex: lambda do |_connection, pattern|
      p = pattern.to_s.strip
      max_len = 512
      p = p[0, max_len]
      if p.start_with?('/') && p.end_with?('/') && p.length >= 2
        Regexp.new(p[1..-2], Regexp::IGNORECASE)
      elsif p.start_with?('re:')
        Regexp.new(p.sub(/^re:/i, ''), Regexp::IGNORECASE)
      else
        Regexp.new(Regexp.escape(p), Regexp::IGNORECASE)
      end
    rescue RegexpError => e
      error("Invalid regex pattern in rules: #{e.message}")
    end,

    normalize_email: lambda do |_connection, email|
      {
        from_email: (email['from_email'] || '').to_s,
        from_name:  (email['from_name']  || '').to_s,
        subject:    (email['subject']    || '').to_s,
        body:       (email['body']       || '').to_s,
        headers:    email['headers'].is_a?(Hash) ? email['headers'] : {},
        message_id: (email['message_id'] || '').to_s
      }
    end,

    evaluate_standard_patterns: lambda do |_connection, email|
      from = "#{email[:from_name]} <#{email[:from_email]}>"
      subj = email[:subject].to_s
      body = email[:body].to_s

      sender_rx = [ /\bno[-_.]?reply\b/i, /\bdo[-_.]?not[-_.]?reply\b/i, /\bdonotreply\b/i, /\bnewsletter\b/i, /\bmailer\b/i, /\bautomated\b/i ]
      subject_rx = [ /\border\s*(no\.|#)?\s*\d+/i, /\b(order|purchase)\s+confirmation\b/i, /\bconfirmation\b/i, /\breceipt\b/i, /\binvoice\b/i, /\b(password\s*reset|verification\s*code|two[-\s]?factor)\b/i ]
      body_rx = [ /\bunsubscribe\b/i, /\bmanage (your )?preferences\b/i, /\bautomated (message|email)\b/i, /\bdo not reply\b/i, /\bview (this|in) browser\b/i ]

      matches = []
      flags_sender  = sender_rx.select { |rx| from.match?(rx) }.map(&:source)
      flags_subject = subject_rx.select { |rx| subj.match?(rx) }.map(&:source)
      flags_body    = body_rx.select  { |rx| body.match?(rx) }.map(&:source)

      flags_sender.each do |src|
        m = from.match(Regexp.new(src, Regexp::IGNORECASE))
        matches << { rule_id: "std:sender:#{src}", rule_type: "sender", rule_pattern: src, action: nil, priority: 1000, field_matched: "sender", sample: m&.to_s }
      end
      flags_subject.each do |src|
        m = subj.match(Regexp.new(src, Regexp::IGNORECASE))
        matches << { rule_id: "std:subject:#{src}", rule_type: "subject", rule_pattern: src, action: nil, priority: 1000, field_matched: "subject", sample: m&.to_s }
      end
      flags_body.each do |src|
        m = body.match(Regexp.new(src, Regexp::IGNORECASE))
        matches << { rule_id: "std:body:#{src}", rule_type: "body", rule_pattern: src, action: nil, priority: 1000, field_matched: "body", sample: m&.to_s }
      end

      { matches: matches, sender_flags: flags_sender, subject_flags: flags_subject, body_flags: flags_body }
    end,

    apply_rules_to_email: lambda do |connection, email, rules, stop_on_first|
      from    = "#{email[:from_name]} <#{email[:from_email]}>"
      subject = email[:subject].to_s
      body    = email[:body].to_s

      out       = []
      evaluated = 0

      rules.each do |r|
        rt      = r['rule_type']
        pattern = r['rule_pattern']
        next if rt.blank? || pattern.blank?

        rx = call(:safe_regex, connection, pattern)

        field = case rt
                when 'sender'  then 'sender'
                when 'subject' then 'subject'
                when 'body'    then 'body'
                else next
                end

        haystack = case field
                  when 'sender'  then from
                  when 'subject' then subject
                  when 'body'    then body
                  end

        evaluated += 1
        m = haystack.match(rx)
        if m
          out << { rule_id: r['rule_id'], rule_type: rt, rule_pattern: pattern, action: r['action'], priority: r['priority'], field_matched: field, sample: m.to_s }
          break if stop_on_first
        end
      end

      { matches: out.sort_by { |h| [h[:priority] || 1000, h[:rule_id].to_s] }, evaluated_count: evaluated }
    end,

    # Orchestrate logic with mapping support + unconditional table-id validation
    evaluate_email_by_rules_exec: lambda do |connection, input, config|
      action_cid = call(:gen_correlation_id)
      started_at = Time.now

      email = call(:normalize_email, connection, input['email'] || {})

      source   = (config && config['rules_source'] || input['rules_source'] || 'standard').to_s
      table_id = (config && config['custom_rules_table_id'] || input['custom_rules_table_id']).to_s.presence

      stop_on      = input.key?('stop_on_first_match') ? !!input['stop_on_first_match'] : true
      fallback_std = input.key?('fallback_to_standard') ? !!input['fallback_to_standard'] : true
      max_rules    = (input['max_rules_to_apply'] || 500).to_i.clamp(1, 10_000)

      selected_action = nil
      used_source     = 'none'
      matches         = []
      evaluated_count = 0

      std = call(:evaluate_standard_patterns, connection, email)

      if source == 'custom'
        error("api_token is required in connector connection to read custom rules from Data Tables. cid=#{action_cid}") unless connection['api_token'].present?
        call(:validate_table_id, table_id)

        # Load table + schema
        table_info = call(:devapi_get_table, connection, table_id)
        schema     = table_info['schema'] || table_info.dig('data', 'schema') || []

        # Build mapping: explicit mapping first; auto‑infer any missing pieces
        mapping = {}
        if config['enable_column_mapping']
          %w[col_rule_id col_rule_type col_rule_pattern col_action col_priority col_active].each do |ck|
            v = (config[ck] || '').to_s.strip
            next if v.empty?
            key = ck.sub(/^col_/, '')
            mapping[key] = v
          end
        end
        mapping = call(:infer_rules_column_mapping, connection, schema, mapping)

        # Minimal required fields to evaluate rules. Priority/active are optional.
        required = %w[rule_type rule_pattern]
        call(:validate_rules_schema!, connection, schema, required, mapping)

        rules     = call(:dt_query_rules_all, connection, table_id, (required + ['action', 'rule_id']).uniq, max_rules, schema, mapping)
        applied   = call(:apply_rules_to_email, connection, email, rules, stop_on)
        matches   = applied[:matches]
        evaluated_count = applied[:evaluated_count]

        if matches.any?
          used_source     = 'custom'
          selected_action = matches.first[:action]
        elsif fallback_std && std[:matches].any?
          used_source = 'standard'
          matches     = std[:matches]
        end
      else
        matches     = std[:matches]
        used_source = matches.any? ? 'standard' : 'none'
      end

      result = {
        pattern_match: matches.any?,
        rule_source:   used_source,
        selected_action: selected_action,
        top_match:     matches.first,
        matches:       matches,
        standard_signals: {
          sender_flags:  std[:sender_flags],
          subject_flags: std[:subject_flags],
          body_flags:    std[:body_flags]
        },
        debug: {
          evaluated_rules_count: evaluated_count,
          schema_validated:      (source == 'custom'),
          errors: []
        },
        telemetry: call(:telemetry_envelope,
                        true,
                        { action: 'classify_by_pattern',
                          rule_source: used_source,
                          evaluated_rules: evaluated_count,
                          has_custom_table: (source == 'custom'),
                          table_id: table_id },
                        started_at,
                        action_cid,
                        true)
      }

      result
    end,

    # Prepare text for AI processing with source-specific cleaning
    prepare_text_for_ai_exec: lambda do |connection, input|
      text = (input['text'] || '').to_s
      source_type = (input['source_type'] || 'general').to_s
      task_type = (input['task_type'] || 'general').to_s
      options = input['options'] || {}

      original_length = text.length
      operations_applied = []
      removed_sections = []
      extracted_urls = []

      # Apply source-specific processing
      if source_type == 'email'
        # Use existing process_email_text method for email content
        email_result = call(:process_email_text, {
          'email_body' => text,
          'remove_quotes' => options['remove_quotes'] != false,
          'remove_signatures' => options['remove_signatures'] != false,
          'remove_disclaimers' => options['remove_disclaimers'] != false,
          'extract_urls' => options['extract_urls'] == true,
          'normalize_whitespace' => options['normalize_whitespace'] != false
        })

        text = email_result[:cleaned_text] || email_result['cleaned_text'] || text
        removed_sections = email_result[:removed_sections] || email_result['removed_sections'] || []
        extracted_urls = email_result[:extracted_urls] || email_result['extracted_urls'] || []

        operations_applied << 'email_preprocessing'
        operations_applied << 'remove_quotes' if options['remove_quotes'] != false
        operations_applied << 'remove_signatures' if options['remove_signatures'] != false
        operations_applied << 'remove_disclaimers' if options['remove_disclaimers'] != false
        operations_applied << 'normalize_whitespace' if options['normalize_whitespace'] != false
      else
        # General text processing for document, chat, general types
        if options['normalize_whitespace'] != false
          text.gsub!(/[ \t]+/, ' ')
          text.gsub!(/\n{3,}/, "\n\n")
          text.strip!
          operations_applied << 'normalize_whitespace'
        end

        if options['extract_urls'] == true
          extracted_urls = text.scan(%r{https?://[^\s<>"'()]+})
          operations_applied << 'extract_urls'
        end
      end

      # Apply max_length if specified
      if options['max_length'] && options['max_length'].to_i > 0
        max_len = options['max_length'].to_i
        if text.length > max_len
          text = text[0, max_len]
          operations_applied << 'truncate_to_max_length'
        end
      end

      # Basic PII removal if requested
      if options['remove_pii'] == true
        # Simple email and phone number masking
        text.gsub!(/\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b/, '[EMAIL]')
        text.gsub!(/\b\d{3}[-.]?\d{3}[-.]?\d{4}\b/, '[PHONE]')
        operations_applied << 'remove_pii'
      end

      final_length = text.length
      word_count = text.split(/\s+/).length

      # Build contract-compliant output
      result = {
        'text' => text,
        'removed_sections' => removed_sections,
        'word_count' => word_count,
        'cleaning_applied' => {
          'source_type' => source_type,
          'task_type' => task_type,
          'operations' => operations_applied,
          'original_length' => original_length,
          'final_length' => final_length,
          'reduction_percentage' => original_length.zero? ? 0 : ((1 - final_length.to_f / original_length) * 100).round(2)
        },
        'metadata' => {
          'source_type' => source_type,
          'task_type' => task_type,
          'processing_timestamp' => Time.now.utc.iso8601,
          'extracted_urls' => extracted_urls
        }
      }

      # Validate contract compliance
      call(:validate_contract, connection, result, 'cleaned_text')

      result
    end,

    # ----- Templates (Data Tables) -----
    pick_templates_from_table: lambda do |connection, config|
      error('api_token is required in connector connection to read templates from Data Tables') unless connection['api_token'].present?

      table_id  = (config['templates_table_id'] || '').to_s.strip
      call(:validate_table_id, table_id)

      display  = (config['template_display_field']  || 'name').to_s
      valuef   = (config['template_value_field']    || '').to_s
      contentf = (config['template_content_field']  || 'content').to_s

      table_info = call(:devapi_get_table, connection, table_id)
      schema     = table_info['schema'] || table_info.dig('data', 'schema') || []
      names      = Array(schema).map { |c| (c['name'] || '').to_s }
      missing    = [display, contentf].reject { |n| names.include?(n) }
      error("Templates table missing required fields: #{missing.join(', ')}") unless missing.empty?

      base   = call(:dt_records_base, connection)
      url    = "#{base}/api/v1/tables/#{table_id}/query"
      select = [display, contentf]
      select << valuef if valuef.present?
      select << 'active' if names.include?('active')
      where  = names.include?('active') ? { 'active' => { '$eq' => true } } : nil
      order  = { by: display, order: 'asc', case_sensitive: false }

      records = []
      cont = nil
      loop do
        body = { select: select.uniq, where: where, order: order, limit: 200, continuation_token: cont }.compact
        resp = call(:execute_with_retry, connection, lambda { 
          post(url)
            .headers('Authorization' => "Bearer #{connection['api_token']}")
            .payload(body) 
        })
        recs = resp['records'] || resp['data'] || []
        records.concat(recs)
        cont = resp['continuation_token']
        break if cont.blank? || records.length >= 2000
      end

      name_to_uuid = call(:schema_field_id_map, connection, schema)
      rows = records.map do |r|
        doc = r['document'] || []
        row = {}
        doc.each do |cell|
          fid  = (cell['field_id'] || '').to_s
          name = name_to_uuid.key(fid) || cell['name']
          row[name.to_s] = cell['value']
        end
        row['$record_id'] = r['record_id'] if r['record_id']
        row
      end

      opts = rows.map do |row|
        disp = (row[display] || row['$record_id']).to_s
        val  = valuef.present? ? row[valuef].to_s : row['$record_id'].to_s
        [disp, val]
      end

      opts.empty? ? [[ "No templates found in selected table", nil ]] : opts
    rescue RestClient::ExceptionWithResponse => e
      hdrs = e.response&.headers || {}
      cid  = hdrs["x-correlation-id"] || hdrs[:x_correlation_id]
      msg  = e.response&.body || e.message
      error("Failed to load templates (#{e.http_code}) cid=#{cid} #{msg}")
    end,

    resolve_template_selection: lambda do |connection, config, selected_value|
      table_id   = (config['templates_table_id'] || '').to_s
      valuef     = (config['template_value_field']   || '').to_s
      contentf   = (config['template_content_field'] || 'content').to_s
      displayf   = (config['template_display_field'] || 'name').to_s

      table_info = call(:devapi_get_table, connection, table_id)
      schema     = table_info['schema'] || table_info.dig('data', 'schema') || []

      base = call(:dt_records_base, connection)

      if valuef.present?
        body = { select: [valuef, displayf, contentf, '$record_id'].uniq, where: { valuef => { '$eq' => selected_value } }, limit: 1 }
        resp = call(:execute_with_retry, connection, lambda { 
          post("#{base}/api/v1/tables/#{table_id}/query")
            .headers('Authorization' => "Bearer #{connection['api_token']}")
            .payload(body) 
        })
        rec  = (resp['records'] || resp['data'] || [])[0]
        return nil unless rec
        row  = call(:dt_decode_record_doc, connection, schema, rec)
        { 'value' => selected_value, 'display' => row[displayf], 'content' => row[contentf], 'record_id' => row['$record_id'] }
      else
        rec = call(:execute_with_retry, connection, lambda { get("#{base}/api/v1/tables/#{table_id}/records/#{selected_value}") })
        row = call(:dt_decode_record_doc, connection, schema, rec)
        { 'value' => selected_value, 'display' => row[displayf], 'content' => row[contentf], 'record_id' => selected_value }
      end
    end,

    dt_decode_record_doc: lambda do |connection, schema, record|
      name_to_uuid = call(:schema_field_id_map, connection, schema)
      doc = record['document'] || []
      row = {}
      doc.each do |cell|
        fid  = (cell['field_id'] || '').to_s
        name = name_to_uuid.key(fid) || cell['name']
        row[name.to_s] = cell['value']
      end
      row['$record_id']  = record['record_id']  if record['record_id']
      row['$created_at'] = record['created_at'] if record['created_at']
      row['$updated_at'] = record['updated_at'] if record['updated_at']
      row
    end,

    validate_contract: lambda do |connection, data, contract_type|
      cid = call(:gen_correlation_id)

      # Normalize keys to strings without mutating caller input
      normalized = {}
      if data.respond_to?(:each)
        data.each { |k, v| normalized[k.to_s] = v }
      else
        error("Contract validation failed. Data must be a hash-like object. cid=#{cid}")
      end

      contracts = {
        # Existing contracts
        'cleaned_text' => {
          required_fields: ['text', 'removed_sections', 'word_count', 'cleaning_applied'],
          field_types: {
            'text' => String, 'removed_sections' => Array, 'word_count' => Integer, 'cleaning_applied' => Hash
          }
        },
        'embedding_request' => {
          required_fields: ['text', 'metadata'],
          field_types: { 'text' => String, 'metadata' => Hash }
        },
        'classification_request' => {
          required_fields: ['content', 'rules_source'],
          field_types: { 'content' => String, 'rules_source' => String }
        },
        'classification_response' => {
          required_fields: ['selected_category', 'confidence'],
          field_types: { 'selected_category' => String, 'confidence' => Float, 'alternatives' => Array, 'usage_metrics' => Hash }
        },
        'classification_result' => {
          required_fields: ['pattern_match', 'rule_source', 'matches', 'debug'],
          field_types: { 'rule_source' => String, 'matches' => Array, 'debug' => Hash }
        },
        'prompt_request' => {
          required_fields: ['context_documents', 'user_query'],
          field_types: { 'context_documents' => Array, 'user_query' => String }
        },

        # NEW: cover result types actually used by your actions
        'email_cleaning_result' => {
          required_fields: %w[cleaned_text original_length cleaned_length],
          field_types: {
            'cleaned_text' => String,
            'removed_sections' => Array,
            'extracted_urls' => Array,
            'removed_sections_count' => Integer,
            'urls_count' => Integer,
            'original_length' => Integer,
            'cleaned_length' => Integer,
            'reduction_percentage' => Float
          }
        },
        'chunking_result' => {
          required_fields: %w[chunks total_chunks],
          field_types: {
            'chunks' => Array,
            'first_chunk' => Hash,
            'chunks_json' => String,
            'total_chunks' => Integer,
            'total_tokens' => Integer
          }
        },
        'similarity_result' => {
          required_fields: %w[similarity_score is_similar similarity_type],
          field_types: {
            'similarity_score' => Float,
            'similarity_percentage' => Float,
            'similarity_type' => String,
            'computation_time_ms' => Integer,
            'threshold_used' => Float
          }
        },
        'document_metadata' => {
          required_fields: %w[document_id file_hash word_count character_count estimated_tokens created_at],
          field_types: {
            'document_id' => String,
            'file_hash' => String,
            'word_count' => Integer,
            'character_count' => Integer,
            'estimated_tokens' => Integer,
            'language' => String,
            'summary' => String
          }
        },
        'change_detection' => {
          required_fields: %w[change_type],
          field_types: {
            'change_type' => String,
            'added_content' => Array,
            'removed_content' => Array,
            'modified_sections' => Array
          }
        },
        'metrics_result' => {
          required_fields: %w[average median min max total_count],
          field_types: {
            'average' => Float, 'median' => Float, 'min' => Float, 'max' => Float,
            'std_deviation' => Float, 'percentile_95' => Float, 'percentile_99' => Float, 'total_count' => Integer
          }
        }
      }

      contract = contracts[contract_type.to_s]
      error("Unknown contract type: #{contract_type}. cid=#{cid}") unless contract

      missing = contract[:required_fields].select { |f| !normalized.key?(f) }
      error("Contract validation failed. Missing required fields: #{missing.join(', ')}. cid=#{cid}") unless missing.empty?

      type_errors = []
      contract[:field_types].each do |field, expected_type|
        next unless normalized.key?(field)
        val = normalized[field]
        case expected_type.name
        when 'String'
          type_errors << "#{field} must be a string" unless val.is_a?(String)
        when 'Integer'
          type_errors << "#{field} must be an integer" unless val.is_a?(Integer)
        when 'Float'
          type_errors << "#{field} must be a float" unless val.is_a?(Float) || val.is_a?(Integer)
        when 'Hash'
          type_errors << "#{field} must be a hash/object" unless val.is_a?(Hash)
        when 'Array'
          type_errors << "#{field} must be an array" unless val.is_a?(Array)
        end
      end
      error("Contract validation failed. Type errors: #{type_errors.join(', ')}. cid=#{cid}") unless type_errors.empty?

      normalized # return a normalized copy; do not mutate caller input
    end

  },

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {
    chunk_object: {
      fields: lambda do
        [
          { name: "chunk_id", type: "string" },
          { name: "chunk_index", type: "integer" },
          { name: "text", type: "string" },
          { name: "token_count", type: "integer" },
          { name: "start_char", type: "integer" },
          { name: "end_char", type: "integer" },
          { name: "metadata", type: "object" }
        ]
      end
    },

    embedding_object: {
      fields: lambda do
        [
          { name: "id", type: "string", sticky: true },
          { name: "vector", type: "array", of: "number", sticky: true },
          { name: "metadata", type: "object", sticky: true }
        ]
      end
    },

    metric_datapoint: {
      fields: lambda do
        [
          { name: "timestamp", type: "timestamp", sticky: true },
          { name: "value", type: "number", sticky: true },
          { name: "metadata", type: "object", sticky: true }
        ]
      end
    },

    email_envelope: {
      fields: lambda do
        [
          { name: "from_email", label: "From email", sticky: true },
          { name: "from_name",  label: "From name", sticky: true },
          { name: "subject",    label: "Subject", sticky: true },
          { name: "body",       label: "Body", control_type: "text-area", sticky: true },
          { name: "headers",    label: "Headers", type: "object", sticky: true },
          { name: "message_id", label: "Message ID", sticky: true },
          { name: "to",         label: "To", type: "array", of: "string", sticky: true },
          { name: "cc",         label: "Cc", type: "array", of: "string", sticky: true }
        ]
      end
    },

    rules_row: {
      fields: lambda do
        [
          { name: "rule_id" }, { name: "rule_type" }, { name: "rule_pattern" },
          { name: "action" }, { name: "priority", type: "integer" }, { name: "field_matched" },
          { name: "sample" }
        ]
      end
    },

    standard_signals: {
      fields: lambda do
        [
          { name: "sender_flags",  type: "array", of: "string" },
          { name: "subject_flags", type: "array", of: "string" },
          { name: "body_flags",    type: "array", of: "string" }
        ]
      end
    },

    validation_rule: {
      fields: lambda do
        [
          { name: "rule_type", sticky: true },
          { name: "rule_value", sticky: true }
        ]
      end
    },

    context_document: {
      fields: lambda do
        [
          { name: "content", type: "string", sticky: true },
          { name: "relevance_score", type: "number", sticky: true },
          { name: "source", type: "string", sticky: true },
          { name: "metadata", type: "object", sticky: true }
        ]
      end
    },

    diff_section: {
      fields: lambda do
        [
          { name: "type" },
          { name: "current_range", type: "array", of: "integer" },
          { name: "previous_range", type: "array", of: "integer" },
          { name: "current_lines", type: "array", of: "string" },
          { name: "previous_lines", type: "array", of: "string" }
        ]
      end
    },

    anomaly: {
      fields: lambda do
        [
          { name: "timestamp", type: "timestamp" },
          { name: "value", type: "number" }
        ]
      end
    },

    vertex_datapoint: {
      fields: lambda do
        [
          { name: "datapoint_id", type: "string" },
          { name: "feature_vector", type: "array", of: "number" },
          { name: "restricts", type: "object" }
        ]
      end
    },


    vertex_batch: {
      fields: lambda do |connection, _config, object_definitions|
        [
          { name: "batch_id", type: "string" },
          { name: "batch_number", type: "integer" },
          { name: "datapoints", type: "array", of: "object", properties: object_definitions["vertex_datapoint"] },
          { name: "size", type: "integer" }
        ]
      end
    },

    chunking_config: {
      fields: lambda do
        [
          { name: "chunk_size", label: "Chunk size (tokens)", type: "integer", default: 1000, convert_input: "integer_conversion", sticky: true, hint: "Maximum tokens per chunk" },
          { name: "chunk_overlap", label: "Chunk overlap (tokens)", type: "integer", default: 100, convert_input: "integer_conversion", sticky: true, hint: "Token overlap between chunks" },
          { name: "preserve_sentences", label: "Preserve sentences", type: "boolean", control_type: "checkbox", default: true, convert_input: "boolean_conversion", sticky: true, hint: "Don't break mid‑sentence" },
          { name: "preserve_paragraphs", label: "Preserve paragraphs", type: "boolean", control_type: "checkbox", default: false, convert_input: "boolean_conversion", sticky: true, hint: "Try to keep paragraphs intact" }
        ]
      end
    },

    chunking_result: {
      fields: lambda do |connection, _config, object_definitions|
        [
          { name: "chunks_count", type: "integer", label: "Number of chunks",
            hint: "Total number of chunks created" },
          { name: "chunks", type: "array", of: "object", properties: object_definitions["chunk_object"] },
          { name: "first_chunk", type: "object", properties: object_definitions["chunk_object"],
            label: "First chunk (quick access)",
            hint: "First chunk for quick recipe access" },
          { name: "chunks_json", type: "string", label: "Chunks as JSON string",
            hint: "All chunks serialized as JSON for bulk operations" },
          { name: "total_chunks", type: "integer" },
          { name: "total_tokens", type: "integer" },
          { name: "average_chunk_size", type: "integer", label: "Average chunk size",
            hint: "Average number of tokens per chunk" },
          { name: "pass_fail", type: "boolean", label: "Chunking success",
            hint: "True if chunking completed successfully" },
          { name: "action_required", type: "string", label: "Action required",
            hint: "Next recommended action based on results" }
        ]
      end
    },

    email_cleaning_options: {
      fields: lambda do
        [
          { name: "remove_signatures",  label: "Remove signatures",     type: "boolean", default: true,  control_type: "checkbox", convert_input: "boolean_conversion", sticky: true, group: "Options" },
          { name: "remove_quotes",      label: "Remove quoted text",    type: "boolean", default: true,  control_type: "checkbox", convert_input: "boolean_conversion", sticky: true, group: "Options" },
          { name: "remove_disclaimers", label: "Remove disclaimers",    type: "boolean", default: true,  control_type: "checkbox", convert_input: "boolean_conversion", sticky: true, group: "Options" },
          { name: "normalize_whitespace", label: "Normalize whitespace", type: "boolean", default: true, control_type: "checkbox", convert_input: "boolean_conversion", sticky: true, group: "Options" },
          { name: "extract_urls",       label: "Extract URLs",          type: "boolean", default: false, control_type: "checkbox", convert_input: "boolean_conversion", sticky: true, group: "Options" }
        ]
      end
    },

    email_cleaning_result: {
      fields: lambda do
        [
          { name: "cleaned_text", type: "string" },
          { name: "extracted_query", type: "string" },
          { name: "removed_sections_count", type: "integer", label: "Removed sections count",
            hint: "Number of sections removed during cleaning" },
          { name: "removed_sections", type: "array", of: "string" },
          { name: "urls_count", type: "integer", label: "URLs count",
            hint: "Number of URLs extracted" },
          { name: "extracted_urls", type: "array", of: "string" },
          { name: "original_length", type: "integer" },
          { name: "cleaned_length", type: "integer" },
          { name: "reduction_percentage", type: "number" },
          { name: "pass_fail", type: "boolean", label: "Cleaning success",
            hint: "True if cleaning completed successfully" },
          { name: "action_required", type: "string", label: "Action required",
            hint: "Next recommended action based on results" },
          { name: "has_content", type: "boolean", label: "Has meaningful content",
            hint: "True if cleaned text contains meaningful content" }
        ]
      end
    },

    similarity_result: {
      fields: lambda do
        [
          { name: "similarity_score",       type: "number", label: "Similarity score", hint: "0–1 for cosine/euclidean; unbounded for dot product" },
          { name: "similarity_percentage",  type: "number", label: "Similarity percentage", hint: "0–100; only for cosine/euclidean" },
          { name: "is_similar",             type: "boolean", control_type: "checkbox", label: "Is similar", hint: "Whether the vectors meet the threshold" },
          { name: "pass_fail",              type: "boolean", label: "Similarity check", hint: "True if vectors are considered similar" },
          { name: "action_required",        type: "string", label: "Action required", hint: "Next recommended action based on similarity" },
          { name: "confidence_level",       type: "string", label: "Confidence level", hint: "high, medium, or low based on score" },
          { name: "similarity_type",        type: "string", label: "Similarity type", hint: "cosine, euclidean, or dot_product" },
          { name: "computation_time_ms",    type: "integer", label: "Computation time (ms)" },
          { name: "threshold_used",         type: "number", label: "Threshold used", optional: true },
          { name: "vectors_normalized",     type: "boolean", control_type: "checkbox", label: "Vectors normalized", optional: true }
        ]
      end
    },

    document_metadata: {
      fields: lambda do
        [
          { name: "document_id", type: "string", label: "Document ID" },
          { name: "file_hash", type: "string", label: "File hash" },
          { name: "word_count", type: "integer", label: "Word count" },
          { name: "character_count", type: "integer", label: "Character count" },
          { name: "estimated_tokens", type: "integer", label: "Estimated tokens" },
          { name: "language", type: "string", label: "Language" },
          { name: "summary", type: "string", label: "Summary" },
          { name: "key_topics", type: "array", of: "string", label: "Key topics" },
          { name: "entities", type: "object", label: "Entities" },
          { name: "created_at", type: "timestamp", label: "Created at" },
          { name: "processing_time_ms", type: "integer", label: "Processing time (ms)" }
        ]
      end
    },

    metrics_result: {
      fields: lambda do
        [
          { name: "average", type: "number", label: "Average" },
          { name: "median", type: "number", label: "Median" },
          { name: "min", type: "number", label: "Minimum" },
          { name: "max", type: "number", label: "Maximum" },
          { name: "std_deviation", type: "number", label: "Standard deviation" },
          { name: "percentile_95", type: "number", label: "95th percentile" },
          { name: "percentile_99", type: "number", label: "99th percentile" },
          { name: "total_count", type: "integer", label: "Total count" },
          { name: "trend", type: "string", label: "Trend" },
          { name: "anomalies_detected", type: "array", of: "object", label: "Anomalies detected",
            properties: [
              { name: "timestamp", type: "timestamp" },
              { name: "value", type: "number" }
            ]
          }
        ]
      end
    },

    change_detection: {
      fields: lambda do
        [
          { name: "has_changed", type: "boolean", control_type: "checkbox", label: "Has changed" },
          { name: "change_type", type: "string", label: "Change type" },
          { name: "change_percentage", type: "number", label: "Change percentage" },
          { name: "added_content", type: "array", of: "string", label: "Added content" },
          { name: "removed_content", type: "array", of: "string", label: "Removed content" },
          { name: "modified_sections", type: "array", of: "object", label: "Modified sections",
            properties: [
              { name: "type", type: "string" },
              { name: "current_range", type: "array", of: "integer" },
              { name: "previous_range", type: "array", of: "integer" },
              { name: "current_lines", type: "array", of: "string" },
              { name: "previous_lines", type: "array", of: "string" }
            ]
          },
          { name: "requires_reindexing", type: "boolean", control_type: "checkbox", label: "Requires reindexing" }
        ]
      end
    },
    classification_result: {
      fields: lambda do
        [
          { name: "pattern_match", type: "boolean", control_type: "checkbox", label: "Pattern match" },
          { name: "rule_source", type: "string", label: "Rule source" },
          { name: "selected_action", type: "string", label: "Selected action" },
          { name: "top_match", type: "object", label: "Top match",
            properties: [
              { name: "rule_id", type: "string" },
              { name: "rule_type", type: "string" },
              { name: "rule_pattern", type: "string" },
              { name: "action", type: "string" },
              { name: "priority", type: "integer" },
              { name: "field_matched", type: "string" },
              { name: "sample", type: "string" }
            ]
          },
          { name: "matches", type: "array", of: "object", label: "Matches" },
          { name: "standard_signals", type: "object", label: "Standard signals",
            properties: [
              { name: "sender_flags", type: "array", of: "string" },
              { name: "subject_flags", type: "array", of: "string" },
              { name: "body_flags", type: "array", of: "string" }
            ]
          },
          { name: "debug", type: "object", label: "Debug",
            properties: [
              { name: "evaluated_rules_count", type: "integer" },
              { name: "schema_validated", type: "boolean", control_type: "checkbox" },
              { name: "errors", type: "array", of: "string" }
            ]
          },
          { name: "telemetry", type: "object", label: "Telemetry",
            properties: [
              { name: "success", type: "boolean" },
              { name: "timestamp", type: "string" },
              { name: "metadata", type: "object" },
              { name: "trace", type: "object",
                properties: [
                  { name: "correlation_id", type: "string" },
                  { name: "duration_ms", type: "integer" }
                ]
              }
            ]
          }
        ]
      end
    }
  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {

    environments: lambda do
      [
        ["Development", "dev"],
        ["Staging", "staging"],
        ["Production", "prod"]
      ]
    end,

    similarity_types: lambda do
      [
        ["Cosine similarity", "cosine"],
        ["Euclidean distance", "euclidean"],
        ["Dot product", "dot_product"]
      ]
    end,

    format_types: lambda do
      [
        ["JSON", "json"],
        ["JSONL", "jsonl"],
        ["CSV", "csv"]
      ]
    end,

    prompt_templates: lambda do |connection, config = {}|
      cfg = config || {}
      template_source        = (cfg['template_source'] || cfg[:template_source] || 'builtin').to_s
      templates_table_id     = cfg['templates_table_id'] || cfg[:templates_table_id]
      template_display_field = (cfg['template_display_field'] || cfg[:template_display_field] || 'name').to_s
      template_value_field   = (cfg['template_value_field'] || cfg[:template_value_field]).to_s
      template_content_field = (cfg['template_content_field'] || cfg[:template_content_field] || 'content').to_s

      if template_source == 'custom'
        if connection['api_token'].blank? || templates_table_id.to_s.strip.empty?
          [[ "Configure API token and Templates table in action config", nil ]]
        else
          call(:pick_templates_from_table, connection, {
            'templates_table_id'      => templates_table_id,
            'template_display_field'  => template_display_field,
            'template_value_field'    => template_value_field,
            'template_content_field'  => template_content_field
          })
        end
      else
        [
          ["Standard RAG",      "standard"],
          ["Customer service",  "customer_service"],
          ["Technical support", "technical"],
          ["Sales inquiry",     "sales"]
        ]
      end
    end,

    file_types: lambda do
      [
        ["PDF", "pdf"], ["Word Document", "docx"], ["Text File", "txt"], ["Markdown", "md"], ["HTML", "html"]
      ]
    end,

    check_types: lambda do
      [
        ["Hash only", "hash"],
        ["Content diff", "content"],
        ["Smart diff", "smart"]
      ]
    end,

    metric_types: lambda do
      [
        ["Response time", "response_time"],
        ["Token usage", "token_usage"],
        ["Cache hit rate", "cache_hit"],
        ["Error rate", "error_rate"],
        ["Throughput", "throughput"]
      ]
    end,

    time_periods: lambda do
      [
        ["Minute", "minute"], ["Hour", "hour"], ["Day", "day"], ["Week", "week"]
      ]
    end,

    optimization_targets: lambda do
      [
        ["Throughput", "throughput"], ["Latency", "latency"], ["Cost", "cost"], ["Accuracy", "accuracy"]
      ]
    end,

    devapi_regions: lambda do
      [
        ["US (www.workato.com)", "www"],
        ["EU (app.eu.workato.com)", "app.eu"],
        ["JP (app.jp.workato.com)", "app.jp"],
        ["SG (app.sg.workato.com)", "app.sg"],
        ["AU (app.au.workato.com)", "app.au"],
        ["IL (app.il.workato.com)", "app.il"]
      ]
    end,

    tables: lambda do |connection|
      if connection['api_token'].blank?
        [[ "Please configure API token in connector connection", nil ]]
      else
        call(:pick_tables, connection)
      end
    end,

    table_columns: lambda do |connection, config = {}|
      cfg = config || {}
      if connection['api_token'].blank?
        [[ "Please configure API token in connector connection", nil ]]
      else
        tbl = (cfg.is_a?(Hash) ? (cfg['custom_rules_table_id'] || cfg[:custom_rules_table_id]) : nil).to_s
        if tbl.empty?
          [[ "Select a Data Table above first", nil ]]
        else
          call(:dt_table_columns, connection, tbl)
        end
      end
  end

  }
}
