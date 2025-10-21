# frozen_string_literal: true
require 'openssl'
require 'base64'
require 'json'
require 'time'
require 'securerandom'
require 'uri'

{
  title: 'Google Discovery Engine',
  version: '0.1.0',
  description: 'Direct access to Discovery Engine (Vertex AI Search / Agent Builder) via service account',
  
  # Discovery Engine 
  # REST API (https://cloud.google.com/generative-ai-app-builder/docs/reference/rest)
  # RPC API (https://cloud.google.com/generative-ai-app-builder/docs/reference/rpc)
  # Method: projects.locations.collections.engines.serviceConfigs.search (https://cloud.google.com/generative-ai-app-builder/docs/reference/rest/v1/projects.locations.collections.engines.servingConfigs/search)

  # IAM Access Control (https://cloud.google.com/generative-ai-app-builder/docs/access-control)
  # Discovery documents for Google APIs (https://developers.google.com/discovery/v1/reference/apis)


  # --------- CONNECTION ---------------------------------------------------
  connection: {
    fields: [
      {
        name: 'service_account_key_json',
        label: 'Service account JSON key',
        control_type: 'text-area',
        optional: false,
        hint: 'Paste full key JSON (client_email, private_key, token_uri).'
      },
      {
        name: 'quota_project',
        label: 'Quota/Billing project (optional)',
        optional: true,
        hint: 'Sets x-goog-user-project if needed.'
      },
      {
        name: 'location',
        label: 'Location',
        optional: true,
        hint: 'global or a region like us; affects base URI',
        default: 'global'
      },
      {
        name: 'api_version',
        label: 'API version',
        control_type: 'select',
        optional: false,
        default: 'v1',
        options: [
          %w[v1 v1],
          %w[v1alpha v1alpha]
        ]
      },
      {
        name: 'scope',
        label: 'OAuth scope',
        optional: true,
        default: 'https://www.googleapis.com/auth/cloud-platform',
        hint: 'Keep cloud-platform unless you need narrower.'
      }
    ],

    base_uri: lambda do |connection|
      loc = (connection['location'] || 'global').to_s.strip.downcase
      if loc == '' || loc == 'global'
        'https://discoveryengine.googleapis.com'
      else
        "https://#{loc}-discoveryengine.googleapis.com"
      end
    end,

    authorization: {
      type: 'custom_auth',

      acquire: lambda do |connection|
        key_json = (connection['service_account_key_json'] || '').strip
        raise 'Missing service_account_key_json' if key_json == ''

        key = JSON.parse(key_json)
        client_email = (key['client_email'] || '').strip
        private_key  = (key['private_key']  || '').strip
        raise 'Key JSON missing client_email' if client_email == ''
        raise 'Key JSON missing private_key'  if private_key  == ''

        now = Time.now.to_i
        claim = {
          iss: client_email,
          scope: (connection['scope'] || 'https://www.googleapis.com/auth/cloud-platform'),
          aud: 'https://oauth2.googleapis.com/token',
          iat: now,
          exp: now + 3600
        }

        header = { alg: 'RS256', typ: 'JWT' }
        enc = lambda { |obj| Base64.urlsafe_encode64(obj.to_json).gsub('=', '') }
        signing_input = "#{enc.call(header)}.#{enc.call(claim)}"

        rsa = OpenSSL::PKey::RSA.new(private_key)
        signature = rsa.sign(OpenSSL::Digest::SHA256.new, signing_input)
        assertion = "#{signing_input}.#{Base64.urlsafe_encode64(signature).gsub('=', '')}"

        token_resp = post('https://oauth2.googleapis.com/token')
                      .request_format_www_form_urlencoded
                      .payload(
                        grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                        assertion: assertion
                      )
                      .after_error_response(/.*/) { |code, body, _h, msg| error("Token error #{code}: #{msg}\n#{body}") }

        access_token = (token_resp['access_token'] || '').to_s
        expires_in   = (token_resp['expires_in'] || 3600).to_i
        raise 'No access_token in token response' if access_token == ''

        # Workato expects these keys for custom_auth
        {
          access_token: access_token,
          expires_at: (Time.now + expires_in - 60).utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
      end,

      apply: lambda do |connection|
        headers('Authorization' => "Bearer #{connection['access_token']}")
        qp = (connection['quota_project'] || '').to_s.strip
        headers('x-goog-user-project' => qp) unless qp == ''
      end,

      refresh_on: [401],
      detect_on:  [401]
    }
  },

  test: lambda do |connection|
    ver = (connection['api_version'] || 'v1').to_s
    get('https://discoveryengine.googleapis.com/$discovery/rest')
      .params(version: ver)
      .after_error_response(/.*/) { |code, body, _h, msg| error("Auth probe #{code}: #{msg}\n#{body}") }
  end,

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {

    answer_request: {
      fields: lambda do |_|
        [
          { name: 'query', type: 'object', display_priority: 10, properties: [
              { name: 'text', label: 'Query text', hint: 'Required. The natural-language question.',
                sticky: true, display_priority: 1 }
            ], optional: false },
          { name: 'userPseudoId', label: 'User pseudo ID', hint: 'Stable, non-PII, ≤128 chars.',
            sticky: true, display_priority: 11, optional: false },
          { name: 'session' },
          { name: 'relatedQuestionsSpec',   type: 'object', properties: [] },
          { name: 'safetySpec',             type: 'object', properties: [] },
          { name: 'answerGenerationSpec',   type: 'object', properties: [] },
          { name: 'searchSpec',             type: 'object', properties: [] },
          { name: 'groundingSpec',          type: 'object', properties: [] },
          { name: 'queryUnderstandingSpec', type: 'object', properties: [] },
          { name: 'endUserSpec',            type: 'object', properties: [] },
          { name: 'userLabels',             type: 'object', properties: [] }
        ]
      end
    },

    answer_response: {
      fields: lambda do |_|
        [
          { name: 'answer',           type: 'object', properties: [] },
          { name: 'relatedQuestions', type: 'array',  of: 'object', properties: [] },
          { name: 'searchResults',    type: 'array',  of: 'object', properties: [] },
          { name: 'conversation',     type: 'object', properties: [] }
        ]
      end
    },

    search_request: {
      fields: lambda do |_|
        [
          { name: 'query', type: 'string', hint: 'Plain string search query.' },
          { name: 'pageSize',   type: 'integer' },
          { name: 'pageToken' },
          { name: 'userPseudoId' },
          { name: 'params',     type: 'object', properties: [] },
          { name: 'filter' },
          { name: 'orderBy' }
        ]
      end
    },

    search_response: {
      fields: lambda do |_|
        [
          { name: 'results',         type: 'array', of: 'object', properties: [] },
          { name: 'totalSize',       type: 'integer' },
          { name: 'attributionToken' },
          { name: 'nextPageToken' }
        ]
      end
    }

  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {
    serving_configs_answer: {
      title: 'servingConfigs:answer',
      subtitle: 'Answer over an Engine serving config',
      description: 'POST .../servingConfigs/*:answer',
      help: 'Required: query.text (string) and userPseudoId (string). Simple mode shows only the essentials; Advanced mode reveals optional specs as raw objects with templates and validation.',

      input_fields: lambda do |object_definitions, connection|
        base = [
          { name: 'project_id', optional: false, sticky: true, display_priority: 1, hint: 'GCP Project ID (not number).' },
          { name: 'location', optional: false, default: (connection['location'] || 'global'), hint: 'Discovery Engine location (e.g., global, us, eu).', sticky: true, display_priority: 2 },
          { name: 'collection_id', optional: false, default: 'default_collection', sticky: true, display_priority: 3 },
          { name: 'engine_id', optional: false, sticky: true, display_priority: 4 },
          { name: 'serving_config_id', optional: false, default: 'default_serving_config', sticky: true, display_priority: 5,
            hint: 'Usually default_serving_config unless you created a custom one.' },
          # UX toggle
          { name: 'simple_mode', label: 'Simple mode', type: 'boolean', control_type: 'checkbox',
            default: true, sticky: true, display_priority: 6,
            hint: 'Checked: only the essentials. Uncheck to show all advanced spec objects.' },
          # Simple-mode flats (we’ll map these into the proper body in execute)
          { name: 'query_text', label: 'Question', optional: false, display_condition: "simple_mode",
            hint: 'Plain text question. Will be sent as query.text.' , display_priority: 7 },
          { name: 'user_pseudo_id', label: 'User pseudo ID', optional: false, display_condition: "simple_mode",
            hint: 'Stable, opaque, non-PII. ≤128 chars. Used for sessioning/metrics.', display_priority: 8 },
        ]
        # Advanced fields appear only when simple_mode is false
        base + object_definitions['answer_request'].map { |f| f.merge(display_condition: "!simple_mode") }
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['answer_response']
      end,
      execute: lambda do |connection, input|
        version = (connection['api_version'] || 'v1')
        path = "/#{version}/projects/#{input['project_id']}/locations/#{input['location']}" \
              "/collections/#{input['collection_id']}/engines/#{input['engine_id']}" \
              "/servingConfigs/#{input['serving_config_id']}:answer"

        # Update the preview field for visibility in UI (best-effort; doesn’t affect request)
        input['path_preview'] = path

        # ---- Validation (fail fast with helpful messages)
        # Path param guards (fail fast with actionable errors)
        %w[project_id location collection_id engine_id serving_config_id].each do |reqk|
          error("#{reqk} is required.") if (input[reqk] || '').to_s.strip.empty?
        end

        if input['simple_mode']
          qt = (input['query_text'] || '').to_s.strip
          up = (input['user_pseudo_id'] || '').to_s
          error('Question is required (query_text).') if qt.empty?
          error('user_pseudo_id is required.') if up.empty?
          error('user_pseudo_id must be ≤ 128 characters.') if up.length > 128
        else
          # Advanced path: require query.text and userPseudoId
          qobj = input['query'] || {}
          qtxt = (qobj['text'] || '').to_s.strip
          up   = (input['userPseudoId'] || '').to_s
          error('query.text is required.') if qtxt.empty?
          error('userPseudoId is required.') if up.empty?
          error('userPseudoId must be ≤ 128 characters.') if up.length > 128
        end
        body = input.reject { |k,_|
          %w[project_id location collection_id engine_id serving_config_id simple_mode
             query_text user_pseudo_id].include?(k)
        }

        # Map simple-mode flats into canonical structure when present
        if input['simple_mode']
          body['query']        = { 'text' => input['query_text'] }
          body['userPseudoId'] = input['user_pseudo_id']
        end
        body.delete_if { |_k, v| v.nil? }

        post(path).payload(body).after_error_response(/.*/) do |code, body_txt, _h, msg|
          # Normalize common 4xx into actionable hints
          friendly =
            if code.to_i == 400 && body_txt.to_s.include?('Unknown name "query"')
              'Body must use query.text (not query.query).'
            elsif code.to_i == 400 && body_txt.to_s.include?('userPseudoId')
              'userPseudoId is required and must be ≤ 128 chars.'
            else
              nil
            end
          err = "Discovery Engine answer error #{code}: #{msg}"
          err += " — #{friendly}" if friendly
          err += "\n#{body_txt}"
          error(err)
        end
      end,
      sample_output: lambda do
        {
          'answer' => { 'summary' => { 'text' => '...' } },
          'relatedQuestions' => [],
          'searchResults' => []
        }
      end
    },

    serving_configs_search: {
      title: 'servingConfigs:search',
      subtitle: 'Search over an Engine serving config',
      description: 'POST .../servingConfigs/*:search',

      input_fields: lambda do |object_definitions, connection|
        [
          { name: 'project_id', optional: false },
          { name: 'location', optional: false, default: (connection['location'] || 'global') },
          { name: 'collection_id', optional: false, default: 'default_collection' },
          { name: 'engine_id', optional: false },
          { name: 'serving_config_id', optional: false, default: 'default_serving_config' }
        ] + object_definitions['search_request']
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['search_response']
      end,
      execute: lambda do |connection, input|
        version = (connection['api_version'] || 'v1')
        path = "/#{version}/projects/#{input['project_id']}/locations/#{input['location']}" \
              "/collections/#{input['collection_id']}/engines/#{input['engine_id']}" \
              "/servingConfigs/#{input['serving_config_id']}:search"

        body = input.reject { |k,_|
          %w[project_id location collection_id engine_id serving_config_id].include?(k)
        }
        post(path).payload(body).after_error_response(/.*/) do |code, body_txt, _h, msg|
          error("Discovery Engine search error #{code}: #{msg}\n#{body_txt}")
        end
      end,
      sample_output: lambda do
        {
          'results' => [{ 'id' => 'doc-1', 'document' => { 'id' => 'doc-1', 'structData' => {} } }],
          'totalSize' => 1,
          'nextPageToken' => nil
        }
      end
    },

    raw_request: {
      title: 'RAW Discovery Engine request',
      subtitle: 'For unsupported endpoints (advanced)',
      description: 'Send any HTTP request to discoveryengine.googleapis.com',
      help: 'Path must start with /v1 or /v1alpha. Body is JSON.',

      input_fields: lambda do |_|
        [
          { name: 'method', control_type: 'select', pick_list: 'http_methods', optional: false, default: 'GET' },
          # Let the user choose a method name from the Discovery doc; we’ll map it to path/verb below
          { name: 'method_name', label: 'Discovery method name', control_type: 'select', pick_list: 'de_method_names',
            optional: true, hint: 'Ex: projects.locations.collections.engines.servingConfigs.search' },
          # Manual override still supported
          { name: 'path', optional: true, hint: 'e.g. /v1/projects/.../something (overrides method_name)' },

          { name: 'query', type: 'object', properties: [], hint: 'Optional query params' },
          { name: 'body',  type: 'object', properties: [], hint: 'Optional JSON body for POST/PATCH' }
        ]
      end,

      output_fields: lambda do |_|
        [{ name: 'raw', type: 'object', properties: [] }]
      end,

      execute: lambda do |connection, input|
          meth = (target['httpMethod'] || meth || 'GET').to_s.upcase
          # Discovery 'path' already includes the version prefix (e.g., "v1/projects/...").
          # Just ensure it starts with a single leading slash.
          tpath = (target['path'] || '').to_s
          tpath = tpath.start_with?('/') ? tpath : "/#{tpath}"
          path  = tpath

        if path == '' && (mn = (input['method_name'] || '').to_s) != ''
          # Resolve method_name -> (httpMethod, path) via Discovery doc
          api = 'discoveryengine'
          ver = (connection['api_version'] || 'v1').to_s
          doc = call(:discovery_get_rest_doc, api, ver)
          methods = call(:discovery_flatten_rest_methods, doc)
          target = methods.find { |m| (m['name'] || '') == mn }
          error("Unknown method_name #{mn} for #{api}:#{ver}") unless target
          # Prefer the verb from the doc; caller’s "method" remains a manual override if they set it explicitly
          meth = (target['httpMethod'] || meth || 'GET').to_s.upcase
          path = "/#{ver}#{(target['path'] || '').to_s.sub(/\A\//, '')}"
        end

        error('Path must start with /v1 or /v1alpha') unless path.start_with?('/v1')
        request = case meth
                  when 'GET'    then get(path).params(input['query'] || {})
                  when 'DELETE' then delete(path).params(input['query'] || {})
                  when 'POST'   then post(path).payload(input['body'] || {}).params(input['query'] || {})
                  when 'PATCH'  then patch(path).payload(input['body'] || {}).params(input['query'] || {})
                  when 'PUT'    then put(path).payload(input['body'] || {}).params(input['query'] || {})
                  else error("Unsupported method #{meth}")
                  end
        request.after_error_response(/.*/) do |code, body, _headers, message|
          error("Discovery Engine raw error #{code}: #{message}\n#{body}")
        end
      end,

      sample_output: lambda do
        { 'raw' => { 'ok' => true } }
      end
    }
  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {
    api_versions: lambda do |_connection|
      [
        %w[v1 v1],
        %w[v1alpha v1alpha]
      ]
    end,

    http_methods: lambda do |_|
      %w[GET POST PATCH PUT DELETE].map { |m| [m, m] }
    end,

    discoveryengine_versions: lambda do |_connection|
      begin
        items = call(:discovery_list_apis, 'discoveryengine', nil)
        items.map { |i| [i['version'], i['version']] }
      rescue => e
        # Safe fallback if directory is unreachable
        [['v1', 'v1'], ['v1alpha', 'v1alpha']]
      end
    end,

    # Live list of Discovery Engine REST methods from the Google Discovery doc
    de_method_names: lambda do |connection|
      api  = 'discoveryengine'
      ver  = (connection['api_version'] || 'v1').to_s
      begin
        doc     = call(:discovery_get_rest_doc, api, ver)
        methods = call(:discovery_flatten_rest_methods, doc)
        # Return [["projects.locations...servingConfigs.search", same], ...]
        out = methods.map { |m| name = (m['name'] || '').to_s; [name, name] }
        out = out.sort_by { |a| a[0] }
        # Fallback if empty
        out.empty? ? [['projects.locations.collections.engines.servingConfigs.search',
                       'projects.locations.collections.engines.servingConfigs.search']] : out
      rescue => e
        # Safe static fallback if Discovery Directory is unreachable
        [
          ['projects.locations.collections.engines.servingConfigs.search',
           'projects.locations.collections.engines.servingConfigs.search'],
          ['projects.locations.collections.engines.servingConfigs.answer',
           'projects.locations.collections.engines.servingConfigs.answer']
        ]
      end
    end,

  },

  # --------- METHODS ------------------------------------------------------
  methods: {

    # --- API DISCOVERY SERVICE --------------------------------------------
    discovery_list_apis: lambda do |name_filter = nil, preferred = nil|
      # List Google APIs (Discovery Directory)
      params = {}
      params['preferred'] = true if preferred
      resp = get('https://www.googleapis.com/discovery/v1/apis').params(params)
      items = resp['items'] || []
      nf = (name_filter || '').to_s.strip
      if nf != ''
        items = items.select { |i| ((i['name'] || '').to_s.include?(nf)) }
      end
      items
    end,
    discovery_get_rest_doc: lambda do |api, version|
      # Get REST discovery document for {api}/{version}
      a = (api || '').to_s.strip
      v = (version || '').to_s.strip
      raise 'api required' if a == ''
      raise 'version required' if v == ''
      get("https://www.googleapis.com/discovery/v1/apis/#{a}/#{v}/rest")
    end,
    discovery_get_rpc_doc: lambda do |api, version|
      # Get RPC discovery document for {api}/{version}
      a = (api || '').to_s.strip
      v = (version || '').to_s.strip
      raise 'api required' if a == ''
      raise 'version required' if v == ''
      get("https://www.googleapis.com/discovery/v1/apis/#{a}/#{v}/rpc")
    end,
    discovery_flatten_rest_methods: lambda do |rest_doc|
      # Flatten REST doc -> list of methods with fully-qualified paths & verbs
      # Returns: [{ "name"=>"projects.locations...servingConfigs.search", "httpMethod"=>"POST", "path"=>"/v1/..." }, ...]
      results = []
      stack = []
      # DFS walk resources
      walker = lambda do |prefix, res|
        # methods at this level
        meths = res['methods'] || {}
        meths.each do |mname, mdef|
          fq_name = (prefix == '' ? mname : "#{prefix}.#{mname}")
          results << {
            'name' => fq_name,
            'httpMethod' => (mdef['httpMethod'] || ''),
            'path' => (mdef['path'] || ''),
            'parameters' => (mdef['parameters'] || {})
          }
        end
        # descend into child resources
        (res['resources'] || {}).each do |rname, rdef|
          new_prefix = (prefix == '' ? rname : "#{prefix}.#{rname}")
          walker.call(new_prefix, rdef)
        end
      end
      walker.call('', rest_doc['resources'] || {})
      results
    end,    
    discovery_preferred_version: lambda do |api_name|
      # Convenience: get preferred version for a given API name
      items = call(:discovery_list_apis, api_name, true) # preferred only
      # If none marked preferred, fall back to first matching
      if items.length == 0
        items = call(:discovery_list_apis, api_name, nil)
      end
      items.length > 0 ? (items.first['version'] || '') : ''
    end

  }

}
