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
          { name: 'query', type: 'object', properties: [
              { name: 'query', label: 'Query string' }
            ]},
          { name: 'userPseudoId', label: 'User pseudo ID' },
          { name: 'session', label: 'Session (optional)' },
          { name: 'relatedQuestionsSpec', type: 'object', properties: [] },
          { name: 'safetySpec', type: 'object', properties: [] },
          { name: 'answerGenerationSpec', type: 'object', properties: [] },
          { name: 'searchSpec', type: 'object', properties: [] },
          { name: 'requestParams', type: 'object', properties: [] },
          { name: 'userLabels', type: 'object', properties: [] }
        ]
      end
    },

    answer_response: {
      fields: lambda do |_|
        [
          { name: 'answer', type: 'object', properties: [] },
          { name: 'relatedQuestions', type: 'array', of: 'object', properties: [] },
          { name: 'searchResults', type: 'array', of: 'object', properties: [] },
          { name: 'conversation', type: 'object', properties: [] }
        ]
      end
    },

    search_request: {
      fields: lambda do |_|
        [
          { name: 'query', label: 'Query string' },
          { name: 'pageSize', type: 'integer' },
          { name: 'pageToken' },
          { name: 'userPseudoId' },
          { name: 'params', type: 'object', properties: [] },
          { name: 'filter' },
          { name: 'orderBy' }
        ]
      end
    },

    search_response: {
      fields: lambda do |_|
        [
          { name: 'results', type: 'array', of: 'object', properties: [] },
          { name: 'totalSize', type: 'integer' },
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
      help: 'Required: query.query (string) and userPseudoId (string).',

      input_fields: lambda do |object_definitions, connection|
        [
          { name: 'project_id', optional: false },
          { name: 'location', optional: false, default: (connection['location'] || 'global') },
          { name: 'collection_id', optional: false, default: 'default_collection' },
          { name: 'engine_id', optional: false },
          # IMPORTANT: default_serving_config (NOT default_search)
          { name: 'serving_config_id', optional: false, default: 'default_serving_config' },

          # Answer-shaped inputs
          { name: 'query', type: 'object', properties: [
              { name: 'query', label: 'Query string' }
            ], optional: false },
          { name: 'userPseudoId', optional: false },

          { name: 'answerGenerationSpec', type: 'object', properties: [] },
          { name: 'searchSpec',           type: 'object', properties: [] },
          { name: 'safetySpec',           type: 'object', properties: [] },
          { name: 'requestParams',        type: 'object', properties: [] },
          { name: 'userLabels',           type: 'object', properties: [] }
        ]
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['answer_response']['fields']
      end,

      execute: lambda do |connection, input|
        version = (connection['api_version'] || 'v1')
        path = "/#{version}/projects/#{input['project_id']}/locations/#{input['location']}" \
              "/collections/#{input['collection_id']}/engines/#{input['engine_id']}" \
              "/servingConfigs/#{input['serving_config_id']}:answer"

        # build a clean answer body; avoid Hash#compact for runtime portability
        body = {
          'query'               => input['query'],
          'userPseudoId'        => input['userPseudoId'],
          'answerGenerationSpec'=> input['answerGenerationSpec'],
          'searchSpec'          => input['searchSpec'],
          'safetySpec'          => input['safetySpec'],
          'requestParams'       => input['requestParams'],
          'userLabels'          => input['userLabels']
        }
        # remove nils manually
        body.delete_if { |_k, v| v.nil? }

        post(path).payload(body).after_error_response(/.*/) do |code, body_txt, headers, message|
          error("Discovery Engine answer error #{code}: #{message}\n#{body_txt}")
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
        ] + object_definitions['search_request']['fields']
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['search_response']['fields']
      end,

      execute: lambda do |connection, input|
        version = (connection['api_version'] || 'v1')
        path = "/#{version}/projects/#{input['project_id']}/locations/#{input['location']}" \
               "/collections/#{input['collection_id']}/engines/#{input['engine_id']}" \
               "/servingConfigs/#{input['serving_config_id']}:search"

        body = input.reject { |k, _| %w[project_id location collection_id engine_id serving_config_id].include?(k) }
        post(path).payload(body).after_error_response(/.*/) do |code, body, headers, message|
          error("Discovery Engine search error #{code}: #{message}\n#{body}")
        end
      end,

      sample_output: lambda do
        {
          'results' => [
            { 'id' => 'doc-1', 'document' => { 'id' => 'doc-1', 'structData' => {} } }
          ],
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
          { name: 'path', optional: false, hint: 'e.g. /v1/projects/.../something' },
          { name: 'query', type: 'object', properties: [], hint: 'Optional query params' },
          { name: 'body',  type: 'object', properties: [], hint: 'Optional JSON body for POST/PATCH' }
        ]
      end,

      output_fields: lambda do |_|
        [{ name: 'raw', type: 'object', properties: [] }]
      end,

      execute: lambda do |_connection, input|
        # Pull doc & flatten methods
        doc = call(:discovery_get_rest_doc, input['api'], input['version'])
        methods = call(:discovery_flatten_rest_methods, doc)
        # Find the selected method
        target = methods.find { |m| (m['name'] || '') == (input['method_name'] || '') }
        error("Unknown method #{input['method_name']} for #{input['api']}:#{input['version']}") unless target
        http_method = (target['httpMethod'] || 'GET').upcase
        path = "/#{input['version']}#{target['path']}" # discovery doc paths omit leading version
        req = case http_method
              when 'GET'    then get(path).params(input['params'] || {})
              when 'DELETE' then delete(path).params(input['params'] || {})
              when 'POST'   then post(path).payload(input['body'] || {}).params(input['params'] || {})
              when 'PATCH'  then patch(path).payload(input['body'] || {}).params(input['params'] || {})
              when 'PUT'    then put(path).payload(input['body'] || {}).params(input['params'] || {})
              else error("Unsupported method #{http_method}")
              end
        req.after_error_response(/.*/) { |code, body, _h, msg| error("Raw call #{code}: #{msg}\n#{body}") }
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
    end

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
