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

  # --------- CONNECTION ---------------------------------------------------
  connection: {
    fields: [
      { name: 'service_account_key_json', label: 'Service account JSON key',
        control_type: 'text-area', optional: false,
        hint: 'Paste the full JSON from Google Cloud (includes client_email, private_key, token_uri).' },

      { name: 'quota_project', label: 'Quota/Billing project (optional)',
        optional: true, hint: 'Sets x-goog-user-project if needed for billing/quota.' },

      { name: 'location', optional: true, default: 'global',
        hint: 'Usually global for Vertex AI Search.' },

      { name: 'api_version', control_type: 'select', pick_list: 'api_versions',
        optional: false, default: 'v1',
        hint: 'Use v1 for GA. Use v1alpha if your feature is only in preview.' },

      { name: 'scope', optional: true,
        default: 'https://www.googleapis.com/auth/cloud-platform',
        hint: 'Leave as cloud-platform unless you need narrower scopes.' }
    ],

    base_uri: lambda do |connection|
      loc = (connection['location'].presence || 'global').downcase
      host = if loc == 'global'
               'https://discoveryengine.googleapis.com'
             else
               # Regional vanity host, e.g. us-discoveryengine.googleapis.com
               "https://#{loc}-discoveryengine.googleapis.com"
             end
      host
    end,


    authorization: {
      type: 'custom_auth',

      acquire: lambda do |connection|
        # --- 1) Build a signed JWT assertion ---
        key = JSON.parse(connection['service_account_key_json'] || '{}')
        raise 'Missing client_email/private_key in service_account_key_json' \
          unless key['client_email'] && key['private_key']

        now   = Time.now.to_i
        iat   = now
        exp   = now + 3600
        iss   = key['client_email']
        aud   = 'https://oauth2.googleapis.com/token'   # IMPORTANT: token endpoint
        scope = (connection['scope'] || 'https://www.googleapis.com/auth/cloud-platform')

        header = { alg: 'RS256', typ: 'JWT' }
        claim  = {
          iss: iss,
          scope: scope,
          aud: aud,
          iat: iat,
          exp: exp
        }

        def urlsafe_b64(data)
          Base64.urlsafe_encode64(data).gsub('=', '')
        end

        signing_input = [urlsafe_b64(header.to_json), urlsafe_b64(claim.to_json)].join('.')
        rsa = OpenSSL::PKey::RSA.new(key['private_key'])
        signature = rsa.sign(OpenSSL::Digest::SHA256.new, signing_input)
        assertion = [signing_input, urlsafe_b64(signature)].join('.')

        # --- 2) Exchange for an access token ---
        token_resp = post('https://oauth2.googleapis.com/token')
                       .request_format_www_form_urlencoded
                       .payload(
                         grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                         assertion: assertion
                       ).after_error_response(/.*/) do |code, body, headers, message|
                         error("#{code} acquiring token: #{message}\n#{body}")
                       end

        access_token = token_resp['access_token']
        expires_in   = (token_resp['expires_in'] || 3600).to_i
        {
          access_token: access_token,
          expires_at: (Time.now + expires_in - 60).utc.strftime('%Y-%m-%dT%H:%M:%SZ')
        }
      end,

      apply: lambda do |connection|
        headers('Authorization' => "Bearer #{connection['access_token']}")
        if (qp = connection['quota_project'].to_s.strip).present?
          headers('x-goog-user-project' => qp)
        end
      end,

      refresh_on: [401],
      detect_on:  [401]
    },

    test: lambda do |connection|
      # Cheap auth probe: call public discovery doc *with* auth header attached.
      # If token is bad, Google still returns 401 and triggers refresh_on/detect_on.
      get('/$discovery/rest').params(version: connection['api_version'] || 'v1')
    end
  },

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
      description: 'POST .../servingConfigs/*:search (supports default_search)',
      help: 'Minimal required fields: query.query and userPseudoId',

      input_fields: lambda do |object_definitions, connection|
        [
          { name: 'project_id', optional: false },
          { name: 'location', optional: false, default: (connection['location'] || 'global') },
          { name: 'collection_id', optional: false, default: 'default_collection' },
          { name: 'engine_id', optional: false },
          { name: 'serving_config_id', optional: false, default: 'default_search' },
          # Map console curl fields
          { name: 'query', label: 'Query string' },
          { name: 'pageSize', type: 'integer' },
          { name: 'queryExpansionSpec', type: 'object', properties: [] },
          { name: 'spellCorrectionSpec', type: 'object', properties: [] },
          { name: 'languageCode' },
          { name: 'contentSearchSpec', type: 'object', properties: [
              { name: 'extractiveContentSpec', type: 'object', properties: [] }
            ] },
          { name: 'userInfo', type: 'object', properties: [
              { name: 'timeZone' }
            ] }
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

        # Build body from the explicit fields above
        body = {
          'query' => input['query'],
          'pageSize' => input['pageSize'],
          'queryExpansionSpec' => input['queryExpansionSpec'],
          'spellCorrectionSpec' => input['spellCorrectionSpec'],
          'languageCode' => input['languageCode'],
          'contentSearchSpec' => input['contentSearchSpec'],
          'userInfo' => input['userInfo']
        }.compact
        post(path).payload(body).after_error_response(/.*/) do |code, body, headers, message|
          error("Discovery Engine answer error #{code}: #{message}\n#{body}")
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

      execute: lambda do |_connection, input|
        meth = input['method'].to_s.upcase
        path = input['path'].to_s
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

      output_fields: lambda do |_|
        [{ name: 'raw', type: 'object', properties: [] }]
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
    end
  },

  # --------- METHODS ------------------------------------------------------
  methods: {
    # (kept empty; all helper logic is in authorization#acquire)
  }

}
