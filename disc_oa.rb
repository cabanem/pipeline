# frozen_string_literal: true
require 'json'
require 'time'

{
  title: 'Discovery→OpenAPI Converter',
  version: '0.1.0',
  description: 'Convert a Google API Discovery document into an OpenAPI 3.x spec',

  connection: {
    fields: [
      { name: 'prod_mode', label: 'Production mode', optional: true, type: 'boolean',
        control_type: 'checkbox', default: true,
        hint: 'When enabled, reduces debug echoes.' }
    ],

    test: lambda do |_connection|
      # stateless; nothing to call
      { success: true }
    end
  },

  actions: {
    discovery_to_openapi: {
      title: 'Convert Discovery → OpenAPI',
      subtitle: 'Generate OpenAPI 3.x spec from a Google Discovery doc',
      help: 'Provide either a raw Discovery JSON or a URL to the discovery document.',
      input_fields: lambda do
        [
          { name: 'discovery_doc_json',       label: 'Discovery document (JSON)', control_type: 'text-area',  optional: true,
            hint: 'Paste the raw Google Discovery document JSON here.' },
          { name: 'discovery_doc_url',        label: 'Discovery document URL',    control_type: 'text',       optional: true,
            hint: 'Example: https://www.googleapis.com/discovery/v1/apis/gmail/v1/rest' },
          { name: 'openapi_version',          label: 'OpenAPI version',           control_type: 'select',     optional: false,
            pick_list: 'openapi_versions', default: '3.0.3' },

          { name: 'override_title',           label: 'Override spec title',                                   optional: true },
          { name: 'override_version',         label: 'Override spec version',                                 optional: true },
          { name: 'server_url',               label: 'Override server URL',                                   optional: true,
            hint: 'If provided, this replaces the derived rootUrl+servicePath.' },
          { name: 'include_internal_schemas', label: 'Include all schemas',       control_type: 'checkbox',   optional: true,
            type: 'boolean', default: false }
        ]
      end,
      output_fields: lambda do |_object_definitions|
        [
          { name: 'started_at' },
          { name: 'duration_ms', type: 'integer' },
          { name: 'openapi_version' },
          { name: 'title' },
          { name: 'version' },
          { name: 'server' },
          { name: 'spec', type: 'object', properties: [
              { name: 'openapi' },
              { name: 'info', type: 'object' },
              { name: 'servers', type: 'array', of: 'object' },
              { name: 'paths', type: 'object' },
              { name: 'components', type: 'object' }
            ]
          },
          { name: 'openapi_json', label: 'OpenAPI (JSON string)' },
          { name: 'validation', type: 'object', properties: [
            { name: 'passed', type: 'boolean' },
            { name: 'error_count', type: 'integer' },
            { name: 'warning_count', type: 'integer' },
            { name: 'errors', type: 'array', of: 'string' },
            { name: 'warnings', type: 'array', of: 'string' }
          ]}
        ]
      end,
      execute: lambda do |_connection, input|
        t0 = Time.now

        doc =
          if input['discovery_doc_json'].present? && input['discovery_doc_url'].present?
            error('Provide either discovery_doc_json OR discovery_doc_url, not both.')
          elsif input['discovery_doc_json'].present?
            JSON.parse(input['discovery_doc_json'])
          elsif input['discovery_doc_url'].present?
            get(input['discovery_doc_url'])
          else
            error('You must provide discovery_doc_json or discovery_doc_url.')
          end

        # ---- Build OpenAPI skeleton
        openapi_version = (input['openapi_version'].presence || '3.0.3')
        title           = input['override_title'].presence  || doc['title'] || doc['name'] || 'API'
        api_version     = input['override_version'].presence || doc['version'] || 'v1'
        root_url        = (input['server_url'].presence || call(:derive_server_url, doc))

        spec = {
          'openapi' => openapi_version.start_with?('3.') ? openapi_version : '3.0.3',
          'info'    => {
            'title'       => title,
            'version'     => api_version,
            'description' => doc['description']
          },
          'servers' => root_url ? [{ 'url' => root_url }] : [],
          'paths'   => {},
          'components' => {
            'schemas'         => {},
            'securitySchemes' => {}
          }
        }

        # ---- Security (OAuth2 scopes → securitySchemes)
        call(:inject_security_schemes!, spec, doc)

        # ---- Schemas (Discovery → components.schemas)
        call(:inject_schemas!, spec, doc, include_all: !!input['include_internal_schemas'])

        # ---- Paths (resources + top-level methods)
        call(:inject_paths!, spec, doc)

        # Cleanup
        spec = call(:deep_compact, spec)

        # Validate
        validation = call(:validate_openapi!, spec, openapi_version)

        {
          started_at: t0.iso8601,
          duration_ms: ((Time.now - t0) * 1000.0).round,
          openapi_version: spec['openapi'],
          title: spec.dig('info', 'title'),
          version: spec.dig('info', 'version'),
          server: spec.dig('servers', 0, 'url'),
          validation: validation,
          spec: spec,
          openapi_json: JSON.pretty_generate(spec)
        }

      end,
      sample_output: lambda do
        {
          started_at: Time.now.iso8601,
          duration_ms: 5,
          openapi_version: '3.0.3',
          title: 'Sample API',
          version: 'v1',
          server: 'https://example.googleapis.com/',
          spec: {
            openapi: '3.0.3',
            info: { title: 'Sample API', version: 'v1' },
            servers: [{ url: 'https://example.googleapis.com/' }],
            paths: {
              '/v1/things/{thingId}' => {
                get: {
                  operationId: 'sample.getThing',
                  parameters: [
                    { name: 'thingId', in: 'path', required: true, schema: { type: 'string' } }
                  ],
                  responses: {
                    '200' => {
                      description: 'OK',
                      content: { 'application/json' => { schema: { '$ref' => '#/components/schemas/Thing' } } }
                    }
                  }
                }
              }
            },
            components: {
              schemas: {
                Thing: { type: 'object', properties: { id: { type: 'string' } } }
              },
              securitySchemes: {
                oauth2: {
                  type: 'oauth2',
                  flows: {
                    authorizationCode: {
                      authorizationUrl: 'https://accounts.google.com/o/oauth2/auth',
                      tokenUrl: 'https://oauth2.googleapis.com/token',
                      scopes: { 'scopeA' => 'scopeA' }
                    }
                  }
                }
              }
            }
          },
          openapi_json: '{...}',
          validation: {
            passed: true,
            error_count: 0,
            warning_count: 1,
            errors: [],
            warnings: ["No global securitySchemes present (optional)."]
          }
        }
      end
    },
    validate_openapi: {
      title: 'Validate OpenAPI 3.x',
      subtitle: 'Run structural checks on an OpenAPI 3.x document',
      help: 'Provide either the OpenAPI JSON string or an object.',
      input_fields: lambda do
        [
          { name: 'openapi_json', label: 'OpenAPI (JSON string)', control_type: 'text-area', optional: true },
          { name: 'openapi_obj',  label: 'OpenAPI (object)', type: 'object', optional: true },
          { name: 'expect_version', label: 'Expected version prefix', hint: 'e.g., 3.0 or 3.1', optional: true, default: '3.' }
        ]
      end,
      execute: lambda do |_connection, input|
        spec =
          if input['openapi_obj'].present?
            input['openapi_obj']
          elsif input['openapi_json'].present?
            JSON.parse(input['openapi_json'])
          else
            error('Provide openapi_json or openapi_obj.')
          end

        validation = call(:validate_openapi!, spec, (input['expect_version'].presence || '3.'))
        {
          openapi_version: spec['openapi'],
          title: spec.dig('info', 'title'),
          version: spec.dig('info', 'version'),
          server: spec.dig('servers', 0, 'url'),
          validation: validation
        }
      end,
      output_fields: lambda do
        [
          { name: 'openapi_version' },
          { name: 'title' },
          { name: 'version' },
          { name: 'server' },
          { name: 'validation', type: 'object', properties: [
            { name: 'passed', type: 'boolean' },
            { name: 'error_count', type: 'integer' },
            { name: 'warning_count', type: 'integer' },
            { name: 'errors', type: 'array', of: 'string' },
            { name: 'warnings', type: 'array', of: 'string' }
          ]}
        ]
      end,
      sample_output: lambda do
        {
          openapi_version: '3.0.3',
          title: 'Sample API',
          version: 'v1',
          server: 'https://example.googleapis.com/',
          validation: {
            passed: true,
            error_count: 0,
            warning_count: 1,
            errors: [],
            warnings: ['No components.schemas found (optional).']
          }
        }
      end
    }

  },

  pick_lists: {
    openapi_versions: lambda do |_|
      [
        %w[3.0.3 3.0.3],
        %w[3.1.0 3.1.0]
      ]
    end
  },

  methods: {

    derive_server_url: lambda do |doc|
      root = doc['rootUrl'].to_s.strip
      svc  = doc['servicePath'].to_s.strip
      raw = if root.present? && svc.present?
              "#{root}#{svc}"
            elsif root.present?
              root
            else
              nil
            end
      call(:normalize_url_like, raw)
    end,

    inject_security_schemes!: lambda do |spec, doc|
      auth = doc.dig('auth', 'oauth2')
      return unless auth

      scopes = auth['scopes']
      scopes = scopes.is_a?(Hash) ? scopes : {}

      flows = {
        'authorizationCode' => {
          'authorizationUrl' => 'https://accounts.google.com/o/oauth2/auth',
          'tokenUrl'         => 'https://oauth2.googleapis.com/token',
          'scopes'           => scopes.transform_values { |v| v.is_a?(String) ? v : v.to_s }
        }
      }

      spec['components']['securitySchemes']['oauth2'] = {
        'type'  => 'oauth2',
        'flows' => flows
      }
    end,

    inject_schemas!: lambda do |spec, doc, include_all: false|
      schemas = doc['schemas'] || {}
      return if schemas.empty?

      # Google Discovery schemas resemble JSON Schema draft 03/04; we keep it simple.
      components = {}
      schemas.each do |name, schema|
        components[name] = call(:translate_schema, schema)
      end

      spec['components']['schemas'] = components

      unless include_all
        # Optional: prune unused schemas later if you add reference tracking.
      end
    end,

    translate_schema: lambda do |schema|
      return { 'type' => 'object' } unless schema.is_a?(Hash)

      t = {}
      t['type'] = schema['type'] if schema['type']
      t['description'] = schema['description'] if schema['description']

      if schema['enum']
        t['enum'] = schema['enum']
      end

      if schema['properties'].is_a?(Hash)
        t['type'] = 'object' unless t['type']
        t['properties'] = {}
        schema['properties'].each do |pname, pschema|
          t['properties'][pname] = call(:translate_schema, pschema)
        end
      end

      if schema['items']
        t['type'] = 'array'
        t['items'] = call(:translate_schema, schema['items'])
      end

      if schema['required'].is_a?(Array)
        t['required'] = schema['required']
      end

      if schema['$ref']
        # Discovery local refs often look like "SchemaName"
        t = { '$ref' => "#/components/schemas/#{schema['$ref']}" }
      end

      t
    end,

    inject_paths!: lambda do |spec, doc|
      paths = spec['paths']
      meths = call(:collect_methods, doc) +
              call(:collect_resource_methods, (doc['resources'] || {}))
      meths.each { |m| call(:add_method_to_paths!, paths, m, doc) }
    end,

    collect_methods: lambda do |doc|
      (doc['methods'] || {}).map do |method_id, method|
        { 'methodId' => method_id }.merge(method || {})
      end
    end,

    collect_resource_methods: lambda do |resources|
      out = []
      return out unless resources.is_a?(Hash)
      resources.each do |_rname, robj|
        (robj['methods'] || {}).each do |mid, mobj|
          out << ({ 'methodId' => mid }.merge(mobj || {}))
        end
        out.concat(call(:collect_resource_methods, robj['resources'])) if robj['resources'].is_a?(Hash)
      end
      out
    end,

    add_method_to_paths!: lambda do |paths, m, doc|
      http_method = (m['httpMethod'] || 'GET').downcase
      raw_path    = m['path'] || m['id'] || ''
      path        = call(:normalize_path, raw_path)
      path        = "/#{path}" unless path.start_with?('/')

      paths[path] ||= {}

      op_id = if doc['name'] && m['methodId']
                "#{doc['name']}.#{m['methodId']}"
              else
                m['id'] || m['methodId'] || "#{http_method}#{path.gsub(%r{[^a-zA-Z0-9_]}, '_')}"
              end

      # Parameters
      params = call(:translate_parameters, m['parameters'])

      # Ensure all templated {vars} in path are present as path params (required)
      template_vars = path.scan(/\{([^}]+)\}/).flatten
      template_vars.each do |v|
        unless params.any? { |p| p['in'] == 'path' && p['name'] == v }
          params << { 'name' => v, 'in' => 'path', 'required' => true, 'schema' => { 'type' => 'string' } }
        end
      end

      # Request body
      request_body = call(:translate_request_body, m['request'])

      # Responses (basic 200 mapping)
      responses = call(:translate_responses, m['response'])

      operation = call(:hcompact, {
        'operationId' => op_id,
        'description' => m['description'],
        'parameters'  => params.empty? ? nil : call(:dedupe_parameters, params),
        'requestBody' => request_body,
        'responses'   => responses
      })

      paths[path][http_method] = operation
    end,

    normalize_path: lambda do |p|
      (p || '').strip.gsub(%r{^/+}, '').gsub(%r{/+}, '/')
    end,

    translate_parameters: lambda do |params|
      out = []
      (params || {}).each do |name, p|
        location = p['location'].to_s
        location = 'query' unless %w[path query header].include?(location) # default to query

        out << call(:hcompact, {
          'name'        => name,
          'in'          => location,
          'required'    => !!p['required'],
          'description' => p['description'],
          'schema'      => call(:param_schema_from_discovery, p)
        })
      end
      out
    end,

    param_schema_from_discovery: lambda do |p|
      # map basic types
      t = p['type'] || 'string'
      fmt = p['format']
      h = { 'type' => t }
      h['format'] = fmt if fmt
      if p['enum']
        h['enum'] = p['enum']
      end
      h
    end,

    translate_request_body: lambda do |req|
      return nil unless req.is_a?(Hash)
      schema = call(:schema_ref_or_inline, req)
      return nil unless schema

      {
        'required' => true,
        'content'  => {
          'application/json' => {
            'schema' => schema
          }
        }
      }
    end,

    translate_responses: lambda do |resp|
      # Simple 200 response with schema if present
      schema = call(:schema_ref_or_inline, resp)
      content = schema ? { 'application/json' => { 'schema' => schema } } : nil
      call(:hcompact, {
        '200' => call(:hcompact, {
          'description' => 'OK',
          'content'     => content
        })
      })
    end,

    schema_ref_or_inline: lambda do |obj|
      return nil unless obj.is_a?(Hash)
      if obj['$ref']
        { '$ref' => "#/components/schemas/#{obj['$ref']}" }
      elsif obj['schema'].is_a?(Hash)
        call(:translate_schema, obj['schema'])
      else
        nil
      end
    end,

    dedupe_parameters: lambda do |params|
      seen = {}
      params.each_with_object([]) do |p, acc|
        key = "#{p['in']}:#{p['name']}"
        next if seen[key]
        seen[key] = true
        acc << p
      end
    end,

    deep_compact: lambda do |obj|
      case obj
      when Hash
        obj.each_with_object({}) do |(k, v), h|
          nv = call(:deep_compact, v)
          h[k] = nv unless nv.nil? || (nv.respond_to?(:empty?) && nv.empty?)
        end
      when Array
        obj.map { |v| call(:deep_compact, v) }.reject { |v| v.nil? || (v.respond_to?(:empty?) && v.empty?) }
      else
        obj
      end
    end,

    validate_openapi!: lambda do |spec, expect_version=nil|
      ev = expect_version || '3.'
      errors, warnings = call(:collect_validation_errors, spec, ev)
      {
        'passed' => errors.empty?,
        'error_count' => errors.size,
        'warning_count' => warnings.size,
        'errors' => errors,
        'warnings' => warnings
      }
    end,

    collect_validation_errors: lambda do |spec, expect_version|
      errors = []
      warnings = []

      # Basic presence & types
      unless spec.is_a?(Hash)
        return [["Spec is not an object"], []]
      end

      ov = spec['openapi']
      if ov.to_s.strip.empty?
        errors << "Missing 'openapi' version string."
      elsif expect_version && !ov.start_with?(expect_version)
        warnings << "OpenAPI version '#{ov}' does not match expected prefix '#{expect_version}'."
      end

      info = spec['info']
      if !info.is_a?(Hash)
        errors << "Missing or invalid 'info' object."
      else
        errors << "Missing 'info.title'."  if info['title'].to_s.strip.empty?
        errors << "Missing 'info.version'." if info['version'].to_s.strip.empty?
      end

      servers = spec['servers']
      if servers && !servers.is_a?(Array)
        errors << "'servers' must be an array."
      elsif servers&.any?
        servers.each_with_index do |s, i|
          if !s.is_a?(Hash) || s['url'].to_s.strip.empty?
            errors << "servers[#{i}] missing 'url'."
          end
        end
      else
        warnings << "No servers defined."
      end

      paths = spec['paths']
      if !paths.is_a?(Hash) || paths.empty?
        errors << "Missing or empty 'paths' object."
      else
        valid_methods = %w[get put post delete options head patch trace]
        path_re = %r{^/}
        paths.each do |p, obj|
          errors << "Path key '#{p}' must start with '/'." unless p.to_s =~ path_re
          unless obj.is_a?(Hash)
            errors << "Path '#{p}' value must be an object."
            next
          end
          obj.each do |meth, op|
            next unless valid_methods.include?(meth)
            unless op.is_a?(Hash)
              errors << "Operation #{meth.upcase} at '#{p}' must be an object."
              next
            end
            if op['operationId'].to_s.strip.empty?
              warnings << "Operation #{meth.upcase} at '#{p}' missing operationId."
            end

            # parameters
            if op['parameters']
              unless op['parameters'].is_a?(Array)
                errors << "Operation #{meth.upcase} at '#{p}': 'parameters' must be an array."
              else
                op['parameters'].each_with_index do |parm, i|
                  unless parm.is_a?(Hash)
                    errors << "Operation #{meth.upcase} at '#{p}': parameters[#{i}] must be an object."
                    next
                  end
                  if parm['name'].to_s.strip.empty? || parm['in'].to_s.strip.empty?
                    errors << "Operation #{meth.upcase} at '#{p}': parameter[#{i}] missing 'name' or 'in'."
                  end
                  if parm['in'] == 'path' && !parm['required']
                    errors << "Operation #{meth.upcase} at '#{p}': path parameter '#{parm['name']}' must be required."
                  end
                end
              end
            end

            # requestBody
            if op['requestBody']
              rb = op['requestBody']
              unless rb.is_a?(Hash)
                errors << "Operation #{meth.upcase} at '#{p}': requestBody must be an object."
              else
                content = rb['content']
                if !content.is_a?(Hash) || content.empty?
                  errors << "Operation #{meth.upcase} at '#{p}': requestBody.content must be a non-empty object."
                end
              end
            end

            # responses
            responses = op['responses']
            if !responses.is_a?(Hash) || responses.empty?
              errors << "Operation #{meth.upcase} at '#{p}': responses must be a non-empty object."
            else
              responses.each do |code, r|
                # accept 'default' or 3-digit
                unless code == 'default' || code.to_s =~ /^\d{3}$/
                  warnings << "Operation #{meth.upcase} at '#{p}': response key '#{code}' should be 'default' or 3-digit."
                end
                unless r.is_a?(Hash)
                  errors << "Operation #{meth.upcase} at '#{p}': response '#{code}' must be an object."
                  next
                end
                if r['content']
                  unless r['content'].is_a?(Hash)
                    errors << "Operation #{meth.upcase} at '#{p}': response '#{code}'.content must be an object."
                  end
                end
              end
            end
          end
        end
      end

      comps = spec['components']
      if comps
        unless comps.is_a?(Hash)
          errors << "'components' must be an object when present."
        else
          if comps['schemas'] && !comps['schemas'].is_a?(Hash)
            errors << "'components.schemas' must be an object."
          end
          if comps['securitySchemes'] && !comps['securitySchemes'].is_a?(Hash)
            errors << "'components.securitySchemes' must be an object."
          end
        end
      end

      # $ref sanity (lightweight)
      ref_re = %r{^#/components/schemas/[^#/]+$}
      _check_refs = lambda do |node, ctx|
        case node
        when Hash
          if node.key?('$ref')
            refv = node['$ref'].to_s
            unless refv =~ ref_re
              warnings << "Non-standard $ref at #{ctx}: '#{refv}'. Expected '#/components/schemas/Name'."
            end
          end
          node.each { |k, v| _check_refs.call(v, "#{ctx}/#{k}") }
        when Array
          node.each_with_index { |v, i| _check_refs.call(v, "#{ctx}[#{i}]") }
        end
      end
      _check_refs.call(spec, '#')

      [errors, warnings]
    end,

    hcompact: lambda do |h|
      return h unless h.is_a?(Hash)
      h.reject { |_k, v| v.nil? }
    end,

    normalize_url_like: lambda do |url|
      return nil if url.to_s.strip.empty?
      s = url.to_s.strip
      s = s.gsub('://', '::__')
      s = s.gsub(%r{//+}, '/')
      s = s.gsub('::__', '://')
      s
    end

  }
}
