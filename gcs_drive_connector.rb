{
  title: 'GCS and Google Drive Utilities',
  description: 'Google Drive utilities with resilience, telemetry.',
  version: "0.4.0",
  custom_action: false,

  connection: {
    fields: [
      #  Developer options 
      { name: 'verbose_errors',  label: 'Verbose errors', type: 'boolean', control_type: 'checkbox', hint: 'Include upstream response bodies in normalized error messages (useful in non-prod).' },
      { name: 'include_trace',   label: 'Include trace in outputs', type: 'boolean', control_type: 'checkbox', default: false, sticky: true },
      
      # Service account details
      { name: 'service_account_email', group: 'Service account', optional: false, hint: 'e.g. my-sa@project.iam.gserviceaccount.com' },
      { name: 'private_key_id',        group: 'Service account', optional: false, hint: 'The key’s private_key_id from the JSON' },
      { name: 'private_key',           group: 'Service account', control_type: 'password', multiline: true, optional: false,
        hint: 'Paste the PEM private key from the JSON. Newlines may appear as \\n; both forms are handled.' },

      # Enable GCS
      { name: 'enable_gcs', label: 'Enable Google Cloud Storage', type: 'boolean', control_type: 'checkbox',
        default: true, sticky: true, hint: 'Adds Cloud Storage scopes for listing/uploading objects (requires reconnect).' },
      { name: 'gcs_user_project', label: 'GCS billing project (userProject)', optional: true, sticky: true,
        hint: 'Required for Requester Pays buckets.' }
    ],

    authorization: {
      type: 'custom_auth',

      acquire: lambda do |connection|
        iss = connection['service_account_email']
        iat = Time.now.to_i
        exp = iat + 3600

        # Normalize + validate key material
        private_key = connection['private_key'].to_s.strip
        private_key = private_key.gsub(/\\n/, "\n").gsub(/\r\n?/, "\n")
        error('Invalid private key: missing BEGIN/END markers') unless private_key.include?('BEGIN PRIVATE KEY')

        aud = call('build_endpoint_url', :oauth2, :token)
        scopes = ['https://www.googleapis.com/auth/drive.readonly']
        scopes << 'https://www.googleapis.com/auth/devstorage.read_write' if connection['enable_gcs'] == true
        claim = {
          'iss'   => iss,
          'scope' => scopes.join(' '),
          'aud'   => aud,
          'iat'   => iat,
          'exp'   => exp
        }

        jwt = workato.jwt_encode(
          claim,
          private_key,
          'RS256',
          kid: connection['private_key_id']
        )

        # Direct HTTP (no custom wrapper)
        resp = post(aud)
                .payload(
                  grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                  assertion:  jwt
                )
                .request_format_www_form_urlencoded
                .after_error_response(/.*/) do |code, body, headers, _message|
                  norm = call('normalize_http_error', code, body, headers, aud,
                              { action: 'Service account JWT exchange',
                                verbose_errors: connection['verbose_errors'] })
                  error(norm)
                end

        {
          access_token: resp['access_token'],
          expires_at:   (Time.now + (resp['expires_in'] || 3600).to_i).iso8601
        }
      end,

      identity: lambda do |connection|
        connection['service_account_email']
      end,

      apply: lambda do |connection|
        headers(Authorization: "Bearer #{connection['access_token']}")
      end,

      refresh_on: [401],
      detect_on: [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /token.*expired/i, /auth.*expired/i]
    },

    base_uri: -> (_connection) { 'https://www.googleapis.com/' }
  },

  test: lambda do |connection|
    # Drive probe
    begin
      get(call('build_endpoint_url', :drive, :about))
        .params(fields: 'user,storageQuota')
    rescue => e
      msg = e.message.to_s
      if msg =~ /unregistered callers|without established identity|Please use API key/i
        error('Authentication not applied: your connection does not have an OAuth/Service Account identity. ' \
              'Fix: open the connection, set **Authentication type** correctly (OAuth2 or Service account), ' \
              'complete the handshake (OAuth consent or SA JWT), then reconnect.')
      else
        raise
      end
    end

    if connection['enable_gcs'] == true
      begin
        # Exercise devstorage scope without assuming a bucket
        get('https://storage.googleapis.com/storage/v1/b/this-bucket-should-not-exist-123456/o')
          .params(maxResults: 1, fields: 'nextPageToken')
      rescue => e
        msg = e.message.to_s
        # Only fail connection on 401/403 (bad/insufficient creds or missing scope)
        raise e if msg =~ /\b401\b/ || msg =~ /\b403\b/
        # 404/NoSuchBucket or other errors are okay for the smoke probe
      end
    end

    true
  end,

  object_definitions: {
    drive_change: {
      fields: lambda do |object_definitions|
        [
          { name: 'change_type' },
          { name: 'time' },
          { name: 'removed', type: 'boolean' },
          { name: 'file_id' },
          { name: 'file', type: 'object', properties: object_definitions['drive_file'] }
        ]
      end
    },

    drive_file: {
      fields: lambda do
        [
          { name: 'id' }, { name: 'name' }, { name: 'mime_type' },
          { name: 'size', type: 'integer' }, { name: 'modified_time' },
          { name: 'checksum' },
          { name: 'owners', type: 'array', of: 'object', properties: [
              { name: 'displayName' }, { name: 'emailAddress' }
          ]}
        ]
      end
    },

    # Drive file + optional content, controlled by input.content_mode
    drive_file_with_content: {
      fields: lambda do |_, _|
        [
          { name: 'id' }, { name: 'name' }, { name: 'mime_type' },
          { name: 'size', type: 'integer' }, { name: 'modified_time' },
          { name: 'checksum' },
          { name: 'owners', type: 'array', of: 'object', properties: [
              { name: 'displayName' }, { name: 'emailAddress' }
          ]},
          # Exactly one of these will be present depending on content_mode
          { name: 'text_content', type: 'string', hint: 'Present only when content_mode=text' },
          { name: 'content_bytes', type: 'string', hint: 'Base64; present only when content_mode=bytes' },
          # Optional, informative
          { name: 'exported_as', label: 'Export MIME type', hint: 'Editors export; e.g., text/plain or text/csv' }
        ]
      end
    },

    gcs_object: {
      fields: lambda do
        [
          { name: 'bucket' }, { name: 'name' }, { name: 'size', type: 'integer' },
          { name: 'content_type' }, { name: 'updated' }, { name: 'generation' },
          { name: 'md5_hash' }, { name: 'crc32c' },
          { name: 'metadata', type: 'object' }
        ]
      end
    },

    # GCS object + optional content, controlled by input.content_mode
    gcs_object_with_content: {
      fields: lambda do
        [
          { name: 'bucket' }, { name: 'name' }, { name: 'size', type: 'integer' },
          { name: 'content_type' }, { name: 'updated' }, { name: 'generation' },
          { name: 'md5_hash' }, { name: 'crc32c' },
          { name: 'metadata', type: 'object' },
          { name: 'text_content', type: 'string', hint: 'Present only when content_mode=text' },
          { name: 'content_bytes', type: 'string', hint: 'Base64; present only when content_mode=bytes' }
        ]
      end
    }
  },

  actions: {
    # DRIVE: POLL
    drive_changes_poll: {
      title: 'Drive: Changes (poll page)',
      subtitle: 'Fetch a page of Drive changes. Provide page_token to continue.',
      help: {
        body: 'If "page_token" is blank, the action fetches a new "startPageToken" and immediately reads the first page from it. Use "next_page_token" from the output to get the next page; when no more pages remain, the response may include "new_start_page_token" to persist for the next polling cycle.'
      },

      input_fields: lambda do
        [
          { name: 'drive_id', label: 'Shared Drive ID', hint: 'Poll a specific Shared Drive. Leave blank for My Drive.' },
          { name: 'page_token', label: 'Page token', hint: 'Use the prior next_page_token to continue.' },
          { name: 'start_page_token', hint: 'Used only when page_token is blank; the action auto-fetches one if this is blank too.' },
          { name: 'page_size', type: 'integer', default: 100, hint: '1–1000' },
          { name: 'include_removed', type: 'boolean', control_type: 'checkbox', default: false,
            hint: 'Include tombstones for removed files.' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'changes', type: 'array', of: 'object', properties: object_definitions['drive_change'] },
          { name: 'count', type: 'integer' },
          { name: 'has_more', type: 'boolean' },
          { name: 'next_page_token' },
          { name: 'new_start_page_token' },
          { name: 'used_page_token' },
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,

      sample_output: lambda do
        {
          'changes' => [
            {
              'change_type' => 'file',
              'time' => '2025-09-30T12:00:00Z',
              'removed' => false,
              'file_id' => '1a2b3c',
              'file' => {
                'id' => '1a2b3c', 'name' => 'Report.txt', 'mime_type' => 'text/plain',
                'size' => 42, 'modified_time' => '2025-09-30T12:00:00Z', 'checksum' => 'abc',
                'owners' => [{ 'displayName' => 'Owner', 'emailAddress' => 'owner@example.com' }]
              }
            }
          ],
          'count' => 1,
          'has_more' => false,
          'next_page_token' => nil,
          'new_start_page_token' => '12345',
          'used_page_token' => '12345'
        }
      end,

      execute: lambda do |connection, input|
        include_trace   = connection['include_trace'] == true
        traces          = []
        cid             = call('gen_correlation_id')

        # 1. Decide token
        token = (input['page_token'].to_s.strip)
        if token == ''
          token = (input['start_page_token'].to_s.strip)
        end
        drive_id = (input['drive_id'] || '').to_s.strip

        # Auto-get startPageToken when neither provided
        if token == ''
          url_tok   = call('build_endpoint_url', :drive, :changes_start_token)
          started_t = Time.now
          code_t    = nil

          params_t = { supportsAllDrives: true }
          params_t[:driveId] = drive_id unless drive_id == ''
          tok_body = get(url_tok)
            .params(params_t)
            .headers('X-Correlation-Id' => cid)
            .after_error_response(/.*/) do |code, body, headers, _msg|
              norm = call('normalize_http_error', code, body, headers, url_tok,
                          { action: 'Drive getStartPageToken', correlation_id: cid, verbose_errors: connection['verbose_errors'] })
              error(norm)
            end
            .after_response do |code, body, _headers|
              code_t = code.to_i
              body
            end

          dur_t = ((Time.now - started_t) * 1000.0).round
          traces << { 'action' => 'Drive getStartPageToken', 'correlation_id' => cid, 'status' => code_t, 'url' => url_tok, 'dur_ms' => dur_t } if include_trace
          token = (tok_body['startPageToken'] || '').to_s
          error('Failed to acquire startPageToken') if token == ''
        end

        # 2. Fetch page of changes
        page_size = [[(input['page_size'] || 100).to_i, 1].max, 1000].min
        url_chg   = call('build_endpoint_url', :drive, :changes)
        started_c = Time.now
        code_c    = nil

        fields = 'nextPageToken,newStartPageToken,changes(changeType,time,removed,fileId,file(id,name,mimeType,modifiedTime,size,md5Checksum,owners(displayName,emailAddress)))'

        params_c = {
          pageToken: token,
          pageSize: page_size,
          includeRemoved: input['include_removed'] == true,
          supportsAllDrives: true,
          # Include Shared drive items when not polling a specific drive.
          # Aligns w/ Drive v3 changes.list param semantics; when 
          # drive_id is '' (user corpus), prevents silent omission of Shared drive changes
          includeItemsFromAllDrives: (drive_id == ''),
          fields: fields
        }
        params_c[:driveId] = drive_id unless drive_id == ''
        body = get(url_chg)
          .params(params_c)
          .headers('X-Correlation-Id' => cid)
          .after_error_response(/.*/) do |c, resp_body, headers, _msg|
            norm = call('normalize_http_error', c, resp_body, headers, url_chg,
                        { action: 'Drive changes.list', correlation_id: cid, verbose_errors: connection['verbose_errors'] })
            error(norm)
          end
          .after_response do |code, resp_body, _headers|
            code_c = code.to_i
            resp_body
          end

        dur_c = ((Time.now - started_c) * 1000.0).round
        traces << { 'action' => 'Drive changes.list', 'correlation_id' => cid, 'status' => code_c, 'url' => url_chg, 'dur_ms' => dur_c } if include_trace

        # 3. Map output
        changes = Array(body['changes']).map do |c|
          f = c['file'] || {}
          {
            'change_type' => c['changeType'],
            'time'        => c['time'],
            'removed'     => !!c['removed'],
            'file_id'     => c['fileId'],
            'file'        => {
              'id'            => f['id'],
              'name'          => f['name'],
              'mime_type'     => f['mimeType'],
              'size'          => (f['size'] || 0).to_i,
              'modified_time' => f['modifiedTime'],
              'checksum'      => f['md5Checksum'],
              'owners'        => Array(f['owners']).map { |o| { 'displayName' => o['displayName'], 'emailAddress' => o['emailAddress'] } }
            }
          }
        end

        next_tok = body['nextPageToken']
        new_start = body['newStartPageToken']

        out = {
          'changes'              => changes,
          'count'                => changes.length,
          'has_more'             => !(next_tok.nil? || next_tok.to_s == ''),
          'next_page_token'      => next_tok,
          'new_start_page_token' => new_start,
          'used_page_token'      => token
        }
        out['trace'] = traces if include_trace
        out
      end

    },

    # DRIVE: LIST
    drive_list_files: {
      title: 'Drive: List files',
      subtitle: 'Query by folder/date/MIME; newest first',
      help: {
        body: 'Returns minimal metadata for Drive files. No content.'
      },
      input_fields: lambda do
        [
          { name: 'folder_id', label: 'Folder ID or URL' },
          { name: 'drive_id',  label: 'Shared Drive ID' },
          { name: 'max_results', type: 'integer', default: 100 },
          { name: 'modified_after',  type: 'date_time' },
          { name: 'modified_before', type: 'date_time' },
          { name: 'mime_type' },
          { name: 'exclude_folders', type: 'boolean', control_type: 'checkbox', default: false },
          { name: 'page_token' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'files', type: 'array', of: 'object', properties: object_definitions['drive_file'] },
          { name: 'count', type: 'integer' },
          { name: 'has_more', type: 'boolean' },
          { name: 'next_page_token' },
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,
      sample_output: lambda do
        {
          'files' => [
            { 'id' => '1', 'name' => 'Report.txt', 'mime_type' => 'text/plain', 'size' => 42,
              'modified_time' => '2025-09-30T12:00:00Z', 'checksum' => 'abc', 'owners' => [] }
          ],
          'count' => 1, 'has_more' => false, 'next_page_token' => nil
        }
      end,
      execute: lambda do |connection, input|
        # 1. Normalize inputs
        local        = call('deep_copy', input)
        folder_id    = call('maybe_extract_id', local['folder_id'])
        drive_id     = (local['drive_id'] || '').to_s
        page_size    = [[(local['max_results'] || 100).to_i, 1].max, 1000].min
        modified_after  = call('to_iso8601', local['modified_after'])
        modified_before = call('to_iso8601', local['modified_before'])
        exclude_folders = local['exclude_folders'] == true

        # 2. Build Drive query 
        q = call('build_drive_query', {
          folder_id:       folder_id,
          modified_after:  modified_after,
          modified_before: modified_before,
          mime_type:       local['mime_type'],
          exclude_folders: exclude_folders
        })

        # 3. Choose corpus 
        corpora =
          if !call('blank?', drive_id)
            'drive'
          elsif !call('blank?', folder_id)
            'allDrives'
          else
            'user'
          end

        # 4) HTTP request (no wrapper)
        include_trace = connection['include_trace'] == true
        traces = []

        action_cid = call('gen_correlation_id')
        files_endpoint = call('build_endpoint_url', :drive, :files)
        params = {
          q: q,
          pageSize: page_size,
          orderBy: 'modifiedTime desc',
          pageToken: local['page_token'],
          spaces: 'drive',
          corpora: corpora,
          supportsAllDrives: true,
          includeItemsFromAllDrives: (corpora != 'user'),
          fields: 'nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress))'
        }.reject { |_k, v| v.nil? || v.to_s == '' }
        params[:driveId] = drive_id unless call('blank?', drive_id)

        started = Time.now
        code = nil
        body = get(files_endpoint)
          .params(params)
          .headers('X-Correlation-Id' => action_cid)
          .after_error_response(/.*/) do |c, b, h, _|
            error(call('normalize_http_error', c, b, h, files_endpoint,
                      { action: 'Drive: List files (simplified)', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }))
          end
          .after_response { |c, b, _h| code = c.to_i; b }

        traces << { 'action' => 'Drive: List files (simplified)', 'correlation_id' => action_cid,
                    'status' => code, 'url' => files_endpoint, 'dur_ms' => ((Time.now - started) * 1000.0).round } if include_trace

        # 5) Map output to canonical shape
        data  = body.is_a?(Hash) ? body : {}
        files = Array(data['files']).map do |f|
          {
            'id'            => f['id'],
            'name'          => f['name'],
            'mime_type'     => f['mimeType'],
            'size'          => (f['size'] || 0).to_i,   # Docs/Sheets often omit size
            'modified_time' => f['modifiedTime'],
            'checksum'      => f['md5Checksum'],
            'owners'        => Array(f['owners']).map { |o|
                                { 'displayName' => o['displayName'], 'emailAddress' => o['emailAddress'] }
                              }
          }
        end

        out = {
          'files'           => files,
          'count'           => files.length,
          'has_more'        => !call('blank?', data['nextPageToken']),
          'next_page_token' => data['nextPageToken']
        }
        out['trace'] = traces if include_trace
        out
      end
    },

    # DRIVE: GET
    drive_get_file: {
      title: 'Drive: Get file',
      subtitle: 'Metadata + optional content (text or bytes)',
      help: { body: 'Editors export to text/csv; non-Editors download as text when textual else bytes.' },

      input_fields: lambda do
        [
          { name: 'file_id', label: 'File ID or URL', optional: false },
          { name: 'content_mode', control_type: 'select', optional: false, default: 'text',
            options: [['None', 'none'], ['Text', 'text'], ['Bytes', 'bytes']],
            hint: 'Editors: bytes not supported; use text (export).'},
          { name: 'acknowledge_abuse', type: 'boolean', control_type: 'checkbox', default: false,
            hint: 'Required to download some flagged files.' },
          { name: 'postprocess', type: 'object', properties: [
              { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false }
          ]}
        ]
      end,

      # Single-object output + optional trace (when connection.include_trace=true)
      output_fields: lambda do |object_definitions|
        object_definitions['drive_file_with_content'] + [
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,

      sample_output: lambda do
        {
          'id' => '1', 'name' => 'Doc', 'mime_type' => 'application/vnd.google-apps.document',
          'size' => 0, 'modified_time' => '2025-09-30T12:00:00Z', 'checksum' => nil, 'owners' => [],
          'text_content' => 'Exported text …', 'exported_as' => 'text/plain'
          # 'trace' will be present only when include_trace=true
        }
      end,

      execute: lambda do |connection, input|
        local         = call('deep_copy', input)
        mode          = (local['content_mode'] || 'text').to_s
        strip         = !!(local.dig('postprocess', 'strip_urls') == true)
        include_trace = connection['include_trace'] == true
        traces        = []

        fid_in, rk_in = call('extract_drive_id_and_key', local['file_id'])
        error('No Drive file ID provided.') if call('blank?', fid_in)

        per_cid = call('gen_correlation_id')
        started = Time.now
        fid, mdata, eff_rk = call('drive_fetch_meta_resolved', fid_in, per_cid,
                                   'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress),shortcutDetails(targetId,targetMimeType,targetResourceKey),resourceKey',
                                   connection['verbose_errors'] == true, rk_in)
        traces << { 'action' => 'Drive get (meta+resolve)', 'correlation_id' => per_cid, 'status' => 200,
                    'url' => '(files.get -> shortcut resolve)', 'dur_ms' => ((Time.now - started) * 1000.0).round } if include_trace

        out   = call('drive_map_output_from_meta', mdata)
        rk_hdr = call('build_rk_header', fid, eff_rk)

        content_fields, c_traces = call('drive_fetch_content_fields', connection, fid, out['mime_type'], mode,
                                        (local['acknowledge_abuse'] == true), strip, rk_hdr, per_cid)
        out.merge!(content_fields)
        traces.concat(c_traces) if include_trace

        out['trace'] = traces if include_trace
        out
       end
      end
    },

    drive_get_files: {
      title: 'Drive: Get files (batch)',
      subtitle: 'Metadata + optional content for multiple files',
      batch: true,

      input_fields: lambda do
        [
          { name: 'file_ids', label: 'File IDs or URLs', type: 'array', of: 'string', optional: true },
          { name: 'files', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'file_id' }
          ], hint: 'Use when mapping from a previous step.' },
          { name: 'content_mode', control_type: 'select', optional: false, default: 'text',
            options: [['None', 'none'], ['Text', 'text'], ['Bytes', 'bytes']],
            hint: 'Editors: bytes not supported; use text (export).'},
          { name: 'acknowledge_abuse', type: 'boolean', control_type: 'checkbox', default: false,
            hint: 'Required to download some flagged files.' },
          { name: 'postprocess', type: 'object', properties: [
              { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false }
          ]}
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'files', type: 'array', of: 'object', properties: object_definitions['drive_file_with_content'] },
          { name: 'failed', type: 'array', of: 'object', properties: [
              { name: 'file_id' }, { name: 'error_message' }, { name: 'error_code' }
          ]},
          { name: 'summary', type: 'object', properties: [
              { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' }
          ]},
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,

      sample_output: lambda do
        {
          'files' => [
            { 'id' => '1', 'name' => 'Report.txt', 'mime_type' => 'text/plain', 'size' => 42,
              'modified_time' => '2025-09-30T12:00:00Z', 'checksum' => 'abc', 'owners' => [],
              'text_content' => 'Hello' }
          ],
          'failed'  => [],
          'summary' => { 'total' => 1, 'success' => 1, 'failed' => 0 }
          # 'trace' appears only when include_trace=true
        }
      end,

      execute: lambda do |connection, input|
        local         = call('deep_copy', input)
        mode          = (local['content_mode'] || 'text').to_s
        strip         = !!(local.dig('postprocess', 'strip_urls') == true)
        include_trace = connection['include_trace'] == true
        traces        = []

        pairs = []
        pairs += Array(local['file_ids'] || []).map { |raw| call('extract_drive_id_and_key', raw) }
        pairs += Array(local['files']    || []).map { |o|
          call('extract_drive_id_and_key', (o.is_a?(Hash) ? (o['id'] || o['file_id']) : o))
        }
        pairs = pairs.select { |id, _| !call('blank?', id) }.uniq
        error('No Drive file IDs provided. Map "file_ids" or "files".') if pairs.empty?

        successes, failures = [], []

        pairs.each do |fid_in, rk_in|
          per_cid = call('gen_correlation_id')
          begin
            started_m = Time.now
            fid, mdata, eff_rk = call('drive_fetch_meta_resolved', fid_in, per_cid,
                                      'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress),shortcutDetails(targetId,targetMimeType,targetResourceKey),resourceKey',
                                      connection['verbose_errors'] == true, rk_in)
            traces << { 'action' => 'Drive get (meta+resolve)', 'correlation_id' => per_cid, 'status' => 200,
                        'url' => '(files.get -> shortcut resolve)', 'dur_ms' => ((Time.now - started_m) * 1000.0).round } if include_trace
            out    = call('drive_map_output_from_meta', mdata)
            rk_hdr = call('build_rk_header', fid, eff_rk)

            content_fields, c_traces = call('drive_fetch_content_fields', connection, fid, out['mime_type'], mode,
                                            (local['acknowledge_abuse'] == true), strip, rk_hdr, per_cid)
            out.merge!(content_fields)
            traces.concat(c_traces) if include_trace

            successes << out
          rescue => e
            failures << {
              'file_id'       => fid_in,
              'error_message' => e.message,
              'error_code'    => call('infer_error_code', e.message)
            }
          end
        end

        out = {
          'files'   => successes,
          'failed'  => failures,
          'summary' => { 'total' => pairs.length, 'success' => successes.length, 'failed' => failures.length }
        }
        out['trace'] = traces if include_trace
        out

      end

    },

    # GCS: LIST
    gcs_list_objects: {
      title: 'GCS: List objects',
      subtitle: 'Prefix, delimiter, pagination',
      input_fields: lambda do
        [
          { name: 'bucket', optional: false },
          { name: 'prefix' }, { name: 'delimiter' }, { name: 'max_results', type: 'integer', default: 1000 },
          { name: 'page_token' }, { name: 'include_versions', type: 'boolean', control_type: 'checkbox', default: false }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'objects', type: 'array', of: 'object', properties: object_definitions['gcs_object'] },
          { name: 'count', type: 'integer' },
          { name: 'has_more', type: 'boolean' },
          { name: 'next_page_token' },
          { name: 'prefixes', type: 'array', of: 'string' },
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,
      sample_output: lambda do
        {
          'objects' => [
            { 'bucket' => 'my-bkt', 'name' => 'exports/2025-09-30/file.txt', 'size' => 123,
              'content_type' => 'text/plain', 'updated' => '2025-09-30T12:00:00Z',
              'generation' => '1700000000000000', 'md5_hash' => 'abc', 'crc32c' => 'def', 'metadata' => {} }
          ],
          'count' => 1, 'has_more' => false, 'next_page_token' => nil, 'prefixes' => []
        }
      end,
      execute: lambda do |connection, input|
        # 1.  Normalize inputs
        local     = call('deep_copy', input)
        bucket_in = (local['bucket'] || '').to_s.strip
        prefix    = (local['prefix'] || '').to_s
        delimiter = (local['delimiter'] || '').to_s
        page_size = [[(local['max_results'] || 1000).to_i, 1].max, 1000].min
        page_tok  = (local['page_token'] || '').to_s
        versions  = local['include_versions'] == true

        # Support 'gs://bucket[/path]' in bucket field (fold path into prefix if caller omitted prefix)
        if bucket_in.start_with?('gs://')
          rest = bucket_in.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0]
          inferred_prefix = parts[1]
          if prefix.to_s == '' && inferred_prefix && inferred_prefix != ''
            prefix = inferred_prefix
          end
        else
          bucket = bucket_in
        end

        # Tidy prefix (GCS prefixes should not start with '/')
        prefix = prefix.sub(/\A\//, '') if prefix

        error('Bucket is required') if call('blank?', bucket)

        # 2. Build request
        include_trace = connection['include_trace'] == true
        traces = []

        action_cid = call('gen_correlation_id')
        list_endpoint = call('build_endpoint_url', :storage, :objects_list, bucket)
        params = {
          prefix:      (prefix == '' ? nil : prefix),
          delimiter:   (delimiter == '' ? nil : delimiter),
          pageToken:   (page_tok == '' ? nil : page_tok),
          maxResults:  page_size,
          versions:    versions,
          fields:      'items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes'
        }.reject { |_k, v| v.nil? || v.to_s == '' }
        up = connection['gcs_user_project'].to_s
        params[:userProject] = up unless up == ''

        started = Time.now
        code    = nil
        body = get(list_endpoint)
          .params(params)
          .headers('X-Correlation-Id' => action_cid)
          .after_error_response(/.*/) do |c, b, h, _|
            error(call('normalize_http_error', c, b, h, list_endpoint,
                      { action: 'GCS: List objects (simplified)', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }))
          end
          .after_response { |c, b, _| code = c.to_i; b }

        traces << { 'action' => 'GCS: List objects (simplified)', 'correlation_id' => action_cid,
                    'status' => code, 'url' => list_endpoint, 'dur_ms' => ((Time.now - started) * 1000.0).round } if include_trace

        # 4. Map output
        data  = body.is_a?(Hash) ? body : {}
        items = Array(data['items']).map do |o|
          {
            'bucket'       => o['bucket'],
            'name'         => o['name'],
            'size'         => (o['size'] || 0).to_i,
            'content_type' => o['contentType'],
            'updated'      => o['updated'],
            'generation'   => o['generation'],
            'md5_hash'     => o['md5Hash'],
            'crc32c'       => o['crc32c'],
            'metadata'     => (o['metadata'] || {})
          }
        end

        next_token = data['nextPageToken']
        out = {
          'objects'         => items,
          'count'           => items.length,
          'has_more'        => !call('blank?', next_token),
          'next_page_token' => next_token,
          'prefixes'        => Array(data['prefixes'] || [])
        }
        out['trace'] = traces if include_trace
        out
      end
    },

    # GCS: GET
    gcs_get_object: {
      title: 'GCS: Get object',
      subtitle: 'Metadata + optional content (text or bytes)',
      input_fields: lambda do
        [
          { name: 'bucket', optional: false },
          { name: 'object_name', optional: false },
          { name: 'content_mode', control_type: 'select', optional: false, default: 'text',
            options: [['None', 'none'], ['Text', 'text'], ['Bytes', 'bytes']] },
          { name: 'postprocess', type: 'object', properties: [
              { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false }
          ]}
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_content'] + [
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,
      sample_output: lambda do
        {
          'bucket' => 'my-bkt', 'name' => 'exports/2025-09-30/file.txt', 'size' => 123,
          'content_type' => 'text/plain', 'updated' => '2025-09-30T12:00:00Z', 'generation' => '1700000000000000',
          'md5_hash' => 'abc', 'crc32c' => 'def', 'metadata' => {},
          'text_content' => 'Hello world'
        }
      end,
      execute: lambda do |connection, input|
        local  = call('deep_copy', input)
        include_trace = connection['include_trace'] == true
        traces        = []
        mode   = (local['content_mode'] || 'text').to_s
        strip  = !!(local.dig('postprocess', 'strip_urls') == true)

        # ---------- Normalize bucket/object ----------
        bucket = (local['bucket'] || '').to_s.strip
        name   = (local['object_name'] || '').to_s

        # Support gs:// in either field
        if bucket.start_with?('gs://')
          rest  = bucket.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0]
          name   = parts[1] if name.to_s == '' && parts[1]
        end
        if name.start_with?('gs://')
          rest  = name.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0] if bucket.to_s == ''
          name   = (parts[1] || '')
        end

        error('Bucket is required') if call('blank?', bucket)
        error('Object name is required') if call('blank?', name)

        # Fetch metadata
        cid  = call('gen_correlation_id')
        url_meta = call('build_endpoint_url', :storage, :object, bucket, name)
        started1 = Time.now
        code1    = nil
        params_m = {}
        up = connection['gcs_user_project'].to_s
        params_m[:userProject] = up unless up == ''
        meta_body = get(url_meta)
          .params(params_m)
          .headers('X-Correlation-Id' => cid)
          .after_error_response(/.*/) do |c, b, h, _|
            error(call('normalize_http_error', c, b, h, url_meta,
                      { action: 'GCS get object (meta)', correlation_id: cid, verbose_errors: connection['verbose_errors'] }))
          end
          .after_response { |c, b, _| code1 = c.to_i; b }

        traces << { 'action' => 'GCS get object (meta)', 'correlation_id' => cid, 'status' => code1,
                    'url' => url_meta, 'dur_ms' => ((Time.now - started1) * 1000.0).round } if include_trace
        md = meta_body.is_a?(Hash) ? meta_body : {}

        out = {
          'bucket'       => md['bucket'] || bucket,
          'name'         => md['name']   || name,
          'size'         => (md['size'] || 0).to_i,
          'content_type' => md['contentType'],
          'updated'      => md['updated'],
          'generation'   => md['generation'],
          'md5_hash'     => md['md5Hash'],
          'crc32c'       => md['crc32c'],
          'metadata'     => (md['metadata'] || {})
        }

        case mode
        when 'none'
          # nothing

        when 'text'
          ct = (out['content_type'] || '').to_s
          error("status=415 Non-text object (#{ct}); use content_mode=bytes or none.") unless call('textual_mime?', ct)

          url_dl = call('build_endpoint_url', :storage, :download, bucket, name)
          started2 = Time.now
          code2    = nil
          p_dl = { alt: 'media' }
          p_dl[:userProject] = up unless up == ''
          dl_body = get(url_dl)
            .params(p_dl)
            .headers('X-Correlation-Id' => cid)
            .response_format_raw
            .after_error_response(/.*/) do |c, b, h, _|
              error(call('normalize_http_error', c, b, h, url_dl,
                        { action: 'GCS download (text)', correlation_id: cid, verbose_errors: connection['verbose_errors'] }))
            end
            .after_response { |c, b, _| code2 = c.to_i; b }

          traces << { 'action' => 'GCS download (text)', 'correlation_id' => cid, 'status' => code2,
                      'url' => url_dl, 'dur_ms' => ((Time.now - started2) * 1000.0).round } if include_trace

          txt = call('safe_utf8', dl_body)
          txt = call('strip_urls_from_text', txt) if strip
          out['text_content'] = txt

        when 'bytes'
          url_dl = call('build_endpoint_url', :storage, :download, bucket, name)
          started3 = Time.now
          code3    = nil
          p_db = { alt: 'media' }
          p_db[:userProject] = up unless up == ''
          dl_body = get(url_dl)
            .params(p_db)
            .headers('X-Correlation-Id' => cid)
            .response_format_raw
            .after_error_response(/.*/) do |c, b, h, _|
              error(call('normalize_http_error', c, b, h, url_dl,
                        { action: 'GCS download (bytes)', correlation_id: cid, verbose_errors: connection['verbose_errors'] }))
            end
            .after_response { |c, b, _| code3 = c.to_i; b }

          traces << { 'action' => 'GCS download (bytes)', 'correlation_id' => cid, 'status' => code3,
                      'url' => url_dl, 'dur_ms' => ((Time.now - started3) * 1000.0).round } if include_trace

          out['content_bytes'] = [dl_body.to_s].pack('m0')

        else
          error("Unsupported content_mode=#{mode}. Use none|text|bytes.")
        end

        out['trace'] = traces if include_trace
        out
      end
    },

    # GCS: PUT
    gcs_put_object: {
      title: 'GCS: Put object',
      subtitle: 'Upload text or bytes (media or multipart)',
      input_fields: lambda do
        [
          { name: 'bucket', optional: false }, { name: 'object_name', optional: false },
          { name: 'content_mode', control_type: 'select', optional: false, default: 'text',
            options: [['Text', 'text'], ['Bytes', 'bytes']] },
          { name: 'text_content', optional: true, ngIf: 'input.content_mode == "text"', control_type: 'text', multiline: true },
          { name: 'content_bytes', optional: true, ngIf: 'input.content_mode == "bytes"', hint: 'Base64 string (e.g., from a prior step\'s content_bytes)' },
          { name: 'content_type', optional: true, hint: 'Defaults: text/plain; charset=UTF-8 (text), application/octet-stream (bytes)' },
          { name: 'custom_metadata', type: 'object', optional: true },
          { name: 'preconditions', type: 'object', properties: [
              { name: 'if_generation_match', type: 'integer' },
              { name: 'if_metageneration_match', type: 'integer' }
          ]},
          { name: 'postprocess', type: 'object', properties: [
              { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, ngIf: 'input.content_mode == "text"' }
          ]}
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'gcs_object', type: 'object', properties: object_definitions['gcs_object'] },
          { name: 'bytes_uploaded', type: 'integer' }
        ]
      end,
      sample_output: lambda do
        {
          'gcs_object' => {
            'bucket' => 'my-bkt', 'name' => 'exports/2025-09-30/file.txt', 'size' => 123,
            'content_type' => 'text/plain', 'updated' => '2025-09-30T12:00:00Z',
            'generation' => '1700000000000000', 'md5_hash' => 'abc', 'crc32c' => 'def', 'metadata' => {}
          },
          'bytes_uploaded' => 123
        }
      end,
      execute: lambda do |connection, input|
        local = call('deep_copy', input)

        # -------- Normalize inputs --------
        mode  = (local['content_mode'] || 'text').to_s
        strip = !!(local.dig('postprocess', 'strip_urls') == true)

        bucket = (local['bucket'] || '').to_s.strip
        name   = (local['object_name'] || '').to_s

        # Support gs:// in either field
        if bucket.start_with?('gs://')
          rest  = bucket.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0]
          name   = parts[1] if name.to_s == '' && parts[1]
        end
        if name.start_with?('gs://')
          rest  = name.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0] if bucket == ''
          name   = (parts[1] || '')
        end

        error('Bucket is required')      if call('blank?', bucket)
        error('Object name is required') if call('blank?', name)

        # Payload prep
        mime  =
          if local['content_type'].to_s != ''
            local['content_type'].to_s
          else
            mode == 'text' ? 'text/plain; charset=UTF-8' : 'application/octet-stream'
          end

        raw_bytes =
          case mode
          when 'text'
            txt = (local['text_content'] || '').to_s
            error('text_content is required when content_mode=text') if txt == ''
            txt = call('strip_urls_from_text', txt) if strip
            txt # UTF-8 string is fine; request sets raw_body
          when 'bytes'
            b64 = (local['content_bytes'] || '').to_s
            error('content_bytes (base64) is required when content_mode=bytes') if b64 == ''
            arr = b64.to_s.unpack('m0')
            decoded = arr && arr[0]
            error('Failed to decode content_bytes (base64)') if decoded.nil?
            decoded
          else
            error("Unsupported content_mode=#{mode}. Use text|bytes.")
          end

        bytes_len = raw_bytes.to_s.bytesize

        # Upload mode selection
        cm_in = local['custom_metadata']
        has_meta = cm_in.is_a?(Hash) && !cm_in.empty?

        # Sanitize metadata to { "k":"v" } strings only
        sanitize_meta = lambda do |obj|
          h = obj.is_a?(Hash) ? obj : {}
          out = {}
          h.each do |k, v|
            next if k.nil?
            out[k.to_s] = v.nil? ? '' : v.to_s
          end
          out
        end
        custom_meta = has_meta ? sanitize_meta.call(cm_in) : {}

        # Preconditions → query params
        pre = local['preconditions'].is_a?(Hash) ? local['preconditions'] : {}
        extra_params = {
          ifGenerationMatch:     pre['if_generation_match'],
          ifMetagenerationMatch: pre['if_metageneration_match']
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        cid = call('gen_correlation_id')

        if has_meta
          # Multipart upload (metadata + media)
          boundary  = "wrkto-#{call('gen_correlation_id').gsub('-', '')}"
          meta_json = JSON.generate({ 'name' => name, 'contentType' => mime, 'metadata' => custom_meta })

          part1 = "--#{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n#{meta_json}\r\n"
          part2 = "--#{boundary}\r\nContent-Type: #{mime}\r\n\r\n"
          part3 = "\r\n--#{boundary}--"

          body = ''.b
          body << part1.dup.force_encoding('ASCII-8BIT')
          body << part2.dup.force_encoding('ASCII-8BIT')
          body << raw_bytes.to_s.b
          body << part3.dup.force_encoding('ASCII-8BIT')

          params = { uploadType: 'multipart' }.merge(extra_params)
          up = connection['gcs_user_project'].to_s
          params[:userProject] = up unless up == ''

          url_up   = call('build_endpoint_url', :storage, :objects_upload_media, bucket)
          started  = Time.now
          code     = nil
          up_body = post(url_up)
            .params(params)
            .headers('Content-Type' => "multipart/related; boundary=#{boundary}", 'X-Correlation-Id' => cid)
            .payload(body)
            .request_format_raw
            .after_error_response(/.*/) do |c, b, h, _|
              error(call('normalize_http_error', c, b, h, url_up,
                        { action: 'GCS upload (multipart)', correlation_id: cid, verbose_errors: connection['verbose_errors'] }))
            end
            .after_response { |c, b, _| code = c.to_i; b }

          up = up_body.is_a?(Hash) ? up_body : (JSON.parse(up_body) rescue {})
        else
          # Media upload (content only)
          params = { uploadType: 'media', name: name }.merge(extra_params)
          up = connection['gcs_user_project'].to_s
          params[:userProject] = up unless up == ''
          url_up  = call('build_endpoint_url', :storage, :objects_upload_media, bucket)
          started = Time.now
          code    = nil
          up_body = post(url_up)
            .params(params)
            .headers('Content-Type' => mime, 'X-Correlation-Id' => cid)
            .payload(raw_bytes)
            .request_format_raw
            .after_error_response(/.*/) do |c, b, h, _|
              error(call('normalize_http_error', c, b, h, url_up,
                        { action: 'GCS upload (media)', correlation_id: cid, verbose_errors: connection['verbose_errors'] }))
            end
            .after_response { |c, b, _| code = c.to_i; b }

          up = up_body.is_a?(Hash) ? up_body : (JSON.parse(up_body) rescue {})

        end

        # Map output
        gmd = {
          'bucket'       => up['bucket']       || bucket,
          'name'         => up['name']         || name,
          'size'         => (up['size']        || bytes_len).to_i,
          'content_type' => up['contentType']  || mime,
          'updated'      => up['updated'],
          'generation'   => up['generation'],
          'md5_hash'     => up['md5Hash'],
          'crc32c'       => up['crc32c'],
          'metadata'     => up['metadata']     || (has_meta ? custom_meta : {})
        }

        {
          'gcs_object'     => gmd,
          'bytes_uploaded' => bytes_len
        }
      end

    },

    # GCS: DELETE
    gcs_delete_object: {
      title: 'GCS: Delete object',
      subtitle: 'Delete by name with optional preconditions',

      input_fields: lambda do
        [
          { name: 'bucket', optional: false },
          { name: 'object_name', optional: false },
          { name: 'preconditions', type: 'object', properties: [
              { name: 'if_generation_match', type: 'integer' },
              { name: 'if_metageneration_match', type: 'integer' }
          ]}
        ]
      end,
      output_fields: lambda do
        [
          { name: 'deleted', type: 'boolean' },
          { name: 'generation' },
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,
      sample_output: lambda do
        { 'deleted' => true, 'generation' => '1700000000000000' }
      end,
      execute: lambda do |connection, input|
        local  = call('deep_copy', input)
        bucket = (local['bucket'] || '').to_s.strip
        name   = (local['object_name'] || '').to_s

        # Support gs:// in either field
        if bucket.start_with?('gs://')
          rest  = bucket.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0]
          name   = parts[1] if name.to_s == '' && parts[1]
        end
        if name.start_with?('gs://')
          rest  = name.sub(/\Ags:\/\//, '')
          parts = rest.split('/', 2)
          bucket = parts[0] if bucket.to_s == ''
          name   = (parts[1] || '')
        end

        error('Bucket is required') if call('blank?', bucket)
        error('Object name is required') if call('blank?', name)

        pre = local['preconditions'].is_a?(Hash) ? local['preconditions'] : {}
        params = {
          ifGenerationMatch:     pre['if_generation_match'],
          ifMetagenerationMatch: pre['if_metageneration_match']
        }.reject { |_k, v| v.nil? || v.to_s == '' }
        up = connection['gcs_user_project'].to_s
        params[:userProject] = up unless up == ''

        cid           = call('gen_correlation_id')
        include_trace = connection['include_trace'] == true
        traces        = []

        url  = call('build_endpoint_url', :storage, :object, bucket, name)
        started = Time.now
        code    = nil
        resp_headers = {}
        delete(url)
          .params(params)
          .headers('X-Correlation-Id' => cid)
          .after_error_response(/.*/) do |c, b, h, _|
            error(call('normalize_http_error', c, b, h, url,
                      { action: 'GCS delete object', correlation_id: cid, verbose_errors: connection['verbose_errors'] }))
          end
          .after_response do |c, _b, h|
            code = c.to_i
            resp_headers = h || {}
          end

        traces << { 'action' => 'GCS delete object', 'correlation_id' => cid, 'status' => code,
                    'url' => url, 'dur_ms' => ((Time.now - started) * 1000.0).round } if include_trace

        gen  = resp_headers['x-goog-generation'] || resp_headers['X-Goog-Generation']
        out = { 'deleted' => true, 'generation' => gen }
        out['trace'] = traces if include_trace
        out
      end
    },

    # TRANSFER: DRIVE → GCS
    transfer_drive_to_gcs: {
      title: 'Transfer: Drive → GCS',
      subtitle: 'Export/download from Drive and upload to GCS',
      batch: true,

      input_fields: lambda do
        [
          { name: 'bucket', optional: false },
          { name: 'gcs_prefix', hint: 'e.g., exports/2025-09-30' },
          { name: 'drive_file_ids', type: 'array', of: 'string', optional: false, hint: 'IDs or URLs' },
          { name: 'content_mode_for_editors', control_type: 'select', default: 'text',
            options: [['Text (export)', 'text'], ['Skip editors', 'skip']],
            hint: 'Editors cannot produce raw bytes.' },
          { name: 'acknowledge_abuse', type: 'boolean', control_type: 'checkbox', default: false,
            hint: 'Required for some binary downloads.' },
          { name: 'naming_template', hint: 'Optional. Tokens: {name}, {ext}, {id}, {uuid}, {modified:yyyyMMdd-HHmmssZ}. Example: {name}-{id}-{modified:yyyyMMdd}.csv' },
          { name: 'prevent_overwrite', type: 'boolean', control_type: 'checkbox', default: false,
            hint: 'Sets ifGenerationMatch=0 so upload fails if object already exists.' },
          { name: 'preconditions', type: 'object', properties: [
              { name: 'if_generation_match', type: 'integer' },
              { name: 'if_metageneration_match', type: 'integer' }
            ],
            hint: 'Advanced: overrides prevent_overwrite when set.' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'uploaded', type: 'array', of: 'object', properties: [
              { name: 'drive_file_id' },
              { name: 'gcs_object', type: 'object', properties: object_definitions['gcs_object'] }
          ]},
          { name: 'failed', type: 'array', of: 'object', properties: [
              { name: 'drive_file_id' }, { name: 'error_message' }, { name: 'error_code' }
          ]},
          { name: 'summary', type: 'object', properties: [
              { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' }
          ]},
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'correlation_id' },
              { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}

        ]
      end,

      sample_output: lambda do
        {
          'uploaded' => [
            { 'drive_file_id' => '1', 'gcs_object' => {
                'bucket' => 'my-bkt', 'name' => 'exports/2025-09-30/Doc.txt', 'size' => 456,
                'content_type' => 'text/plain', 'updated' => '2025-09-30T12:00:00Z',
                'generation' => '1700000000000001', 'md5_hash' => 'ghi', 'crc32c' => 'jkl', 'metadata' => {
                  'src_drive_id' => '1', 'src_drive_mime' => 'application/vnd.google-apps.document'
                }
            }}
          ],
          'failed' => [],
          'summary' => { 'total' => 1, 'success' => 1, 'failed' => 0 }
        }
      end,
      
      execute: lambda do |connection, input|
        local  = call('deep_copy', input)

        bucket = (local['bucket'] || '').to_s.strip
        error('Bucket is required') if call('blank?', bucket)

        prefix = (local['gcs_prefix'] || '').to_s.gsub(%r{^/+|/+$}, '')

        pairs = Array(local['drive_file_ids'] || [])
                  .map { |raw| call('extract_drive_id_and_key', raw) }
                  .select { |id, _| !call('blank?', id) }
        error('No Drive file IDs provided. Map "drive_file_ids".') if pairs.empty?

        editors_mode = (local['content_mode_for_editors'] || 'text').to_s
        error('content_mode_for_editors must be "text" or "skip"') unless %w[text skip].include?(editors_mode)

        explicit_pre = local['preconditions'].is_a?(Hash) ? local['preconditions'] : {}
        extra_params = call('build_gcs_preconditions', explicit_pre, local['prevent_overwrite'] == true)

        upload_with_meta = lambda do |bytes, content_type, object_name, provenance, cid, params_extra|
          meta_hash = call('sanitize_metadata_hash', provenance)
          call('gcs_multipart_upload', connection, bucket, object_name, bytes, content_type, meta_hash, cid, (params_extra || {}))
        end

        include_trace = connection['include_trace'] == true
        traces        = []

        uploaded, failed = [], []

        pairs.each do |fid_in, rk_in|
          per_cid = call('gen_correlation_id')
          begin
            # 1. Get Drive metadata (resolve shortcuts)
            started_m = Time.now
            fid, fm, eff_rk = call('drive_fetch_meta_resolved', fid_in, per_cid,
                                  'id,name,mimeType,modifiedTime,md5Checksum,shortcutDetails(targetId,targetMimeType,targetResourceKey),resourceKey',
                                  connection['verbose_errors'] == true, rk_in)
            traces << { 'action' => 'Transfer: Get Drive meta+resolve', 'correlation_id' => per_cid, 'status' => 200,
                        'url' => '(files.get -> shortcut resolve)', 'dur_ms' => ((Time.now - started_m) * 1000.0).round } if include_trace
            rk_hdr = call('build_rk_header', fid, eff_rk)

            mime = (fm['mimeType'] || '').to_s
            name = (fm['name'] || '').to_s
            modified_iso = fm['modifiedTime'].to_s

            # 2. Acquire content (export Editors, download others)
            bytes = nil
            upload_mime = nil

            if mime.start_with?('application/vnd.google-apps.')
              if editors_mode == 'skip'
                failed << { 'drive_file_id' => fid_in, 'error_message' => "Skipped Editors file type #{mime} by configuration.", 'error_code' => 'SKIPPED' }
                next
              end
              export_mime = call('get_export_mime', mime)
              error("Export mapping not defined for Editors type #{mime}.") if export_mime.nil?

              url_exp = call('build_endpoint_url', :drive, :export, fid)
              req = get(url_exp)
                      .params(mimeType: export_mime)
                      .headers('X-Correlation-Id' => per_cid)
              req = req.headers('X-Goog-Drive-Resource-Keys' => rk_hdr) if rk_hdr
              exp_body = req.response_format_raw
                          .after_error_response(/.*/) do |c,b,h,_|
                            error(call('normalize_http_error', c, b, h, url_exp,
                                      { action: 'Transfer: Export Drive file', correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }))
                          end
                          .after_response { |_c,b,_| b }
              bytes = exp_body
              upload_mime = export_mime
            else
              url_dl = call('build_endpoint_url', :drive, :download, fid)
              req = get(url_dl)
                      .params(supportsAllDrives: true, acknowledgeAbuse: (local['acknowledge_abuse'] == true))
                      .headers('X-Correlation-Id' => per_cid)
              req = req.headers('X-Goog-Drive-Resource-Keys' => rk_hdr) if rk_hdr
              dl_body = req.response_format_raw
                        .after_error_response(/.*/) do |c,b,h,_|
                          error(call('normalize_http_error', c, b, h, url_dl,
                                    { action: 'Transfer: Download Drive file', correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }))
                        end
                        .after_response { |_c,b,_| b }
              bytes = dl_body
              upload_mime = (mime == '' ? 'application/octet-stream' : mime)
            end

            # 3. Choose GCS object name (prefix + optional template)
            export_ext = call('mime_ext_for_mime', upload_mime)
            template   = (local['naming_template'] || '').to_s
            object_name = call('render_naming_template', prefix, name, fid, modified_iso, export_ext, template)

            # 4. Provenance metadata
            provenance = {
              'src_drive_id'       => (fm['id'] || fid).to_s,
              'src_drive_mime'     => mime,
              'src_drive_modified' => modified_iso
            }

            # 5. Upload to GCS (multipart) with overwrite/preconditions
            up = upload_with_meta.call(bytes, upload_mime, object_name, provenance, per_cid, extra_params)

            # 6. Map success
            gmd = {
              'bucket'       => up['bucket']      || bucket,
              'name'         => up['name']        || object_name,
              'size'         => (up['size']       || bytes.to_s.bytesize).to_i,
              'content_type' => up['contentType'] || upload_mime,
              'updated'      => up['updated'],
              'generation'   => up['generation'],
              'md5_hash'     => up['md5Hash'],
              'crc32c'       => up['crc32c'],
              'metadata'     => up['metadata']    || {}
            }

            uploaded << { 'drive_file_id' => fid, 'gcs_object' => gmd }
          rescue => e
            failed << {
              'drive_file_id' => fid_in,
              'error_message' => e.message,
              'error_code'    => call('infer_error_code', e.message)
            }
            next
          end
        end

        out = {
          'uploaded' => uploaded,
          'failed'   => failed,
          'summary'  => { 'total' => pairs.length, 'success' => uploaded.length, 'failed' => failed.length }
        }
        out['trace'] = traces if include_trace
        out
      end

    },

    devtools_http_migration_smoke: {
      title: 'DEV: HTTP migration smoke',
      subtitle: 'One-page sanity for list/get/download & GCS list',
      input_fields: lambda do
        [
          { name: 'any_drive_file_id', hint: 'Optional: a small text file id to test download' },
          { name: 'gcs_bucket', hint: 'Optional: will call storage.objects.list' }
        ]
      end,
      output_fields: lambda do
        [
          { name: 'drive_list_status', type: 'integer' },
          { name: 'drive_get_status', type: 'integer' },
          { name: 'gcs_list_status', type: 'integer' },
          { name: 'sample_name' },
          { name: 'trace', type: 'array', of: 'object', properties: [
              { name: 'action' }, { name: 'status', type: 'integer' }, { name: 'url' }, { name: 'dur_ms', type: 'integer' }
          ]}
        ]
      end,
      execute: lambda do |connection, input|
        traces = []

        # Drive files.list (status only)
        url_list = call('build_endpoint_url', :drive, :files)
        started  = Time.now
        code     = nil
        body = get(url_list)
          .params(pageSize: 1, orderBy: 'modifiedTime desc', spaces: 'drive', corpora: 'user',
                  supportsAllDrives: true, includeItemsFromAllDrives: false,
                  fields: 'files(id,name)')
          .after_error_response(/.*/) do |c, b, h, _|
            error(call('normalize_http_error', c, b, h, url_list, { action: 'drive.files.list', verbose_errors: connection['verbose_errors'] }))
          end
          .after_response { |c, b, _| code = c.to_i; b }
        traces << { 'action' => 'drive.files.list', 'status' => code, 'url' => url_list, 'dur_ms' => ((Time.now - started) * 1000.0).round }
        name = Array(body['files']).dig(0, 'name')

        # Optional: Drive files.get alt=media for a tiny text
        get_status = nil
        if (fid = input['any_drive_file_id'].to_s.strip) != ''
          url_get = call('build_endpoint_url', :drive, :download, fid)
          started = Time.now
          get(url_get)
            .params(supportsAllDrives: true)
            .response_format_raw
            .after_error_response(/.*/) do |c, b, h, _|
              error(call('normalize_http_error', c, b, h, url_get, { action: 'drive.files.get(media)' }))
            end
            .after_response { |c, _b, _| get_status = c.to_i }
          traces << { 'action' => 'drive.files.get(media)', 'status' => get_status, 'url' => url_get, 'dur_ms' => ((Time.now - started) * 1000.0).round }
        end

        # Optional: storage.objects.list
        gcs_status = nil
        if (bk = input['gcs_bucket'].to_s.strip) != ''
          url_gcs = call('build_endpoint_url', :storage, :objects_list, bk)
          started = Time.now
          get(url_gcs)
            .params(maxResults: 1, fields: 'items(name)')
            .after_error_response(/.*/) do |c, b, h, _|
              error(call('normalize_http_error', c, b, h, url_gcs, { action: 'storage.objects.list' }))
            end
            .after_response { |c, _b, _| gcs_status = c.to_i }
          traces << { 'action' => 'storage.objects.list', 'status' => gcs_status, 'url' => url_gcs, 'dur_ms' => ((Time.now - started) * 1000.0).round }
        end

        {
          'drive_list_status' => code,
          'drive_get_status'  => get_status,
          'gcs_list_status'   => gcs_status,
          'sample_name'       => name,
          'trace'             => traces
        }
      end
    }
  },

  methods: {

    # ---------- URL + endpoint helpers ----------

    build_endpoint_url: lambda do |service, op, *args|
      s = service.to_s
      o = op.to_s

      case s
      when 'oauth2'
        case o
        when 'authorize' then 'https://accounts.google.com/o/oauth2/v2/auth'
        when 'token'     then 'https://oauth2.googleapis.com/token'
        else
          error("Unknown oauth2 op=#{o}")
        end

      when 'drive'
        base = 'https://www.googleapis.com/drive/v3'
        case o
        when 'about'    then "#{base}/about"
        when 'files'    then "#{base}/files"
        when 'file'
          fid = (args[0] || '').to_s
          error('fileId required') if fid == ''
          "#{base}/files/#{fid}"
        when 'download'
          # Caller will pass alt=media via .params(alt: 'media') for consistency
          fid = (args[0] || '').to_s
          error('fileId required') if fid == ''
          "#{base}/files/#{fid}"
        when 'export'
          fid = (args[0] || '').to_s
          error('fileId required') if fid == ''
          "#{base}/files/#{fid}/export"
        when 'changes'            then "#{base}/changes"
        when 'changes_start_token' then "#{base}/changes/startPageToken"
        else
          error("Unknown drive op=#{o}")
        end

      when 'storage'
        # NOTE: For object path params we must percent-encode the whole name INCLUDING slashes as %2F.
        api_base   = 'https://storage.googleapis.com/storage/v1'
        upload_base = 'https://www.storage.googleapis.com/upload/storage/v1'
        case o
        when 'objects_list'
          bucket = (args[0] || '').to_s
          error('bucket required') if bucket == ''
          "#{api_base}/b/#{bucket}/o"
        when 'object', 'download'
          bucket = (args[0] || '').to_s
          object = (args[1] || '').to_s
          error('bucket required') if bucket == ''
          error('object required') if object == ''
          enc = call('encode_gcs_object', object)
          # For storage downloads, callers already pass alt=media explicitly.
          "#{api_base}/b/#{bucket}/o/#{enc}"
        when 'objects_upload_media'
          bucket = (args[0] || '').to_s
          error('bucket required') if bucket == ''
          "#{upload_base}/b/#{bucket}/o"
        else
          error("Unknown storage op=#{o}")
        end

      else
        error("Unknown service=#{s}")
      end
    end,

    build_query_string: lambda do |params|
      h = params.is_a?(Hash) ? params : {}
      pairs = h.each_with_object([]) do |(k, v), acc|
        next if k.nil? || v.nil?
        acc << "#{k}=#{call('url_encode_component', v.to_s)}"
      end
      pairs.join('&')
    end,

    url_encode_component: lambda do |str|
      # Percent-encode for **path segments** (RFC 3986 unreserved only).
      s = str.to_s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '')
      s.bytes.map { |b|
        if (b >= 0x30 && b <= 0x39) || (b >= 0x41 && b <= 0x5A) || (b >= 0x61 && b <= 0x7A) || [0x2D,0x2E,0x5F,0x7E].include?(b)
          b.chr
        else
          '%%%02X' % b
        end
      }.join
    end,

    encode_gcs_object: lambda do |name|
      # Encode entire object name as a single path segment (slashes included)
      call('url_encode_component', name.to_s)
    end,

    # ---------- HTTP wrapper with retries, JSON-error normalization ----------

    normalize_http_error: lambda do |code, body, headers, url, context|
      status_i = code.to_i
      status_s = call('status_text', status_i)

      # Try to extract Google JSON error info
      gerr = {}
      if body.is_a?(Hash) && body['error'].is_a?(Hash)
        gerr = body['error']
      elsif body.is_a?(String) && body.lstrip.start_with?('{')
        begin
          parsed = JSON.parse(body) rescue {}
          gerr = parsed['error'] if parsed.is_a?(Hash) && parsed['error'].is_a?(Hash)
        rescue
          # ignore parser failure
        end
      end

      reason  = (gerr['errors'] && gerr['errors'].is_a?(Array) && gerr['errors'][0].is_a?(Hash)) ? gerr['errors'][0]['reason'] : nil
      statusN = (gerr['status'] || '').to_s
      msg     = (gerr['message'] || '').to_s

      action  = (context[:action] || context['action']).to_s
      h       = headers.is_a?(Hash) ? headers : {}
      cid     = (context[:correlation_id] || context['correlation_id'] || h['X-Correlation-Id'] || h['x-correlation-id']).to_s
      verbose = !!(context[:verbose_errors] || context['verbose_errors'])

      pieces = []
      pieces << "HTTP #{status_i} #{status_s}"
      pieces << statusN unless statusN.empty?
      pieces << reason  unless reason.to_s.empty?
      lead   = pieces.join(' – ')
      lead   = "HTTP #{status_i} #{status_s}" if lead.strip.empty?

      tail   = []
      tail << "action=#{action}" unless action.empty?
      tail << "cid=#{cid}" unless cid.empty?
      tail << "url=#{url}"

      # Only include concise upstream body snippet when verbose
      if verbose
        snippet =
          if body.is_a?(String)
            body[0, 512]
          elsif body.is_a?(Hash)
            JSON.generate(body)[0, 512]
          else
            body.to_s[0, 512]
          end
        tail << "upstream=#{snippet}"
      end

      # Helpful hints
      raw = (body.is_a?(String) ? body : (body.to_s rescue ''))
      upstream_text = [msg, raw].join(' ')
      if upstream_text =~ /unregistered callers|without established identity|Please use API key/i
        tail << "hint=Auth header missing. Verify connection auth_type and reconnect (OAuth2: complete consent; Service Account: service_account_email/private_key [+ subject_email for DWD])."
      end
      if upstream_text =~ /userProjectMissing|requester[\s-]?pays/i
        tail << "hint=GCS requester-pays: set **GCS billing project (userProject)** on the connection."
      end
      if upstream_text =~ /exportSizeLimitExceeded/i
        tail << "hint=Drive files.export max is 10 MB. Reduce size, split sheets, or export a different format."
      end
      if upstream_text =~ /cannotDownloadAbusiveFile/i
        tail << "hint=File flagged as abusive. Set 'acknowledge_abuse=true' to proceed."
      end
      if upstream_text =~ /downloadQuotaExceeded/i
        tail << "hint=Drive download quota exceeded for this file. Try later or copy the file to reset quotas."
      end

      [lead, msg].reject(&:empty?).join(': ') + " (#{tail.join(' | ')})"
    end,

    status_text: lambda do |code|
      {
        200 => 'OK', 201 => 'Created', 202 => 'Accepted', 204 => 'No Content',
        304 => 'Not Modified',
        400 => 'Bad Request', 401 => 'Unauthorized', 403 => 'Forbidden',
        404 => 'Not Found', 409 => 'Conflict', 412 => 'Precondition Failed',
        413 => 'Payload Too Large', 415 => 'Unsupported Media Type',
        429 => 'Too Many Requests',
        500 => 'Internal Server Error', 502 => 'Bad Gateway',
        503 => 'Service Unavailable', 504 => 'Gateway Timeout'
      }[code.to_i] || ''
    end,

    # ---------- Correlation/trace ----------

    gen_correlation_id: lambda do
      begin
        SecureRandom.uuid
      rescue
        t = (Time.now.to_f * 1000).to_i
        r = (rand * 1_000_000_000).to_i
        "wrkto-#{t}-#{r}"
      end
    end,

    # Optional trace pack for future use (attach to outputs if include_trace=true)
    trace_pack: lambda do |context, http_result|
      {
        'action'         => (context[:action] || context['action']).to_s,
        'correlation_id' => (context[:correlation_id] || context['correlation_id']).to_s,
        'status'         => http_result['status'].to_i,
        'url'            => (context[:url] || context['url']).to_s,
        'dur_ms'         => (http_result['duration'] || 0).to_i
      }
    end,

    # ---------- Type/format utilities ----------

    to_iso8601: lambda do |dt|
      return nil if dt.nil? || dt.to_s.strip == ''
      t =
        case dt
        when Integer then Time.at(dt)
        when Float   then Time.at(dt)
        when String  then Time.parse(dt) rescue nil
        else
          dt.respond_to?(:to_time) ? dt.to_time : nil
        end 
      t ? t.utc.iso8601 : nil # fallback
    end,

    safe_utf8: lambda do |bytes|
      s = bytes.to_s
      s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '�')
    end,

    textual_mime?: lambda do |mime|
      m = (mime || '').to_s
      m.start_with?('text/') || %w[application/json application/xml text/csv image/svg+xml].include?(m)
    end,

    strip_urls_from_text: lambda do |text|
      t = text.to_s
      # Remove http/https/ftp and bare www.* URLs.
      # Preserve emails and filesystem paths.
      t.gsub(%r{(?<!@)\b((?:https?|ftp)://[^\s<>"')]+|www\.[^\s<>"')]+)}i, '')
    end,

    deep_copy: lambda do |obj|
      JSON.parse(JSON.generate(obj)) rescue obj
    end,

    blank?: lambda do |v|
      v.nil? || (v.respond_to?(:empty?) && v.empty?) || v.to_s.strip == ''
    end,

    # --- Naming + overwrite helpers ---

    mime_ext_for_mime: lambda do |m|
      case (m || '')
      when 'text/plain'    then 'txt'
      when 'text/csv'      then 'csv'
      when 'image/svg+xml' then 'svg'
      when 'application/json' then 'json'
      when 'application/xml'  then 'xml'
      when 'text/html'        then 'html'
      when 'text/markdown'    then 'md'
      else nil
      end
    end,

    format_modified_for_pattern: lambda do |modified_iso, pattern|
      t0  = (Time.parse(modified_iso.to_s) rescue (Time.iso8601(modified_iso.to_s) rescue Time.now)).utc
      fmt = pattern.to_s
                  .gsub('yyyy', '%Y')
                  .gsub('MM',   '%m')
                  .gsub('dd',   '%d')
                  .gsub('HH',   '%H')
                  .gsub('mm',   '%M')
                  .gsub('ss',   '%S')
                  .gsub('Z',    'Z')
      t0.strftime(fmt)
    end,

    render_naming_template: lambda do |prefix_in, orig_name, fid, modified_iso, ext_override, tmpl|
      base = (orig_name || '').to_s
      if (idx = base.rindex('.'))
        name_noext = base[0...idx]
        orig_ext   = base[(idx + 1)..-1]
      else
        name_noext = base
        orig_ext   = ''
      end
      ext = (ext_override || orig_ext || '').to_s

      # Default path if no template: keep/append extension
      if tmpl.to_s.strip == ''
        fname =
          if ext != '' && (orig_ext == '' || orig_ext.downcase != ext.downcase)
            "#{name_noext}.#{ext}"
          elsif orig_ext != ''
            "#{name_noext}.#{orig_ext}"
          else
            name_noext
          end
        return call('safe_object_name', prefix_in, fname)
      end

      t = tmpl.to_s.dup
      # {modified:pattern}
      t = t.gsub(/\{modified:([^}]+)\}/) { call('format_modified_for_pattern', modified_iso, Regexp.last_match(1)) }

      uuid = (SecureRandom.uuid rescue call('gen_correlation_id'))
      t = t.gsub('{name}', name_noext)
          .gsub('{ext}',  ext)
          .gsub('{id}',   fid.to_s)
          .gsub('{uuid}', uuid.to_s)

      # Tidy slashes and edges; keep internal folders from template
      t = t.gsub(%r{/{2,}}, '/').gsub(%r{\A/+|/+\z}, '')
      whole = prefix_in.to_s == '' ? t : "#{prefix_in.to_s.gsub(%r{\A/+|/+\z}, '')}/#{t}"
      call('safe_object_name', '', whole)
    end,

    build_gcs_preconditions: lambda do |explicit_pre, prevent_overwrite|
      if explicit_pre.is_a?(Hash) && !explicit_pre.empty?
        {
          ifGenerationMatch:     explicit_pre['if_generation_match'],
          ifMetagenerationMatch: explicit_pre['if_metageneration_match']
        }.reject { |_k, v| v.nil? || v.to_s == '' }
      elsif prevent_overwrite == true
        { ifGenerationMatch: 0 }
      else
        {}
      end
    end,

    # ---------- Drive helpers ----------

    drive_fetch_meta_resolved: lambda do |fid_in, correlation_id, fields, verbose_errors=false, resource_key=nil|
      fid = (fid_in || '').to_s
      base_fields = (fields || 'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress),shortcutDetails(targetId,targetMimeType,targetResourceKey),resourceKey')

      # Initial metadata (may require a resourceKey)
      hdr = call('build_rk_header', fid, resource_key)
      url_meta = call('build_endpoint_url', :drive, :file, fid)
      req = get(url_meta)
              .params(fields: base_fields, supportsAllDrives: true)
              .headers('X-Correlation-Id' => correlation_id)
      req = req.headers('X-Goog-Drive-Resource-Keys' => hdr) if hdr
      meta = req.after_error_response(/.*/) do |c,b,h,_|
              error(call('normalize_http_error', c, b, h, url_meta,
                          { action: 'Drive get (meta)', correlation_id: correlation_id, verbose_errors: verbose_errors }))
            end

      m = meta.is_a?(Hash) ? meta : {}
      # Resolve shortcuts; target can have its own resourceKey
      if m['mimeType'] == 'application/vnd.google-apps.shortcut' && m.dig('shortcutDetails','targetId')
        fid2  = m.dig('shortcutDetails','targetId').to_s
        rk2   = m.dig('shortcutDetails','targetResourceKey') || resource_key
        hdr2  = call('build_rk_header', fid2, rk2)
        url2  = call('build_endpoint_url', :drive, :file, fid2)

        req2 = get(url2)
                .params(fields: base_fields.sub(/,?shortcutDetails\([^)]*\)/, ''), supportsAllDrives: true)
                .headers('X-Correlation-Id' => correlation_id)
        req2 = req2.headers('X-Goog-Drive-Resource-Keys' => hdr2) if hdr2

        meta2 = req2.after_error_response(/.*/) do |c,b,h,_|
                  error(call('normalize_http_error', c, b, h, url2,
                            { action: 'Drive get (target meta)', correlation_id: correlation_id, verbose_errors: verbose_errors }))
                end
        return [fid2, (meta2.is_a?(Hash) ? meta2 : {}), rk2]
      end

      [fid, m, (m['resourceKey'] || resource_key)]
    end,

    extract_drive_file_id: lambda do |raw|
      s = raw.to_s.strip
      return nil if s == ''
      return s unless s.include?('/') || s.include?('?')

      # Common patterns: /file/d/{id}, /document/d/{id}, /spreadsheets/d/{id}, /presentation/d/{id}, /uc?id={id}, ?id={id}
      patterns = [
        %r{/d/([a-zA-Z0-9_-]{10,})},               # /.../d/{id}
        %r{[?&]id=([a-zA-Z0-9_-]{10,})},           # ?id={id}
        %r{/file/u/\d+/d/([a-zA-Z0-9_-]{10,})}     # /file/u/x/d/{id}
      ]
      patterns.each do |rx|
        m = s.match(rx)
        return m[1] if m && m[1]
      end
      nil
    end,

    extract_drive_folder_id: lambda do |raw|
      s = raw.to_s.strip
      return nil if s == ''
      return s unless s.include?('/') || s.include?('?')

      # Patterns: /folders/{id}, ?id={id}
      patterns = [
        %r{/folders/([a-zA-Z0-9_-]{10,})},
        %r{[?&]id=([a-zA-Z0-9_-]{10,})}
      ]
      patterns.each do |rx|
        m = s.match(rx)
        return m[1] if m && m[1]
      end
      nil
    end,

    extract_drive_id_and_key: lambda do |raw|
      s = raw.to_s.strip
      return [nil, nil] if s == ''
      id = call('extract_drive_file_id', s) || s
      m  = s.match(/[?&](?:resourcekey|resourceKey)=([^&]+)/)
      [id, (m && m[1])]
    end,

    maybe_extract_id: lambda do |raw|
      s = raw.to_s.strip
      return '' if s == ''
      call('extract_drive_file_id', s) || call('extract_drive_folder_id', s) || s
    end,

    build_drive_query: lambda do |opts|
      o = opts || {}
      clauses = ["trashed=false"]

      if (fid = o[:folder_id] || o['folder_id']).to_s.strip != ''
        clauses << "'#{fid}' in parents"
      end

      if (aft = o[:modified_after] || o['modified_after']).to_s.strip != ''
        clauses << "modifiedTime >= '#{aft}'"
      end
      if (bef = o[:modified_before] || o['modified_before']).to_s.strip != ''
        clauses << "modifiedTime <= '#{bef}'"
      end

      if (mt = o[:mime_type] || o['mime_type']).to_s.strip != ''
        safe = mt.gsub("'", "\\'")
        clauses << "mimeType = '#{safe}'"
      end

      exclude_folders = !!(o[:exclude_folders] || o['exclude_folders'])
      if exclude_folders
        clauses << "mimeType != 'application/vnd.google-apps.folder'"
      end

      clauses.join(' and ')
    end,

    build_rk_header: lambda do |file_id, rk|
      fid = file_id.to_s.strip
      key = rk.to_s.strip
      return nil if fid.empty? || key.empty?
      "#{fid}/#{key}"
    end,

    get_export_mime: lambda do |editors_mime|
      case (editors_mime || '').to_s
      when 'application/vnd.google-apps.document'   then 'text/plain'
      when 'application/vnd.google-apps.spreadsheet' then 'text/csv'
      when 'application/vnd.google-apps.presentation' then 'text/plain'
      when 'application/vnd.google-apps.drawing'    then 'image/svg+xml'
      else
        nil
      end
    end,

    # ---------- HTTP + Drive content helpers (new) ----------

    drive_build_headers: lambda do |cid, rk_hdr=nil|
      h = { 'X-Correlation-Id' => cid }
      h['X-Goog-Drive-Resource-Keys'] = rk_hdr if rk_hdr
      h
    end,

    http_get_raw_with_trace: lambda do |url, params, headers, action, verbose|
      started = Time.now
      status  = nil
      body = get(url)
               .params(params || {})
               .headers(headers || {})
               .response_format_raw
               .after_error_response(/.*/) do |c, b, h, _|
                 error(call('normalize_http_error', c, b, h, url,
                            { action: action, correlation_id: (headers || {})['X-Correlation-Id'], verbose_errors: verbose }))
               end
               .after_response { |c, b, _| status = c.to_i; b }
      trace = {
        'action' => action,
        'correlation_id' => (headers || {})['X-Correlation-Id'],
        'status' => status,
        'url' => url,
        'dur_ms' => ((Time.now - started) * 1000.0).round
      }
      [body, trace]
    end,

    drive_export_text_with_trace: lambda do |connection, fid, export_mime, cid, rk_hdr|
      url     = call('build_endpoint_url', :drive, :export, fid)
      params  = { mimeType: export_mime }
      headers = call('drive_build_headers', cid, rk_hdr)
      raw, tr = call('http_get_raw_with_trace', url, params, headers, 'Drive export (text)', connection['verbose_errors'] == true)
      [call('safe_utf8', raw), tr]
    end,

    drive_export_raw_with_trace: lambda do |connection, fid, export_mime, cid, rk_hdr|
      url     = call('build_endpoint_url', :drive, :export, fid)
      params  = { mimeType: export_mime }
      headers = call('drive_build_headers', cid, rk_hdr)
      raw, tr = call('http_get_raw_with_trace', url, params, headers, 'Drive export (bytes)', connection['verbose_errors'] == true)
      [raw, export_mime, tr]
    end,

    drive_download_blob_with_trace: lambda do |connection, fid, ack, cid, rk_hdr, action|
      # Use files.get with alt=media, not a hard-coded ?alt=media URL
      url     = call('build_endpoint_url', :drive, :file, fid)
      params  = { alt: 'media', supportsAllDrives: true, acknowledgeAbuse: !!ack }
      headers = call('drive_build_headers', cid, rk_hdr)
      call('http_get_raw_with_trace', url, params, headers, action, connection['verbose_errors'] == true)
    end,

    drive_map_output_from_meta: lambda do |mdata|
      {
        'id'            => mdata['id'],
        'name'          => mdata['name'],
        'mime_type'     => mdata['mimeType'],
        'size'          => (mdata['size'] || 0).to_i,
        'modified_time' => mdata['modifiedTime'],
        'checksum'      => mdata['md5Checksum'],
        'owners'        => Array(mdata['owners']).map { |o|
                            { 'displayName' => o['displayName'], 'emailAddress' => o['emailAddress'] }
                          }
      }
    end,

    # Returns [fields_hash, traces_array]
    drive_fetch_content_fields: lambda do |connection, fid, mime, mode, ack, strip, rk_hdr, cid|
      traces = []
      out = {}
      case mode.to_s
      when 'none'
        return [out, traces]
      when 'bytes'
        if mime.to_s.start_with?('application/vnd.google-apps.')
          error('Editors files require content_mode=text (export).')
        end
        raw, tr = call('drive_download_blob_with_trace', connection, fid, ack, cid, rk_hdr, 'Drive download (bytes)')
        traces << tr
        out['content_bytes'] = [raw.to_s].pack('m0')
        return [out, traces]
      else # 'text'
        if mime.to_s.start_with?('application/vnd.google-apps.')
          export_mime = call('get_export_mime', mime)
          error("Export mapping not defined for Editors type #{mime}.") if export_mime.nil?
          txt, tr = call('drive_export_text_with_trace', connection, fid, export_mime, cid, rk_hdr)
          traces << tr
          txt = call('strip_urls_from_text', txt) if strip
          out['text_content'] = txt
          out['exported_as']  = export_mime
          return [out, traces]
        end
        if call('textual_mime?', mime)
          raw, tr = call('drive_download_blob_with_trace', connection, fid, ack, cid, rk_hdr, 'Drive download (text)')
          traces << tr
          txt = call('safe_utf8', raw)
          txt = call('strip_urls_from_text', txt) if strip
          out['text_content'] = txt
          return [out, traces]
        else
          raw, tr = call('drive_download_blob_with_trace', connection, fid, ack, cid, rk_hdr, 'Drive download (auto-bytes)')
          traces << tr
          out['content_bytes'] = [raw.to_s].pack('m0')
          return [out, traces]
        end
      end
    end,

    # For transfer: return raw bytes + content type + trace(s)
    drive_fetch_bytes_for_transfer: lambda do |connection, fid, mime, editors_mode, ack, rk_hdr, cid|
      traces = []
      if mime.to_s.start_with?('application/vnd.google-apps.')
        if editors_mode.to_s == 'skip'
          return [nil, nil, traces, :skipped]
        end
        export_mime = call('get_export_mime', mime)
        error("Export mapping not defined for Editors type #{mime}.") if export_mime.nil?
        raw, upload_mime, tr = call('drive_export_raw_with_trace', connection, fid, export_mime, cid, rk_hdr)
        traces << tr
        return [raw, upload_mime, traces, nil]
      else
        raw, tr = call('drive_download_blob_with_trace', connection, fid, ack, cid, rk_hdr, 'Transfer: Download Drive file')
        traces << tr
        upload_mime = (mime.to_s == '' ? 'application/octet-stream' : mime)
        return [raw, upload_mime, traces, nil]
      end
    end,

    # ---------- GCS helpers ----------

    sanitize_metadata_hash: lambda do |h|
      out = {}
      (h.is_a?(Hash) ? h : {}).each do |k, v|
        next if k.nil?
        out[k.to_s] = v.nil? ? '' : v.to_s
      end
      out
    end,

    safe_object_name: lambda do |prefix, filename|
      p = (prefix || '').to_s.gsub(%r{\A/+|/+\z}, '')
      fn = (filename || '').to_s
      # Remove control chars, collapse whitespace, strip leading ./ and spaces
      fn = fn.gsub(/[\u0000-\u001F]/, '').gsub(/\s+/, ' ').gsub(/\A[\.\/\s]+/, '')
      # Fall back if empty
      fn = 'unnamed' if fn == ''
      parts = []
      parts << p unless p == ''
      parts << fn
      parts.join('/')
    end,

    gcs_multipart_upload: lambda do |connection, bucket, object_name, bytes, content_type, metadata_hash, correlation_id, extra_params|
      boundary  = "wrkto-#{(call('gen_correlation_id') || '').gsub('-', '')}"
      meta_json = JSON.generate({
        'name'        => object_name,
        'contentType' => content_type,
        'metadata'    => (metadata_hash || {})
      })

      part1 = "--#{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n#{meta_json}\r\n"
      part2 = "--#{boundary}\r\nContent-Type: #{content_type}\r\n\r\n"
      part3 = "\r\n--#{boundary}--"

      body = ''.b
      body << part1.dup.force_encoding('ASCII-8BIT')
      body << part2.dup.force_encoding('ASCII-8BIT')
      body << bytes.to_s.b
      body << part3.dup.force_encoding('ASCII-8BIT')

      url = call('build_endpoint_url', :storage, :objects_upload_media, bucket)
      params = { uploadType: 'multipart' }.merge(extra_params || {})
      up = connection['gcs_user_project'].to_s rescue ''
      params[:userProject] = up unless up == ''

      started = Time.now
      code    = nil
      resp_body = post(url)
        .params(params)
        .headers('Content-Type' => "multipart/related; boundary=#{boundary}", 'X-Correlation-Id' => correlation_id)
        .payload(body)
        .request_format_raw
        .after_error_response(/.*/) do |c, b, h, _|
          error(call('normalize_http_error', c, b, h, url,
                    { action: 'GCS upload (multipart)', correlation_id: correlation_id, verbose_errors: connection['verbose_errors'] }))
        end
        .after_response { |c, b, _| code = c.to_i; b }

      resp_body.is_a?(Hash) ? resp_body : (JSON.parse(resp_body) rescue {})
    end,

    gcs_normalize_bucket_and_name: lambda do |bucket_in, name_in|
      bucket = (bucket_in || '').to_s.strip
      name   = (name_in || '').to_s
      if bucket.start_with?('gs://')
        rest  = bucket.sub(/\Ags:\/\//, '')
        parts = rest.split('/', 2)
        bucket = parts[0]
        name   = parts[1] if name.to_s == '' && parts[1]
      end
      if name.start_with?('gs://')
        rest  = name.sub(/\Ags:\/\//, '')
        parts = rest.split('/', 2)
        bucket = parts[0] if bucket.to_s == ''
        name   = (parts[1] || '')
      end
      [bucket, name]
    end,

    # ---------- Error code mapping (for batch summaries etc.) ----------

    infer_error_code: lambda do |msg|
      m = msg.to_s
      return 'UNAUTHORIZED'        if m =~ /\b401\b|unauth/i
      return 'FORBIDDEN'           if m =~ /\b403\b|forbidden/i
      return 'NOT_FOUND'           if m =~ /\b404\b|not found/i
      return 'CONFLICT'            if m =~ /\b409\b|conflict/i
      return 'PRECONDITION_FAILED' if m =~ /\b412\b|precondition/i
      return 'PAYLOAD_TOO_LARGE'   if m =~ /\b413\b|payload too large/i
      return 'UNSUPPORTED_MEDIA'   if m =~ /\b415\b|unsupported media/i
      return 'RATE_LIMITED'        if m =~ /\b429\b|rateLimitExceeded|userRateLimitExceeded/i
      return 'INTERNAL'            if m =~ /\b5\d\d\b|internalError|backendError/i
      return 'BAD_REQUEST'         if m =~ /\b400\b|bad request/i
      nil
    end

  }

}

