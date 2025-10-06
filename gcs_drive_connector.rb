{
  title: 'Drive Utilities',
  description: 'Google Drive utilities with enterprise resilience, telemetry, and multi-auth',
  version: "0.2.1",
  custom_action: false,

  connection: {
    fields: [
      # -------- Developer options --------
      { name: 'verbose_errors',  label: 'Verbose errors', type: 'boolean', control_type: 'checkbox', hint: 'Include upstream response bodies in normalized error messages (useful in non-prod).' },
      { name: 'include_trace',   label: 'Include trace in outputs', type: 'boolean', control_type: 'checkbox', default: true, sticky: true },

      # -------- Authentication selection --------
      { name: 'auth_type', label: 'Authentication type', control_type: 'select', optional: false, default: 'oauth2', extends_schema: true, hint: 'Choose how to authenticate to Google Drive.',
        options: [
          ['OAuth 2.0 (user delegated)', 'oauth2'],
          ['Service account (JWT)',       'custom']
        ] },
      { name: 'enable_gcs', label: 'Enable Google Cloud Storage', type: 'boolean', control_type: 'checkbox',
        default: false, sticky: true, hint: 'Adds Cloud Storage scopes for listing/uploading objects (requires reconnect).' }
    ],

    authorization: {
      type: 'multi',

      selected: lambda do |connection|
        (connection['auth_type'] || 'oauth2').to_s
      end,

      identity: lambda do |connection|
        selected = (connection['auth_type'] || 'custom').to_s
        if selected == 'oauth2'
          begin
            info = call('http_request',
              method: :get,
              url: 'https://openidconnect.googleapis.com/v1/userinfo',
              headers: {}, # auth applied by 'apply'
              context: { action: 'OIDC userinfo' }
            )['data'] || {}
            email = info['email'] || '(no email)'
            name  = info['name']
            sub   = info['sub']
            [name, email, sub].compact.join(' / ')
          rescue
            'OAuth2 (Google) - identity unavailable'
          end
        else
          connection['service_account_email']
        end
      end,

      options: {
        oauth2: {
          type: 'oauth2',
          fields: [
            { name: 'client_id',     group: 'OAuth 2.0 (user delegated)', optional: false, hint: 'Google Cloud OAuth2 client ID' },
            { name: 'client_secret', group: 'OAuth 2.0 (user delegated)', optional: false, control_type: 'password', hint: 'Google Cloud OAuth2 client secret' }],

          authorization_url: lambda do |connection|
            scopes = ['https://www.googleapis.com/auth/drive.readonly']
            # If GCS enabled, append
            scopes << 'https://www.googleapis.com/auth/devstorage.read_write' if connection['enable_gcs'] == true
            q = call('build_query_string', {
              client_id:  connection['client_id'],
              response_type: 'code',
              scope: scopes.join(' '),
              access_type: 'offline',
              include_granted_scopes: 'true',
              prompt: 'consent',
              redirect_uri: 'https://www.workato.com/oauth/callback'
            })
            "#{call('build_endpoint_url', :oauth2, :authorize)}?#{q}"
          end,

          acquire: lambda do |connection, auth_code|
            resp = call('http_request',
              method: :post,
              url: call('build_endpoint_url', :oauth2, :token),
              payload: {
                client_id:     connection['client_id'],
                client_secret: connection['client_secret'],
                grant_type:    'authorization_code',
                code:          auth_code,
                redirect_uri:  'https://www.workato.com/oauth/callback'
              },
              www_form_urlencoded: true,
              context: { action: 'OAuth token exchange', verbose_errors: connection['verbose_errors'] }
            )
            resp['data']
          end,

          refresh: lambda do |connection, refresh_token|
            resp = call('http_request',
              method: :post,
              url: call('build_endpoint_url', :oauth2, :token),
              payload: {
                client_id:     connection['client_id'],
                client_secret: connection['client_secret'],
                grant_type:    'refresh_token',
                refresh_token: refresh_token
              },
              www_form_urlencoded: true,
              context: { action: 'OAuth token refresh', verbose_errors: connection['verbose_errors'] }
            )
            resp['data']
          end,

          apply: lambda do |_connection, access_token|
            headers(Authorization: "Bearer #{access_token}")
          end
        },

        custom: {
          type: 'custom_auth',
          fields: [
            { name: 'service_account_email', group: 'Service account', optional: false, hint: 'e.g. my-sa@project.iam.gserviceaccount.com' },
            { name: 'client_id',             group: 'Service account', optional: false, hint: 'Service account client ID (used as JWT kid)' },
            { name: 'private_key_id',        group: 'Service account', optional: false, hint: 'The key’s private_key_id from the JSON' },
            { name: 'private_key',           group: 'Service account', control_type: 'password', multiline: true, optional: false, hint: 'Paste the PEM private key from the JSON. Newlines may appear as \\n; both forms are handled.' }
          ],

          acquire: lambda do |connection|
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
              'iss'   => connection['service_account_email'],
              'scope' => scopes.join(' '), # space-separated per Google
              'aud'   => aud,
              'iat'   => iat,
              'exp'   => exp
            }

            jwt = workato.jwt_encode(
              claim,
              private_key,
              'RS256',
              kid: connection['private_key_id'] # must be the key's private_key_id, not client_id
            )

            resp = call('http_request',
              method: :post,
              url: aud,
              payload: {
                grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                assertion: jwt
              },
              www_form_urlencoded: true,
              context: { action: 'Service account JWT exchange' }
            )

          {
            access_token: resp['data']['access_token'],
            expires_at:  (Time.now + resp['data']['expires_in'].to_i).iso8601
          }
          end,
          apply: lambda do |connection|
            headers(Authorization: "Bearer #{connection['access_token']}")
          end,
          refresh_on: [401]
        }
      }
    }
  },

  test: lambda do |connection|
    # Drive probe
    call('http_request',
      method: :get,
      url:    call('build_endpoint_url', :drive, :about),
      params: { fields: 'user,storageQuota' },
      context: { action: 'Drive about' }
    )

    if connection['enable_gcs'] == true
      # Ask Google which scopes are on this token
      ti = call('http_request',
        method: :get,
        url: 'https://www.googleapis.com/oauth2/v1/tokeninfo',
        params: {}, # token inferred from Authorization header
        context: { action: 'Token info' }
      )['data']

      scopes = (ti['scope'] || '').split(/\s+/)
      error('GCS scope missing (devstorage.read_write). Reconnect with “Enable Google Cloud Storage”.') \
        unless scopes.include?('https://www.googleapis.com/auth/devstorage.read_write')
    end

    true
  end,

  object_definitions: {
    # System
    telemetry: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'success',   type: 'boolean' },
          { name: 'timestamp' },
          { name: 'metadata',  type: 'object' },
          { name: 'trace',     type: 'object', properties: [
              { name: 'correlation_id' }, { name: 'duration_ms', type: 'integer' }
          ] }
        ]
      end
    },
    http_meta: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'status',  type: 'integer' },
          { name: 'headers', type: 'object' },
          { name: 'request', type: 'object', properties: [
              { name: 'method' }, { name: 'url' }, { name: 'params', type: 'object' }
          ] }
        ]
      end
    },
    # Drive
    drive_file_basic: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'id' }, { name: 'name' }, { name: 'mime_type' },
          { name: 'size', type: 'integer' }, { name: 'modified_time' },
          { name: 'checksum' },
          { name: 'owners', type: 'array', of: 'object', properties: [
              { name: 'displayName' }, { name: 'emailAddress' }
          ] }
        ]
      end
    },
    drive_file_full: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'id' }, { name: 'name' }, { name: 'mime_type' },
          { name: 'size', type: 'integer' }, { name: 'modified_time' },
          { name: 'checksum' },
          { name: 'owners', type: 'array', of: 'object', properties: [
              { name: 'displayName' }, { name: 'emailAddress' }
          ]},
          { name: 'text_content',     type: 'string' },
          { name: 'needs_processing', type: 'boolean' },
          { name: 'export_mime_type' },
          { name: 'fetch_method' },
          { name: 'telemetry', type: 'object', properties: [
              { name: 'success', type: 'boolean' }, { name: 'timestamp' }, { name: 'metadata', type: 'object' },
              { name: 'trace', type: 'object', properties: [
                  { name: 'correlation_id' }, { name: 'duration_ms', type: 'integer' }
              ]}
          ]},
          { name: 'api_meta',    type: 'object', properties: [
              { name: 'status', type: 'integer' },
              { name: 'headers', type: 'object' },
              { name: 'request', type: 'object', properties: [
                  { name: 'method' }, { name: 'url' }, { name: 'params', type: 'object' } ] }]},
          { name: 'api_content', type: 'object', properties: [
              { name: 'status', type: 'integer' },
              { name: 'headers', type: 'object' },
              { name: 'request', type: 'object', properties: [
                  { name: 'method' }, { name: 'url' }, { name: 'params', type: 'object' }] }]},
        ]
      end
    },
    # Google Cloud Platform
    gcs_object_basic: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'bucket' }, { name: 'name' }, { name: 'size', type: 'integer' },
          { name: 'content_type' }, { name: 'updated' }, { name: 'generation' },
          { name: 'md5_hash' }, { name: 'crc32c' },
          { name: 'metadata', type: 'object' }
        ]
      end
    },
    gcs_object_full: {
      fields: lambda do |_connection, _config_fields|
        [
          { name: 'bucket' }, { name: 'name' }, { name: 'size', type: 'integer' },
          { name: 'content_type' }, { name: 'updated' }, { name: 'generation' },
          { name: 'md5_hash' }, { name: 'crc32c' }, { name: 'metadata', type: 'object' },
          { name: 'text_content', type: 'string' },
          { name: 'needs_processing', type: 'boolean' },
          { name: 'fetch_method' },
          { name: 'telemetry', type: 'object', properties: [
              { name: 'success', type: 'boolean' }, { name: 'timestamp' }, { name: 'metadata', type: 'object' },
              { name: 'trace', type: 'object', properties: [
                  { name: 'correlation_id' }, { name: 'duration_ms', type: 'integer' }
              ]}
          ]}
        ]
      end
    }
  },

  actions: {
    # Google Drive
    fetch_drive_file: {
      title: 'Drive: Fetch file',
      subtitle: 'Download file content/metadata',
      display_priority: 9,
      help: {
        learn_more_url: 'https://developers.google.com/drive/api/v3/reference/files/get',
        learn_more_text: 'Google Drive API: files.get',
        body: 'Exports Docs/Sheets/Slides to text, downloads other files directly. Always returns metadata; content is optional.'
      },
      input_fields: lambda do
        [
          { name: 'file_id', label: 'File ID or URL', optional: false, hint: 'Paste a Drive file ID or full URL' },
          { name: 'include_content', type: 'boolean', control_type: 'checkbox', default: true, hint: 'Disable to fetch only metadata' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, hint: 'Remove URLs from text_content' }

        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['drive_file_full']
      end,
      execute: lambda do |connection, input|
        local = call('deep_copy', input)
        fid   = call('extract_drive_file_id', local['file_id'])
        include_content = !!local['include_content']
        strip_urls      = local['strip_urls'] == true
        call('fetch_drive_file_record', connection, fid, include_content, strip_urls)
      end

    },
    list_drive_files: {
      title: 'Drive: List files',
      subtitle: 'Query files by folder/date/MIME type',
      display_priority: 10,
      help: {
        learn_more_url: 'https://developers.google.com/drive/api/v3/reference/files/list',
        learn_more_text: 'Google Drive API: files.list',
        body: 'Returns newest files first. Supports folder, date range, MIME filter, and pagination.'
      },
      input_fields: lambda do
        [
          { name: 'folder_id', label: 'Folder ID or URL', hint: 'Leave empty for My Drive root' },
          { name: 'max_results', type: 'integer', default: 100, hint: '1–1000 (default 100)' },
          { name: 'modified_after',  type: 'date_time' },
          { name: 'modified_before', type: 'date_time' },
          { name: 'mime_type',       label: 'MIME type' },
          { name: 'exclude_folders', type: 'boolean', control_type: 'checkbox' },
          { name: 'page_token',      label: 'Page token' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'files', type: 'array', of: 'object', properties: object_definitions['drive_file_basic'] },
          { name: 'count', type: 'integer' },
          { name: 'has_more', type: 'boolean' },
          { name: 'next_page_token' },
          { name: 'query_used' },
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] },
          { name: 'api', type: 'object', properties: object_definitions['http_meta'] }
        ]
      end,
      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local = call('deep_copy', input)

        folder_id = call('maybe_extract_id', local['folder_id'])
        q = call('build_drive_query', {
          folder_id:       folder_id,
          modified_after:  call('to_iso8601', local['modified_after']),
          modified_before: call('to_iso8601', local['modified_before']),
          mime_type:       local['mime_type'],
          exclude_folders: !!local['exclude_folders']
        })

        page_size = [[(local['max_results'] || 100).to_i, 1].max, 1000].min
        
        # Safely determine drive/corpora
        corpora     = 'user'
        drive_id    = nil
        use_shared  = false

        if !call('blank?', folder_id)
          begin
            fmeta = call('http_request',
              method: :get,
              url: call('build_endpoint_url', :drive, :file, folder_id),
              params: { fields: 'id,mimeType,driveId', supportsAllDrives: true },
              headers: { 'X-Correlation-Id' => action_cid },
              context: { action: 'Probe Drive folder', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }
            )['data'] || {}
            if !call('blank?', fmeta['driveId'])
              corpora    = 'drive'
              drive_id   = fmeta['driveId']
              use_shared = true
            end
          rescue
            # Fall back to My Drive if probe fails (keeps listing robust)
            corpora = 'user'
          end
        end

        params = {
          q: q,
          pageSize: page_size,
          fields: 'nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum,owners)',
          orderBy: 'modifiedTime desc',
          pageToken: local['page_token'],
          spaces: 'drive',
          corpora: corpora
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        if use_shared
          params[:supportsAllDrives]         = true
          params[:includeItemsFromAllDrives] = true
          params[:driveId]                   = drive_id
        end

        resp = call('http_request',
          method: :get,
          url: call('build_endpoint_url', :drive, :files),
          params: {
            q: q,
            pageSize: page_size,
            fields: 'nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum,owners)',
            orderBy: 'modifiedTime desc',
            pageToken: local['page_token'],
            supportsAllDrives: true,
            includeItemsFromAllDrives: true
          }.reject { |_k, v| v.nil? || v.to_s == '' },
          headers: { 'X-Correlation-Id' => action_cid },
          context: { action: 'List Drive files', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }
        )

        data  = resp['data'].is_a?(Hash) ? resp['data'] : {}
        files = (data['files'] || []).map do |f|
          {
            'id' => f['id'], 'name' => f['name'], 'mime_type' => f['mimeType'],
            'size' => (f['size'] || 0).to_i, 'modified_time' => f['modifiedTime'],
            'checksum' => f['md5Checksum'], 'owners' => f['owners'] || []
          }
        end

        {
          'files'           => files,
          'count'           => files.length,
          'has_more'        => !call('blank?', data['nextPageToken']),
          'next_page_token' => data['nextPageToken'],
          'query_used'      => q,
          'api'             => resp['http'],
          'telemetry'       => call('telemetry_envelope',
                                    true,
                                    { action: 'list_drive_files', folder_id: folder_id, page_size: page_size },
                                    started_at,
                                    action_cid,
                                    connection['include_trace'])
        }
      end
    },
    batch_fetch_drive_files: {
      title: 'Drive: Batch fetch files',
      subtitle: 'Fetch multiple files with metrics',
      display_priority: 9,
      batch: true,
      help: {
        learn_more_url: 'https://developers.google.com/drive/api/v3/reference/files/get',
        learn_more_text: 'Google Drive API: files.get',
        body: 'Sequentially fetches IDs with clear success/failure accounting. Continue-on-error is default.'
      },
      # INPUT
      input_fields: lambda do
        [
          { name: 'file_ids', type: 'array', of: 'string', optional: false, hint: 'IDs or URLs' },
          { name: 'include_content', type: 'boolean', control_type: 'checkbox', default: true },
          { name: 'skip_errors',     type: 'boolean', control_type: 'checkbox', default: true, hint: 'Fail-fast if unchecked' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false }
        ]
      end,
      # OUTPUT
      output_fields: lambda do |object_definitions|
        [
          { name: 'successful_files', type: 'array', of: 'object', properties: object_definitions['drive_file_full'] },
          { name: 'failed_files',     type: 'array', of: 'object', properties: [
              { name: 'file_id' }, { name: 'error_message' }, { name: 'error_code' }
          ]},
          { name: 'successful_file_ids', type: 'array', of: 'string' },
          { name: 'metrics', type: 'object', properties: [
              { name: 'total_processed', type: 'integer' },
              { name: 'success_count',   type: 'integer' },
              { name: 'failure_count',   type: 'integer' },
              { name: 'success_rate',    type: 'number' }
          ]},
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
        ]
      end,
      # EXECUTE
      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local = call('deep_copy', input)

        ids = Array(local['file_ids'] || [])
        include_content = !!local['include_content']
        skip_errors     = local['skip_errors'] != false

        successes, failures = [], []

        ids.each do |raw|
          begin
            strip_urls = local['strip_urls'] == true
            fid = call('extract_drive_file_id', raw)
            rec = call('fetch_drive_file_record', connection, fid, include_content, strip_urls)
            successes << rec
          rescue => e
            failures << {
              'file_id' => raw,
              'error_message' => e.message,
              'error_code' => call('infer_error_code', e.message)
            }
            error("Stopped on #{raw}: #{e.message}") unless skip_errors
          end
        end

        total = ids.length
        succ  = successes.length
        failc = failures.length
        rate  = total > 0 ? ((succ.to_f / total) * 100).round(2) : 0.0

        succ_ids = successes.map { |rec| rec['id'] }.compact.uniq

        {
          'successful_files'    => successes,
          'failed_files'        => failures,
          'successful_file_ids' => succ_ids,                     # <-- new
          'metrics'             => { 'total_processed' => total, 'success_count' => succ, 'failure_count' => failc, 'success_rate' => rate },
          'telemetry'           => call('telemetry_envelope',
                                        failures.empty?,
                                        { action: 'batch_fetch_drive_files', total: total },
                                        started_at,
                                        action_cid,
                                        connection['include_trace'])
        }
      end
    },
    monitor_drive_changes: {
      title: 'Drive: Monitor changes',
      subtitle: 'Incremental changes with page tokens',
      display_priority: 8,
      help: {
        learn_more_url: 'https://developers.google.com/drive/api/v3/manage-changes',
        learn_more_text: 'Google Drive API: changes',
        body: 'First run returns a checkpoint token. Subsequent runs return changed files and a new token.'
      },
      input_fields: lambda do
        [
          { name: 'page_token',   hint: 'Leave blank on first run' },
          { name: 'folder_id',    label: 'Folder ID or URL', hint: 'Optional: restrict changes to a folder (best-effort)' },
          { name: 'include_removed',       type: 'boolean', control_type: 'checkbox', default: false },
          { name: 'include_shared_drives', type: 'boolean', control_type: 'checkbox', default: false },
          { name: 'page_size',    type: 'integer', default: 100, hint: '1–1000' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'changes', type: 'array', of: 'object', properties: [
              { name: 'changeType' }, { name: 'time' }, { name: 'removed', type: 'boolean' }, { name: 'fileId' },
              { name: 'file', type: 'object', properties: [
                  { name: 'id' }, { name: 'name' }, { name: 'mimeType' }, { name: 'modifiedTime' },
                  { name: 'size', type: 'integer' }, { name: 'md5Checksum' }, { name: 'trashed', type: 'boolean' },
                  { name: 'parents', type: 'array', of: 'string' }
              ]}
          ]},
          { name: 'new_page_token' },
          { name: 'files_added',    type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'name' }, { name: 'mimeType' }, { name: 'modifiedTime' }
          ]},
          { name: 'files_modified', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'name' }, { name: 'mimeType' }, { name: 'modifiedTime' }, { name: 'checksum' }
          ]},
          { name: 'files_removed',  type: 'array', of: 'object', properties: [
              { name: 'fileId' }, { name: 'time' }
          ]},
          { name: 'summary', type: 'object', properties: [
              { name: 'total_changes', type: 'integer' }, { name: 'added_count', type: 'integer' },
              { name: 'modified_count', type: 'integer' }, { name: 'removed_count', type: 'integer' },
              { name: 'has_more', type: 'boolean' }
          ]},
          { name: 'is_initial_token', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] },
          { name: 'api', type: 'object', properties: object_definitions['http_meta'] }
        ]
      end,
      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local = call('deep_copy', input)

        page_token      = local['page_token'].to_s
        include_removed = !!local['include_removed']
        include_shared  = !!local['include_shared_drives']
        page_size       = [[(local['page_size'] || 100).to_i, 1].max, 1000].min
        folder_id       = call('maybe_extract_id', local['folder_id'])

        if call('blank?', page_token)
          start = call('http_request',
            method: :get,
            url: call('build_endpoint_url', :drive, :start_page_token),
            params: { supportsAllDrives: include_shared },
            headers: { 'X-Correlation-Id' => action_cid },
            context: { action: 'Get Drive start token', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }
          )
          # Return initial token
          return {
            'changes'         => [],
            'new_page_token'  => (start['data'].is_a?(Hash) ? start['data']['startPageToken'] : nil),
            'files_added'     => [],
            'files_modified'  => [],
            'files_removed'   => [],
            'summary'         => { 'total_changes' => 0, 'added_count' => 0, 'modified_count' => 0, 'removed_count' => 0, 'has_more' => false },
            'is_initial_token'=> true,
            'api'             => start['http'],
            'telemetry'       => call('telemetry_envelope',
                                      true,
                                      { action: 'monitor_drive_changes:init' },
                                      started_at,
                                      action_cid,
                                      connection['include_trace'])
          }
        end

        # Return changes
        chg = call('http_request',
          method: :get,
          url: call('build_endpoint_url', :drive, :changes),
          params: {
            pageToken: page_token,
            pageSize: page_size,
            fields: 'nextPageToken,newStartPageToken,changes(changeType,time,removed,fileId,file(id,name,mimeType,modifiedTime,size,md5Checksum,trashed,parents))',
            supportsAllDrives: include_shared,
            includeRemoved: include_removed
          },
          headers: { 'X-Correlation-Id' => action_cid },
          context: { action: 'List Drive changes', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }
        )

        data        = chg['data'].is_a?(Hash) ? chg['data'] : {}
        all_changes = data['changes'] || []

        # Optional folder filter
        if !call('blank?', folder_id)
          all_changes = all_changes.select do |c|
            file = c['file']
            file.is_a?(Hash) && Array(file['parents']).include?(folder_id)
          end
        end

        added, modified, removed = [], [], []
        seen = {}

        all_changes.each do |c|
          if c['removed']
            removed << { 'fileId' => c['fileId'], 'time' => c['time'] }
            next
          end
          file = c['file'] || {}
          next if (file['trashed'] == true) && !include_removed

          fid = file['id'].to_s
          summary = { 'id' => fid, 'name' => file['name'], 'mimeType' => file['mimeType'],
                      'modifiedTime' => file['modifiedTime'], 'checksum' => file['md5Checksum'] }

          if seen[fid]
            modified << summary
          else
            added << summary
            seen[fid] = true
          end
        end

        next_token = data['nextPageToken'] || data['newStartPageToken']
        has_more   = !call('blank?', data['nextPageToken'])

        # Return changes
        {
          'changes'         => all_changes,
          'new_page_token'  => next_token,
          'files_added'     => added,
          'files_modified'  => modified,
          'files_removed'   => removed,
          'summary'         => {
            'total_changes' => all_changes.length,
            'added_count'   => added.length,
            'modified_count'=> modified.length,
            'removed_count' => removed.length,
            'has_more'      => has_more
          },
          'is_initial_token'=> false,
          'api'             => chg['http'],
          'telemetry'       => call('telemetry_envelope',
                                    true,
                                    { action: 'monitor_drive_changes', page_size: page_size },
                                    started_at,
                                    action_cid,
                                    connection['include_trace'])
        }
      end,

      sample_output: lambda do
        {
          'changes' => [],
          'new_page_token' => 'abc123',
          'files_added' => [], 'files_modified' => [], 'files_removed' => [],
          'summary' => { 'total_changes' => 0, 'added_count' => 0, 'modified_count' => 0, 'removed_count' => 0, 'has_more' => false },
          'is_initial_token' => true,
          'telemetry' => { 'success' => true, 'timestamp' => '2025-01-01T00:00:00Z',
            'metadata' => { 'action' => 'monitor_drive_changes' },
            'trace'    => { 'correlation_id' => 'cid', 'duration_ms' => 10 } }
        }
      end
    },

    # Google Cloud Storage
    gcs_list_objects: {
      title: 'GCS: List objects',
      subtitle: 'List objects in a bucket (prefix, delimiter, pagination)',
      display_priority: 7,
      help: {
        learn_more_url: 'https://cloud.google.com/storage/docs/listing-objects',
        learn_more_text: 'Cloud Storage: List objects',
        body: 'Lists objects ordered lexicographically by name. Supports prefix/delimiter to emulate folders.'
      },
      input_fields: lambda do
        [
          { name: 'bucket', optional: false, hint: 'Target bucket name', sticky: true, group: 'Google Cloud Storage' },
          { name: 'prefix', hint: 'Filter by object name prefix (e.g. docs/2025/)', group: 'Google Cloud Storage' },
          { name: 'delimiter', hint: 'Use "/" to get pseudo-folders', group: 'Google Cloud Storage' },
          { name: 'max_results', type: 'integer', default: 1000, hint: '1–1000', group: 'Google Cloud Storage' },
          { name: 'page_token', label: 'Page token', group: 'Google Cloud Storage' },
          { name: 'include_versions', type: 'boolean', control_type: 'checkbox', default: false, group: 'Google Cloud Storage' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'objects', type: 'array', of: 'object', properties: object_definitions['gcs_object_basic'] },
          { name: 'count', type: 'integer' },
          { name: 'has_more', type: 'boolean' },
          { name: 'next_page_token' },
          { name: 'prefixes', type: 'array', of: 'string' },
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
        ]
      end,
      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local = call('deep_copy', input)

        bucket = local['bucket'].to_s
        params = {
          prefix: local['prefix'],
          delimiter: local['delimiter'],
          pageToken: local['page_token'],
          maxResults: [[(local['max_results'] || 1000).to_i, 1].max, 1000].min,
          versions: !!local['include_versions'],
          fields: 'items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes'
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        r = call('http_request',
          method: :get,
          url: call('build_endpoint_url', :storage, :objects_list, bucket),
          params: params,
          headers: { 'X-Correlation-Id' => action_cid },
          context: { action: 'List GCS objects', correlation_id: action_cid, verbose_errors: connection['verbose_errors'] }
        )

        data = r['data'].is_a?(Hash) ? r['data'] : {}
        items = (data['items'] || []).map do |o|
          {
            'bucket' => o['bucket'], 'name' => o['name'], 'size' => (o['size'] || 0).to_i,
            'content_type' => o['contentType'], 'updated' => o['updated'],
            'generation' => o['generation'], 'md5_hash' => o['md5Hash'], 'crc32c' => o['crc32c'],
            'metadata' => o['metadata'] || {}
          }
        end

        {
          'objects'         => items,
          'count'           => items.length,
          'has_more'        => !call('blank?', data['nextPageToken']),
          'next_page_token' => data['nextPageToken'],
          'prefixes'        => data['prefixes'] || [],
          'telemetry'       => call('telemetry_envelope', true, { action: 'gcs_list_objects', bucket: bucket }, started_at, action_cid, connection['include_trace'])
        }
      end
    },
    gcs_fetch_object: {
      title: 'GCS: Fetch object',
      subtitle: 'Get metadata and (optionally) text content',
      display_priority: 6,
      help: {
        learn_more_url: 'https://cloud.google.com/storage/docs/json_api/v1/objects/get',
        learn_more_text: 'Cloud Storage: objects.get',
        body: 'Returns object metadata, and if the content-type is textual, also returns inline text content (via alt=media).'
      },
      input_fields: lambda do
        [
          { name: 'bucket', optional: false, group: 'Google Cloud Storage' },
          { name: 'object_name', label: 'Object name', optional: false, hint: 'Full object key/path', group: 'Google Cloud Storage' },
          { name: 'include_content', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, hint: 'Remove URLs from text_content', group: 'Options' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_full']
      end,
      execute: lambda do |connection, input|
        started = Time.now
        cid = call('gen_correlation_id')
        local = call('deep_copy', input)

        bucket = local['bucket'].to_s
        name   = local['object_name'].to_s

        meta = call('gcs_get_metadata', connection, bucket, name, cid)
        content = if local['include_content'] != false
                    call('gcs_download_text', connection, bucket, name, (meta['content_type'] || ''), cid)
                  else
                    { 'text_content' => '', 'needs_processing' => false, 'fetch_method' => 'skipped' }
                  end

        text_out = content['text_content']
        text_out = call('strip_urls_from_text', text_out) if local['strip_urls'] == true

        {
          'bucket'         => meta['bucket'],
          'name'           => meta['name'],
          'size'           => meta['size'],
          'content_type'   => meta['content_type'],
          'updated'        => meta['updated'],
          'generation'     => meta['generation'],
          'md5_hash'       => meta['md5_hash'],
          'crc32c'         => meta['crc32c'],
          'metadata'       => meta['metadata'],
          'text_content'   => text_out,
          'needs_processing' => content['needs_processing'],
          'fetch_method'   => content['fetch_method'],
          'telemetry'      => call('telemetry_envelope', true, { action: 'gcs_fetch_object', bucket: bucket, object: name }, started, cid, connection['include_trace'])
        }
      end
    },
    gcs_batch_fetch_objects: {
      title: 'GCS: Batch fetch objects',
      subtitle: 'Fetch metadata and (optionally) text content for many objects',
      display_priority: 6,
      batch: true,
      help: {
        learn_more_url: 'https://cloud.google.com/storage/docs/json_api/v1/objects/get',
        learn_more_text: 'Cloud Storage: objects.get',
        body: 'Sequentially fetches object metadata and, when textual, inline content (alt=media). Continue-on-error by default.'
      },

      # CONFIG — mirrors transfer_drive_to_gcs for a consistent UI
      config_fields: [
        { name: 'object_input_mode', label: 'Object input', control_type: 'select', default: 'recipe_array',
          sticky: true, extends_schema: true, options: [
            ['Manual', 'manual'],
            ['Mapped array', 'recipe_array']
          ]}
      ],

      # INPUT
      input_fields: lambda do
        [
          { name: 'bucket', optional: false, group: 'Google Cloud Storage', sticky: true, hint: 'Target bucket name' },

          # From previous step (e.g., gcs_list_objects.objects)
          { name: 'objects', label: 'GCS objects (from previous step)', type: 'array', of: 'object', optional: true,
            group: 'Source: GCS', hint: 'Drop “objects” here; we’ll extract the names.',
            ngIf: 'input.object_input_mode == "recipe_array"',
            properties: [
              { name: 'name' }, { name: 'bucket' }, { name: 'content_type' }
            ]},

          # Manual list of names
          { name: 'object_names', label: 'Object names', type: 'array', of: 'string',
            group: 'Source: GCS', hint: 'Full object keys (e.g., exports/2025-09-30/foo.txt)',
            ngIf: 'input.object_input_mode == "manual"' },

          # Options
          { name: 'include_content', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options',
            hint: 'If the object is textual (text/*, JSON, XML), download inline content.' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, group: 'Options',
            hint: 'Remove URLs from text_content.' },
          { name: 'skip_errors', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options',
            hint: 'Fail-fast if unchecked.' }
        ]
      end,

      # OUTPUT
      output_fields: lambda do |object_definitions|
        [
          { name: 'successful_objects', type: 'array', of: 'object',
            properties: object_definitions['gcs_object_full'] },
          { name: 'failed_objects', type: 'array', of: 'object', properties: [
              { name: 'object_name' }, { name: 'error_message' }, { name: 'error_code' }, { name: 'correlation_id' }
          ]},
          { name: 'successful_object_names', type: 'array', of: 'string' },
          { name: 'metrics', type: 'object', properties: [
              { name: 'total_processed', type: 'integer' },
              { name: 'success_count',   type: 'integer' },
              { name: 'failure_count',   type: 'integer' },
              { name: 'success_rate',    type: 'number' }
          ]},
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
        ]
      end,

      # EXECUTE
      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local      = call('deep_copy', input)

        bucket = local['bucket'].to_s
        names  = call('coerce_gcs_object_names', local)
        error('No GCS object names provided. Map “object_names” or “objects”.') if names.empty?

        include_content = local['include_content'] != false
        strip_urls      = local['strip_urls'] == true
        skip_errors     = local['skip_errors'] != false

        successes, failures = [], []

        names.each do |name|
          per_cid = call('gen_correlation_id')
          begin
            meta    = call('gcs_get_metadata', connection, bucket, name, per_cid)
            content = if include_content
                        call('gcs_download_text', connection, bucket, name, (meta['content_type'] || ''), per_cid)
                      else
                        { 'text_content' => '', 'needs_processing' => false, 'fetch_method' => 'skipped' }
                      end

            text = content['text_content']
            text = call('strip_urls_from_text', text) if strip_urls

            successes << {
              'bucket'           => meta['bucket'],
              'name'             => meta['name'],
              'size'             => meta['size'],
              'content_type'     => meta['content_type'],
              'updated'          => meta['updated'],
              'generation'       => meta['generation'],
              'md5_hash'         => meta['md5_hash'],
              'crc32c'           => meta['crc32c'],
              'metadata'         => meta['metadata'],
              'text_content'     => text,
              'needs_processing' => content['needs_processing'],
              'fetch_method'     => content['fetch_method'],
              'telemetry'        => call('telemetry_envelope',
                                        true,
                                        { action: 'gcs_batch_fetch_objects:item', bucket: bucket, object: name },
                                        Time.now,
                                        per_cid,
                                        true)
            }
          rescue => e
            failures << {
              'object_name'    => name,
              'error_message'  => e.message,                       # includes normalized upstream detail + cid
              'error_code'     => call('infer_error_code', e.message),
              'correlation_id' => per_cid
            }
            error("Stopped on #{name}: #{e.message}") unless skip_errors
          end
        end

        total = names.length
        succ  = successes.length
        failc = failures.length
        rate  = total > 0 ? ((succ.to_f / total) * 100).round(2) : 0.0

        {
          'successful_objects'     => successes,
          'failed_objects'         => failures,
          'successful_object_names'=> successes.map { |o| o['name'] }.compact.uniq,
          'metrics'                => {
            'total_processed' => total, 'success_count' => succ, 'failure_count' => failc, 'success_rate' => rate
          },
          'telemetry'              => call('telemetry_envelope',
                                          failures.empty?,
                                          { action: 'gcs_batch_fetch_objects', bucket: bucket, total: total },
                                          started_at,
                                          action_cid,
                                          connection['include_trace'])
        }
      end
    },
    gcs_write_object: {
      title: 'GCS: Write object',
      subtitle: 'Upload text or raw bytes to a bucket (media or multipart)',
      display_priority: 5,
      help: {
        learn_more_url: 'https://cloud.google.com/storage/docs/json_api/v1/objects/insert',
        learn_more_text: 'Cloud Storage: objects.insert (upload)',
        body: 'Uploads content to GCS. Use upload type "multipart" when you want to set custom metadata. Defaults to continue on overwrite (i.e., new generation).'
      },

      # Config to match your style (like transfer_* actions)
      config_fields: [
        { name: 'content_input_mode', label: 'Content input', control_type: 'select', default: 'text',
          sticky: true, extends_schema: true, options: [
            ['Text (string)', 'text'],
            ['Raw bytes (from previous step)', 'raw_bytes']
          ] }
      ],

      input_fields: lambda do
        [
          # GCS
          { name: 'bucket',       optional: false, sticky: true, group: 'Google Cloud Storage', hint: 'Target bucket name' },
          { name: 'object_name',  label: 'Object name', optional: false, group: 'Google Cloud Storage', hint: 'Full object key/path' },

          # Content (text)
          { name: 'text_content', label: 'Text content', control_type: 'text', multiline: true, optional: true, group: 'Source: Content',
            ngIf: 'input.content_input_mode == "text"' },

          # Content (raw bytes)
          { name: 'raw_bytes', label: 'Raw bytes', optional: true, group: 'Source: Content',
            hint: 'Map a binary/string field from a previous step',
            ngIf: 'input.content_input_mode == "raw_bytes"' },

          # Options
          { name: 'upload_type', label: 'Upload type', control_type: 'select', default: 'media', group: 'Options',
            options: [['Media (content only)', 'media'], ['Multipart (content + metadata)', 'multipart']] },
          { name: 'content_type', label: 'Content type', optional: true, group: 'Options',
            hint: 'Defaults to text/plain; charset=UTF-8 for text mode, application/octet-stream for raw bytes' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, group: 'Options',
            hint: 'Remove URLs from text_content before upload' },

          # Metadata (only for multipart)
          { name: 'custom_metadata', label: 'Custom metadata', type: 'object', group: 'Options',
            ngIf: 'input.upload_type == "multipart"',
            hint: 'Key-value pairs stored under object metadata' },

          # Preconditions (safe, API-native query params)
          { name: 'if_generation_match',      type: 'integer', group: 'Preconditions', hint: 'Only if object generation matches this value (0 = only if object does not exist)' },
          { name: 'if_metageneration_match',  type: 'integer', group: 'Preconditions', hint: 'Only if object metageneration matches this value' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'gcs_object', type: 'object', properties: object_definitions['gcs_object_basic'] },
          { name: 'bytes_uploaded', type: 'integer' },
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
        ]
      end,

      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local      = call('deep_copy', input)

        bucket     = local['bucket'].to_s
        object     = local['object_name'].to_s
        mode       = (local['content_input_mode'] || 'text').to_s
        upload     = (local['upload_type'] || 'media').to_s

        # Decide payload + content type
        if mode == 'text'
          txt = (local['text_content'] || '').to_s
          txt = call('strip_urls_from_text', txt) if local['strip_urls'] == true
          bytes = txt
          mime  = (local['content_type'].to_s == '' ? 'text/plain; charset=UTF-8' : local['content_type'].to_s)
        else
          bytes = (local['raw_bytes'] || '')
          mime  = (local['content_type'].to_s == '' ? 'application/octet-stream' : local['content_type'].to_s)
        end

        extra_params = {
          ifGenerationMatch: local['if_generation_match'],
          ifMetagenerationMatch: local['if_metageneration_match']
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        per_cid = call('gen_correlation_id')
        up = case upload
            when 'multipart'
              meta_hash = call('sanitize_metadata_hash', local['custom_metadata'])
              call('gcs_multipart_upload', connection, bucket, object, bytes, mime, meta_hash, per_cid, extra_params)
            else
              call('gcs_media_upload', connection, bucket, object, bytes, mime, per_cid, extra_params)
            end

        gmd = {
          'bucket'       => up['bucket'] || bucket,
          'name'         => up['name']   || object,
          'size'         => (up['size']  || bytes.to_s.bytesize).to_i,
          'content_type' => up['contentType'] || mime,
          'updated'      => up['updated'],
          'generation'   => up['generation'],
          'md5_hash'     => up['md5Hash'],
          'crc32c'       => up['crc32c'],
          'metadata'     => up['metadata'] || {}
        }

        {
          'gcs_object'     => gmd,
          'bytes_uploaded' => bytes.to_s.bytesize,
          'telemetry'      => call('telemetry_envelope',
                                  true,
                                  { action: 'gcs_write_object', bucket: bucket, object: gmd['name'], upload_type: upload },
                                  started_at,
                                  action_cid,
                                  connection['include_trace'])
        }
      end
    },
    gcs_batch_write_objects: {
      title: 'GCS: Batch write objects',
      subtitle: 'Upload many objects (text or raw bytes) with per-item telemetry',
      batch: true,
      display_priority: 5,
      help: {
        learn_more_url: 'https://cloud.google.com/storage/docs/json_api/v1/objects/insert',
        learn_more_text: 'Cloud Storage: objects.insert (upload)',
        body: 'Sequentially uploads each item. Continue-on-error by default. Use multipart if you need custom metadata.'
      },

      # Minimal, consistent config
      config_fields: [
        { name: 'item_input_mode', label: 'Item input', control_type: 'select', default: 'recipe_array',
          sticky: true, extends_schema: true, options: [
            ['Mapped array', 'recipe_array'],
            ['Manual', 'manual']
          ] }
      ],

      input_fields: lambda do
        [
          # GCS
          { name: 'bucket', optional: false, sticky: true, group: 'Google Cloud Storage', hint: 'Target bucket name' },

          # Items (recipe array)
          { name: 'items', label: 'Items (from previous step)', type: 'array', of: 'object', optional: true,
            group: 'Source: Items', ngIf: 'input.item_input_mode == "recipe_array"',
            hint: 'Map an array of objects to upload',
            properties: [
              { name: 'object_name', optional: false },
              { name: 'content_source', control_type: 'select', optional: false, default: 'text',
                options: [['Text', 'text'], ['Raw bytes', 'raw_bytes']] },
              { name: 'text_content' },
              { name: 'raw_bytes' },
              { name: 'content_type' },
              { name: 'custom_metadata', type: 'object' }
            ]},

          # Items (manual)
          { name: 'manual_items', label: 'Manual items', type: 'array', of: 'object', group: 'Source: Items',
            ngIf: 'input.item_input_mode == "manual"', hint: 'Enter object_name + text/raw_bytes per item',
            properties: [
              { name: 'object_name', optional: false },
              { name: 'content_source', control_type: 'select', optional: false, default: 'text',
                options: [['Text', 'text'], ['Raw bytes', 'raw_bytes']] },
              { name: 'text_content' },
              { name: 'raw_bytes' },
              { name: 'content_type' },
              { name: 'custom_metadata', type: 'object' }
            ]},

          # Options (apply to all items unless overridden per-item)
          { name: 'upload_type', label: 'Upload type', control_type: 'select', default: 'media', group: 'Options',
            options: [['Media (content only)', 'media'], ['Multipart (content + metadata)', 'multipart']] },
          { name: 'default_text_content_type', label: 'Default content type (text)', group: 'Options', hint: 'Default for text items',
            default: 'text/plain; charset=UTF-8' },
          { name: 'default_bytes_content_type', label: 'Default content type (raw bytes)', group: 'Options',
            default: 'application/octet-stream' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, group: 'Options',
            hint: 'When content_source=text, remove URLs before upload' },
          { name: 'skip_errors', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options',
            hint: 'Fail-fast if unchecked' },

          # Preconditions (apply to all items)
          { name: 'if_generation_match',      type: 'integer', group: 'Preconditions' },
          { name: 'if_metageneration_match',  type: 'integer', group: 'Preconditions' }
        ]
      end,

      output_fields: lambda do |object_definitions|
        [
          { name: 'uploaded', type: 'array', of: 'object', properties: [
              { name: 'object_name' },
              { name: 'gcs_object', type: 'object', properties: object_definitions['gcs_object_basic'] },
              { name: 'bytes_uploaded', type: 'integer' },
              { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
          ]},
          { name: 'failed', type: 'array', of: 'object', properties: [
              { name: 'object_name' }, { name: 'error_message' }, { name: 'error_code' }, { name: 'correlation_id' }
          ]},
          { name: 'summary', type: 'object', properties: [
              { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' },
              { name: 'success_rate', type: 'number' }
          ]},
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
        ]
      end,

      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local      = call('deep_copy', input)

        bucket     = local['bucket'].to_s
        upload     = (local['upload_type'] || 'media').to_s
        d_txt_ct   = (local['default_text_content_type'] || 'text/plain; charset=UTF-8').to_s
        d_bin_ct   = (local['default_bytes_content_type'] || 'application/octet-stream').to_s
        strip_urls = local['strip_urls'] == true
        skip_err   = local['skip_errors'] != false

        items = []
        items += Array(local['items'] || [])
        items += Array(local['manual_items'] || [])
        items = items.compact.select { |i| i.is_a?(Hash) && !call('blank?', i['object_name']) }

        error('No items provided. Map “items” or “manual_items”.') if items.empty?

        extra_params = {
          ifGenerationMatch: local['if_generation_match'],
          ifMetagenerationMatch: local['if_metageneration_match']
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        uploaded, failed = [], []

        items.each do |it|
          per_cid = call('gen_correlation_id')
          begin
            name = it['object_name'].to_s
            src  = (it['content_source'] || 'text').to_s

            if src == 'text'
              txt = (it['text_content'] || '').to_s
              txt = call('strip_urls_from_text', txt) if strip_urls
              bytes = txt
              mime  = (it['content_type'].to_s == '' ? d_txt_ct : it['content_type'].to_s)
            else
              bytes = (it['raw_bytes'] || '')
              mime  = (it['content_type'].to_s == '' ? d_bin_ct : it['content_type'].to_s)
            end

            up = if upload == 'multipart'
                  meta_hash = call('sanitize_metadata_hash', it['custom_metadata'])
                  call('gcs_multipart_upload', connection, bucket, name, bytes, mime, meta_hash, per_cid, extra_params)
                else
                  call('gcs_media_upload', connection, bucket, name, bytes, mime, per_cid, extra_params)
                end

            gmd = {
              'bucket'       => up['bucket'] || bucket,
              'name'         => up['name']   || name,
              'size'         => (up['size']  || bytes.to_s.bytesize).to_i,
              'content_type' => up['contentType'] || mime,
              'updated'      => up['updated'],
              'generation'   => up['generation'],
              'md5_hash'     => up['md5Hash'],
              'crc32c'       => up['crc32c'],
              'metadata'     => up['metadata'] || {}
            }

            uploaded << {
              'object_name'    => name,
              'gcs_object'     => gmd,
              'bytes_uploaded' => bytes.to_s.bytesize,
              'telemetry'      => call('telemetry_envelope', true, { action: 'gcs_batch_write_objects:item', bucket: bucket, object: name }, Time.now, per_cid, true)
            }
          rescue => e
            failed << {
              'object_name'   => (it['object_name'] || ''),
              'error_message' => e.message,
              'error_code'    => call('infer_error_code', e.message),
              'correlation_id'=> per_cid
            }
            error("Stopped on #{it['object_name']}: #{e.message}") unless skip_err
          end
        end

        total = items.length
        succ  = uploaded.length
        failc = failed.length
        rate  = total > 0 ? ((succ.to_f / total) * 100).round(2) : 0.0

        {
          'uploaded'  => uploaded,
          'failed'    => failed,
          'summary'   => { 'total' => total, 'success' => succ, 'failed' => failc, 'success_rate' => rate },
          'telemetry' => call('telemetry_envelope',
                              failed.empty?,
                              { action: 'gcs_batch_write_objects', bucket: bucket, total: total, success: succ, failed: failc, upload_type: upload },
                              started_at,
                              action_cid,
                              connection['include_trace'])
        }
      end
    },
    # COMPOUND
    transfer_drive_to_gcs: {
      title: 'Transfer: Drive → GCS',
      subtitle: 'Export/download from Drive and upload to Cloud Storage',
      batch: true,
      display_priority: 4,
      help: {
        learn_more_url: 'https://cloud.google.com/storage/docs/json_api/v1/objects/insert',
        learn_more_text: 'Cloud Storage: objects.insert (upload)',
        body: 'Exports Google Docs/Sheets/Slides to text (or CSV for Sheets) by default, downloads other files, and uploads to the specified GCS bucket using a simple media upload.'
      },
      # CONFIG
      config_fields: [
        { name: 'file_input_mode', label: 'File input', control_type: 'select', default: 'recipe_array',
          sticky: true, extends_schema: true, options: [
            ['Manual', 'manual'],
            ['Mapped array', 'recipe_array']
          ]}
      ],
      # INPUT
      input_fields: lambda do
        [
          { name: 'bucket', optional: false, group: 'Google Cloud Storage' },
          { name: 'gcs_prefix', label: 'Object prefix (folder path)', group: 'Google Cloud Storage', hint: 'e.g. exports/2025-09-30' },
          { name: 'drive_files', label: 'Drive files (from previous step)', type: 'array', of: 'object', optional: true, group: 'Source: Drive',
            hint: 'Drop “successful_files” here; we’ll extract the IDs.', ngIf: 'input.file_input_mode == "recipe_array"',
            properties: [
              { name: 'id' }, { name: 'name' }, { name: 'mime_type' }
            ] },
          # Preconditions
          { name: 'if_generation_match', type: 'integer', group: 'Preconditions', hint: 'Use 0 to only create if object does not already exist.' }

          # Options grouping
          { name: 'drive_file_ids', type: 'array', of: 'string', optional: true, hint: 'Drive IDs or URLs', ngIf: 'input.file_input_mode == "manual"', group: 'Source: Drive' },
          { name: 'export_google_docs', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options',
            hint: 'When true, Google Docs/Sheets/Slides are exported to text/csv before upload. Others are downloaded as-is.' },
          { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false, group: 'Options',
            hint: 'When exporting to text/csv, remove URLs from the uploaded text.' },
          { name: 'fail_fast', type: 'boolean', control_type: 'checkbox', default: false, group: 'Options',
            hint: 'Stop on first failure.' },
          { name: 'preserve_drive_id_in_name', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options',
            hint: 'Prefix object path with drive/<fileId>/ to make the Drive ID part of the name.' },
          { name: 'attach_source_metadata', type: 'boolean', control_type: 'checkbox', default: true, group: 'Options',
            hint: 'Write Drive provenance into GCS custom metadata (src_drive_id, src_drive_mime, etc.).' }

        ]
      end,
      output_fields: lambda do |object_definitions|
        [
          { name: 'uploaded', type: 'array', of: 'object', properties: [
              { name: 'drive_file_id' }, { name: 'gcs_object', type: 'object', properties: object_definitions['gcs_object_basic'] },
              { name: 'export_mime' }, { name: 'bytes_uploaded', type: 'integer' },
              { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
          ]},
          { name: 'failed', type: 'array', of: 'object', properties: [
              { name: 'drive_file_id' }, { name: 'error_message' }, { name: 'error_code' }
          ]},
          { name: 'summary', type: 'object', properties: [
              { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' }
          ]},
          { name: 'telemetry', type: 'object', properties: object_definitions['telemetry'] }
        ]
      end,
      # EXECUTE
      execute: lambda do |connection, input|
        action_cid = call('gen_correlation_id')
        started_at = Time.now
        local = call('deep_copy', input)

        bucket  = local['bucket'].to_s
        prefix  = local['gcs_prefix'].to_s
        ids = call('coerce_drive_ids', local)
        error('No Drive file IDs provided. Map “drive_file_ids” or “drive_files”.') if ids.empty?

        fail_fast = local['fail_fast'] == true
        export_docs = local['export_google_docs'] != false
        strip = local['strip_urls'] == true

        successes, failures = [], []

        ids.each do |raw|
          per_cid = call('gen_correlation_id')
          begin
            fid = call('extract_drive_file_id', raw)

            # Drive metadata
            meta = call('http_request',
              method: :get,
              url: call('build_endpoint_url', :drive, :file, fid),
              params: { fields: 'id,name,mimeType,shortcutDetails(targetId,targetMimeType)', supportsAllDrives: true },
              headers: { 'X-Correlation-Id' => per_cid },
              context: { action: 'Get Drive file (for transfer)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
            )
            fm   = meta['data'].is_a?(Hash) ? meta['data'] : {}
            mime = fm['mimeType'].to_s
            name = fm['name'].to_s

            # Follow shortcuts
            if mime == 'application/vnd.google-apps.shortcut' && fm.dig('shortcutDetails','targetId')
              fid = fm.dig('shortcutDetails','targetId')
              meta = call('http_request',
                method: :get,
                url: call('build_endpoint_url', :drive, :file, fid),
                params: { fields: 'id,name,mimeType', supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                context: { action: 'Get Drive file target (for transfer)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              fm   = meta['data'].is_a?(Hash) ? meta['data'] : {}
              mime = fm['mimeType'].to_s
              name = fm['name'].to_s
            end

            obj_name = call('choose_object_name_for_drive',
                            prefix,
                            local['preserve_drive_id_in_name'] == true,
                            fm['id'],
                            fm['name'])

            export_mime = export_docs ? call('choose_export_mime_for_docs', mime) : nil
            if export_mime
              exp = call('http_request',
                method: :get,
                url: call('build_endpoint_url', :drive, :export, fid),
                params: { mimeType: export_mime, supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                raw_response: true,
                context: { action: 'Export Drive file (transfer)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              # Only strip URLs for textual exports
              texty = export_mime.to_s.start_with?('text/') || %w[application/json application/xml image/svg+xml].include?(export_mime.to_s)
              bytes = (strip && texty) ? call('strip_urls_from_text', exp['data'].to_s.force_encoding('UTF-8')) : exp['data']
              upload_mime = export_mime
            else
              # Don’t try alt=media on Editors files without a mapping; it will 403
              if mime.to_s.start_with?('application/vnd.google-apps.')
                error("Export required for Google Editors type #{mime} but no export mapping is defined in this connector.")
              end
              dl = call('http_request',
                method: :get,
                url: call('build_endpoint_url', :drive, :download, fid),
                params: { supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                raw_response: true,
                context: { action: 'Download Drive file (transfer)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              bytes = dl['data']
              upload_mime = (mime.to_s == '' ? 'application/octet-stream' : mime)
            end

            extra_params = { ifGenerationMatch: local['if_generation_match'] }.reject { |_k, v| v.nil? }

            # Upload 
            up =
              if local['attach_source_metadata'] == true
                meta_hash = call('sanitize_metadata_hash', call('build_drive_source_metadata', fm))
                call('gcs_multipart_upload', connection, bucket, obj_name, bytes, upload_mime, meta_hash, per_cid, extra_params)
              else
                call('gcs_media_upload', connection, bucket, obj_name, bytes, upload_mime, per_cid, extra_params)
              end

            # Get canonical metadata back
            gmd = {
              'bucket' => up['bucket'] || bucket,
              'name' => up['name'] || obj_name,
              'size' => (up['size'] || bytes.to_s.bytesize).to_i,
              'content_type' => up['contentType'] || upload_mime,
              'updated' => up['updated'],
              'generation' => up['generation'],
              'md5_hash' => up['md5Hash'],
              'crc32c' => up['crc32c'],
              'metadata' => up['metadata'] || {}
            }

            successes << {
              'drive_file_id' => fid,
              'gcs_object' => gmd,
              'export_mime' => export_mime,
              'bytes_uploaded' => bytes.to_s.bytesize,
              'telemetry' => call('telemetry_envelope', true, { action: 'transfer_drive_to_gcs:item', file_id: fid, object: gmd['name'] }, Time.now, per_cid, true)
            }
          rescue => e
            failures << { 'drive_file_id' => raw, 'error_message' => e.message, 'error_code' => call('infer_error_code', e.message) }
            error("Stopped on #{raw}: #{e.message}") if fail_fast
          end
        end

        {
          'uploaded' => successes,
          'failed'   => failures,
          'summary'  => { 'total' => ids.length, 'success' => successes.length, 'failed' => failures.length },
          'telemetry'=> call('telemetry_envelope',
                   failures.empty?,
                   { action: 'transfer_drive_to_gcs',
                     total: ids.length,
                     success: successes.length,
                     failed: failures.length },
                   started_at,
                   action_cid,
                   connection['include_trace'])
        }
      end
    }
  },

  methods: {
    # ---------------------- Core utils ----------------------
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
        obj.each_with_object({}) { |(k, v), h| h[k] = call('deep_copy', v) }
      elsif obj.is_a?(Array)
        obj.map { |v| call('deep_copy', v) }
      else
        obj
      end
    end,

    blank?: lambda do |v|
      v.nil? || (v.is_a?(String) && v.strip == '') || (v.respond_to?(:empty?) && v.empty?)
    end,

    to_iso8601: lambda do |v|
      return nil if v.nil? || v.to_s == ''
      (v.is_a?(String) ? Time.parse(v) : v).utc.iso8601 rescue v.to_s
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

    parse_json: lambda do |str|
      JSON.parse(str) rescue nil
    end,

    # ---------------------- Endpoint + HTTP ----------------------
    build_endpoint_url: lambda do |service, endpoint, *ids|
      case service.to_s
      # DRIVE
      when 'drive'
        id = ids[0]
        base = 'https://www.googleapis.com/drive/v3'
        case endpoint.to_s
        when 'about'            then "#{base}/about"
        when 'file'             then "#{base}/files/#{id}"
        when 'export'           then "#{base}/files/#{id}/export"
        when 'download'         then "#{base}/files/#{id}?alt=media"
        when 'files'            then "#{base}/files"
        when 'changes'          then "#{base}/changes"
        when 'start_page_token' then "#{base}/changes/startPageToken"
        else error("Unknown Drive endpoint: #{endpoint}")
        end
      # OAUTH2
      when 'oauth2'
        case endpoint.to_s
        when 'authorize' then 'https://accounts.google.com/o/oauth2/v2/auth'
        when 'token'     then 'https://oauth2.googleapis.com/token'
        else error("Unknown OAuth2 endpoint: #{endpoint}")
        end
      # GCS
      when 'storage'
        bucket = ids[0]
        object = ids[1]
        case endpoint.to_s
        when 'objects_list'            then "https://storage.googleapis.com/storage/v1/b/#{call('url_encode', bucket)}/o"
        when 'object'                  then "https://storage.googleapis.com/storage/v1/b/#{call('url_encode', bucket)}/o/#{call('url_encode', object)}"
        when 'download'                then "https://storage.googleapis.com/storage/v1/b/#{call('url_encode', bucket)}/o/#{call('url_encode', object)}"
        when 'objects_upload_media'    then "https://www.googleapis.com/upload/storage/v1/b/#{call('url_encode', bucket)}/o"
        when 'objects_upload_resumable' then "https://www.googleapis.com/upload/storage/v1/b/#{call('url_encode', bucket)}/o"
        else error("Unknown Storage endpoint: #{endpoint}")
        end
      else
        error("Unknown service for build_endpoint_url: #{service}")
      end
    end,
  
    sanitize_headers: lambda do |hdrs|
      h = hdrs.is_a?(Hash) ? hdrs : {}
      # Drop sensitive or noisy headers by name
      h.each_with_object({}) do |(k, v), out|
        key = k.to_s
        next if key =~ /\A(set-cookie|authorization|proxy-authorization)\z/i
        out[key] = v
      end
    end,

    # Simple, dependency-free querystring builder (no Rails)
    url_encode: lambda do |str|
      s = str.to_s.dup
      bytes = s.bytes
      safe = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.~'
      bytes.map { |b|
        c = b.chr
        if safe.include?(c)
          c
        else
          "%#{('%02X' % b)}"
        end
      }.join
    end,

    build_query_string: lambda do |params|
      (params || {}).map { |k, v| "#{k}=#{call('url_encode', v)}" }.join('&')
    end,

    http_request: lambda do |opts|
      method  = (opts[:method] || :get).to_sym
      url     = opts[:url]
      params  = opts[:params]
      headers = opts[:headers] || {}
      payload = opts[:payload]
      ctx     = opts[:context] || {}
      started = Time.now
      cid     = headers['X-Correlation-Id'] || call('gen_correlation_id')

      req =
        case method
        when :get    then get(url)
        when :post   then payload ? post(url, payload) : post(url)
        when :put    then put(url, payload)
        when :delete then delete(url)
        else error("Unsupported HTTP method: #{method}")
        end

      req = req.params(params) if params
      req = req.headers(headers.merge('X-Correlation-Id' => cid))

      # request/response format controls
      req = req.request_format_www_form_urlencoded if opts[:www_form_urlencoded] == true
      req = req.request_format_raw                 if opts[:raw_body] == true
      req = req.response_format_raw                if opts[:raw_response] == true

      req.after_error_response(/.*/) do |code, body, _hdr, message|
        error(call('normalize_http_error', code, body, message, ctx.merge(correlation_id: cid)))
      end

      # Always expose status + headers + request info
      result = req.after_response do |code, body, hdrs|
        {
          'data' => body,
          'http' => {
            'status'  => code,
            'headers' => call('sanitize_headers', hdrs),
            'request' => { 'method' => method.to_s, 'url' => url, 'params' => params }
          }
        }
      end

      # add timing trace
      result['trace'] = { 'correlation_id' => cid, 'duration_ms' => ((Time.now - started) * 1000).round }
      result
    end,

    normalize_http_error: lambda do |code, body, message, context|
      action = (context || {})[:action] || 'HTTP request'
      cid    = (context || {})[:correlation_id]
      verbose = (context || {})[:verbose_errors] == true

      hint =
        case code.to_i
        when 400 then 'Verify input values and query syntax.'
        when 401 then 'Reauthenticate the connection.'
        when 403 then 'Insufficient permission. For Drive, share the file or enable API; for GCS, ensure bucket IAM and that the token has devstorage.read_write.'
        when 404 then 'Check the ID/URL. The resource may not exist or access is restricted.'
        when 429 then 'Slow down or add backoff; Drive throttled the request.'
        when 500..599 then 'Temporary upstream error; retry with exponential backoff.'
        else 'See upstream error details.'
        end

      detail = begin
        parsed = call('parse_json', body) rescue nil
        parsed && (parsed.dig('error', 'message') || parsed['message'])
      end

      msg = "[#{code}] #{action} failed"
      msg += " — #{detail}" if detail
      msg += " — #{message}" if message && message != detail
      msg += " (cid: #{cid})" if cid
      msg += "\nHint: #{hint}"
      msg += "\nRaw: #{body}" if verbose
      msg
    end,

    # ---------------------- Drive helpers ----------------------
    extract_drive_file_id: lambda do |url_or_id|
      s = (url_or_id || '').to_s.strip
      return s if s =~ /\A[a-zA-Z0-9_-]+\z/
      m = s.match(%r{/d/([a-zA-Z0-9_-]+)}) and (return m[1])
      m = s.match(/[?&]id=([a-zA-Z0-9_-]+)/) and (return m[1])
      s
    end,

    maybe_extract_id: lambda do |maybe_url|
      s = (maybe_url || '').to_s
      return nil if s.strip == ''
      call('extract_drive_file_id', s)
    end,

    get_export_mime: lambda do |mime|
      map = {
        'application/vnd.google-apps.document'     => 'text/plain',
        'application/vnd.google-apps.spreadsheet'  => 'text/csv',
        'application/vnd.google-apps.presentation' => 'text/plain',
        'application/vnd.google-apps.drawing'      => 'image/svg+xml'
      }
      map[mime]
    end,

    fetch_file_content: lambda do |connection, file_id, mime_type, include_content, cid|
      base = { 'text_content' => '', 'needs_processing' => false, 'fetch_method' => 'skipped', 'export_mime_type' => nil, 'http' => nil }
      return base unless include_content

      export_mime = call('get_export_mime', mime_type)
      if export_mime
        r = call('http_request',
          method: :get,
          url: call('build_endpoint_url', :drive, :export, file_id),
          params: { mimeType: export_mime, supportsAllDrives: true },
          headers: { 'X-Correlation-Id' => cid },
          raw_response: true,
          context: { action: 'Export Drive file', file_id: file_id, correlation_id: cid, verbose_errors: connection['verbose_errors'] }
        )
        is_text_export = export_mime.to_s.start_with?('text/') || %w[application/json application/xml image/svg+xml].include?(export_mime.to_s)
        return {
          'text_content'     => (is_text_export ? r['data'].to_s.force_encoding('UTF-8') : ''),
          'needs_processing' => !is_text_export,
          'fetch_method'     => 'export',
          'export_mime_type' => export_mime,
          'http'             => r['http']
        }
      end

      # Editors file but no safe text export mapping -> do NOT hit alt=media (it will 403)
      if mime_type.to_s.start_with?('application/vnd.google-apps.')
        return { 'text_content' => '', 'needs_processing' => true, 'fetch_method' => 'export-required', 'export_mime_type' => nil, 'http' => nil }
      end

      r = call('http_request',
        method: :get,
        url: call('build_endpoint_url', :drive, :download, file_id),
        params: { supportsAllDrives: true },
        headers: { 'X-Correlation-Id' => cid },
        raw_response: true,
        context: { action: 'Download Drive file', file_id: file_id, correlation_id: cid, verbose_errors: connection['verbose_errors'] }
      )

      is_text = (mime_type.to_s.start_with?('text/')) || %w[application/json application/xml image/svg+xml].include?(mime_type.to_s)
      needs_processing = !!(mime_type.to_s.start_with?('application/pdf') || mime_type.to_s.start_with?('image/'))

      {
        'text_content'     => (is_text ? r['data'].to_s.force_encoding('UTF-8') : ''),
        'needs_processing' => needs_processing,
        'fetch_method'     => 'download',
        'export_mime_type' => nil,
        'http'             => r['http']
      }
    end,

    build_drive_query: lambda do |opts|
      p = []
      p << "trashed = false"
      if !call('blank?', opts[:folder_id])
        p << "'#{opts[:folder_id]}' in parents"
      end
      if !call('blank?', opts[:modified_after])
        p << "modifiedTime > '#{opts[:modified_after]}'"
      end
      if !call('blank?', opts[:modified_before])
        p << "modifiedTime < '#{opts[:modified_before]}'"
      end
      if !call('blank?', opts[:mime_type])
        p << "mimeType = '#{opts[:mime_type]}'"
      end
      if opts[:exclude_folders] == true
        p << "mimeType != 'application/vnd.google-apps.folder'"
      end
      p.join(' and ')
    end,

    infer_error_code: lambda do |msg|
      return '401' if msg.include?('[401]')
      return '403' if msg.include?('[403]')
      return '404' if msg.include?('[404]')
      return '429' if msg.include?('[429]')
      'ERROR'
    end,

    # Shared record builder for single/batch
    fetch_drive_file_record: lambda do |connection, fid, include_content, strip_urls = false|
      file_cid = call('gen_correlation_id')
      started  = Time.now

      meta = call('http_request',
        method: :get,
        url: call('build_endpoint_url', :drive, :file, fid),
        params: { fields: 'id,name,mimeType,size,modifiedTime,md5Checksum,owners,shortcutDetails(targetId,targetMimeType)', supportsAllDrives: true },
        headers: { 'X-Correlation-Id' => file_cid },
        context: { action: 'Get Drive file', file_id: fid, correlation_id: file_cid, verbose_errors: connection['verbose_errors'] }
      )
      mdata = meta['data'].is_a?(Hash) ? meta['data'] : {}

      # Follow Drive shortcuts transparently
      if mdata['mimeType'] == 'application/vnd.google-apps.shortcut' && mdata.dig('shortcutDetails', 'targetId')
        target_id = mdata.dig('shortcutDetails', 'targetId')
        return call('fetch_drive_file_record', connection, target_id, include_content, strip_urls)
      end

      content = call('fetch_file_content', connection, fid, (mdata['mimeType'] || ''), include_content, file_cid)
      text    = content['text_content']
      text    = call('strip_urls_from_text', text) if strip_urls

      {
        'id'               => mdata['id'],
        'name'             => mdata['name'],
        'mime_type'        => mdata['mimeType'],
        'size'             => (mdata['size'] || 0).to_i,
        'modified_time'    => mdata['modifiedTime'],
        'checksum'         => mdata['md5Checksum'],
        'owners'           => mdata['owners'] || [],
        'text_content'     => text,
        'needs_processing' => content['needs_processing'],
        'export_mime_type' => content['export_mime_type'],
        'fetch_method'     => content['fetch_method'],
        'api_meta'         => meta['http'],
        'api_content'      => content['http'],
        'telemetry'       => call('telemetry_envelope',
                                   true,
                                   { action: 'fetch_drive_file', file_id: fid },
                                   started,
                                   file_cid,
                                   true) # always include trace for per-file
      }
    end, 

    build_drive_source_metadata: lambda do |drive_meta|
      {
        'src_system'          => 'drive',
        'src_drive_id'        => drive_meta['id'].to_s,
        'src_drive_name'      => drive_meta['name'].to_s,
        'src_drive_mime'      => drive_meta['mimeType'].to_s,
        'src_drive_modified'  => drive_meta['modifiedTime'].to_s,
        'src_drive_md5'       => drive_meta['md5Checksum'].to_s
      }.reject { |_k, v| v.nil? || v == '' }
    end,

    choose_object_name_for_drive: lambda do |prefix, keep_in_name, file_id, file_name|
      if keep_in_name
        # e.g. <prefix>/drive/<fileId>/<sanitized-name>
        pre = prefix.to_s
        dir = pre == '' ? "drive/#{file_id}" : "#{pre.gsub(%r{^/+|/+$}, '')}/drive/#{file_id}"
        call('safe_object_name', dir, file_name)
      else
        call('safe_object_name', prefix, file_name)
      end
    end,

    # ---------- Text utilities ----------
    strip_urls_from_text: lambda do |s|
      t = s.to_s
      return t if t == ''
      # Remove http(s):// and bare www. links
      t.gsub(%r{(?:https?://|www\.)\S+}i, '')
    end,

    sanitize_metadata_hash: lambda do |obj|
      h = obj.is_a?(Hash) ? obj : {}
      out = {}
      h.each do |k, v|
        next if k.nil?
        key = k.to_s
        val = v.nil? ? '' : v.to_s
        out[key] = val
      end
      out
    end,

    # ---------- GCS helpers ----------
    gcs_media_upload: lambda do |connection, bucket, object_name, bytes, content_type, cid, extra_params = nil|
      params = { uploadType: 'media', name: object_name }
      params.merge!(extra_params || {})
      resp = call('http_request',
        method: :post,
        url: call('build_endpoint_url', :storage, :objects_upload_media, bucket),
        params: params,
        headers: { 'Content-Type' => (content_type.to_s == '' ? 'application/octet-stream' : content_type),
                  'X-Correlation-Id' => cid },
        payload: bytes, raw_body: true,
        context: { action: 'GCS upload (media)', bucket: bucket, object: object_name, correlation_id: cid, verbose_errors: connection['verbose_errors'] }
      )
      resp['data'].is_a?(Hash) ? resp['data'] : {}
    end,

    gcs_download_text: lambda do |connection, bucket, object_name, content_type, cid|
      # If the object looks textual, fetch content; else just metadata
      is_text = content_type.to_s.start_with?('text/') || %w[application/json application/xml].include?(content_type.to_s)
      return { 'text_content' => '', 'needs_processing' => !is_text, 'fetch_method' => 'skipped' } unless is_text

      r = call('http_request',
        method: :get,
        url: call('build_endpoint_url', :storage, :download, bucket, object_name),
        params: { alt: 'media' }, headers: { 'X-Correlation-Id' => cid },
        raw_response: true,
        context: { action: 'GCS download (media)', bucket: bucket, object: object_name, correlation_id: cid, verbose_errors: connection['verbose_errors'] }
      )
      { 'text_content' => r['data'].to_s.force_encoding('UTF-8'), 'needs_processing' => false, 'fetch_method' => 'download' }
    end,

    gcs_get_metadata: lambda do |connection, bucket, object_name, cid|
      m = call('http_request',
        method: :get,
        url: call('build_endpoint_url', :storage, :object, bucket, object_name),
        headers: { 'X-Correlation-Id' => cid },
        context: { action: 'GCS get object', bucket: bucket, object: object_name, correlation_id: cid, verbose_errors: connection['verbose_errors'] }
      )
      md = m['data'].is_a?(Hash) ? m['data'] : {}
      {
        'bucket'       => md['bucket'],
        'name'         => md['name'],
        'size'         => (md['size'] || 0).to_i,
        'content_type' => md['contentType'],
        'updated'      => md['updated'],
        'generation'   => md['generation'],
        'md5_hash'     => md['md5Hash'],
        'crc32c'       => md['crc32c'],
        'metadata'     => md['metadata'] || {}
      }
    end,

    safe_object_name: lambda do |prefix, filename|
      pre = (prefix.to_s == '' ? '' : prefix.to_s.gsub(%r{^/+|/+$}, ''))
      base = filename.to_s.strip.gsub(/[^\S\r\n]+/, ' ').gsub(%r{[^\w.\- /]}, '_')
      key  = pre == '' ? base : "#{pre}/#{base}"
      # The API requires path-safe encoding for object path part. We encode full name when building URL later.
      key
    end,

    choose_export_mime_for_docs: lambda do |mime|
      call('get_export_mime', mime) # reuse your Drive mapping (Docs->text/plain, Sheets->text/csv, Slides->text/plain)
    end,

    gcs_multipart_upload: lambda do |connection, bucket, object_name, bytes, content_type, custom_metadata, cid, extra_params = nil|
      boundary = "wrkto-#{call('gen_correlation_id').gsub('-', '')}"
      meta = { 'name' => object_name, 'contentType' => (content_type.to_s == '' ? 'application/octet-stream' : content_type) }
      cm = call('sanitize_metadata_hash', custom_metadata)
      meta['metadata'] = cm unless cm.empty?

      json_part = JSON.generate(meta)
      # Build multipart/related body with CRLF delimiters
      body = []
      body << "--#{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n#{json_part}\r\n"
      body << "--#{boundary}\r\nContent-Type: #{meta['contentType']}\r\n\r\n"
      body << bytes.to_s
      body << "\r\n--#{boundary}--"
      multipart_body = body.join

      params = { uploadType: 'multipart' }
      params.merge!(extra_params || {})

      resp = call('http_request',
        method: :post,
        url: call('build_endpoint_url', :storage, :objects_upload_media, bucket),
        params: params,
        headers: {
          'Content-Type'   => "multipart/related; boundary=#{boundary}",
          'X-Correlation-Id' => cid
        },
        payload: multipart_body, raw_body: true,
        context: { action: 'GCS upload (multipart)', bucket: bucket, object: object_name, correlation_id: cid, verbose_errors: connection['verbose_errors'] }
      )
      resp['data'].is_a?(Hash) ? resp['data'] : {}
    end,

    coerce_drive_ids: lambda do |local|
      ids = []
      # From array<string>
      ids += Array(local['drive_file_ids'] || [])
      # From array<object> with id
      ids += Array(local['drive_files'] || []).map { |o|
        o.is_a?(Hash) ? (o['id'] || o['file_id']) : nil
      }
      ids = ids.compact.map { |raw| call('extract_drive_file_id', raw) }
      ids.reject { |s| call('blank?', s) }.uniq
    end,

    coerce_gcs_object_names: lambda do |local|
      names = []
      names += Array(local['object_names'] || [])
      names += Array(local['objects'] || []).map { |o|
        o.is_a?(Hash) ? (o['name'] || o['object_name']) : nil
      }
      names.compact.map(&:to_s).reject { |s| call('blank?', s) }.uniq
    end

  }
}
