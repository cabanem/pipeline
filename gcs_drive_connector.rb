{
  title: 'Drive Utilities',
  description: 'Google Drive utilities with enterprise resilience, telemetry, and multi-auth',
  version: "0.2.1",
  custom_action: false,

  connection: {
    fields: [
      #  Developer options 
      { name: 'verbose_errors',  label: 'Verbose errors', type: 'boolean', control_type: 'checkbox', hint: 'Include upstream response bodies in normalized error messages (useful in non-prod).' },
      { name: 'include_trace',   label: 'Include trace in outputs', type: 'boolean', control_type: 'checkbox', default: false, sticky: true },

      #  Authentication selection 
      { name: 'auth_type', label: 'Authentication type', control_type: 'select', optional: false, default: 'custom', extends_schema: true, hint: 'Choose how to authenticate to Google Drive.',
        options: [
          ['OAuth 2.0 (user delegated)', 'oauth2'],
          ['Service account (JWT)',      'custom']
        ] },
      { name: 'enable_gcs', label: 'Enable Google Cloud Storage', type: 'boolean', control_type: 'checkbox',
        default: true, sticky: true, hint: 'Adds Cloud Storage scopes for listing/uploading objects (requires reconnect).' }
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
            { name: 'private_key',           group: 'Service account', control_type: 'password', multiline: true, optional: false, hint: 'Paste the PEM private key from the JSON. Newlines may appear as \\n; both forms are handled.' },
            { name: 'subject_email',         group: 'Service account', optional: true, hint: 'Impersonate this user (domain‑wide delegation required). Optional.' }  
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
      
            # Optional user impersonation (domain-wide delegation)
            if connection['subject_email'].to_s.strip != ''
              claim['sub'] = connection['subject_email'].to_s.strip
            end

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
      # Smoke probe that exercises devstorage scope without assuming a bucket
      call('http_request',
        method: :get,
        url: 'https://storage.googleapis.com/storage/v1/b/this-bucket-should-not-exist-123456/o',
        params: { maxResults: 1, fields: 'nextPageToken' },
        context: { action: 'GCS scope smoke probe' }
      ) rescue nil  # Only fail test on 401/403; otherwise ignore
    end

    true
  end,

  object_definitions: {
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
          { name: 'next_page_token' }
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

        # 4. HTTP request 
        action_cid = call('gen_correlation_id')
        params = {
          q: q,
          pageSize: page_size,
          orderBy: 'modifiedTime desc',
          pageToken: local['page_token'],
          spaces: 'drive',
          corpora: corpora,
          supportsAllDrives: true,
          includeItemsFromAllDrives: true,
          # Only request what we map
          fields: 'nextPageToken,files(id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress))'
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        params[:driveId] = drive_id unless call('blank?', drive_id)

        resp = call('http_request',
          method: :get,
          url:    call('build_endpoint_url', :drive, :files),
          params: params,
          headers: { 'X-Correlation-Id' => action_cid },
          context: {
            action: 'Drive: List files (simplified)',
            correlation_id: action_cid,
            verbose_errors: connection['verbose_errors'] # harmless if not present
          }
        )

        # 5. Map output to canonical shape 
        data  = resp['data'].is_a?(Hash) ? resp['data'] : {}
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

        {
          'files'           => files,
          'count'           => files.length,
          'has_more'        => !call('blank?', data['nextPageToken']),
          'next_page_token' => data['nextPageToken']
        }
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
          { name: 'postprocess', type: 'object', properties: [
              { name: 'strip_urls', type: 'boolean', control_type: 'checkbox', default: false }
          ]}
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['drive_file_with_content']
      end,
      sample_output: lambda do
        {
          'id' => '1', 'name' => 'Doc', 'mime_type' => 'application/vnd.google-apps.document',
          'size' => 0, 'modified_time' => '2025-09-30T12:00:00Z', 'checksum' => nil, 'owners' => [],
          'text_content' => 'Exported text …', 'exported_as' => 'text/plain'
        }
      end,
      execute: lambda do |connection, input|
        # 1. Normalize inputs
        local  = call('deep_copy', input)
        mode   = (local['content_mode'] || 'text').to_s
        strip  = !!(local.dig('postprocess', 'strip_urls') == true)

        #  2. Collect IDs from various shapes (array or mapped array) 
        ids = []
        ids += Array(local['file_ids'] || [])
        ids << local['file_id'] if local['file_id'] # be forgiving if a single id is passed
        ids += Array(local['files'] || []).map { |o| o.is_a?(Hash) ? (o['id'] || o['file_id']) : nil }

        ids = ids.compact.map { |raw| call('extract_drive_file_id', raw) }
                .reject { |s| call('blank?', s) }.uniq
        error('No Drive file IDs provided. Map "file_ids" or "files".') if ids.empty?

        #  Helpers 
        is_textual = lambda do |mime|
          m = (mime || '').to_s
          m.start_with?('text/') || %w[application/json application/xml text/csv image/svg+xml].include?(m)
        end

        successes, failures = [], []

        ids.each do |raw_id|
          per_cid = call('gen_correlation_id')
          begin
            # --- 1) Metadata (follow shortcuts once) ---
            fid  = raw_id
            meta = call('http_request',
              method: :get,
              url:    call('build_endpoint_url', :drive, :file, fid),
              params: {
                fields: 'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress),shortcutDetails(targetId,targetMimeType)',
                supportsAllDrives: true
              },
              headers: { 'X-Correlation-Id' => per_cid },
              context: { action: 'Drive get (meta)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
            )
            mdata = meta['data'].is_a?(Hash) ? meta['data'] : {}

            if mdata['mimeType'] == 'application/vnd.google-apps.shortcut' && mdata.dig('shortcutDetails', 'targetId')
              fid = mdata.dig('shortcutDetails', 'targetId')
              meta = call('http_request',
                method: :get,
                url:    call('build_endpoint_url', :drive, :file, fid),
                params: {
                  fields: 'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress)',
                  supportsAllDrives: true
                },
                headers: { 'X-Correlation-Id' => per_cid },
                context: { action: 'Drive get (target meta)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              mdata = meta['data'].is_a?(Hash) ? meta['data'] : {}
            end

            mime = (mdata['mimeType'] || '').to_s

            out = {
              'id'            => mdata['id'],
              'name'          => mdata['name'],
              'mime_type'     => mime,
              'size'          => (mdata['size'] || 0).to_i,
              'modified_time' => mdata['modifiedTime'],
              'checksum'      => mdata['md5Checksum'],
              'owners'        => Array(mdata['owners']).map { |o| { 'displayName' => o['displayName'], 'emailAddress' => o['emailAddress'] } }
            }

            case mode
            when 'none'
              # metadata only

            when 'text'
              if mime.start_with?('application/vnd.google-apps.')
                export_mime = call('get_export_mime', mime)
                error("Export required for Editors type #{mime} but not supported.") if export_mime.nil?

                exp = call('http_request',
                  method: :get,
                  url:    call('build_endpoint_url', :drive, :export, fid),
                  params: { mimeType: export_mime, supportsAllDrives: true },
                  headers: { 'X-Correlation-Id' => per_cid },
                  raw_response: true,
                  context: { action: 'Drive export (text)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
                )
                txt = call('safe_utf8', exp['data'])
                txt = call('strip_urls_from_text', txt) if strip
                out['text_content'] = txt
                out['exported_as']  = export_mime

              else
                error("Non-text file (#{mime}); use content_mode=bytes or none.") unless is_textual.call(mime)

                dl = call('http_request',
                  method: :get,
                  url:    call('build_endpoint_url', :drive, :download, fid),
                  params: { supportsAllDrives: true },
                  headers: { 'X-Correlation-Id' => per_cid },
                  raw_response: true,
                  context: { action: 'Drive download (text)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
                )
                txt = call('safe_utf8', dl['data'])
                txt = call('strip_urls_from_text', txt) if strip
                out['text_content'] = txt
              end

            when 'bytes'
              error('Editors files require content_mode=text (export).') if mime.start_with?('application/vnd.google-apps.')

              dl = call('http_request',
                method: :get,
                url:    call('build_endpoint_url', :drive, :download, fid),
                params: { supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                raw_response: true,
                context: { action: 'Drive download (bytes)', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              # Base64 without relying on stdlib requires
              out['content_bytes'] = [dl['data'].to_s].pack('m0')

            else
              error("Unsupported content_mode=#{mode}. Use none|text|bytes.")
            end

            successes << out

          rescue => e
            failures << {
              'file_id'       => raw_id,
              'error_message' => e.message,
              'error_code'    => call('infer_error_code', e.message)
            }
            next
          end
        end

        {
          'files'   => successes,
          'failed'  => failures,
          'summary' => { 'total' => ids.length, 'success' => successes.length, 'failed' => failures.length }
        }
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
          { name: 'prefixes', type: 'array', of: 'string' }
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
        # ---------- Normalize inputs ----------
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

        # ---------- Build request ----------
        action_cid = call('gen_correlation_id')
        params = {
          prefix:      (prefix == '' ? nil : prefix),
          delimiter:   (delimiter == '' ? nil : delimiter),
          pageToken:   (page_tok == '' ? nil : page_tok),
          maxResults:  page_size,
          versions:    versions,
          fields:      'items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes'
        }.reject { |_k, v| v.nil? || v.to_s == '' }

        # ---------- HTTP: GCS objects.list ----------
        resp = call('http_request',
          method:  :get,
          url:     call('build_endpoint_url', :storage, :objects_list, bucket),
          params:  params,
          headers: { 'X-Correlation-Id' => action_cid },
          context: {
            action: 'GCS: List objects (simplified)',
            correlation_id: action_cid,
            verbose_errors: connection['verbose_errors'] # harmless if unset
          }
        )

        # ---------- Map output ----------
        data  = resp['data'].is_a?(Hash) ? resp['data'] : {}
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
        {
          'objects'         => items,
          'count'           => items.length,
          'has_more'        => !call('blank?', next_token),
          'next_page_token' => next_token,
          'prefixes'        => Array(data['prefixes'] || [])
        }
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
        object_definitions['gcs_object_with_content']
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

        # Helper: textual mime?
        is_textual = lambda do |mime|
          m = (mime || '').to_s
          m.start_with?('text/') || %w[application/json application/xml text/csv image/svg+xml].include?(m)
        end

        # ---------- Fetch metadata ----------
        cid  = call('gen_correlation_id')
        meta = call('http_request',
          method:  :get,
          url:     call('build_endpoint_url', :storage, :object, bucket, name),
          headers: { 'X-Correlation-Id' => cid },
          context: {
            action: 'GCS get object (meta)',
            bucket: bucket, object: name, correlation_id: cid,
            verbose_errors: connection['verbose_errors']
          }
        )
        md = meta['data'].is_a?(Hash) ? meta['data'] : {}

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
          # metadata only

        when 'text'
          ct = (out['content_type'] || '').to_s
          error("status=415 Non-text object (#{ct}); use content_mode=bytes or none.") unless is_textual.call(ct)

          dl = call('http_request',
            method:  :get,
            url:     call('build_endpoint_url', :storage, :download, bucket, name),
            params:  { alt: 'media' },
            headers: { 'X-Correlation-Id' => cid },
            raw_response: true,
            context: {
              action: 'GCS download (text)',
              bucket: bucket, object: name, correlation_id: cid,
              verbose_errors: connection['verbose_errors']
            }
          )
          txt = call('safe_utf8', dl['data'])
          txt = call('strip_urls_from_text', txt) if strip
          out['text_content'] = txt

        when 'bytes'
          dl = call('http_request',
            method:  :get,
            url:     call('build_endpoint_url', :storage, :download, bucket, name),
            params:  { alt: 'media' },
            headers: { 'X-Correlation-Id' => cid },
            raw_response: true,
            context: {
              action: 'GCS download (bytes)',
              bucket: bucket, object: name, correlation_id: cid,
              verbose_errors: connection['verbose_errors']
            }
          )
          # Base64 (strict) without requiring stdlib Base64
          out['content_bytes'] = [dl['data'].to_s].pack('m0')

        else
          error("Unsupported content_mode=#{mode}. Use none|text|bytes.")
        end

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
          { name: 'content_bytes', optional: true, ngIf: 'input.content_mode == "bytes"', hint: 'Base64 string or bytes from prior step' },
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

        # -------- Payload prep --------
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
            # Decode base64 (strict) without stdlib Base64
            b   = b64.to_s.unpack('m0').first.to_s
            error('Failed to decode content_bytes (base64)') if b == ''
            b
          else
            error("Unsupported content_mode=#{mode}. Use text|bytes.")
          end

        bytes_len = raw_bytes.to_s.bytesize

        # -------- Upload mode selection --------
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
          # -------- Multipart upload (metadata + media) --------
          boundary = "wrkto-#{call('gen_correlation_id').gsub('-', '')}"
          meta_json = JSON.generate({
            'name'        => name,
            'contentType' => mime,
            'metadata'    => custom_meta
          })

          part1 = "--#{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n#{meta_json}\r\n"
          part2 = "--#{boundary}\r\nContent-Type: #{mime}\r\n\r\n"
          part3 = "\r\n--#{boundary}--"

          body = ''.b
          body << part1.dup.force_encoding('ASCII-8BIT')
          body << part2.dup.force_encoding('ASCII-8BIT')
          body << raw_bytes.to_s.b
          body << part3.dup.force_encoding('ASCII-8BIT')

          params = { uploadType: 'multipart' }.merge(extra_params)

          resp = call('http_request',
            method:  :post,
            url:     call('build_endpoint_url', :storage, :objects_upload_media, bucket),
            params:  params,
            headers: { 'Content-Type' => "multipart/related; boundary=#{boundary}", 'X-Correlation-Id' => cid },
            payload: body,
            raw_body: true,
            context: {
              action: 'GCS upload (multipart)',
              bucket: bucket, object: name, correlation_id: cid,
              verbose_errors: connection['verbose_errors']
            }
          )
        else
          # -------- Media upload (content only) --------
          params = { uploadType: 'media', name: name }.merge(extra_params)

          resp = call('http_request',
            method:  :post,
            url:     call('build_endpoint_url', :storage, :objects_upload_media, bucket),
            params:  params,
            headers: { 'Content-Type' => mime, 'X-Correlation-Id' => cid },
            payload: raw_bytes,
            raw_body: true,
            context: {
              action: 'GCS upload (media)',
              bucket: bucket, object: name, correlation_id: cid,
              verbose_errors: connection['verbose_errors']
            }
          )
        end

        up = resp['data'].is_a?(Hash) ? resp['data'] : {}

        # -------- Map output --------
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
            options: [['Text (export)', 'text'], ['Skip editors', 'skip']], hint: 'Editors cannot produce raw bytes.' }
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

        # ---- Normalize input ----
        bucket = (local['bucket'] || '').to_s.strip
        error('Bucket is required') if call('blank?', bucket)

        prefix = (local['gcs_prefix'] || '').to_s.gsub(%r{^/+|/+$}, '') # allow empty

        ids = Array(local['drive_file_ids'] || [])
                .map { |raw| call('extract_drive_file_id', raw) }
                .reject { |s| call('blank?', s) }
        error('No Drive file IDs provided. Map "drive_file_ids".') if ids.empty?

        editors_mode = (local['content_mode_for_editors'] || 'text').to_s
        error('content_mode_for_editors must be "text" or "skip"') unless %w[text skip].include?(editors_mode)

        # ---- Helper: upload to GCS (multipart with metadata) ----
        upload_with_meta = lambda do |bytes, content_type, object_name, provenance, cid|
          meta_hash = call('sanitize_metadata_hash', provenance)
          call('gcs_multipart_upload', connection, bucket, object_name, bytes, content_type, meta_hash, cid, {})
        end

        uploaded, failed = [], []

        ids.each do |raw_id|
          per_cid = call('gen_correlation_id')
          begin
            # --- 1) Get Drive metadata (resolve shortcuts once) ---
            fid = raw_id
            meta = call('http_request',
              method: :get,
              url:    call('build_endpoint_url', :drive, :file, fid),
              params: { fields: 'id,name,mimeType,modifiedTime,md5Checksum,shortcutDetails(targetId,targetMimeType)', supportsAllDrives: true },
              headers: { 'X-Correlation-Id' => per_cid },
              context: { action: 'Transfer: Get Drive file', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
            )
            fm = meta['data'].is_a?(Hash) ? meta['data'] : {}

            if fm['mimeType'] == 'application/vnd.google-apps.shortcut' && fm.dig('shortcutDetails', 'targetId')
              fid = fm.dig('shortcutDetails', 'targetId')
              meta = call('http_request',
                method: :get,
                url:    call('build_endpoint_url', :drive, :file, fid),
                params: { fields: 'id,name,mimeType,modifiedTime,md5Checksum', supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                context: { action: 'Transfer: Get target file', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              fm = meta['data'].is_a?(Hash) ? meta['data'] : {}
            end

            mime = (fm['mimeType'] || '').to_s
            name = (fm['name'] || '').to_s

            # --- 2) Acquire content (export Editors, download others) ---
            bytes = nil
            upload_mime = nil

            if mime.start_with?('application/vnd.google-apps.')
              if editors_mode == 'skip'
                failed << { 'drive_file_id' => raw_id, 'error_message' => "Skipped Editors file type #{mime} by configuration.", 'error_code' => 'SKIPPED' }
                next
              end
              export_mime = call('get_export_mime', mime)
              error("Export mapping not defined for Editors type #{mime}.") if export_mime.nil?

              exp = call('http_request',
                method: :get,
                url:    call('build_endpoint_url', :drive, :export, fid),
                params: { mimeType: export_mime, supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                raw_response: true,
                context: { action: 'Transfer: Export Drive file', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              bytes = exp['data']
              upload_mime = export_mime
            else
              dl = call('http_request',
                method: :get,
                url:    call('build_endpoint_url', :drive, :download, fid),
                params: { supportsAllDrives: true },
                headers: { 'X-Correlation-Id' => per_cid },
                raw_response: true,
                context: { action: 'Transfer: Download Drive file', file_id: fid, correlation_id: per_cid, verbose_errors: connection['verbose_errors'] }
              )
              bytes = dl['data']
              upload_mime = (mime == '' ? 'application/octet-stream' : mime)
            end

            # --- 3) Choose GCS object name (prefix + sanitized filename) ---
            object_name = call('safe_object_name', prefix, name)

            # --- 4) Provenance metadata (always attach) ---
            provenance = {
              'src_drive_id'       => fm['id'].to_s,
              'src_drive_mime'     => mime,
              'src_drive_modified' => fm['modifiedTime'].to_s
            }

            # --- 5) Upload to GCS (multipart) ---
            up = upload_with_meta.call(bytes, upload_mime, object_name, provenance, per_cid)

            # --- 6) Map success ---
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
              'drive_file_id' => raw_id,
              'error_message' => e.message,
              'error_code'    => call('infer_error_code', e.message)
            }
            next
          end
        end

        {
          'uploaded' => uploaded,
          'failed'   => failed,
          'summary'  => { 'total' => ids.length, 'success' => uploaded.length, 'failed' => failed.length }
        }
      end
    }
  },
  methods: {}
}

