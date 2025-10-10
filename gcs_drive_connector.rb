{
  title: 'Google Drive with Cloud Storage',
  version: '0.5',

  # --------- CONNECTION ---------------------------------------------------
  connection: {
    fields: [
      {
        name: 'service_account_key_json',
        label: 'Service account JSON key',
        control_type: 'text-area',
        optional: false,
        hint: 'Paste the full JSON from Google Cloud (includes client_email, private_key, token_uri).'
      },
      {
        name: 'user_project',
        label: 'User project for requester-pays (optional)',
        hint: 'Project ID for billing (userProject)',
        optional: true
      }
    ],

    authorization: {
      # Custom JWT-bearer --> OAuth access token exchange
      type: 'custom',

      # Attach the access token to every request
      apply: lambda do |connection|
        headers('Authorization': "Bearer #{connection['access_token']}")
      end,

      # Obtain/refresh the access token
      acquire: lambda do |connection|
        # Use default superset (Drive + GCS). Cached per scope set.
        token_str = call(:auth_build_access_token!, connection, scopes: call(:const_default_scopes))
        # Build a stable shape for Workato’s connection store.
        # Pull from cache so we have expires_in/at without re-exchanging:
        scope_key = call(:const_default_scopes).join(' ')
        cached = (connection['__token_cache'] ||= {})[scope_key]

        {
          access_token: token_str,
          token_type:   'Bearer',
          expires_in:   (cached && cached['expires_in']) || 3600,
          expires_at:   (cached && cached['expires_at']) || (Time.now + 3600 - 60).utc.iso8601
        }
      end,

      # URL: token
      token_url: 'https://oauth2.googleapis.com/token',
      
      # Refresh rules
      refresh_on: [401],

      detect_on: [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    }
  },

  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |_connection|
    get('https://www.googleapis.com/drive/v3/about')
      .params(fields: 'user,storageQuota')
  end,

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {
    # --- Base definitions
    drive_file_base_fields: {
      fields: lambda do |_connection|
        [
          { name: 'id', type: 'string' },
          { name: 'name', type: 'string' },
          { name: 'mime_type', type: 'string' },
          { name: 'size', type: 'integer' },
          { name: 'modified_time', type: 'date_time' },
          { name: 'checksum', type: 'string' },
          { name: 'web_view_url', type: 'string', hint: 'Open in Drive' },
          { name: 'owners', type: 'array', of: 'object', properties: [{ name: 'display_name' }, { name: 'email' }] }
        ]
      end
    },
    gcs_object_base_fields: {
      fields: lambda do |_connection|
        [
          { name: 'bucket', type: 'string' },
          { name: 'name', type: 'string' },
          { name: 'size', type: 'integer' },
          { name: 'content_type', type: 'string' },
          { name: 'updated', type: 'date_time' },
          { name: 'generation', type: 'string' },
          { name: 'md5_hash', type: 'string' },
          { name: 'crc32c', type: 'string' },
          { name: 'metadata', type: 'object' }
        ]
      end
    },

    # --- Full composite definitions
    # Prefer full composite over pass-through (pt via concat can
    # fail to resolve at runtime).
    gcs_object_with_content: {
      fields: lambda do |object_definitions|
        Array(object_definitions['gcs_object_base_fields']) + [
          { name: 'text_content', type: 'string' },
          { name: 'content_bytes', type: 'string', hint: 'Base64' },
          { name: 'content_md5', type: 'string', hint: 'Computed from fetched content' },
          { name: 'content_sha256', type: 'string', hint: 'Computed from fetched content' }
        ] + Array(object_definitions['envelope_fields'])
      end
    },
    gcs_object_with_bytes_uploaded: {
      fields: lambda do |object_definitions|
        Array(object_definitions['gcs_object_base_fields']) + [
          { name: 'bytes_uploaded', type: 'integer' }
        ] + Array(object_definitions['envelope_fields'])
      end
    },

    # --- Composite definitions
    # Call other object definitions
    drive_file_min: {
      fields: lambda do |_|
        [
          { name: 'id', type: 'string' },
          { name: 'name', type: 'string' }
        ]
      end
    },
    drive_file_full: {
      fields: lambda do |object_definitions|
        Array(object_definitions['drive_file_base_fields']) + [
          { name: 'exported_as', type: 'string' },
          { name: 'text_content', type: 'string' },
          { name: 'content_bytes', type: 'string', hint: 'Base64' },
          { name: 'content_md5', type: 'string', hint: 'Computed from fetched content' },
          { name: 'content_sha256', type: 'string', hint: 'Computed from fetched content' }
        ] + Array(object_definitions['envelope_fields'])
      end
    },
    gcs_list_page: {
      fields: lambda do |object_definitions|
        base = Array(object_definitions['list_page_meta'])
        ([
          { name: 'objects', type: 'array', of: 'object',
            properties: object_definitions['gcs_object_base_fields'] },
          { name: 'prefixes', type: 'array', of: 'string' },
          { name: 'object_names', type: 'array', of: 'string', hint: 'Convenience: names from objects[].name' }
        ] + base + Array(object_definitions['envelope_fields']))
      end
    },
    drive_list_page: {
      fields: lambda do |object_definitions|
        base = Array(object_definitions['list_page_meta'])
        ([
          {
            name: 'files',
            type: 'array',
            of: 'object',
            properties: object_definitions['drive_file_base_fields']  # full per-file metadata
          },
          {
            name: 'file_ids',
            type: 'array',
            of: 'string',
            hint: 'Convenience: IDs extracted from files[].id'
          },
          {
            name: 'items_for_transfer',
            type: 'array',
            of: 'object',
            properties: call(:schema_transfer_batch_plan_item_fields), # matches batch transfer items[]
            hint: 'Directly map into Transfer (batch) → items'
          }
        ] + base + Array(object_definitions['envelope_fields']))
      end
    },
    # --- Schema
    transfer_result: {
      fields: lambda do |object_definitions|
        ([
          { name: 'uploaded', type: 'array', of: 'object', properties: [
            { name: 'drive_file_id' }, { name: 'drive_name' }, { name: 'bucket' }, { name: 'gcs_object_name' },
            { name: 'bytes_uploaded', type: 'integer' }, { name: 'content_type' },
            { name: 'content_md5', type: 'string' }, { name: 'content_sha256', type: 'string' }
          ]},
          { name: 'failed', type: 'array', of: 'object', properties: [
            { name: 'drive_file_id' }, { name: 'drive_name' }, { name: 'bucket' }, { name: 'gcs_object_name' },
            { name: 'error_code' }, { name: 'error_message' }
          ]},
          { name: 'summary', type: 'object', properties: [
            { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' }
          ]}
        ] + Array(object_definitions['envelope_fields']))
      end
    },
    transfer_batch_plan_item: {
      fields: lambda do |_|
        call(:schema_transfer_batch_plan_item_fields)
      end
    },
    transfer_batch_result: {
      fields: lambda do |object_definitions|
        ([
          { name: 'uploaded', type: 'array', of: 'object', properties: [
            { name: 'drive_file_id' }, { name: 'drive_name' }, { name: 'bucket' }, { name: 'gcs_object_name' },
            { name: 'bytes_uploaded', type: 'integer' }, { name: 'content_type' },
            { name: 'content_md5', type: 'string' }, { name: 'content_sha256', type: 'string' }
          ]},
          { name: 'failed', type: 'array', of: 'object', properties: [
            { name: 'drive_file_id' }, { name: 'drive_name' }, { name: 'bucket' }, { name: 'gcs_object_name' },
            { name: 'error_code' }, { name: 'error_message' }
          ]},
          { name: 'summary', type: 'object', properties: [
            { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' }
          ]}
        ] + Array(object_definitions['envelope_fields']))
      end
    },
    envelope_fields: {
      fields: lambda do |_|
        [
          { name: 'ok', type: 'boolean' },
          { name: 'telemetry', type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message', type: 'string' },
            { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id', type: 'string' }
          ] }
        ]
      end
    },
    list_page_meta: {
      fields: lambda do |_connection|
        [
          { name: 'count', type: 'integer' },
          { name: 'has_more', type: 'boolean' },
          { name: 'next_page_token', type: 'string' }
        ]
      end
    }
  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {

    # 1) drive_list_files
    drive_list_files: {
      title: 'Drive: List files',
      subtitle: 'List files in Drive',
      help: lambda do |_|
        {
          body: 'Return a page of Drive files with minimal metadata (newest first).'
        }
      end,
      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_drive_list_inputs, config_fields)
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['drive_list_page']
      end,
      execute: lambda do |_connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        folder_id = call(:util_extract_drive_id, input['folder_id_or_url'])
        q = ["trashed=false"]

        filters = input['filters'] || {}
        paging  = input['paging']  || {}

        page_size = [[(paging['max_results'] || 100).to_i, 1].max, 1000].min

        # Date filtering (ISO-8601)
        if filters['modified_after'].present?
          q << "modifiedTime >= '#{call(:util_to_iso8601_utc, filters['modified_after'])}'"
        end
        if filters['modified_before'].present?
          q << "modifiedTime <= '#{call(:util_to_iso8601_utc, filters['modified_before'])}'"
        end
        # MIME filters
        if filters['mime_types'].present?
          ors = filters['mime_types'].map { |mt| "mimeType='#{mt}'" }.join(' or ')
          q << "(#{ors})"
        end
        # Exclude folders
        if !!filters['exclude_folders']
          q << "mimeType != 'application/vnd.google-apps.folder'"
        end
        # Folder filter
        if folder_id.present?
          q << "'#{folder_id}' in parents"
        end

        corpora, drive_id = if input['drive_id'].present?
                              ['drive', input['drive_id']]
                            elsif folder_id.present?
                              ['allDrives', nil]
                            else
                              ['user', nil]
                            end
        res = get('https://www.googleapis.com/drive/v3/files')
              .params(
                q: q.join(' and '),
                pageSize: page_size,
                pageToken: paging['page_token'],
                orderBy: 'modifiedTime desc',
                spaces: 'drive',
                corpora: corpora,
                driveId: drive_id,
                supportsAllDrives: true,
                includeItemsFromAllDrives: true, # should remain TRUE - else user config settings can omit shared files
                fields: 'files(id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress)),nextPageToken'
              )
        files = (res['files'] || []).map { |f| call(:map_drive_meta, f) }
        items_for_transfer = files.map do |f|
          {
            'drive_file_id_or_url' => f['id'],
            # The rest are optional; user can override later in the batch action
            'target_object_name'   => nil,
            'editors_mode'         => nil,
            'content_type'         => nil,
            'custom_metadata'      => nil
          }
        end
        next_token = res['nextPageToken']
        base = {
          'files'               => files,
          'file_ids'            => files.map { |f| f['id'] }.compact,
          'items_for_transfer'  => items_for_transfer,
          'count'               => files.length,
          'has_more'            => next_token.present?,
          'next_page_token'     => next_token
        }
        base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      rescue => e
        # Predictable empty shape (string keys) on failure
        {}.merge(
          'files'            => [],
          'file_ids'         => [],
          'items_for_transfer' => [],
          'count'            => 0,
          'has_more'         => false,
          'next_page_token'  => nil
        ).merge(
          call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s)
        )
      end,
      sample_output: lambda do
        {
          'files' => [
            {
              'id'            => '1AbCdEfGhIjK',
              'name'          => 'example.txt',
              'mime_type'     => 'text/plain',
              'size'          => 42,
              'modified_time' => '2024-01-01T12:00:00Z',
              'checksum'      => 'd41d8cd98f00b204e9800998ecf8427e',
              'web_view_url'  => 'https://drive.google.com/file/d/1AbCdEfGhIjK/view',
              'owners'        => [{ 'display_name' => 'Drive Bot', 'email' => 'bot@example.com' }]
            }
          ],
          'file_ids' => ['1AbCdEfGhIjK'],
          'items_for_transfer' => [
            {
              'drive_file_id_or_url' => '1AbCdEfGhIjK',
              'target_object_name'   => nil,
              'editors_mode'         => nil,
              'content_type'         => nil,
              'custom_metadata'      => nil
            }
          ],
          'count'           => 1,
          'has_more'        => false,
          'next_page_token' => nil,
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 2) drive_get_file
    drive_get_file: {
      title: 'Drive: Get file',
      subtitle: 'Fetch Drive file metadata and content',
      help: lambda do |_|
        {
          body: 'Fetch Drive file metadata and optionally content (text or bytes). Shortcuts are resolved once.'
        }
      end,

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_drive_get_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['drive_file_full']
      end,

      execute: lambda do |_connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        file_id = call(:util_extract_drive_id, input['file_id_or_url'])
        meta = call(:drive_get_meta_resolving_shortcut, file_id)

        result = call(:map_drive_meta, meta)

        mode = (input['content_mode'] || 'none').to_s
        strip = input.dig('postprocess', 'util_strip_urls') ? true : false

        if mode == 'none'
          result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

        elsif mode == 'text'
          if call(:util_is_google_editors_mime?, meta['mimeType'])
            export_mime = call(:util_editors_export_mime, meta['mimeType'])
            bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}/export")
                    .params(mimeType: export_mime, supportsAllDrives: true)
                    .response_format_raw # treat response as new
            raw  = bytes.to_s
            text = call(:util_force_utf8, raw)
            text = call(:util_strip_urls, text) if strip
            cs = call(:util_compute_checksums, raw)
            result.merge(exported_as: export_mime, text_content: text, content_md5: cs['md5'], content_sha256: cs['sha256'])
                  .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

          else
            if call(:util_is_textual_mime?, meta['mimeType'])
              bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}")
                      .params(alt: 'media', supportsAllDrives: true)
                      .response_format_raw
              raw  = bytes.to_s
              text = call(:util_force_utf8, raw)
              text = call(:util_strip_urls, text) if strip
              cs = call(:util_compute_checksums, raw)
              result.merge(text_content: text, content_md5: cs['md5'], content_sha256: cs['sha256'])
                    .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
            else
              error('415 Unsupported Media Type - Non-text file; use content_mode=bytes or none.')
            end
          end
        elsif mode == 'bytes'
          if call(:util_is_google_editors_mime?, meta['mimeType'])
            error('400 Bad Request - Editors files require content_mode=text (export).')
          end
          bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}")
                  .params(alt: 'media', supportsAllDrives: true, acknowledgeAbuse: false)
                  .response_format_raw
          raw = bytes.to_s
          cs  = call(:util_compute_checksums, raw)
          result.merge(content_bytes: Base64.strict_encode64(raw), content_md5: cs['md5'], content_sha256: cs['sha256'])
                .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        else
          error("400 Bad Request - Unknown content_mode: #{mode}")
        end
      rescue => e
        # Predictable shape on error
        {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
      end
    },

    # 3) gcs_list_objects
    gcs_list_objects: {
      title: 'GCS: List objects',
      subtitle: 'List objects in Google Cloud Storage bucket',
      help: lambda do |_|
        {
          body: 'List objects in a bucket, optionally using prefix and delimiter.'
        }
      end,

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_gcs_list_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['gcs_list_page']
      end,

      execute: lambda do |connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        filters = input['filters'] || {}
        paging  = input['paging']  || {}
        page_size = [[(paging['max_results'] || 1000).to_i, 1].max, 1000].min
        res = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(input['bucket'])}/o")
              .params(
                prefix: filters['prefix'],
                delimiter: filters['delimiter'],
                pageToken: paging['page_token'],
                maxResults: page_size,
                versions: !!filters['include_versions'],
                fields: 'items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes',
                userProject: connection['user_project']
              )
        items = (res['items'] || []).map { |o| call(:map_gcs_meta, o) }
        next_token = res['nextPageToken']
        base = {
          'objects'         => items,
          'prefixes'        => res['prefixes'] || [],
          'object_names'    => items.map { |o| o['name'] }.compact,
          'count'           => items.length,
          'has_more'        => next_token.present?,
          'next_page_token' => next_token
        }
        base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      rescue => e
        {}.merge(
          'objects'         => [],
          'prefixes'        => [],
          'count'           => 0,
          'has_more'        => false,
          'next_page_token' => nil
        ).merge(
          call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s)
        )
      end,

      sample_output: lambda do
        {
          'objects' => [
            {
              'bucket' => 'my-bucket',
              'name' => 'path/to/file.txt',
              'size' => 123,
              'content_type' => 'text/plain',
              'updated' => '2025-01-01T00:00:00Z',
              'generation' => '1735689600000000',
              'md5_hash' => '1B2M2Y8AsgTpgAmY7PhCfg==',
              'crc32c' => 'AAAAAA==',
              'metadata' => { 'source' => 'ingest' }
            }
          ],
          'prefixes' => ['path/to/'],
          'count' => 1,
          'has_more' => false,
          'next_page_token' => nil,
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 4) gcs_get_object
    gcs_get_object: {
      title: 'GCS: Get object',
      subtitle: 'Fetch an object from Google Cloud Storage bucket',
      help: lambda do |_|
        {
          body: 'Fetch GCS object metadata and optionally content (text or bytes).'
        }
      end,

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_gcs_get_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_content']
      end,

      execute: lambda do |connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        bucket = input['bucket']
        name = input['object_name']
        meta = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o/#{ERB::Util.url_encode(name)}")
               .params(alt: 'json', userProject: connection['user_project'])
        base = call(:map_gcs_meta, meta)
        mode = (input['content_mode'] || 'none').to_s
        strip = input.dig('postprocess', 'util_strip_urls') ? true : false
        ctype = meta['contentType']
        # Branched execution

        # - None
        if mode == 'none'
          base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

        # - Text
        elsif mode == 'text'
          unless call(:util_is_textual_mime?, ctype)
            error('415 Unsupported Media Type - Non-text object; use content_mode=bytes or none.')
          end
          bytes = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o/#{ERB::Util.url_encode(name)}")
                  .params(alt: 'media', userProject: connection['user_project'])
                  .response_format_raw
          raw  = bytes.to_s
          text = call(:util_force_utf8, raw)
          text = call(:util_strip_urls, text) if strip
          cs = call(:util_compute_checksums, raw)
          base.merge(
            'text_content'    => text,
            'content_md5'     => cs['md5'],
            'content_sha256'  => cs['sha256']
          ).merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

        # - Bytes
        elsif mode == 'bytes'
          bytes = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o/#{ERB::Util.url_encode(name)}")
                  .params(alt: 'media', userProject: connection['user_project'])
                  .response_format_raw
          raw = bytes.to_s
          cs  = call(:util_compute_checksums, raw)
          base.merge(
            'content_bytes'   => Base64.strict_encode64(raw),
            'content_md5'     => cs['md5'],
            'content_sha256'  => cs['sha256']
          ).merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        else
          error("400 Bad Request - Unknown content_mode: #{mode}")
        end
      rescue => e
        {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
      end,

      sample_output: lambda do
        {
          'bucket' => 'my-bucket',
          'name' => 'path/to/file.txt',
          'size' => 123,
          'content_type' => 'text/plain',
          'updated' => '2025-01-01T00:00:00Z',
          'generation' => '1735689600000000',
          'md5_hash' => '1B2M2Y8AsgTpgAmY7PhCfg==',
          'crc32c' => 'AAAAAA==',
          'metadata' => { 'source' => 'ingest' },
          'text_content' => 'hello world',
          'content_bytes' => nil,
          'content_md5' => 'd41d8cd98f00b204e9800998ecf8427e',
          'content_sha256' => 'e3b0c44298fc1c149afbf4c8996fb924...',
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 5) gcs_put_object
    gcs_put_object: {
      title: 'GCS: Put object',
      subtitle: 'Upload an object to Google Cloud Storage bucket',
      help: lambda do |_|
        {
          body: 'Upload text or bytes to GCS. Returns created object metadata and bytes_uploaded.'
        }
      end,

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_gcs_put_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_bytes_uploaded']
      end,

      execute: lambda do |connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        bucket = input['bucket']
        name = input['object_name']
        mode = input['content_mode']
        strip = input.dig('postprocess', 'util_strip_urls') ? true : false
        adv  = input['advanced'] || {}
        meta = adv['custom_metadata']
        meta = meta.transform_values { |v| v.nil? ? nil : v.to_s } if meta.present?

        # Branched execution
        body_bytes, ctype =

          # - Text
          if mode == 'text'
            text = input['text_content']
            error('400 Bad Request - text_content is required when content_mode=text.') if text.nil?
            text = call(:util_strip_urls, text) if strip
            [text.to_s.dup.force_encoding('UTF-8'), (adv['content_type'].presence || 'text/plain; charset=UTF-8')]
          elsif mode == 'bytes'
            b64 = input['content_bytes']
            error('400 Bad Request - content_bytes is required when content_mode=bytes.') if b64.nil?
            [Base64.decode64(b64.to_s), (adv['content_type'].presence || 'application/octet-stream')]
          else
            error("400 Bad Request - Unknown content_mode: #{mode}")
          end
        bytes_uploaded = body_bytes.bytesize
        q = {
          ifGenerationMatch: adv.dig('preconditions', 'if_generation_match'),
          ifMetagenerationMatch: adv.dig('preconditions', 'if_metageneration_match'),

          userProject: connection['user_project']
        }.compact
        created =
          if meta.present?
            boundary = "workato-multipart-#{SecureRandom.hex(8)}"
            meta_json = { name: name, contentType: ctype, metadata: meta }.to_json
            multipart =
              "--#{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n#{meta_json}\r\n" \
              "--#{boundary}\r\nContent-Type: #{ctype}\r\nContent-Transfer-Encoding: binary\r\n\r\n#{body_bytes}\r\n" \
              "--#{boundary}--\r\n"
            post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
              .params(q.merge(uploadType: 'multipart'))
              .headers('Content-Type': "multipart/related; boundary=#{boundary}")
              .request_body(multipart)
          else
            post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
              .params(q.merge(uploadType: 'media', name: name))
              .headers('Content-Type': ctype)
              .request_body(body_bytes)
          end
        call(:map_gcs_meta, created)
          .merge('bytes_uploaded' => bytes_uploaded)
          .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      rescue => e
        {}.merge(call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s))
      end,

      sample_output: lambda do
        {
          'bucket' => 'my-bucket',
          'name' => 'path/to/file.txt',
          'size' => 123,
          'content_type' => 'text/plain',
          'updated' => '2025-01-01T00:00:00Z',
          'generation' => '1735689600000000',
          'md5_hash' => '1B2M2Y8AsgTpgAmY7PhCfg==',
          'crc32c' => 'AAAAAA==',
          'metadata' => { 'source' => 'ingest' },
          'bytes_uploaded' => 123,
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 6) transfer_drive_to_gcs
    transfer_drive_to_gcs: {
      title: 'Transfer: Drive → GCS',
      subtitle: 'Transfer a single file from Drive to Cloud Storage bucket',
      help: lambda do |_|
        {
          body: 'For each Drive file ID, fetch content (export Editors to text if selected) and upload to GCS under a prefix.'
        }
      end,

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_transfer_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['transfer_result']
      end,

      execute: lambda do |connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        bucket       = input['bucket']
        prefix       = call(:util_normalize_prefix, input['gcs_prefix'])
        editors_mode = (input['content_mode_for_editors'] || 'text').to_s

        uploaded, failed = [], []
        drive_files = Array(input['drive_file_ids']).map(&:to_s).flat_map { |s| s.split(/[\s,]+/) }
                        .map(&:strip).reject(&:blank?)

        drive_files.each do |raw|
          file_id = call(:util_extract_drive_id, raw)
          next if file_id.blank?
          res = call(:transfer_one_drive_to_gcs, connection, file_id, bucket, "#{prefix}", editors_mode, nil, nil)
          if res['ok']
            ok = res['ok']
            ok[:gcs_object_name] = "#{prefix}#{ok[:gcs_object_name]}" if prefix.present? && !ok[:gcs_object_name].to_s.start_with?(prefix)
            uploaded << ok
          else
            failed << res['error']
          end
        end

        summary = {
          'total'   => (uploaded.length + failed.length),
          'success' => uploaded.length,
          'failed'  => failed.length
        }
        base = {
          'uploaded' => uploaded,
          'failed'   => failed,
          'summary'  => summary
        }
        ok = failed.empty?
        code = ok ? 200 : 207
        msg  = ok ? 'OK' : 'Partial'
        base.merge(call(:telemetry_envelope, t0, corr, ok, code, msg))
      rescue => e
        {
          'uploaded' => [],
          'failed'   => [],
          'summary'  => { 'total' => 0, 'success' => 0, 'failed' => 0 }
        }.merge(
          call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s)
        )
      end,

      sample_output: lambda do
        {
          'uploaded' => [
            {
              'drive_file_id' => '1AbCd',
              'drive_name' => 'foo.txt',
              'bucket' => 'my-bucket',
              'gcs_object_name' => 'prefix/foo.txt',
              'bytes_uploaded' => 42,
              'content_type' => 'text/plain',
              'content_md5' => 'd41d8cd98f00b204e9800998ecf8427e',
              'content_sha256' => 'e3b0c44298fc1c149afbf4c8996fb924...'
            }
          ],
          'failed' => [],
          'summary' => { 'total' => 1, 'success' => 1, 'failed' => 0 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 7) transfer_drive_to_gcs_batch
    transfer_drive_to_gcs_batch: {
      title: 'Transfer: Drive → GCS',
      subtitle: 'Transfer multiple items from Drive to Cloud Storage bucket',
      help: lambda do |_|
        {
          body: 'Upload many Drive files to GCS in one run, with optional per-item overrides (name, Editors mode, content-type, metadata). Partial success is returned.'
        }
      end,
      batch: true,

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_transfer_batch_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['transfer_batch_result']
      end,

      execute: lambda do |connection, input|
        t0 = Time.now
        corr = SecureRandom.uuid
        bucket       = input['bucket']
        prefix   = call(:util_normalize_prefix, input['gcs_prefix'])
        def_mode = (input['default_editors_mode'] || 'text').to_s
        def_ct   = input['default_content_type']
        def_meta = input['default_custom_metadata']
        stop_on_error = !!input['stop_on_error']

        uploaded, failed = [], []
        Array(input['items']).each_with_index do |it, idx|
          file_id = call(:util_extract_drive_id, it['drive_file_id_or_url'])
          next if file_id.blank?
          editors_mode = (it['editors_mode'].presence || def_mode).to_s
          ctype        = (it['content_type'].presence || def_ct)
          meta         = (it['custom_metadata'].presence || def_meta)
          target_name  = (it['target_object_name'].presence || nil)
          object_name  = target_name.present? ? "#{prefix}#{target_name}" : nil

          res = call(:transfer_one_drive_to_gcs, connection, file_id, bucket, (object_name || ''), editors_mode, ctype, meta)
          if res['ok']
            ok = res['ok']
            ok[:gcs_object_name] = (object_name.presence || "#{prefix}#{ok[:gcs_object_name]}")
            uploaded << ok
          else
            failed << res['error']
            break if stop_on_error
          end
        end

        summary = {
          'total'   => (uploaded.length + failed.length),
          'success' => uploaded.length,
          'failed'  => failed.length
        }
        base = {
          'uploaded' => uploaded,
          'failed'   => failed,
          'summary'  => summary
        }
        ok = failed.empty?
        code = ok ? 200 : 207
        msg  = ok ? 'OK' : 'Partial'
        base.merge(call(:telemetry_envelope, t0, corr, ok, code, msg))
      rescue => e
        {
          'uploaded' => [],
          'failed'   => [],
          'summary'  => { 'total' => 0, 'success' => 0, 'failed' => 0 }
        }.merge(
          call(:telemetry_envelope, t0, corr, false, call(:telemetry_parse_error_code, e), e.to_s)
        )
      end,

      sample_output: lambda do
        {
          'uploaded' => [
            {
              'drive_file_id' => '1AbCd',
              'drive_name' => 'foo.txt',
              'bucket' => 'my-bucket',
              'gcs_object_name' => 'prefix/foo.txt',
              'bytes_uploaded' => 42,
              'content_type' => 'text/plain',
              'content_md5' => 'd41d8cd98f00b204e9800998ecf8427e',
              'content_sha256' => 'e3b0c44298fc1c149afbf4c8996fb924...'
            }
          ],
          'failed' => [],
          'summary' => { 'total' => 1, 'success' => 1, 'failed' => 0 },
          'ok' => true,
          'telemetry' => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 8) permission_probe
    permission_probe: {
      title: 'Permission probe (Drive & GCS)',
      subtitle: 'Quickly verify SA token, bucket access, and requester-pays',
      input_fields: lambda do
        [
          { name: 'bucket', optional: false, label: 'GCS bucket' }
        ]
      end,
      output_fields: lambda do
        [
          { name: 'ok', type: 'boolean' },
          { name: 'drive', type: 'object', properties: [
              { name: 'ok', type: 'boolean' },
              { name: 'user_email', type: 'string' },
              { name: 'error', type: 'string' }
          ]},
          { name: 'gcs', type: 'object', properties: [
              { name: 'ok', type: 'boolean' },
              { name: 'bucket_project', type: 'string' },
              { name: 'error', type: 'string' }
          ]},
          { name: 'notes', type: 'string' }
        ]
      end,
      execute: lambda do |connection, input|
        t0 = Time.now; corr = SecureRandom.uuid

        drive_ok = {}; gcs_ok = {}
        begin
          about = get('https://www.googleapis.com/drive/v3/about').params(fields: 'user')
          drive_ok = { 'ok' => true, 'user_email' => about.dig('user','emailAddress') }
        rescue => e
          drive_ok = { 'ok' => false, 'error' => e.to_s }
        end

        begin
          b = get("https://storage.googleapis.com/storage/v1/b/#{ERB::Util.url_encode(input['bucket'])}")
              .params(userProject: connection['user_project'])
          gcs_ok = { 'ok' => true, 'bucket_project' => b['projectNumber'].to_s }
        rescue => e
          gcs_ok = { 'ok' => false, 'error' => e.to_s }
        end

        {
          'ok' => drive_ok['ok'] && gcs_ok['ok'],
          'drive' => drive_ok,
          'gcs' => gcs_ok,
          'notes' => 'If GCS fails with 403, check billing on user_project, SA role serviceUsageConsumer on that project, and storage roles on the bucket.'
        }.merge(call(:telemetry_envelope, t0, corr, (drive_ok['ok'] && gcs_ok['ok']), (drive_ok['ok'] && gcs_ok['ok']) ? 200 : 403, (drive_ok['ok'] && gcs_ok['ok']) ? 'OK' : 'Forbidden'))
      end
    }
  },

  # --------- PICK LISTS ---------------------------------------------------
  pick_lists: {
    content_modes: lambda do |_|
      [
        %w[none none],
        %w[text text],
        %w[bytes bytes]
      ]
    end,
    content_modes_write: lambda do |_|
      [
        %w[text text],
        %w[bytes bytes]
      ]
    end,
    editors_modes: lambda do |_|
      [
        ['text (export Editors to plain/csv/svg per mapping)', 'text'],
        ['skip (do not transfer Editors files)', 'skip']
      ]
    end
  },

  # --------- METHODS ------------------------------------------------------
  methods: {

    # --- 1. URL BUILDERS + CONSTS --------
    const_default_scopes: -> { [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/devstorage.read_write'
      #'https://www.googleapis.com/auth/cloud-platform`',
    ] },
    const_drive_scope: -> { [ 'https://www.googleapis.com/auth/drive' ] },
    const_gcs_scope: -> { [ 'https://www.googleapis.com/auth/devstorage.read_write' ] },

    # --- 2. UTILITIES (PURE) -------------
    util_extract_drive_id: lambda do |str|
      s = (str || '').to_s.strip
      return nil if s.empty?
      m = s.match(%r{/d/([a-zA-Z0-9_-]+)}) ||
          s.match(%r{/folders/([a-zA-Z0-9_-]+)}) ||
          s.match(/[?&]id=([a-zA-Z0-9_-]+)/)
      m ? m[1] : s
    end,

    util_to_iso8601_utc: lambda do |t|
      Time.parse(t.to_s).utc.iso8601
    rescue
      t
    end,

    util_to_int_or_nil: lambda do |val|
      v = val.to_s
      v.empty? ? nil : v.to_i
    end,

    util_is_textual_mime?: lambda do |mime|
      m = (mime || '').downcase
      m.start_with?('text/') || %w[application/json application/xml image/svg+xml].include?(m)
    end,

    util_is_google_editors_mime?: lambda do |mime|
      (mime || '').start_with?('application/vnd.google-apps.')
    end,

    util_editors_export_mime: lambda do |mime|
      case mime
      when 'application/vnd.google-apps.document'     then 'text/plain'
      when 'application/vnd.google-apps.spreadsheet'  then 'text/csv'
      when 'application/vnd.google-apps.presentation' then 'text/plain'
      when 'application/vnd.google-apps.drawing'      then 'image/svg+xml'
      else
        # Defensive default; Google will 400 for unsupported combos anyway.
        'text/plain'
      end
    end,

    util_strip_urls: lambda do |text|
      text.to_s.gsub(%r{https?://\S+|www\.\S+}, '')
    end,

    util_force_utf8: lambda do |bytes|
      s = bytes.to_s
      s.force_encoding('UTF-8')
      s.valid_encoding? ? s : s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '')
    end,

    # Compute MD5 and SHA-256 from raw bytes
    util_compute_checksums: lambda do |raw|
      s = raw.to_s
      md5 = OpenSSL::Digest::MD5.hexdigest(s)
      sha = OpenSSL::Digest::SHA256.hexdigest(s)
      { 'md5' => md5, 'sha256' => sha }
    end,

    util_normalize_prefix: lambda do |prefix|
      p = (prefix || '').to_s
      return '' if p.empty?
      p.end_with?('/') ? p : "#{p}/"
    end,

    # --- 3. TELEMETRY --------------------

    telemetry_envelope: lambda do |started_at, correlation_id, ok, code, message|
      dur = ((Time.now - started_at) * 1000.0).to_i
      {
        'ok' => !!ok,
        'telemetry' => {
          'http_status'    => code.to_i,
          'message'        => (message || (ok ? 'OK' : 'ERROR')).to_s,
          'duration_ms'    => dur,
          'correlation_id' => correlation_id
        }
      }
    end,

    telemetry_parse_error_code: lambda do |err|
      # Pull first 3-digit code; fallback 0
      m = err.to_s.match(/\b(\d{3})\b/)
      m ? m[1].to_i : 0
    end,

    # --- 4. MAPPERS (UPSTREAM → SCHEMA) --

    map_gcs_meta: lambda do |o|
      {
        'bucket'       => o['bucket'],
        'name'         => o['name'],
        'size'         => call(:util_to_int_or_nil, o['size']),
        'content_type' => o['contentType'],
        'updated'      => o['updated'],
        'generation'   => o['generation'].to_s,
        'md5_hash'     => o['md5Hash'],
        'crc32c'       => o['crc32c'],
        'metadata'     => o['metadata'] || {}
      }
    end,

    map_drive_meta: lambda do |f|
      {
        'id'            => f['id'],
        'name'          => f['name'],
        'mime_type'     => f['mimeType'],
        'size'          => call(:util_to_int_or_nil, f['size']),
        'modified_time' => f['modifiedTime'],
        'checksum'      => f['md5Checksum'],
        'web_view_url'  => (f['id'].present? ? "https://drive.google.com/file/d/#{f['id']}/view" : nil),
        'owners'        => (f['owners'] || []).map { |o| { 'display_name' => o['displayName'], 'email' => o['emailAddress'] } }
      }
    end,

    # --- 5. AUTH HELPERS -----------------

    # Base64url without padding
    b64url: lambda do |bytes|
      Base64.urlsafe_encode64(bytes).gsub(/=+$/, '')
    end,

    # Sign a JWT (RS256) using the service account private key
    jwt_sign_rs256: lambda do |claims, private_key_pem|
      header = { alg: 'RS256', typ: 'JWT' }

      encoded_header  = call(:b64url, header.to_json)
      encoded_payload = call(:b64url, claims.to_json)
      signing_input   = "#{encoded_header}.#{encoded_payload}"

      rsa = OpenSSL::PKey::RSA.new(private_key_pem.to_s)
      signature = rsa.sign(OpenSSL::Digest::SHA256.new, signing_input)

      "#{signing_input}.#{call(:b64url, signature)}"
    end,

    auth_normalize_scopes: lambda do |scopes|
      arr =
        case scopes
        when nil    then call(:const_default_scopes)
        when String then scopes.split(/\s+/)
        when Array  then scopes
        else              call(:const_default_scopes)
        end
      arr.map(&:to_s).reject(&:empty?).uniq
    end,

    auth_token_cache_get: lambda do |connection, scope_key|
      cache = (connection['__token_cache'] ||= {})
      tok   = cache[scope_key]

      return nil unless tok.is_a?(Hash) && tok['access_token'].present? && tok['expires_at'].present?
      
      exp   = Time.parse(tok['expires_at']) rescue nil
      
      return nil unless exp && Time.now < (exp - 60) # valid until exp-60s
      tok
    end,

    auth_token_cache_put: lambda do |connection, scope_key, token_hash|
      cache = (connection['__token_cache'] ||= {})
      cache[scope_key] = token_hash
      token_hash
    end,

    auth_issue_token!: lambda do |connection, scopes|
      key = JSON.parse(connection['service_account_key_json'].to_s)
      token_url = (key['token_uri'].presence || 'https://oauth2.googleapis.com/token')
      now = Time.now.to_i

      scope_str = scopes.join(' ')
      payload = {
        iss:   key['client_email'],
        scope: scope_str,
        aud:   token_url,
        iat:   now,
        exp:   now + 3600 # Google max 1h
      }

      assertion = call(:jwt_sign_rs256, payload, key['private_key'])

      res = post(token_url)
              .payload(
                grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                assertion:  assertion
              )
              .request_format_www_form_urlencoded

      {
        'access_token' => res['access_token'],
        'token_type'   => res['token_type'],
        'expires_in'   => res['expires_in'],
        'expires_at'   => (Time.now + res['expires_in'].to_i).utc.iso8601,
        'scope_key'    => scope_str
      }
    end,

    # Public surface: returns *string* access token, cached by scope set
    auth_build_access_token!: lambda do |connection, scopes: nil|
      set = call(:auth_normalize_scopes, scopes)
      scope_key = set.join(' ')
      if (cached = call(:auth_token_cache_get, connection, scope_key))
        return cached['access_token']
      end
      fresh = call(:auth_issue_token!, connection, set)
      call(:auth_token_cache_put, connection, scope_key, fresh)['access_token']
    end,

    # --- 6. CORE WORKFLOWS ---------------

    # transfer one Drive file to GCS. Returns {:ok=>hash} or {:error=>hash}.
    transfer_one_drive_to_gcs: lambda do |connection, file_id, bucket, object_name, editors_mode, global_content_type, global_metadata|
      user_project = connection['user_project']
      begin
        meta = call(:drive_get_meta_resolving_shortcut, file_id)
        fname = meta['name'].to_s
        oname = (object_name.presence || fname)

        # Editors branch
        if call(:util_is_google_editors_mime?, meta['mimeType'])
          # Skipped editors
          if editors_mode == 'skip'
            return { 'error' => {
              'drive_file_id'  => file_id,
              'drive_name'     => fname,
              'gcs_object_name'=> oname,
              'error_code'     => 'SKIPPED',
              'error_message'  => 'Skipped Editors file (set editors to text to export).'
            } }
          end
          export_mime = call(:util_editors_export_mime, meta['mimeType'])
          body = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}/export")
                .params(mimeType: export_mime, supportsAllDrives: true)
                .response_format_raw
                .to_s
          cs = call(:util_compute_checksums, body)
          created = post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                    .params(uploadType: 'media', name: oname, userProject: user_project)
                    .headers('Content-Type': (global_content_type.presence || export_mime))
                    .request_body(body)
          return { 'ok' => {
            'drive_file_id'  => file_id,
            'drive_name'     => fname,
            'bucket'         => created['bucket'],
            'gcs_object_name'=> created['name'],
            'bytes_uploaded' => body.bytesize,
            'content_type'   => (global_content_type.presence || export_mime),
            'content_md5'    => cs['md5'],
            'content_sha256' => cs['sha256']
          } }

        # Non-editors branch
        else
          ctype = (global_content_type.presence || meta['mimeType'])
          body = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}")
                .params(alt: 'media', supportsAllDrives: true)
                .response_format_raw
                .to_s
          cs = call(:util_compute_checksums, body)

          # If metadata provided, switch to multipart upload to set metadata atomically.
          meta_hash = call(:safe_string_object_metadata, global_metadata || {})
          created =
            if meta_hash.present?
              boundary = "workato-multipart-#{SecureRandom.hex(8)}"
              meta_json = { name: oname, contentType: ctype, metadata: meta_hash }.to_json
              multipart =
                "--#{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n#{meta_json}\r\n" \
                "--#{boundary}\r\nContent-Type: #{ctype}\r\nContent-Transfer-Encoding: binary\r\n\r\n#{body}\r\n" \
                "--#{boundary}--\r\n"
              post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                .params(uploadType: 'multipart', userProject: user_project)
                .headers('Content-Type': "multipart/related; boundary=#{boundary}")
                .request_body(multipart)
            else
              post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                .params(uploadType: 'media', name: oname, userProject: user_project)
                .headers('Content-Type': ctype)
                .request_body(body)
            end
          return { 'ok' => {
            'drive_file_id'  => file_id,
            'drive_name'     => fname,
            'bucket'         => created['bucket'],
            'gcs_object_name'=> created['name'],
            'bytes_uploaded' => body.bytesize,
            'content_type'   => ctype,
            'content_md5'    => cs['md5'],
            'content_sha256' => cs['sha256']
          } }
        end
      rescue => e
        code = (e.to_s[/\b(\d{3})\b/, 1] || 'ERROR')
        return { 'error' => {
          'drive_file_id'   => file_id,
          'drive_name'      => (defined?(fname) ? fname : nil),
          'gcs_object_name' => object_name,
          'error_code'      => code,
          'error_message'   => e.to_s
        } }
      end
    end,

    # --- 7. DRIVE/GCS METADATA -----------

    drive_get_meta_resolving_shortcut: lambda do |file_id|
      meta = get("https://www.googleapis.com/drive/v3/files/#{file_id}")
             .params(
               supportsAllDrives: true,
               fields: 'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress),shortcutDetails'
             )
      target = meta.dig('shortcutDetails', 'targetId')
      if target.present?
        get("https://www.googleapis.com/drive/v3/files/#{target}")
          .params(
            supportsAllDrives: true,
            fields: 'id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress)'
          )
      else
        meta
      end
    end,

    safe_string_object_metadata: lambda do |meta|
      return {} unless meta.is_a?(Hash)
      meta.transform_values { |v| v.nil? ? nil : v.to_s }
    end,

    # --- 8. UI BUILDERS ------------------

    # Pick-list field with extends_schema for dynamic re-rendering
    ui_content_mode_field: lambda do |pick_list_key, default|
      {
        name: 'content_mode',
        label: 'Content mode',
        control_type: 'select',
        pick_list: pick_list_key,
        optional: false,
        default: default,
        extends_schema: true,
        hint: 'Switch to reveal relevant inputs.'
      }
    end,

    # For PUT (write): either text content + postprocess, or base64 bytes
    ui_write_body_fields: lambda do |mode|
      if mode == 'bytes'
        [
          { name: 'content_bytes', optional: false, label: 'Content (Base64)',
            hint: 'Required when mode is bytes.' }
        ]
      else
        [
          { name: 'text_content', optional: false, label: 'Content (text)',
            control_type: 'text-area',
            hint: 'Required when mode is text.' },
          { name: 'postprocess', type: 'object', optional: true, label: 'Post-process',
            properties: [
              { name: 'util_strip_urls', type: 'boolean', control_type: 'checkbox',
                label: 'Strip URLs from text', default: false }
            ] }
        ]
      end
    end,

    # For GET (read): show postprocess only for text mode
    ui_read_postprocess_if_text: lambda do |mode|
      return [] unless mode == 'text'
      [{
        name: 'postprocess', type: 'object', optional: true, label: 'Post-process',
        properties: [
          { name: 'util_strip_urls', type: 'boolean', control_type: 'checkbox',
            label: 'Strip URLs from text', default: false }
        ]
      }]
    end,

    # Advanced drawer shared by actions that talk to GCS
    ui_gcs_advanced: lambda do
      [{
        name: 'advanced', type: 'object', optional: true, label: 'Advanced',
        properties: [
          { name: 'content_type', optional: true, label: 'Content-Type',
            hint: 'Defaults: text/plain; charset=UTF-8 (text), application/octet-stream (bytes).' },
          { name: 'custom_metadata', type: 'object', optional: true, label: 'Custom metadata' },
          { name: 'preconditions', type: 'object', optional: true, label: 'Preconditions',
            properties: [
              { name: 'if_generation_match', optional: true, label: 'If-Generation-Match' },
              { name: 'if_metageneration_match', optional: true, label: 'If-Metageneration-Match' }
            ] }
        ]
      }]
    end,

    # Assemble inputs for GCS PUT
    ui_gcs_put_inputs: lambda do |config_fields|
      mode = (config_fields['content_mode'] || 'text').to_s
      base = [
        { name: 'bucket', optional: false, label: 'Bucket' },
        { name: 'object_name', optional: false, label: 'Object name' },
        call(:ui_content_mode_field, 'content_modes_write', 'text')
      ]
      base + call(:ui_write_body_fields, mode) + call(:ui_gcs_advanced)
    end,

    # Assemble inputs for GCS GET
    ui_gcs_get_inputs: lambda do |config_fields|
      mode = (config_fields['content_mode'] || 'none').to_s
      base = [
        { name: 'bucket', optional: false, label: 'Bucket' },
        { name: 'object_name', optional: false, label: 'Object name' },
        call(:ui_content_mode_field, 'content_modes', 'none')
      ]
      base + call(:ui_read_postprocess_if_text, mode)
    end,

    # Assemble inputs for Drive GET
    ui_drive_get_inputs: lambda do |config_fields|
      mode = (config_fields['content_mode'] || 'none').to_s
      base = [
        { name: 'file_id_or_url', optional: false, label: 'File ID or URL' },
        call(:ui_content_mode_field, 'content_modes', 'none')
      ]
      base + call(:ui_read_postprocess_if_text, mode)
    end,

    # Assemble inputs for Drive LIST
    ui_drive_list_inputs: lambda do |_config_fields|
      [
        { name: 'folder_id_or_url', label: 'Folder ID or URL', optional: true,
          hint: 'Leave blank to search My Drive / corpus.' },
        { name: 'drive_id', label: 'Shared drive ID', optional: true },
        {
          name: 'filters', type: 'object', optional: true, label: 'Filters',
          properties: [
            { name: 'modified_after', type: 'date_time', optional: true, label: 'Modified after' },
            { name: 'modified_before', type: 'date_time', optional: true, label: 'Modified before' },
            { name: 'mime_types', type: 'array', of: 'string', optional: true,
              hint: 'Exact mimeType values (OR).' },
            { name: 'exclude_folders', type: 'boolean', control_type: 'checkbox',
              optional: true, default: false, label: 'Exclude folders' }
          ]
        },
        {
          name: 'paging', type: 'object', optional: true, label: 'Paging',
          properties: [
            { name: 'max_results', type: 'integer', optional: true, label: 'Max results',
              hint: '1–1000, default 100' },
            { name: 'page_token', type: 'string', optional: true }
          ]
        }
      ]
    end,

    # Assemble inputs for GCS LIST
    ui_gcs_list_inputs: lambda do |_config_fields|
      [
        { name: 'bucket', optional: false, label: 'Bucket' },
        {
          name: 'filters', type: 'object', optional: true, label: 'Filters',
          properties: [
            { name: 'prefix', optional: true, label: 'Prefix' },
            { name: 'delimiter', optional: true, label: 'Delimiter',
              hint: 'Use "/" to emulate folders.' },
            { name: 'include_versions', type: 'boolean', control_type: 'checkbox',
              optional: true, default: false, label: 'Include noncurrent versions' }
          ]
        },
        {
          name: 'paging', type: 'object', optional: true, label: 'Paging',
          properties: [
            { name: 'max_results', type: 'integer', optional: true, label: 'Max results',
              hint: '1–1000, default 1000' },
            { name: 'page_token', optional: true, label: 'Page token' }
          ]
        }
      ]
    end,

    # Assemble inputs for Drive -> GCS transfer
    ui_transfer_inputs: lambda do |_config_fields|
      [
        { name: 'bucket', optional: false, label: 'Destination bucket' },
        { name: 'gcs_prefix', optional: true, label: 'Destination prefix',
          hint: 'E.g. "ingest/". Drive file name is used for object name.' },
        {
          name: 'drive_file_ids',
          type: 'array', of: 'string',
          optional: false,
          label: 'Drive file IDs or URLs',
          hint: 'Map from List files → file_ids, or paste multiple.'
        },
        { name: 'content_mode_for_editors', control_type: 'select', pick_list: 'editors_modes',
          optional: true, default: 'text', label: 'Editors files handling' }
      ]
    end,

    # Assemble inputs for batch Drive -> GCS transfer
    ui_transfer_batch_inputs: lambda do |_config_fields|
      [
        { name: 'bucket', optional: false, label: 'Destination bucket' },
        { name: 'gcs_prefix', optional: true, label: 'Destination prefix' },
        { name: 'default_editors_mode', control_type: 'select', pick_list: 'editors_modes',
          optional: true, default: 'text', label: 'Default Editors handling' },
        { name: 'default_content_type', optional: true, label: 'Default Content-Type' },
        { name: 'default_custom_metadata', type: 'object', optional: true, label: 'Default custom metadata' },
        { name: 'stop_on_error', type: 'boolean', control_type: 'checkbox', default: false, label: 'Stop on first error' },
        { name: 'items', type: 'array', of: 'object', label: 'Items',
          properties: call(:schema_transfer_batch_plan_item_fields), optional: false }
      ]
    end,

    # --- 9. SCHEMA -----------------------
    schema_transfer_batch_plan_item_fields: lambda do
      [
        { name: 'drive_file_id_or_url', label: 'Drive file ID or URL', optional: false },
        { name: 'target_object_name',   label: 'Override GCS object name', optional: true,
          hint: 'If blank, uses Drive file name.' },
        { name: 'editors_mode', label: 'Editors handling (override)',
          optional: true,
          control_type: 'select',
          pick_list: 'editors_modes',
          hint: 'If blank, the action-level Editors setting is used.' },
        { name: 'content_type', label: 'Content-Type override', optional: true },
        { name: 'custom_metadata', label: 'Custom metadata (override)', type: 'object', optional: true }
      ]
    end

  }
}
