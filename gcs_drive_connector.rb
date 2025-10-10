{
  title: 'Google Drive + GCS Utilities',

  # --------- CONNECTION --------------------------------------------------
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
        key = JSON.parse(connection['service_account_key_json'].to_s)
        token_url = (key['token_uri'].presence || 'https://oauth2.googleapis.com/token')
        now = Time.now.to_i

        scopes = [
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/drive.readonly'
        ].join(' ')

        payload = {
          iss: key['client_email'],
          scope: scopes,
          aud: token_url,
          iat: now,
          exp: now + 3600 # max 1 hour
        }

        assertion = call(:jwt_sign_rs256, payload, key['private_key'])

        res = post(token_url)
                .payload(
                  grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer',
                  assertion: assertion
                )
                .request_format_www_form_urlencoded

        {
          access_token: res['access_token'],
          token_type: res['token_type'],
          expires_in: res['expires_in'],
          # cushion refresh by 60s
          expires_at: (Time.now + res['expires_in'].to_i - 60).utc.iso8601
        }
      end,

      # URL: base
      #base_uri: lambda do |connection|
      #end,

      # URL: authorization
      authorization_url: lambda do |connection|
        scopes = [
          'https://www.googleapis.com/auth/cloud-platform',
          'https://www.googleapis.com/auth/drive.readonly'
        ].join(' ')
        "https://accounts.google.com/o/oauth2/v2/auth?response_type=code&access_type=offline&include_granted_scopes=true&scope=#{CGI.escape(scopes)}"
      end,

      # URL: token
      token_url: 'https://oauth2.googleapis.com/token',
      
      # Refresh rules
      refresh_on: [401, 403],

      detect_on: [/UNAUTHENTICATED/i, /invalid[_-]?token/i, /expired/i, /insufficient/i]
    }
  },

  # --------- CONNECTION TEST ---------------------------------------------
  test: lambda do |_connection|
    get('https://www.googleapis.com/drive/v3/about')
      .params(fields: 'user,storageQuota')
  end,

  # --------- OBJECT DEFINITIONS ------------------------------------------
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
        object_definitions['gcs_object_base_fields'] + [
          { name: 'text_content', type: 'string' },
          { name: 'content_bytes', type: 'string', hint: 'Base64' }
        ]
      end
    },
    gcs_object_with_bytes_uploaded: {
      fields: lambda do |object_definitions|
        object_definitions['gcs_object_base_fields'] + [
          { name: 'bytes_uploaded', type: 'integer' }
        ]
      end
    },

    # --- Composite definitions
    drive_file_min: {
      fields: lambda do |object_definitions|
        object_definitions['drive_file_base_fields']
      end
    },
    drive_file_full: {
      fields: lambda do |object_definitions|
        object_definitions['drive_file_base_fields'] + [
          { name: 'exported_as', type: 'string' },
          { name: 'text_content', type: 'string' },
          { name: 'content_bytes', type: 'string', hint: 'Base64' }
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
    },
    gcs_list_page: {
      fields: lambda do |object_definitions|
        [
          { name: 'objects', type: 'array', of: 'object', properties: object_definitions['gcs_object_base_fields'] },
          { name: 'prefixes', type: 'array', of: 'string' }
        ] + object_definitions['list_page_meta']
      end
    },
    # Mirrors drive_list_files execute payload: { files: [...], <page_meta> }
    drive_list_page: {
      fields: lambda do |object_definitions|
        [
          { name: 'files', type: 'array', of: 'object', properties: object_definitions['drive_file_base_fields'] }
        ] + object_definitions['list_page_meta']
      end
    },
    transfer_result: {
      fields: lambda do |_connection|
        [
          { name: 'uploaded', type: 'array', of: 'object', properties: [
            { name: 'drive_file_id' }, { name: 'drive_name' }, { name: 'bucket' }, { name: 'gcs_object_name' }, { name: 'bytes_uploaded', type: 'integer' }, { name: 'content_type' }
          ]},
          { name: 'failed', type: 'array', of: 'object', properties: [
            { name: 'drive_file_id' }, { name: 'error_message' }, { name: 'error_code' }
          ]},
          { name: 'summary', type: 'object', properties: [
            { name: 'total', type: 'integer' }, { name: 'success', type: 'integer' }, { name: 'failed', type: 'integer' }
          ]}
        ]
      end
    }
  },

  # --------- ACTIONS -----------------------------------------------------
  actions: {

    # 1) drive_list_files
    drive_list_files: {
      title: 'Drive: List files',
      description: 'Return a page of Drive files with minimal metadata (newest first).',
      input_fields: lambda do |_|
        [
          { name: 'folder_id_or_url', label: 'Folder ID or URL', optional: true },
          { name: 'drive_id', label: 'Shared drive ID', optional: true },
          { name: 'modified_after', type: 'date_time', optional: true },
          { name: 'modified_before', type: 'date_time', optional: true },
          { name: 'mime_types', type: 'array', of: 'string', optional: true, hint: "Exact mimeType matches; any of these (OR)." },
          { name: 'exclude_folders', type: 'boolean', optional: true, default: false },
          { name: 'max_results', type: 'integer', optional: true, hint: '1-1000, default 100' },
          { name: 'page_token', type: 'string', optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['drive_list_page']
      end,
      execute: lambda do |_connection, input|
        folder_id = call(:extract_drive_id, input['folder_id_or_url'])
        page_size = [[(input['max_results'] || 100).to_i, 1].max, 1000].min
        q = ["trashed=false"]

        # Date filtering (ISO-8601)
        if input['modified_after'].present?
          q << "modifiedTime >= '#{call(:to_iso8601_utc, input['modified_after'])}'"
        end
        if input['modified_before'].present?
          q << "modifiedTime <= '#{call(:to_iso8601_utc, input['modified_before'])}'"
        end
        # MIME filters
        if input['mime_types'].present?
          ors = input['mime_types'].map { |mt| "mimeType='#{mt}'" }.join(' or ')
          q << "(#{ors})"
        end
        # Exclude folders
        if input['exclude_folders']
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
                pageToken: input['page_token'],
                orderBy: 'modifiedTime desc',
                spaces: 'drive',
                corpora: corpora,
                driveId: drive_id,
                supportsAllDrives: true,
                includeItemsFromAllDrives: true,
                fields: 'files(id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress)),nextPageToken'
              )
        files = (res['files'] || []).map do |f|
          call(:map_drive_meta, f)
        end
        next_token = res['nextPageToken']
        {
          files: files,
          count: files.length,
          has_more: next_token.present?,
          next_page_token: next_token
        }
      end
    },

    # 2) drive_get_file
    drive_get_file: {
      title: 'Drive: Get file (meta + optional content)',
      description: 'Fetch Drive file metadata and optionally content (text or bytes). Shortcuts are resolved once.',
      input_fields: lambda do |_|
        [
          { name: 'file_id_or_url', optional: false },
          { name: 'content_mode', control_type: 'select', pick_list: 'content_modes', optional: false, default: 'none' },
          { name: 'postprocess', type: 'object', properties: [{ name: 'strip_urls', type: 'boolean', default: false }], optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['drive_file_full']
      end,
      execute: lambda do |_connection, input|
        file_id = call(:extract_drive_id, input['file_id_or_url'])
        meta = call(:drive_get_meta_resolving_shortcut, file_id)

        result = call(:map_drive_meta, meta)

        mode = (input['content_mode'] || 'none').to_s
        strip = input.dig('postprocess', 'strip_urls') ? true : false

        if mode == 'none'
          result
        elsif mode == 'text'
          if call(:is_google_editors_mime?, meta['mimeType'])
            export_mime = call(:editors_export_mime, meta['mimeType'])
            bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}/export")
                    .params(mimeType: export_mime, supportsAllDrives: true)
                    .response_format_raw # treat response as new
            text = call(:force_utf8, bytes.to_s)
            text = call(:strip_urls, text) if strip
            result.merge(exported_as: export_mime, text_content: text)
          else
            if call(:is_textual_mime?, meta['mimeType'])
              bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}")
                      .params(alt: 'media', supportsAllDrives: true)
                      .response_format_raw
              text = call(:force_utf8, bytes.to_s)
              text = call(:strip_urls, text) if strip
              result.merge(text_content: text)
            else
              error('415 Unsupported Media Type - Non-text file; use content_mode=bytes or none.')
            end
          end
        elsif mode == 'bytes'
          if call(:is_google_editors_mime?, meta['mimeType'])
            error('400 Bad Request - Editors files require content_mode=text (export).')
          end
          bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}")
                  .params(alt: 'media', supportsAllDrives: true, acknowledgeAbuse: false)
                  .response_format_raw
          result.merge(content_bytes: Base64.strict_encode64(bytes.to_s))
        else
          error("400 Bad Request - Unknown content_mode: #{mode}")
        end
      end
    },

    # 3) gcs_list_objects
    gcs_list_objects: {
      title: 'GCS: List objects',
      description: 'List objects in a bucket, optionally using prefix and delimiter.',
      input_fields: lambda do |_|
        [
          { name: 'bucket', optional: false },
          { name: 'prefix', optional: true },
          { name: 'delimiter', optional: true, hint: 'Use "/" to emulate folders' },
          { name: 'include_versions', type: 'boolean', optional: true, default: false },
          { name: 'max_results', type: 'integer', optional: true, hint: '1–1000, default 1000' },
          { name: 'page_token', optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['gcs_list_page']
      end,
      execute: lambda do |connection, input|
        page_size = [[(input['max_results'] || 1000).to_i, 1].max, 1000].min
        res = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(input['bucket'])}/o")
              .params(
                prefix: input['prefix'],
                delimiter: input['delimiter'],
                pageToken: input['page_token'],
                maxResults: page_size,
                versions: !!input['include_versions'],
                fields: 'items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes',
                userProject: connection['user_project']
              )
        items = (res['items'] || []).map do |o|
          call(:map_gcs_meta, o)
        end
        next_token = res['nextPageToken']
        {
          objects: items,
          prefixes: res['prefixes'] || [],
          count: items.length,
          has_more: next_token.present?,
          next_page_token: next_token
        }
      end
    },

    # 4) gcs_get_object
    gcs_get_object: {
      title: 'GCS: Get object (meta + optional content)',
      description: 'Fetch GCS object metadata and optionally content (text or bytes).',
      input_fields: lambda do |_|
        [
          { name: 'bucket', optional: false },
          { name: 'object_name', optional: false },
          { name: 'content_mode', control_type: 'select', pick_list: 'content_modes', optional: false, default: 'none' },
          { name: 'postprocess', type: 'object', properties: [{ name: 'strip_urls', type: 'boolean', default: false }], optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_content']
      end,
      execute: lambda do |connection, input|
        bucket = input['bucket']
        name = input['object_name']
        meta = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o/#{ERB::Util.url_encode(name)}")
               .params(alt: 'json', userProject: connection['user_project'])
        base = call(:map_gcs_meta, meta)
        mode = (input['content_mode'] || 'none').to_s
        strip = input.dig('postprocess', 'strip_urls') ? true : false
        ctype = meta['contentType']
        if mode == 'none'
          base
        elsif mode == 'text'
          unless call(:is_textual_mime?, ctype)
            error('415 Unsupported Media Type - Non-text object; use content_mode=bytes or none.')
          end
          bytes = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o/#{ERB::Util.url_encode(name)}")
                  .params(alt: 'media', userProject: connection['user_project'])
                  .response_format_raw
          text = call(:force_utf8, bytes.to_s)
          text = call(:strip_urls, text) if strip
          base.merge(text_content: text)
        elsif mode == 'bytes'
          bytes = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o/#{ERB::Util.url_encode(name)}")
                  .params(alt: 'media', userProject: connection['user_project'])
                  .response_format_raw
          base.merge(content_bytes: Base64.strict_encode64(bytes.to_s))
        else
          error("400 Bad Request - Unknown content_mode: #{mode}")
        end
      end
    },

    # 5) gcs_put_object
    gcs_put_object: {
      title: 'GCS: Put object',
      description: 'Upload text or bytes to GCS. Returns created object metadata and bytes_uploaded.',
      input_fields: lambda do |_|
        [
          { name: 'bucket', optional: false },
          { name: 'object_name', optional: false },
          { name: 'content_mode', control_type: 'select', pick_list: 'content_modes_write', optional: false, default: 'text' },
          { name: 'text_content', optional: true, hint: 'Required when content_mode=text' },
          { name: 'content_bytes', optional: true, hint: 'Base64; required when content_mode=bytes' },
          { name: 'content_type', optional: true, hint: 'Default text/plain; charset=UTF-8 for text, application/octet-stream for bytes' },
          { name: 'custom_metadata', type: 'object', optional: true },
          {
            name: 'preconditions',
            type: 'object',
            optional: true,
            properties: [
              { name: 'if_generation_match', optional: true },
              { name: 'if_metageneration_match', optional: true }
            ]
          },
          { name: 'postprocess', type: 'object', properties: [{ name: 'strip_urls', type: 'boolean', default: false }], optional: true }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_bytes_uploaded']
      end,
      execute: lambda do |connection, input|
        bucket = input['bucket']
        name = input['object_name']
        mode = input['content_mode']
        strip = input.dig('postprocess', 'strip_urls') ? true : false
        meta = input['custom_metadata']
        meta = meta.transform_values { |v| v.nil? ? nil : v.to_s } if meta.present?
        body_bytes, ctype =
          if mode == 'text'
            text = input['text_content']
            error('400 Bad Request - text_content is required when content_mode=text.') if text.nil?
            text = call(:strip_urls, text) if strip
            [text.to_s.dup.force_encoding('UTF-8'), (input['content_type'].presence || 'text/plain; charset=UTF-8')]
          elsif mode == 'bytes'
            b64 = input['content_bytes']
            error('400 Bad Request - content_bytes is required when content_mode=bytes.') if b64.nil?
            [Base64.decode64(b64.to_s), (input['content_type'].presence || 'application/octet-stream')]
          else
            error("400 Bad Request - Unknown content_mode: #{mode}")
          end
        bytes_uploaded = body_bytes.bytesize
        q = {
          ifGenerationMatch: input.dig('preconditions', 'if_generation_match'),
          ifMetagenerationMatch: input.dig('preconditions', 'if_metageneration_match'),
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
        call(:map_gcs_meta, created).merge(bytes_uploaded: bytes_uploaded)
      end
    },

    # 6) transfer_drive_to_gcs
    transfer_drive_to_gcs: {
      title: 'Transfer: Drive → GCS',
      description: 'For each Drive file ID, fetch content (export Editors to text if selected) and upload to GCS under a prefix.',
      input_fields: lambda do |_|
        [
          { name: 'bucket', optional: false },
          { name: 'gcs_prefix', optional: true, hint: 'E.g. "ingest/". Drive file name is used as GCS object name.' },
          {
            name: 'drive_file_ids',
            type: 'array', of: 'string',
            control_type: 'text-area',
            optional: false,
            hint: 'Paste one Drive file ID or URL per line.'
          },
          { name: 'content_mode_for_editors', control_type: 'select', pick_list: 'editors_modes', optional: true, default: 'text' }
        ]
      end,
      output_fields: lambda do |object_definitions|
        object_definitions['transfer_result']
      end,
      execute: lambda do |connection, input|
        bucket = input['bucket']
        prefix = (input['gcs_prefix'] || '').to_s
        prefix = "#{prefix}/" unless prefix.empty? || prefix.end_with?('/')
        editors_mode = (input['content_mode_for_editors'] || 'text').to_s
        user_project = connection['user_project']
        uploaded = []
        failed = []

        raw_input = input['drive_file_ids'].to_s
        drive_files = raw_input.split(/[\s,]+/).map(&:strip).reject(&:blank?)

        drive_files.each do |raw|
          begin
            file_id = call(:extract_drive_id, raw.strip)
            next if file_id.blank?
            meta = call(:drive_get_meta_resolving_shortcut, file_id)
            fname = meta['name'].to_s
            object_name = "#{prefix}#{fname}"
            if call(:is_google_editors_mime?, meta['mimeType'])
              if editors_mode == 'skip'
                failed << { drive_file_id: file_id, error_message: 'Skipped Editors file (set content_mode_for_editors=text to export).', error_code: 'SKIPPED' }
                next
              end
              export_mime = call(:editors_export_mime, meta['mimeType'])
              bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}/export")
                      .params(mimeType: export_mime, supportsAllDrives: true)
                      .response_format_raw
              body = bytes.to_s
              created = post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                        .params(uploadType: 'media', name: object_name, userProject: user_project)
                        .headers('Content-Type': export_mime)
                        .request_body(body)
              uploaded << {
                drive_file_id: file_id,
                drive_name: fname,
                bucket: created['bucket'],
                gcs_object_name: created['name'],
                bytes_uploaded: body.bytesize,
                content_type: export_mime
              }
            else
              ctype = meta['mimeType']
              bytes = get("https://www.googleapis.com/drive/v3/files/#{meta['id']}")
                      .params(alt: 'media', supportsAllDrives: true)
                      .response_format_raw
              body = bytes.to_s
              created = post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                        .params(uploadType: 'media', name: object_name, userProject: user_project)
                        .headers('Content-Type': ctype)
                        .request_body(body)
              uploaded << {
                drive_file_id: file_id,
                drive_name: fname,
                bucket: created['bucket'],
                gcs_object_name: created['name'],
                bytes_uploaded: body.bytesize,
                content_type: ctype
              }
            end
          rescue => e
            code = (e.to_s[/\b(\d{3})\b/, 1] || 'ERROR')
            failed << { drive_file_id: raw.strip, error_message: e.to_s, error_code: code }
          end
        end
        {
          uploaded: uploaded,
          failed: failed,
          summary: {
            total: (uploaded.length + failed.length),
            success: uploaded.length,
            failed: failed.length
          }
        }
      end
    }
  },

  # --------- PICK LISTS --------------------------------------------------
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

  # --------- METHODS -----------------------------------------------------
  methods: {
    # --- Shared rules ---
    extract_drive_id: lambda do |str|
      s = (str || '').to_s.strip
      return nil if s.empty?
      m = s.match(%r{/d/([a-zA-Z0-9_-]+)}) ||
          s.match(%r{/folders/([a-zA-Z0-9_-]+)}) ||
          s.match(/[?&]id=([a-zA-Z0-9_-]+)/)
      m ? m[1] : s
    end,

    to_iso8601_utc: lambda do |t|
      Time.parse(t.to_s).utc.iso8601
    rescue
      t
    end,

    to_int_or_nil: lambda do |val|
      v = val.to_s
      v.empty? ? nil : v.to_i
    end,

    is_textual_mime?: lambda do |mime|
      m = (mime || '').downcase
      m.start_with?('text/') || %w[application/json application/xml image/svg+xml].include?(m)
    end,

    is_google_editors_mime?: lambda do |mime|
      (mime || '').start_with?('application/vnd.google-apps.')
    end,

    editors_export_mime: lambda do |mime|
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

    strip_urls: lambda do |text|
      text.to_s.gsub(%r{https?://\S+|www\.\S+}, '')
    end,

    force_utf8: lambda do |bytes|
      s = bytes.to_s
      s.force_encoding('UTF-8')
      s.valid_encoding? ? s : s.encode('UTF-8', invalid: :replace, undef: :replace, replace: '')
    end,

    map_gcs_meta: lambda do |o|
      {
        bucket:       o['bucket'],
        name:         o['name'],
        size:         call(:to_int_or_nil, o['size']),
        content_type: o['contentType'],
        updated:      o['updated'],
        generation:   o['generation'].to_s,
        md5_hash:     o['md5Hash'],
        crc32c:       o['crc32c'],
        metadata:     o['metadata'] || {}
      }
    end,

    map_drive_meta: lambda do |f|
      {
        id: f['id'],
        name: f['name'],
        mime_type: f['mimeType'],
        size: call(:to_int_or_nil, f['size']),
        modified_time: f['modifiedTime'],
        checksum: f['md5Checksum'],
        owners: (f['owners'] || []).map { |o| { display_name: o['displayName'], email: o['emailAddress'] } }
      }
    end,

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
    end
  }
}
