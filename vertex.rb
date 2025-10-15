{
  title: 'Google Drive with Cloud Storage',
  version: '1.0.0',

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
      },
      {
        name: 'set_defaults_for_probe', type: 'boolean', control_type: 'checkbox',
        extends_schema: true, optional: false,
        hint: 'Optionally set default bucket and Drive folder for connection test'
      },
      {
        name: 'default_probe_bucket',
        label: 'Default probe bucket (GCS)',
        hint: 'Used by connection test + Permission probe when no bucket is provided.',
        optional: true, ngIf: 'input.set_defaults_for_probe == "true"'
      },
      {
        name: 'canary_drive_folder_id_or_url',
        label: 'Canary Drive folder (ID or URL)',
        hint: 'Probe lists files here (first 3). Leave blank to probe corpus without parent filter.',
        optional: true, ngIf: 'input.set_defaults_for_probe == "true"'
      },
      {
        name: 'canary_shared_drive_id',
        label: 'Canary shared drive ID (optional)',
        hint: 'If set, probe uses corpora=drive and this driveId.',
        optional: true, ngIf: 'input.set_defaults_for_probe == "true"'
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
  test: lambda do |connection|
    # Leverage the same probe logic as the action.
    bucket   = connection['default_probe_bucket']
    folder   = connection['canary_drive_folder_id_or_url']
    drive_id = connection['canary_shared_drive_id']
    res = call(:do_permission_probe, connection, bucket, folder, drive_id, 'probes/')
    # A non-200 probe still returns JSON; Workato considers any returned
    # object a "pass" unless you raise. We intentionally do NOT raise,
    # so users see structured hints in the test result.
    # Return a slim object so the UI shows something human-readable.
    {
      ok: res['ok'],
      whoami: res['whoami'],
      drive_ok: Array(res['drive_access']).any?,
      gcs_ok: Array(res['gcs_access']).any?,
      requester_pays_detected: !!res['requester_pays_detected']
    }
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
          { name: 'files',              type: 'array', of: 'object',
            properties: object_definitions['drive_file_base_fields']  },
          { name: 'file_ids',           type: 'array', of: 'string', 
            hint: 'Convenience: IDs extracted from files[].id' },
          { name: 'items_for_transfer', type: 'array', of: 'object',
            properties: call(:schema_transfer_batch_plan_item_fields),  hint: 'Directly map into Transfer (batch) → items' },
          { name: 'debug',              type: 'object', optional: true, properties: [
              { name: 'q' }, { name: 'corpora' }, { name: 'drive_id' },
              { name: 'page_size', type: 'integer' },
              { name: 'supportsAllDrives', type: 'boolean' },
              { name: 'includeItemsFromAllDrives', type: 'boolean' },
              { name: 'folder_id' } ] }
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
          { name: 'ok',         type: 'boolean' },
          { name: 'telemetry',  type: 'object', properties: [
            { name: 'http_status', type: 'integer' },
            { name: 'message', type: 'string' },
            { name: 'duration_ms', type: 'integer' },
            { name: 'correlation_id', type: 'string' } ] },
          { name: 'upstream',   type: 'object', optional: true, properties: [
            { name: 'code', type: 'integer' },
            { name: 'status', type: 'string' },
            { name: 'reason', type: 'string' },
            { name: 'domain', type: 'string' },
            { name: 'location', type: 'string' },
            { name: 'message', type: 'string' } ] }
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
      display_priority: 10,
      help: lambda do |_|
        {
          body: 'Return a page of Drive files with minimal metadata (newest first).'
        }
      end,

      # PURPOSE
      #   Fetch Drive file metadata and, optionally, content as text or bytes.
      #   Google Editors files are exported when `content_mode=text` with a sensible default/override.
      #
      # CONTENT RULES
      #   - Editors types (Docs/Sheets/Slides/Drawings) cannot be downloaded via `alt=media` → must export.
      #   - Non-text binaries reject `content_mode=text` with 415 (guardrail to avoid mojibake).
      #
      # OUTPUT STABILITY
      #   Uses drive_file_full schema (adds text_content/content_bytes and checksums) for consistent data pills.


      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_drive_list_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['drive_list_page']
      end,

      execute: lambda do |_connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = SecureRandom.uuid

        # Normalize folder id (accepts ID or full url)
        folder_id = call(:util_extract_drive_id, input['folder_id_or_url'])
        strict = !!input['strict_on_access_errors']
        q = ["trashed=false"]

        filters = input['filters'] || {}
        paging  = input['paging']  || {}
        recursive = !!input['recursive']
        max_depth = (input['max_depth'].to_i rescue 0)
        overall_limit = (input['overall_limit'].presence || 1000).to_i

        # Enforce Drive pageSize bounds (1..1000)
        page_size     = [[(paging['max_results'] || 100).to_i, 1].max, 1000].min
        supports      = true
        include_items = true

        # Date filters (Google expects RFC3339/ISO-8601 UTC)
        if filters['modified_after'].present?
          q << "modifiedTime >= '#{call(:util_to_iso8601_utc, filters['modified_after'])}'"
        end
        if filters['modified_before'].present?
          q << "modifiedTime <= '#{call(:util_to_iso8601_utc, filters['modified_before'])}'"
        end
        # MIME filters or group. Caller provides exact mimeType vals
        if filters['mime_types'].present?
          ors = filters['mime_types'].map { |mt| "mimeType='#{mt}'" }.join(' or ')
          q << "(#{ors})"
        end
        # Folder filters - exclude folders when caller declines them (folders have special MIME)
        if !!filters['exclude_folders']
          q << "mimeType != 'application/vnd.google-apps.folder'"
        end
        # Restrict to parent folder if requested
        if folder_id.present?
          q << "'#{folder_id}' in parents"
        end

        # Preflight: if a folder is specified, verify access and that it's actually a folder
        if folder_id.present?
          begin
            fmeta = get("https://www.googleapis.com/drive/v3/files/#{folder_id}")
                    .params(supportsAllDrives: true, fields: 'id,mimeType')
            unless fmeta['mimeType'] == 'application/vnd.google-apps.folder'
              error('400 Bad Request - Provided ID is not a folder (mimeType=' + fmeta['mimeType'].to_s + ').') if strict
            end
          rescue => e
            details = call(:google_error_extract, e)
            if strict
              error("#{(details['code'] || 403)} #{(details['status'] || 'Forbidden')} - #{(details['message'] || e.to_s)}")
            end
            # non-strict: fall through; the main list will handle and return stable empty result
          end
        end

        # Corpora/driveId are critical for correctness across My Drive vs Shared
        corpora, drive_id = if input['drive_id'].present?
                              ['drive', input['drive_id']]
                            elsif folder_id.present?
                              ['allDrives', nil]
                            else
                              ['user', nil]
                            end
        debug_block = {
          'q' => nil, 'corpora' => corpora, 'drive_id' => drive_id,
          'page_size' => page_size, 'supportsAllDrives' => supports,
          'includeItemsFromAllDrives' => include_items, 'folder_id' => folder_id
        }
        if recursive && folder_id.present?
          # BFS over the subtree rooted at folder_id (returns a single combined result (not paginated))
          # Honors filters and caps result w/ 'overall_limit', 'max_depth'
          files = call(:drive_bfs_collect_files!, folder_id, filters, corpora, drive_id, overall_limit, max_depth)
          # Prep read-to-map batch transfer plan items (aligns w/recipe UI)
          items_for_transfer = files.map { |f|
            {
              'drive_file_id_or_url' => f['id'],
              'target_object_name'   => nil,
              'editors_mode'         => nil,
              'content_type'         => nil,
              'custom_metadata'      => nil
            }
          }
          base = {
            'files'               => files,
            'file_ids'            => files.map { |f| f['id'] }.compact,
            'items_for_transfer'  => items_for_transfer,
            'count'               => files.length,
            'has_more'            => false,
            'next_page_token'      => nil,
            'debug'                => debug_block.merge('q' => q.join(' and '))
          }
          base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        else
          # Single page list (caller may iterate using next_page_token)
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
                  includeItemsFromAllDrives: include_items,
                  fields: 'files(id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress)),nextPageToken'
                )
          # Map upstream → schema: coerces size, derives web_view_url, normalizes owners[].
          files = (res['files'] || []).map { |f| call(:map_drive_meta, f) }
          items_for_transfer = files.map do |f|
            {
              'drive_file_id_or_url' => f['id'],
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
            'next_page_token'     => next_token,
            'debug'               => debug_block.merge('q' => q.join(' and '))
          }
          base.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
        end
      rescue => e
        # Predictable empty page on failure, plus telemetry. Keeps data pills stable.
        details   = call(:google_error_extract, e)
        response  = {}.merge(
          'files'            => [],
          'file_ids'         => [],
          'items_for_transfer' => [],
          'count'            => 0,
          'has_more'         => false,
          'next_page_token'  => nil,
          'debug'            => (debug_block || {})
        ).merge(call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details))
        # If strict flag is enabled, raise instead of swallowing
        if strict
          error("#{(details['code'] || 500)} #{(details['status'] || 'DriveError')} - #{(details['message'] || e.to_s)}")
        end
        response

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
          'debug'           => { 'q' => "trashed=false", 'corpora' => 'user', 'drive_id' => nil,
                                'page_size' => 100, 'supportsAllDrives' => true,
                                'includeItemsFromAllDrives' => true, 'folder_id' => nil },
          'ok'              => true,
          'telemetry'       => { 'http_status' => 200, 'message' => 'OK', 'duration_ms' => 1, 'correlation_id' => 'sample' }
        }
      end
    },

    # 2) drive_get_file
    drive_get_file: {
      title: 'Drive: Get file',
      subtitle: 'Fetch Drive file metadata and content',
      display_priority: 10,
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
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = SecureRandom.uuid

        # Normalize folder id (shortcuts resolved 1x via meta_get_resolve_shortcut)
        file_id = call(:util_extract_drive_id, input['file_id_or_url'])
        meta = call(:meta_get_resolve_shortcut, file_id)
        result = call(:map_drive_meta, meta)

        # Eval content mode, url processing behavior
        mode = (input['content_mode'] || 'none').to_s
        strip = input.dig('postprocess', 'util_strip_urls') ? true : false

        # No mode specified on input
        if mode == 'none'
          result.merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))

        elsif mode == 'text'
          # Editors branch → export to requested/compat MIME
          if call(:util_is_google_editors_mime?, meta['mimeType'])
            export_mime = call(:util_editors_export_mime, meta['mimeType'], input['editors_export_format'])
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
            # Regular file (only allow textual MIME in this case)
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
              # Guardrail: caller likely wanted bytes for binaries
              error('415 Unsupported Media Type - Non-text file; use content_mode=bytes or none.')
            end
          end
        elsif mode == 'bytes'
          # Editors cannot be downloaded as bytes; force export via text
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
        details = call(:google_error_extract, e)
        {}.merge(
          call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details)
        )
      end
    },

    # 3) gcs_list_objects
    gcs_list_objects: {
      title: 'GCS: List objects',
      subtitle: 'List objects in Google Cloud Storage bucket',
      display_priority: 10,
      help: lambda do |_|
        {
          body: 'List objects in a bucket, optionally using prefix and delimiter.'
        }
      end,

      # PURPOSE
      #   List a page of objects and common prefixes from a GCS bucket.
      #   Mirrors GCS list API while returning a Workato-friendly shape for mapping + paging.
      #
      # REQUESTER-PAYS
      #   `userProject` is forwarded from the connection when set, avoiding 403s on RP buckets.


      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_gcs_list_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['gcs_list_page']
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = SecureRandom.uuid
        filters = input['filters'] || {}
        paging  = input['paging']  || {}

        # Enforce GCS maxResults bounds (1..1000)
        page_size = [[(paging['max_results'] || 1000).to_i, 1].max, 1000].min

        # Use fields projection for lean response, faster UI
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

        # Map upstream → schema, surface flat list of names for easy mapping
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
        details = call(:google_error_extract, e)
        {}.merge(
          'objects'         => [],
          'prefixes'        => [],
          'count'           => 0,
          'has_more'        => false,
          'next_page_token' => nil
        ).merge(
          call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details)
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
      display_priority: 10,
      help: lambda do |_|
        {
          body: 'Fetch GCS object metadata and optionally content (text or bytes).'
        }
      end,

      # PURPOSE
      #   Fetch GCS object metadata and optionally its content as text (UTF-8) or bytes (Base64),
      #   with MD5/SHA-256 checksums for integrity/traceability.
      #
      # CONTENT RULES
      #   - `content_mode=text` allowed only for textual MIME; otherwise 415 to prevent garbage text.

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_gcs_get_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_content']
      end,

      execute: lambda do |connection, input|
        # Correlation id and duration for logs / analytics
        t0 = Time.now
        corr = SecureRandom.uuid

        bucket = input['bucket']
        name = input['object_name']
        
        # Fetch metadata first (derive contentType for branch decisions)
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
          # Guardrail: only textual MIME types → decode to UTF-8 (optionally strip URLs)
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
          # Always allowed, returns Base64 plus checksums
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
        details = call(:google_error_extract, e)
        {}.merge(call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details))
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
      display_priority: 10,
      help: lambda do |_|
        {
          body: 'Upload text or bytes to GCS. Returns created object metadata and bytes_uploaded.'
        }
      end,

      # PURPOSE
      #   Upload text or bytes to GCS with optional Content-Type, metadata, and preconditions.
      #   Calculates `bytes_uploaded` and returns canonical object metadata for mapping.
      #
      # MULTIPART VS MEDIA
      #   - If caller supplies custom metadata → use multipart upload so meta + content are atomic.
      #   - Else → media upload with `name` query param.

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_gcs_put_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['gcs_object_with_bytes_uploaded']
      end,

      execute: lambda do |connection, input|
        # Correlation ID and duration for logs/analytics
        t0 = Time.now
        corr = SecureRandom.uuid

        bucket  = input['bucket']
        name    = input['object_name']
        mode    = input['content_mode']
        strip   = input.dig('postprocess', 'util_strip_urls') ? true : false
        adv     = input['advanced'] || {}
        meta    = adv['custom_metadata']
        meta    = meta.transform_values { |v| v.nil? ? nil : v.to_s } if meta.present?

        # Branched execution
        body_bytes, ctype =

          # - Text
          if mode == 'text'
            # Guardrail: text_content is required; optionally strip URLs
            text = input['text_content']
            error('400 Bad Request - text_content is required when content_mode=text.') if text.nil?
            text = call(:util_strip_urls, text) if strip
            [text.to_s.dup.force_encoding('UTF-8'), (adv['content_type'].presence || 'text/plain; charset=UTF-8')]
          elsif mode == 'bytes'
            # Guardrail: content_bytes required; decode Base64 and default to octet stream
            b64 = input['content_bytes']
            error('400 Bad Request - content_bytes is required when content_mode=bytes.') if b64.nil?
            [Base64.decode64(b64.to_s), (adv['content_type'].presence || 'application/octet-stream')]
          else
            error("400 Bad Request - Unknown content_mode: #{mode}")
          end
        bytes_uploaded = body_bytes.bytesize
        # Preconditions + requester-pays (userProject) forwarded to server
        q = {
          ifGenerationMatch: adv.dig('preconditions', 'if_generation_match'),
          ifMetagenerationMatch: adv.dig('preconditions', 'if_metageneration_match'),

          userProject: connection['user_project']
        }.compact
        created =
          if meta.present?
            # Multipart upload: sets metadata and content in one request
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
            # Media upload: simplest path when no custom metadata is needed
            post("https://www.googleapis.com/upload/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
              .params(q.merge(uploadType: 'media', name: name))
              .headers('Content-Type': ctype)
              .request_body(body_bytes)
          end
        # Return normalized metadata and bytes_uploaded for downstream mapping
        call(:map_gcs_meta, created)
          .merge('bytes_uploaded' => bytes_uploaded)
          .merge(call(:telemetry_envelope, t0, corr, true, 200, 'OK'))
      rescue => e
        details = call(:google_error_extract, e)
        {}.merge(call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details))
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
      display_priority: 10,
      help: lambda do |_|
        { body: 'For each Drive file ID, fetch content (export Editors to text if selected) and upload to GCS under a prefix.' }
      end,

      # PURPOSE
      #   Transfer one or more Drive files by ID/URL into a GCS bucket/prefix.
      #   Editors files are exported (default `text`) unless caller sets `skip`.
      #
      # OBJECT NAMING
      #   Uses Drive file name by default; optional `gcs_prefix` prepended for folder-like organization.
      #
      # PARTIALS & TELEMETRY
      #   Returns uploaded[] & failed[] arrays with a summary; http_status=207 when any item fails.

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_transfer_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['transfer_result']
      end,

      execute: lambda do |connection, input|
        # Correlation id, duration for logging/analytics
        t0 = Time.now
        corr = SecureRandom.uuid

        bucket       = input['bucket']
        prefix       = call(:util_normalize_prefix, input['gcs_prefix'])
        editors_mode = (input['content_mode_for_editors'] || 'text').to_s
        editors_fmt  = input['editors_export_format']

        uploaded, failed = [], []
        # Accepts IDs or URLs (splits on whitespace, commas for ui)
        drive_files = Array(input['drive_file_ids']).map(&:to_s).flat_map { |s| s.split(/[\s,]+/) }
                        .map(&:strip).reject(&:blank?)

        drive_files.each do |raw|
          file_id = call(:util_extract_drive_id, raw)
          next if file_id.blank?

          # Core transfer - handles errors vs binary, metadata propogation, checksums, RP buckets
          res = call(:transfer_one_drive_to_gcs, connection, file_id, bucket, "#{prefix}", editors_mode, editors_fmt, nil, nil)
          if res['ok']
            ok = res['ok']
            # Ensure returned object_name reflects prefix (cosmetic)
            ok[:gcs_object_name] = "#{prefix}#{ok[:gcs_object_name]}" if prefix.present? && !ok[:gcs_object_name].to_s.start_with?(prefix)
            uploaded << ok
          else
            failed << res['error']
          end
        end

        # Summarize for dashboard-style analytics, recipe guardrails
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
        details = call(:google_error_extract, e)
        {
          'uploaded' => [],
          'failed'   => [],
          'summary'  => { 'total' => 0, 'success' => 0, 'failed' => 0 }
        }.merge(
          call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details)
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
      display_priority: 10,
      help: lambda do |_|
        { body: 'Upload many Drive files to GCS in one run, with optional per-item overrides (name, Editors mode, content-type, metadata). Partial success is returned.' }
      end,
      batch: true,

      # PURPOSE
      #   Batch version of Drive→GCS transfer. Each item may override Editors handling,
      #   Content-Type, metadata, and target object name. Partial success is allowed.
      #
      # STOP-ON-ERROR
      #   Optional boolean to fail fast for strict workflows; otherwise continues and returns 207.

      input_fields: lambda do |_obj, _conn, config_fields|
        call(:ui_transfer_batch_inputs, config_fields)
      end,

      output_fields: lambda do |object_definitions|
        object_definitions['transfer_batch_result']
      end,

      execute: lambda do |connection, input|
        # Correlation ID, duration
        t0 = Time.now
        corr = SecureRandom.uuid

        bucket       = input['bucket']
        prefix   = call(:util_normalize_prefix, input['gcs_prefix'])
        def_mode = (input['default_editors_mode'] || 'text').to_s
        def_fmt  = input['default_editors_export_format']
        def_ct   = input['default_content_type']
        def_meta = input['default_custom_metadata']
        stop_on_error = !!input['stop_on_error']

        uploaded, failed = [], []
        Array(input['items']).each_with_index do |it, idx|
          # Normalize per-item overrides; fall back to defaults where absent
          file_id = call(:util_extract_drive_id, it['drive_file_id_or_url'])
          next if file_id.blank?
          editors_mode = (it['editors_mode'].presence || def_mode).to_s
          ctype        = (it['content_type'].presence || def_ct)
          meta         = (it['custom_metadata'].presence || def_meta)
          target_name  = (it['target_object_name'].presence || nil)
          object_name  = target_name.present? ? "#{prefix}#{target_name}" : nil

          editors_fmt  = (it['editors_export_format'].presence || def_fmt)
          # Single file transfer core (editors/binary branches, RP via userProject)
          res = call(:transfer_one_drive_to_gcs, connection, file_id, bucket, (object_name || ''), editors_mode, editors_fmt, ctype, meta)
          if res['ok']
            ok = res['ok']
            ok[:gcs_object_name] = (object_name.presence || "#{prefix}#{ok[:gcs_object_name]}")
            uploaded << ok
          else
            failed << res['error']
            break if stop_on_error
          end
        end

        # Return both success/failure lanes and computed summary
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
        details = call(:google_error_extract, e)
        {
          'uploaded' => [],
          'failed'   => [],
          'summary'  => { 'total' => 0, 'success' => 0, 'failed' => 0 }
        }.merge(
          call(:telemetry_envelope, t0, corr, false, (details['code'] || 0), details['message'] || e.to_s, details)
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
      subtitle: 'Verify token identity, Drive visibility, GCS access, and requester-pays',
      display_priority: 1,
      # PURPOSE
      #   One-shot diagnostic to verify:
      #     1) Token identity (email)
      #     2) Drive visibility (first 3 files in canary folder or corpus)
      #     3) GCS access to gs://bucket/prefix (first 3 objects)
      #     4) Requester-pays detection (auto retries with userProject if configured)
      #
      # SHAPE
      #   Returns a human-readable summary plus telemetry, so connection test and recipes
      #   can surface actionable hints without raising (reduces setup friction).
      input_fields: lambda do |_|
        [
          { name: 'bucket', label: 'GCS bucket', optional: true, hint: 'Defaults to connection.default_probe_bucket' },
          { name: 'gcs_prefix', label: 'Probe prefix', optional: true, hint: 'Defaults to "probes/"' },
          { name: 'drive_canary_folder_id_or_url', label: 'Drive canary folder (ID/URL)', optional: true,
            hint: 'Defaults to connection.canary_drive_folder_id_or_url' },
          { name: 'drive_id', label: 'Shared drive ID', optional: true,
            hint: 'Defaults to connection.canary_shared_drive_id' },
          { name: 'strict', type: 'boolean', control_type: 'checkbox', label: 'Fail on access errors',
            optional: true, default: false }
        ]
      end,
      output_fields: lambda do |object_definitions|
        # Spec-compliant shape
        [
          { name: 'ok', type: 'boolean' },
          { name: 'whoami', type: 'string', hint: 'Token principal (email)' },
          { name: 'supportsAllDrives', type: 'boolean' },
          { name: 'includeItemsFromAllDrives', type: 'boolean' },
          { name: 'requester_pays_detected', type: 'boolean' },
          { name: 'drive_access', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'name' }, { name: 'web_view_url' }
          ]},
          { name: 'gcs_access', type: 'array', of: 'object', properties: [
              { name: 'name' }, { name: 'size', type: 'integer' }, { name: 'updated', type: 'date_time' }
          ]}
        ] + Array(object_definitions['envelope_fields'])
      end,
      execute: lambda do |connection, input|
        bucket   = (input['bucket'].presence || connection['default_probe_bucket'])
        prefix   = (input['gcs_prefix'].presence || 'probes/')
        folder   = (input['drive_canary_folder_id_or_url'].presence || connection['canary_drive_folder_id_or_url'])
        drive_id = (input['drive_id'].presence || connection['canary_shared_drive_id'])

        strict   = !!input['strict']
        # Delegate to shared method used by connection test for single-source of truth
        res = call(:do_permission_probe, connection, bucket, folder, drive_id, prefix)
        if strict && res['upstream'].is_a?(Hash) && res['upstream']['drive_error'].is_a?(Hash)
          de = res['upstream']['drive_error']
          error("#{(de['code'] || 403)} #{(de['status'] || 'Forbidden')} - #{(de['message'] || 'Drive access failed')}")
        end
        res
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
    end,
    editors_export_formats: lambda do |_|
      # The value is the MIME we will pass to files.export.
      [
        ['Docs → PDF',  'application/pdf'],
        ['Docs → DOCX', 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'],
        ['Sheets → CSV (first sheet)', 'text/csv'],
        ['Sheets → XLSX', 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'],
        ['Slides → PDF', 'application/pdf'],
        ['Slides → TXT (speaker notes omitted)', 'text/plain'],
        ['Drawings → SVG', 'image/svg+xml'],
        ['Drawings → PNG', 'image/png']
      ]
    end
  },

  # --------- METHODS ------------------------------------------------------
  methods: {

    # --- 1. URL BUILDERS + CONSTS --------
    const_default_scopes: -> { [
      'https://www.googleapis.com/auth/drive',
      'https://www.googleapis.com/auth/devstorage.read_write'
      #'https://www.googleapis.com/auth/cloud-platform`', # n/a in our tenant
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

    util_editors_export_mime: lambda do |source_mime, preferred_export_mime|
      # Choose the export MIME for Google Drive files
      # If caller provides preferred_export_mime, use it *when compatible* with the source type,
      # otherwise fall back to a sensible default that Google accepts.
      src = (source_mime || '')
      pref = preferred_export_mime.to_s

      case src
      when 'application/vnd.google-apps.document'
        allowed = %w[
          application/pdf
          application/vnd.openxmlformats-officedocument.wordprocessingml.document
          text/plain
        ]
        allowed.include?(pref) ? pref : 'application/pdf'

      when 'application/vnd.google-apps.spreadsheet'
        allowed = %w[
          text/csv
          application/vnd.openxmlformats-officedocument.spreadsheetml.sheet
        ]
        allowed.include?(pref) ? pref : 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'

      when 'application/vnd.google-apps.presentation'
        allowed = %w[
          application/pdf
          text/plain
        ]
        allowed.include?(pref) ? pref : 'application/pdf'

      when 'application/vnd.google-apps.drawing'
        allowed = %w[
          image/svg+xml
          image/png
        ]
        allowed.include?(pref) ? pref : 'image/svg+xml'

      else
        # Non-editors or unknown → caller should not invoke export.
        pref.presence || 'application/octet-stream'
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

    util_compute_checksums: lambda do |raw|
      # Compute MD5 and SHA-256 from raw bytes
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
    telemetry_envelope: lambda do |started_at, correlation_id, ok, code, message, upstream_details=nil|
      # Build base telemetry data
      dur   = ((Time.now - started_at) * 1000.0).to_i
      base  = {
        'ok' => !!ok,
        'telemetry' => {
          'http_status'    => code.to_i,
          'message'        => (message || (ok ? 'OK' : 'ERROR')).to_s,
          'duration_ms'    => dur,
          'correlation_id' => correlation_id
        }
      }
      # Append available error/upstream data
      upstream_details.present? ? base.merge('upstream' => upstream_details) : base
    end,

    google_error_extract: lambda do |err|
      # Extract structured Google error details from Workato exceptions
      s = err.to_s
      code = (s[/\b(\d{3})\b/, 1] || nil)
      blob = nil
      begin
        # Look for a JSON object containing "error": { ... }
        json_str = s[s.index('{') || 0, s.length]
        blob = JSON.parse(json_str) rescue nil
      rescue
        blob = nil
      end
      e = blob.is_a?(Hash) ? blob['error'] : nil
      first = (e && e['errors'].is_a?(Array) && e['errors'].first) || {}
      {
        'code'     => (e && e['code']) || (code && code.to_i),
        'status'   => e && e['status'],
        'reason'   => first['reason'],
        'domain'   => first['domain'],
        'location' => first['location'],
        'message'  => (e && e['message']) || s
      }.compact
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

    map_drive_min_for_probe: lambda do |f|
      {
        'id' => f['id'],
        'name' => f['name'],
        'web_view_url' => (f['id'].present? ? "https://drive.google.com/file/d/#{f['id']}/view" : nil)
      }
    end,

    do_permission_probe: lambda do |connection, bucket, canary_folder_id_or_url, canary_drive_id, gcs_prefix|
      # Core probe used by connection test and action
      # Returns the full spec'd object (merged with telemetry by the caller when appropriate),
      # but here we also add telemetry to keep it symmetrical with actions that expect it.
      t0 = Time.now
      corr = SecureRandom.uuid

      # 1) whoami via tokeninfo (fallback to Drive about user)
      who = nil
      begin
        tok = call(:auth_build_access_token!, connection, scopes: call(:const_default_scopes))
        ti  = get('https://oauth2.googleapis.com/tokeninfo').params(access_token: tok)
        who = ti['email'].to_s.presence
      rescue => _e
        # ignore; fallback below
      end
      if who.blank?
        begin
          about = get('https://www.googleapis.com/drive/v3/about').params(fields: 'user')
          who = about.dig('user','emailAddress').to_s.presence
        rescue => _e
          # leave nil
        end
      end

      # 2) Drive access: first 3 files in canary (if provided) or corpus
      supports = true
      include_items = true
      drive_list = []
      drive_err = nil
      begin
        q = ["trashed=false"]
        folder_id = call(:util_extract_drive_id, canary_folder_id_or_url)
        corpora, drive_id = if canary_drive_id.present?
                              ['drive', canary_drive_id]
                            elsif folder_id.present?
                              ['allDrives', nil]
                            else
                              ['user', nil]
                            end
        q << "'#{folder_id}' in parents" if folder_id.present?
        res = get('https://www.googleapis.com/drive/v3/files')
              .params(
                q: q.join(' and '),
                pageSize: 3,
                orderBy: 'modifiedTime desc',
                spaces: 'drive',
                corpora: corpora,
                driveId: drive_id,
                supportsAllDrives: supports,
                includeItemsFromAllDrives: include_items,
                fields: 'files(id,name)'
              )
        (res['files'] || []).first(3).each do |f|
          drive_list << call(:map_drive_min_for_probe, f)
        end
      rescue => e
        drive_err = call(:google_error_extract, e)
      end

      # 3) GCS access: first 3 under gs://bucket/prefix
      gcs_list = []
      requester_pays_detected = false
      begin
        if bucket.present?
          # First try WITHOUT userProject to detect requester-pays via 403 body
          begin
            r = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                .params(prefix: (gcs_prefix.presence || 'probes/'), maxResults: 3,
                        fields: 'items(name,size,updated)')
            (r['items'] || []).first(3).each do |o|
              gcs_list << {
                'name' => o['name'],
                'size' => call(:util_to_int_or_nil, o['size']),
                'updated' => o['updated']
              }
            end
          rescue => e1
            text = e1.to_s
            # Detect requester-pays phrases in 403 body text
            if text =~ /(requester[- ]pays|user\s*project|User\s*project)/i
              # Retry WITH userProject if configured
              up = connection['user_project']
              if up.present?
                begin
                  r2 = get("https://storage.googleapis.com/storage/v1/b/#{URI.encode_www_form_component(bucket)}/o")
                        .params(prefix: (gcs_prefix.presence || 'probes/'), maxResults: 3,
                                fields: 'items(name,size,updated)', userProject: up)
                  requester_pays_detected = true
                  (r2['items'] || []).first(3).each do |o|
                    gcs_list << {
                      'name' => o['name'],
                      'size' => call(:util_to_int_or_nil, o['size']),
                      'updated' => o['updated']
                    }
                  end
                rescue => _e2
                  # keep detection true; list may remain empty on perms issues
                end
              else
                requester_pays_detected = true
              end
            end
          end
        end
      rescue => _e
        # Ignore; gcs_list stays empty
      end

      ok = (who.present? && drive_err.nil?)
      res = {
        'ok' => !!ok,
        'whoami' => who,
        'supportsAllDrives' => supports,
        'includeItemsFromAllDrives' => include_items,
        'requester_pays_detected' => !!requester_pays_detected,
        'drive_access' => drive_list,
        'gcs_access' => gcs_list
      }
      # On Drive fail, carry the error in upstream.drive_error and set http_status from it
      if drive_err
        res = res.merge('upstream' => { 'drive_error' => drive_err })
      end
      http_code = drive_err ? (drive_err['code'] || 207) : (ok ? 200 : 207)
      msg = drive_err ? (drive_err['message'] || 'Partial') : (ok ? 'OK' : 'Partial')
      res.merge(call(:telemetry_envelope, t0, corr, ok, http_code, msg))
    end,

    # --- 5. AUTH HELPERS -----------------
    b64url: lambda do |bytes|
      # Base64url without padding
      Base64.urlsafe_encode64(bytes).gsub(/=+$/, '')
    end,

    jwt_sign_rs256: lambda do |claims, private_key_pem|
      # Sign a JWT (RS256) using the service account private key
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

    auth_build_access_token!: lambda do |connection, scopes: nil|
      # Public surface: returns *string* access token, cached by scope set
      set = call(:auth_normalize_scopes, scopes)
      scope_key = set.join(' ')
      if (cached = call(:auth_token_cache_get, connection, scope_key))
        return cached['access_token']
      end
      fresh = call(:auth_issue_token!, connection, set)
      call(:auth_token_cache_put, connection, scope_key, fresh)['access_token']
    end,

    # --- 6. CORE WORKFLOWS ---------------
    transfer_one_drive_to_gcs: lambda do |connection, file_id, bucket, object_name, editors_mode, editors_export_format, global_content_type, global_metadata|
      # transfer one Drive file to GCS. Returns {:ok=>hash} or {:error=>hash}.
      user_project = connection['user_project']
      begin
        meta = call(:meta_get_resolve_shortcut, file_id)
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
          export_mime = call(:util_editors_export_mime, meta['mimeType'], editors_export_format)
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

    # --- 7. METADATA ---------------------
    meta_get_resolve_shortcut: lambda do |file_id|
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

    # --- 8. DRIVE ------------------------
    drive_query_children!: lambda do |parent_id, q_extra_arr, page_size, page_token, corpora, drive_id|
      # Builds and executes files.list for children of a single parent.
      # q_extra_arr: array of additional q fragments (already sanitized).
      q_parts = ["trashed=false", "'#{parent_id}' in parents"] + Array(q_extra_arr)
      res = get('https://www.googleapis.com/drive/v3/files')
              .params(
                q: q_parts.join(' and '),
                pageSize: [[page_size.to_i, 1].max, 1000].min,
                pageToken: page_token,
                orderBy: 'modifiedTime desc',
                spaces: 'drive',
                corpora: corpora,
                driveId: drive_id,
                supportsAllDrives: true,
                includeItemsFromAllDrives: true,
                fields: 'files(id,name,mimeType,size,modifiedTime,md5Checksum,owners(displayName,emailAddress)),nextPageToken'
              )
      [res['files'] || [], res['nextPageToken']]
    end,

    drive_bfs_collect_files!: lambda do |root_id, filters, corpora, drive_id, overall_limit, max_depth|
      # Breadth-first traversal from root_id.
      # Returns array of mapped file metas (not including folders when filters['exclude_folders'] is true).
      # Always discovers folders regardless of mime filter, then lists files for each folder honoring filters.
      folder_mime = 'application/vnd.google-apps.folder' # official folder MIME.
      limit = [[overall_limit.to_i, 1].max, 10_000].min
      depth_cap = max_depth.to_i <= 0 ? Float::INFINITY : max_depth.to_i

      # Build file filter fragments for q
      q_files = []
      if filters['modified_after'].present?
        q_files << "modifiedTime >= '#{call(:util_to_iso8601_utc, filters['modified_after'])}'"
      end
      if filters['modified_before'].present?
        q_files << "modifiedTime <= '#{call(:util_to_iso8601_utc, filters['modified_before'])}'"
      end
      if filters['mime_types'].present?
        ors = Array(filters['mime_types']).map { |mt| "mimeType='#{mt}'" }.join(' or ')
        q_files << "(#{ors})"
      end
      if !!filters['exclude_folders']
        q_files << "mimeType != '#{folder_mime}'"
      end

      results = []
      queue = [[root_id, 1]] # [folder_id, depth]

      until queue.empty? || results.length >= limit
        parent_id, depth = queue.shift

        # 1) Discover subfolders at this parent (do not apply user mime filters here)
        folder_page_token = nil
        loop do
          folders, folder_page_token = call(
            :drive_query_children!, parent_id, ["mimeType='#{folder_mime}'"], 1000, folder_page_token, corpora, drive_id)
          folders.each do |f|
            queue << [f['id'], depth + 1] if depth < depth_cap
          end
          break if folder_page_token.blank?
        end

        # 2) List files at this parent honoring user filters
        file_page_token = nil
        loop do
          files, file_page_token = call(
            :drive_query_children!, parent_id, q_files, (limit - results.length), file_page_token, corpora, drive_id)
          mapped = files.map { |f| call(:map_drive_meta, f) }
          results.concat(mapped)
          break if file_page_token.blank? || results.length >= limit
        end
      end

      results
    end,

    # --- 9. UI BUILDERS ------------------
    ui_content_mode_field: lambda do |pick_list_key, default|
      # Pick-list field with extends_schema for dynamic re-rendering
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
   
    ui_write_body_fields: lambda do |mode|
      # For PUT (write): either text content + postprocess, or base64 bytes
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

    ui_read_postprocess_if_text: lambda do |mode|
      # For GET (read): show postprocess only for text mode
      return [] unless mode == 'text'
      [{
        name: 'postprocess', type: 'object', optional: true, label: 'Post-process',
        properties: [
          { name: 'util_strip_urls', type: 'boolean', control_type: 'checkbox',
            label: 'Strip URLs from text', default: false }
        ]
      }]
    end,

    ui_gcs_advanced: lambda do
      # Advanced drawer shared by actions that talk to GCS
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

    ui_gcs_put_inputs: lambda do |config_fields|
      # Assemble inputs for GCS PUT
      mode = (config_fields['content_mode'] || 'text').to_s
      base = [
        { name: 'bucket', optional: false, label: 'Bucket' },
        { name: 'object_name', optional: false, label: 'Object name' },
        call(:ui_content_mode_field, 'content_modes_write', 'text')
      ]
      base + call(:ui_write_body_fields, mode) + call(:ui_gcs_advanced)
    end,

    ui_gcs_get_inputs: lambda do |config_fields|
      # Assemble inputs for GCS GET
      mode = (config_fields['content_mode'] || 'none').to_s
      base = [
        { name: 'bucket', optional: false, label: 'Bucket' },
        { name: 'object_name', optional: false, label: 'Object name' },
        call(:ui_content_mode_field, 'content_modes', 'none')
      ]
      base + call(:ui_read_postprocess_if_text, mode)
    end,

    ui_drive_get_inputs: lambda do |config_fields|
      # Assemble inputs for Drive GET
      mode = (config_fields['content_mode'] || 'none').to_s
      base = [
        { name: 'file_id_or_url', optional: false, label: 'File ID or URL' },
        call(:ui_content_mode_field, 'content_modes', 'none')
      ]
      extra =
        if mode == 'text'
          [
            { name: 'editors_export_format', label: 'Editors export format',
              control_type: 'select', pick_list: 'editors_export_formats', optional: true,
              hint: 'Only used when the file is a Google Editors type. Default: PDF for Docs, XLSX for Sheets, PDF for Slides, SVG for Drawings.' }
          ] + call(:ui_read_postprocess_if_text, mode)
        else
          []
        end
      base + extra
    end,
 
    ui_drive_list_inputs: lambda do |_config_fields|
      # Assemble inputs for Drive LIST
      [
        { name: 'folder_id_or_url', label: 'Folder ID or URL', optional: true, hint: 'Leave blank to search My Drive / corpus.' },
        { name: 'drive_id', label: 'Shared drive ID', optional: true },
        { name: 'strict_on_access_errors', type: 'boolean', control_type: 'checkbox', label: 'Fail on access errors (403/404)', optional: true,
          default: true, hint: 'If enabled and the folder is inaccessible or not a folder, the step fails instead of returning an empty page.' },
        { name: 'recursive', type: 'boolean', control_type: 'checkbox', label: 'Recurse into subfolders',
          optional: true, default: false, hint: 'When enabled and a Folder is provided, lists files in the entire subtree.' },
        { name: 'max_depth', type: 'integer', optional: true, label: 'Maximum depth',
          hint: '0 or blank = unlimited. Depth 1 = just the folder itself, 2 = its subfolders, etc.' },
        { name: 'overall_limit', type: 'integer', optional: true, label: 'Overall result limit',
          hint: 'Hard cap across the whole subtree. Default 1000. Upper bound 10000.' },
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

    ui_gcs_list_inputs: lambda do |_config_fields|
      # Assemble inputs for GCS LIST
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

    ui_transfer_inputs: lambda do |_config_fields|
      # Assemble inputs for Drive -> GCS transfer
      [
        { name: 'bucket', optional: false, label: 'Destination bucket' },
        { name: 'gcs_prefix', optional: true, label: 'Destination prefix',
          hint: 'E.g. "ingest/". Drive file name is used for object name.' },
        { name: 'drive_file_ids', type: 'array', of: 'string', optional: false,
          label: 'Drive file IDs or URLs', hint: 'Map from List files → file_ids, or paste multiple.' },
        { name: 'content_mode_for_editors', control_type: 'select', pick_list: 'editors_modes',
          optional: true, default: 'text', label: 'Editors files handling' },
        { name: 'editors_export_format', label: 'Editors export format (when Editors=text)',
          control_type: 'select', pick_list: 'editors_export_formats', optional: true }
      ]
    end,

    ui_transfer_batch_inputs: lambda do |_config_fields|
      # Assemble inputs for batch Drive -> GCS transfer
      [
        { name: 'bucket', optional: false, label: 'Destination bucket' },
        { name: 'gcs_prefix', optional: true, label: 'Destination prefix' },
        { name: 'default_editors_mode', control_type: 'select', pick_list: 'editors_modes',
          optional: true, default: 'text', label: 'Default Editors handling' },
        { name: 'default_editors_export_format', control_type: 'select', pick_list: 'editors_export_formats',
          optional: true, label: 'Default Editors export format' },
        { name: 'default_content_type', optional: true, label: 'Default Content-Type' },
        { name: 'default_custom_metadata', type: 'object', optional: true, label: 'Default custom metadata' },
        { name: 'stop_on_error', type: 'boolean', control_type: 'checkbox', default: false, label: 'Stop on first error' },
        { name: 'items', type: 'array', of: 'object', label: 'Items',
          properties: call(:schema_transfer_batch_plan_item_fields), optional: false }
      ]
    end,

    # --- 10. SCHEMA ----------------------
    schema_transfer_batch_plan_item_fields: lambda do
      [
        { name: 'drive_file_id_or_url', label: 'Drive file ID or URL', optional: false },
        { name: 'target_object_name',   label: 'Override GCS object name', optional: true,
          hint: 'If blank, uses Drive file name.' },
        { name: 'editors_mode', label: 'Editors handling (override)', optional: true, control_type: 'select',
          pick_list: 'editors_modes',  hint: 'If blank, the action-level Editors setting is used.' },
        { name: 'editors_export_format', label: 'Editors export format (override)',
          control_type: 'select', pick_list: 'editors_export_formats', optional: true,
          hint: 'Only used when editors_mode=text and file is a Google Editors type.' },
        { name: 'content_type', label: 'Content-Type override', optional: true },
        { name: 'custom_metadata', label: 'Custom metadata (override)', type: 'object', optional: true }
      ]
    end

  }
}
