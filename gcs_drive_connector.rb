{
  title: "Google Drive & GCS Connector",
  
  connection: {
    fields: [
      {
        name: "client_id",
        label: "Client ID",
        optional: false,
        hint: "OAuth 2.0 Client ID from Google Cloud Console"
      },
      {
        name: "client_secret", 
        label: "Client Secret",
        optional: false,
        hint: "OAuth 2.0 Client Secret",
        control_type: "password"
      }
    ],
    
    authorization: {
      type: "oauth2",
      
      authorization_url: lambda do |connection|
        "https://accounts.google.com/o/oauth2/v2/auth"
      end,
      
      acquire: lambda do |connection, auth_code, redirect_uri|
        response = post("https://oauth2.googleapis.com/token").
          payload(
            client_id: connection["client_id"],
            client_secret: connection["client_secret"],
            grant_type: "authorization_code",
            code: auth_code,
            redirect_uri: redirect_uri
          ).
          request_format_www_form_urlencoded
        
        {
          access_token: response["access_token"],
          refresh_token: response["refresh_token"]
        }
      end,
      
      refresh: lambda do |connection, refresh_token|
        response = post("https://oauth2.googleapis.com/token").
          payload(
            client_id: connection["client_id"],
            client_secret: connection["client_secret"],
            grant_type: "refresh_token",
            refresh_token: refresh_token
          ).
          request_format_www_form_urlencoded
        
        { access_token: response["access_token"] }
      end,
      
      client_id: lambda do |connection|
        connection["client_id"]
      end,
      
      client_secret: lambda do |connection|
        connection["client_secret"]
      end,
      
      scopes: lambda do |connection|
        [
          "https://www.googleapis.com/auth/drive.readonly",
          "https://www.googleapis.com/auth/devstorage.read_write",
          "openid",
          "email",
          "profile"
        ]
      end,
      
      detect_on: [401],
      
      apply: lambda do |connection, access_token|
        headers("Authorization": "Bearer #{access_token}")
      end
    }
  },
  
  test: lambda do |connection|
    get("https://www.googleapis.com/drive/v3/about").
      params(fields: "user")
  end,
  
  actions: {
    drive_list_files: {
      title: "List Drive files",
      subtitle: "List files from Google Drive with optional filtering",
      
      input_fields: lambda do
        [
          { name: "folder_id", optional: true, hint: "Folder ID or URL to list files from" },
          { name: "max_results", type: "integer", default: 100, hint: "1-1000" },
          { name: "page_token", optional: true },
          { name: "modified_after", type: "datetime", optional: true },
          { name: "modified_before", type: "datetime", optional: true },
          { name: "mime_type", optional: true, hint: "Filter by MIME type" },
          { name: "exclude_folders", type: "boolean", default: false },
          { name: "drive_id", optional: true, hint: "Shared drive ID" }
        ]
      end,
      
      execute: lambda do |connection, input|
        # Extract folder ID from URL if needed
        folder_id = input["folder_id"]
        if folder_id&.include?("/")
          folder_id = folder_id[/\/d\/([^\/\?]+)/, 1] || folder_id[/[?&]id=([^&]+)/, 1] || folder_id
        end
        
        # Build query
        query_parts = ["trashed=false"]
        query_parts << "'#{folder_id}' in parents" if folder_id.present?
        
        if input["modified_after"].present?
          query_parts << "modifiedTime >= '#{input["modified_after"].to_time.iso8601}'"
        end
        
        if input["modified_before"].present?
          query_parts << "modifiedTime <= '#{input["modified_before"].to_time.iso8601}'"
        end
        
        if input["mime_type"].present?
          query_parts << "mimeType = '#{input["mime_type"]}'"
        end
        
        if input["exclude_folders"] == true
          query_parts << "mimeType != 'application/vnd.google-apps.folder'"
        end
        
        # Setup corpus and drive params
        params = {
          q: query_parts.join(" and "),
          pageSize: [1, [input["max_results"] || 100, 1000].min].max,
          orderBy: "modifiedTime desc",
          spaces: "drive",
          supportsAllDrives: true,
          includeItemsFromAllDrives: true,
          fields: "files(id,name,mimeType,size,modifiedTime,md5Checksum,owners),nextPageToken"
        }
        
        params[:pageToken] = input["page_token"] if input["page_token"].present?
        
        if input["drive_id"].present?
          params[:corpora] = "drive"
          params[:driveId] = input["drive_id"]
        elsif folder_id.present?
          params[:corpora] = "allDrives"
        else
          params[:corpora] = "user"
        end
        
        response = get("https://www.googleapis.com/drive/v3/files").
          params(params)
        
        {
          files: response["files"]&.map do |file|
            {
              id: file["id"],
              name: file["name"],
              mime_type: file["mimeType"],
              size: file["size"]&.to_i,
              modified_time: file["modifiedTime"],
              checksum: file["md5Checksum"],
              owners: file["owners"]
            }
          end || [],
          count: response["files"]&.size || 0,
          has_more: response["nextPageToken"].present?,
          next_page_token: response["nextPageToken"]
        }
      end,
      
      output_fields: lambda do
        [
          { name: "files", type: "array", of: "object", properties: [
            { name: "id" },
            { name: "name" },
            { name: "mime_type" },
            { name: "size", type: "integer" },
            { name: "modified_time" },
            { name: "checksum" },
            { name: "owners", type: "array", of: "object", properties: [
              { name: "displayName" },
              { name: "emailAddress" }
            ]}
          ]},
          { name: "count", type: "integer" },
          { name: "has_more", type: "boolean" },
          { name: "next_page_token" }
        ]
      end
    },
    
    drive_get_file: {
      title: "Get Drive file",
      subtitle: "Get file metadata and optionally content",
      
      input_fields: lambda do
        [
          { name: "file_id", optional: false, hint: "File ID or URL" },
          { name: "content_mode", control_type: "select", 
            pick_list: [["None", "none"], ["Text", "text"], ["Bytes", "bytes"]],
            default: "none" },
          { name: "postprocess", type: "object", properties: [
            { name: "strip_urls", type: "boolean", default: false }
          ]}
        ]
      end,
      
      execute: lambda do |connection, input|
        # Extract file ID from URL if needed
        file_id = input["file_id"]
        if file_id&.include?("/")
          file_id = file_id[/\/d\/([^\/\?]+)/, 1] || file_id[/[?&]id=([^&]+)/, 1] || file_id
        end
        
        # Get metadata
        metadata = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
          params(
            supportsAllDrives: true,
            fields: "id,name,mimeType,size,modifiedTime,md5Checksum,owners,shortcutDetails"
          )
        
        # Follow shortcut if needed
        if metadata["shortcutDetails"].present?
          file_id = metadata["shortcutDetails"]["targetId"]
          metadata = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
            params(
              supportsAllDrives: true,
              fields: "id,name,mimeType,size,modifiedTime,md5Checksum,owners"
            )
        end
        
        result = {
          id: metadata["id"],
          name: metadata["name"],
          mime_type: metadata["mimeType"],
          size: metadata["size"]&.to_i,
          modified_time: metadata["modifiedTime"],
          checksum: metadata["md5Checksum"],
          owners: metadata["owners"]
        }
        
        # Handle content modes
        case input["content_mode"]
        when "text"
          if metadata["mimeType"]&.start_with?("application/vnd.google-apps.")
            # Google Editors - export
            export_map = {
              "application/vnd.google-apps.document" => "text/plain",
              "application/vnd.google-apps.spreadsheet" => "text/csv",
              "application/vnd.google-apps.presentation" => "text/plain",
              "application/vnd.google-apps.drawing" => "image/svg+xml"
            }
            
            export_mime = export_map[metadata["mimeType"]]
            if export_mime.nil?
              error("Unsupported Google Editors type for export")
            end
            
            content = get("https://www.googleapis.com/drive/v3/files/#{file_id}/export").
              params(
                mimeType: export_mime,
                supportsAllDrives: true
              ).response_format_raw
            
            result[:text_content] = content
            result[:exported_as] = export_mime
            
          elsif ["text/", "application/json", "application/xml", "text/csv", "image/svg+xml"].
                any? { |t| metadata["mimeType"]&.include?(t) }
            # Textual non-Editors
            content = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
              params(
                alt: "media",
                supportsAllDrives: true
              ).response_format_raw
            
            result[:text_content] = content
            
          else
            error("Non-text file; use content_mode=bytes or none")
          end
          
          # Strip URLs if requested
          if input.dig("postprocess", "strip_urls") && result[:text_content]
            result[:text_content] = result[:text_content].gsub(/https?:\/\/[^\s]+/, "")
          end
          
        when "bytes"
          if metadata["mimeType"]&.start_with?("application/vnd.google-apps.")
            error("Google Editors files require content_mode=text (export)")
          end
          
          content = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
            params(
              alt: "media",
              supportsAllDrives: true
            ).response_format_raw
          
          result[:content_bytes] = content.encode_base64
        end
        
        result
      end,
      
      output_fields: lambda do
        [
          { name: "id" },
          { name: "name" },
          { name: "mime_type" },
          { name: "size", type: "integer" },
          { name: "modified_time" },
          { name: "checksum" },
          { name: "owners", type: "array", of: "object", properties: [
            { name: "displayName" },
            { name: "emailAddress" }
          ]},
          { name: "text_content" },
          { name: "content_bytes" },
          { name: "exported_as" }
        ]
      end
    },
    
    gcs_list_objects: {
      title: "List GCS objects",
      subtitle: "List objects in a Google Cloud Storage bucket",
      
      input_fields: lambda do
        [
          { name: "bucket", optional: false },
          { name: "prefix", optional: true },
          { name: "delimiter", optional: true, hint: "Use '/' to emulate folders" },
          { name: "max_results", type: "integer", default: 1000, hint: "1-1000" },
          { name: "page_token", optional: true },
          { name: "include_versions", type: "boolean", default: false }
        ]
      end,
      
      execute: lambda do |connection, input|
        params = {
          maxResults: [1, [input["max_results"] || 1000, 1000].min].max,
          fields: "items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes"
        }
        
        params[:prefix] = input["prefix"] if input["prefix"].present?
        params[:delimiter] = input["delimiter"] if input["delimiter"].present?
        params[:pageToken] = input["page_token"] if input["page_token"].present?
        params[:versions] = input["include_versions"] if input["include_versions"] == true
        
        response = get("https://storage.googleapis.com/storage/v1/b/#{input["bucket"]}/o").
          params(params)
        
        {
          objects: response["items"]&.map do |item|
            {
              bucket: item["bucket"],
              name: item["name"],
              size: item["size"]&.to_i,
              content_type: item["contentType"],
              updated: item["updated"],
              generation: item["generation"],
              md5_hash: item["md5Hash"],
              crc32c: item["crc32c"],
              metadata: item["metadata"]
            }
          end || [],
          prefixes: response["prefixes"] || [],
          count: response["items"]&.size || 0,
          has_more: response["nextPageToken"].present?,
          next_page_token: response["nextPageToken"]
        }
      end,
      
      output_fields: lambda do
        [
          { name: "objects", type: "array", of: "object", properties: [
            { name: "bucket" },
            { name: "name" },
            { name: "size", type: "integer" },
            { name: "content_type" },
            { name: "updated" },
            { name: "generation" },
            { name: "md5_hash" },
            { name: "crc32c" },
            { name: "metadata", type: "object" }
          ]},
          { name: "prefixes", type: "array", of: "string" },
          { name: "count", type: "integer" },
          { name: "has_more", type: "boolean" },
          { name: "next_page_token" }
        ]
      end
    },
    
    gcs_get_object: {
      title: "Get GCS object",
      subtitle: "Get object metadata and optionally content",
      
      input_fields: lambda do
        [
          { name: "bucket", optional: false },
          { name: "object_name", optional: false },
          { name: "content_mode", control_type: "select",
            pick_list: [["None", "none"], ["Text", "text"], ["Bytes", "bytes"]],
            default: "none" },
          { name: "postprocess", type: "object", properties: [
            { name: "strip_urls", type: "boolean", default: false }
          ]}
        ]
      end,
      
      execute: lambda do |connection, input|
        # Get metadata
        metadata = get("https://storage.googleapis.com/storage/v1/b/#{input["bucket"]}/o/#{input["object_name"].encode_url}")
        
        result = {
          bucket: metadata["bucket"],
          name: metadata["name"],
          size: metadata["size"]&.to_i,
          content_type: metadata["contentType"],
          updated: metadata["updated"],
          generation: metadata["generation"],
          md5_hash: metadata["md5Hash"],
          crc32c: metadata["crc32c"],
          metadata: metadata["metadata"]
        }
        
        # Handle content modes
        case input["content_mode"]
        when "text"
          if ["text/", "application/json", "application/xml", "text/csv", "image/svg+xml"].
             any? { |t| metadata["contentType"]&.include?(t) }
            content = get("https://storage.googleapis.com/storage/v1/b/#{input["bucket"]}/o/#{input["object_name"].encode_url}").
              params(alt: "media").
              response_format_raw
            
            result[:text_content] = content
            
            # Strip URLs if requested
            if input.dig("postprocess", "strip_urls")
              result[:text_content] = result[:text_content].gsub(/https?:\/\/[^\s]+/, "")
            end
          else
            error("Non-text object; use content_mode=bytes or none")
          end
          
        when "bytes"
          content = get("https://storage.googleapis.com/storage/v1/b/#{input["bucket"]}/o/#{input["object_name"].encode_url}").
            params(alt: "media").
            response_format_raw
          
          result[:content_bytes] = content.encode_base64
        end
        
        result
      end,
      
      output_fields: lambda do
        [
          { name: "bucket" },
          { name: "name" },
          { name: "size", type: "integer" },
          { name: "content_type" },
          { name: "updated" },
          { name: "generation" },
          { name: "md5_hash" },
          { name: "crc32c" },
          { name: "metadata", type: "object" },
          { name: "text_content" },
          { name: "content_bytes" }
        ]
      end
    },
    
    gcs_put_object: {
      title: "Upload to GCS",
      subtitle: "Upload content to Google Cloud Storage",
      
      input_fields: lambda do
        [
          { name: "bucket", optional: false },
          { name: "object_name", optional: false },
          { name: "content_mode", control_type: "select",
            pick_list: [["Text", "text"], ["Bytes", "bytes"]],
            optional: false },
          { name: "text_content", optional: true, hint: "Required when content_mode=text" },
          { name: "content_bytes", optional: true, hint: "Base64 encoded. Required when content_mode=bytes" },
          { name: "content_type", optional: true, hint: "MIME type" },
          { name: "custom_metadata", type: "object", optional: true },
          { name: "preconditions", type: "object", properties: [
            { name: "if_generation_match" },
            { name: "if_metageneration_match" }
          ], optional: true },
          { name: "postprocess", type: "object", properties: [
            { name: "strip_urls", type: "boolean", default: false }
          ]}
        ]
      end,
      
      execute: lambda do |connection, input|
        # Validate inputs
        if input["content_mode"] == "text" && input["text_content"].blank?
          error("text_content is required when content_mode=text")
        elsif input["content_mode"] == "bytes" && input["content_bytes"].blank?
          error("content_bytes is required when content_mode=bytes")
        end
        
        # Prepare content and determine upload method
        if input["content_mode"] == "text"
          content = input["text_content"]
          
          # Strip URLs if requested
          if input.dig("postprocess", "strip_urls")
            content = content.gsub(/https?:\/\/[^\s]+/, "")
          end
          
          mime_type = input["content_type"] || "text/plain; charset=UTF-8"
          payload_bytes = content
          
        else # bytes
          content = input["content_bytes"].decode_base64
          mime_type = input["content_type"] || "application/octet-stream"
          payload_bytes = content
        end
        
        # Build request
        url = "https://storage.googleapis.com/upload/storage/v1/b/#{input["bucket"]}/o"
        params = {}
        
        # Add preconditions
        if input.dig("preconditions", "if_generation_match").present?
          params[:ifGenerationMatch] = input["preconditions"]["if_generation_match"]
        end
        if input.dig("preconditions", "if_metageneration_match").present?
          params[:ifMetagenerationMatch] = input["preconditions"]["if_metageneration_match"]
        end
        
        # Upload based on whether we have custom metadata
        if input["custom_metadata"].present?
          # Multipart upload
          params[:uploadType] = "multipart"
          
          boundary = "==boundary=="
          metadata_json = {
            name: input["object_name"],
            contentType: mime_type,
            metadata: input["custom_metadata"]
          }.to_json
          
          multipart_body = [
            "--#{boundary}",
            "Content-Type: application/json; charset=UTF-8",
            "",
            metadata_json,
            "--#{boundary}",
            "Content-Type: #{mime_type}",
            "",
            payload_bytes,
            "--#{boundary}--"
          ].join("\r\n")
          
          response = post(url).
            params(params).
            headers("Content-Type": "multipart/related; boundary=#{boundary}").
            request_body(multipart_body)
            
        else
          # Simple media upload
          params[:uploadType] = "media"
          params[:name] = input["object_name"]
          
          response = post(url).
            params(params).
            headers("Content-Type": mime_type).
            request_body(payload_bytes)
        end
        
        {
          gcs_object: {
            bucket: response["bucket"],
            name: response["name"],
            size: response["size"]&.to_i,
            content_type: response["contentType"],
            updated: response["updated"],
            generation: response["generation"],
            md5_hash: response["md5Hash"],
            crc32c: response["crc32c"],
            metadata: response["metadata"]
          },
          bytes_uploaded: payload_bytes.bytesize
        }
      end,
      
      output_fields: lambda do
        [
          { name: "gcs_object", type: "object", properties: [
            { name: "bucket" },
            { name: "name" },
            { name: "size", type: "integer" },
            { name: "content_type" },
            { name: "updated" },
            { name: "generation" },
            { name: "md5_hash" },
            { name: "crc32c" },
            { name: "metadata", type: "object" }
          ]},
          { name: "bytes_uploaded", type: "integer" }
        ]
      end
    },
    
    transfer_drive_to_gcs: {
      title: "Transfer Drive files to GCS",
      subtitle: "Copy multiple files from Drive to Cloud Storage",
      
      input_fields: lambda do
        [
          { name: "bucket", optional: false },
          { name: "gcs_prefix", optional: true, hint: "Prefix for GCS object names" },
          { name: "drive_file_ids", type: "array", of: "string", optional: false,
            hint: "Array of Drive file IDs or URLs" },
          { name: "content_mode_for_editors", control_type: "select",
            pick_list: [["Export as text", "text"], ["Skip", "skip"]],
            default: "text" }
        ]
      end,
      
      execute: lambda do |connection, input|
        uploaded = []
        failed = []
        
        input["drive_file_ids"].each do |file_id_or_url|
          begin
            # Extract file ID
            file_id = file_id_or_url
            if file_id&.include?("/")
              file_id = file_id[/\/d\/([^\/\?]+)/, 1] || file_id[/[?&]id=([^&]+)/, 1] || file_id
            end
            
            # Get metadata
            metadata = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
              params(
                supportsAllDrives: true,
                fields: "id,name,mimeType,size,shortcutDetails"
              )
            
            # Follow shortcut if needed
            if metadata["shortcutDetails"].present?
              file_id = metadata["shortcutDetails"]["targetId"]
              metadata = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
                params(
                  supportsAllDrives: true,
                  fields: "id,name,mimeType,size"
                )
            end
            
            # Determine how to get content
            if metadata["mimeType"]&.start_with?("application/vnd.google-apps.")
              # Google Editors file
              if input["content_mode_for_editors"] == "skip"
                next # Skip this file
              end
              
              # Export to text format
              export_map = {
                "application/vnd.google-apps.document" => "text/plain",
                "application/vnd.google-apps.spreadsheet" => "text/csv", 
                "application/vnd.google-apps.presentation" => "text/plain",
                "application/vnd.google-apps.drawing" => "image/svg+xml"
              }
              
              export_mime = export_map[metadata["mimeType"]]
              if export_mime.nil?
                failed << {
                  drive_file_id: file_id,
                  error_message: "Unsupported Google Editors type",
                  error_code: 400
                }
                next
              end
              
              content = get("https://www.googleapis.com/drive/v3/files/#{file_id}/export").
                params(
                  mimeType: export_mime,
                  supportsAllDrives: true
                ).response_format_raw
              
              # Determine file extension for exported file
              ext_map = {
                "text/plain" => ".txt",
                "text/csv" => ".csv",
                "image/svg+xml" => ".svg"
              }
              file_name = metadata["name"] + ext_map[export_mime]
              content_type = export_mime
              
            else
              # Regular file - download as-is
              content = get("https://www.googleapis.com/drive/v3/files/#{file_id}").
                params(
                  alt: "media",
                  supportsAllDrives: true
                ).response_format_raw
              
              file_name = metadata["name"]
              content_type = metadata["mimeType"]
            end
            
            # Build GCS object name
            gcs_name = input["gcs_prefix"].present? ? 
              "#{input["gcs_prefix"]}#{file_name}" : file_name
            
            # Upload to GCS
            response = post("https://storage.googleapis.com/upload/storage/v1/b/#{input["bucket"]}/o").
              params(
                uploadType: "media",
                name: gcs_name
              ).
              headers("Content-Type": content_type).
              request_body(content)
            
            uploaded << {
              drive_file_id: file_id,
              gcs_object: {
                bucket: response["bucket"],
                name: response["name"],
                size: response["size"]&.to_i,
                content_type: response["contentType"]
              }
            }
            
          rescue => e
            error_code = e.respond_to?(:code) ? e.code : 500
            failed << {
              drive_file_id: file_id_or_url,
              error_message: e.message,
              error_code: error_code
            }
          end
        end
        
        {
          uploaded: uploaded,
          failed: failed,
          summary: {
            total: input["drive_file_ids"].size,
            success: uploaded.size,
            failed: failed.size
          }
        }
      end,
      
      output_fields: lambda do
        [
          { name: "uploaded", type: "array", of: "object", properties: [
            { name: "drive_file_id" },
            { name: "gcs_object", type: "object", properties: [
              { name: "bucket" },
              { name: "name" },
              { name: "size", type: "integer" },
              { name: "content_type" }
            ]}
          ]},
          { name: "failed", type: "array", of: "object", properties: [
            { name: "drive_file_id" },
            { name: "error_message" },
            { name: "error_code", type: "integer" }
          ]},
          { name: "summary", type: "object", properties: [
            { name: "total", type: "integer" },
            { name: "success", type: "integer" },
            { name: "failed", type: "integer" }
          ]}
        ]
      end
    }
  }
}
