# Rebuild notes

## Shared rules
1. **ID/URL handling (Drive)**: if an input accepts a "File/Folder ID or URL", extract the ID from the URL (that is `/d/{id}` or `?id={id}`); else treat the string as the ID.
2. **Timestamps**: when building queries, convert `modified_*` values to ISO-8601 UTC
3. **Sizes**: Google returns sizes as strings; cast to integers in outputs
4. **Text vs binary**: treat as textual when the MIME type starts with `text/` or is one of: `application/json`, `application/xml`, `text/csv`, `image/svg+xml`; everything else is non-text.
5. **Editors export map (Drive)**:
    - Docs &rarr; `text/plain`
    - Sheets &rarr; `text/csv`
    - Slides &rarr; `text/plain`
    - Drawings &rarr; `image/svg+xml`
6. **Content bytes in JSON**: when returning or accepting raw bytes, use base64 strings
7. **Pagination flags**: `has_more = next_page_token` present
8. **Shortcuts (Drive)**: if the file is a shortcut, resolve to the target file oncce before proceeding
9. **Post-processing**: only apply `postprocess.strip_urls` to text content (never to bytes)

## Action-by-action

### 1. `drive_list_files`

#### Goal
Return a page of Drive files (newest first) with minimal metadata

#### Steps
1. Normalize inputs
  - Extract `folder_id` from URL, if provided
  - Clamp `max_results` to 1-1000 (default 100)
  - Convert `modified_after`/`modified_before` to ISO-8601

2. Build Drive query
  - Always include `trashed=false`
  - Add `'folder_id' in parents` if a folder was given
  - Add date and MIME filters if provided
  - Add `mimeType != 'application/vnd.google-apps.folder` if `exclude_folders=true`

3. Pick corpus
  - If `drive_id` is set &rarr; `corpora=drive`, `driveId=<drive_id>`
  - Else if `folder_id` present &rarr; `corpora=allDrives`
  - Else &rarr; `corpora=user`
  - Always set `supportsAllDrives=true`, `includeItemsFromAllDrives=true`, `spaces=drive`

4. Call Drive `files.list`
  - Use `page_size`, `page_token`, `orderBy=modifiedTime desc`
  - Request only fields you map (id, name, mimeType, size, modifiedTime, md5Checksum, owners)

5. Map output
  - For each file, map to `{ id, name, mime_type, size:int, modified_time, checksum, owners[] }
  - Set `count`, `has_more`, `next_page_token`
#### Notes


### 2. `drive_get_file`

#### Goal
Return Drive file metadata plus optional content based upon `content_mode`

#### Inputs

- `file_id`
- `content_mode: none|text|bytes`
- `postprocess.strip_urls?`

#### Steps
1. Extract the file ID from URL/ID; fetch metadata (files.get) with fields:
  - `id`, `name`, `mimeType`, `size`, `modifiedTime`, `md5Checksum`, `owners`, `shortcutDetails`
  - If the file represents a shortcut, follow to the target and refetch metadata
2. If `content_mode = none`
  - Return metadata only (no content fields)
3. If `content_mode = text`
  - Editors type (`application/vnd.google-apps.*`): export using mapping above; return `text_content` (UTF-8 string) and set `exported_as` to the export MIME.
  - Non-Editors
    - If MIME is textual (see rule), download with `alt=media`, decode as UTF-8 &rarr; `text_content`
    - If MIME is not textual &rarr; error 415: "Non-text file; use `content_mode=bytes` or `none`"
4. If `content_mode = bytes`
  - Editors type: error 400: “Editors files require `content_mode=text` (export).”
  - Non‑Editors: download with `alt=media`, base64‑encode &rarr; `content_bytes`.
5. Post‑process
  - If `strip_urls = true` and `text_content` exists, remove URLs.
6. Return
  - Always include canonical metadata fields.
  - Include either `text_content` or `content_bytes` (or neither for `none`).
  - Include `exported_as` only when an export was performed.

### 3. `gcs_list_objects`

#### Goal
List objects in a bucket with optional prefix/folder semantics

#### Inputs
- `bucket` (required)
- `prefix?`
- `delimiter?`
- `max_results?`
- `page_token?`
- `include_versions?`

#### Steps
1. Normalize inputs
  - Clamp `max_results` to 1–1000 (default 1000).
2. Call GCS `objects.list`
  - Send `bucket`, optional `prefix`, `delimiter`, `pageToken`, `versions`, `maxResults`.
  - Request fields: 
    - `items(bucket,name,size,contentType,updated,generation,md5Hash,crc32c,metadata),nextPageToken,prefixes`
3. Map output
  - For each item, map to `{ bucket, name, size:int, content_type, updated, generation, md5_hash, crc32c, metadata{} }`
  - Set `prefixes` from response.
  - Set `count`, `has_more`, `next_page_token`.


### 4. `gcs_get_object`

#### Goal
Return GCS object metadata plus optional content based on `content_mode`

#### Inputs
- `bucket`
- `object_name`
- `content_mode: none|text|bytes`
- `postprocess.strip_urls?`

#### Steps
1. Get metadata via objects.get (JSON). Map to canonical fields.
2. If content_mode = none
  - Return metadata only.
3. If content_mode = text
  - Check content_type from metadata:
    - If textual (rule above), fetch content with alt=media, decode as UTF‑8 → text_content.
    - If not textual → error 415: “Non‑text object; use content_mode=bytes or none.”
4. If content_mode = bytes
  - Fetch with alt=media, base64‑encode → content_bytes.
5. Post‑process
  - If strip_urls = true and text_content exists, remove URLs.
6. Return
  - Canonical metadata plus the chosen content field.

### 5. `gcs_put_object`

#### Goal
Upload new content to GCS and return the created object metadata

#### Inputs
- `bucket`
- `object_name`
- `content_mode: text|bytes`,
- `text_content?` (when text)
- `content_bytes?` (when bytes),
- `content_type?`
- `custom_metadata?`
- `preconditions?: { if_generation_match?, if_metageneration_match? }`
- `postprocess.strip_urls? `(only for text)

#### Steps
1. Validate inputs
  - If `content_mode=text`, require `text_content`.
  - If `content_mode=bytes`, require `content_bytes` (base64).
2. Prepare payload
  - For text:
    - If `strip_urls = true`, remove URLs from `text_content`.
    - Determine MIME: use `content_type` or default `text/plain; charset=UTF-8`.
      - Convert to bytes (UTF‑8).
  - For bytes:
    - Determine MIME: use `content_type` or default `application/octet-stream`.
    - Base64‑decode `content_bytes` to raw bytes.
3. Choose upload mode
  - If `custom_metadata` is present → multipart upload (metadata JSON + media).
Else → media upload (content only).
4. Apply preconditions (if provided)
  - Pass `ifGenerationMatch` / `ifMetagenerationMatch` as query params.
5. Upload via objects.insert
  - On success, map response to `{ gcs_object: <canonical>, bytes_uploaded }`.
  - `bytes_uploaded` = length of the bytes actually sent.
6. Errors
  - If preconditions fail, surface the upstream 412 as is (clear message).


### 6. `transfer_drive_to_gcs`

#### Goal
For each drive file ID, fetch it's content (export, if needed) and upload to GCS under a prefix, returning success and failures

#### Inputs
- `bucket` (required)
- `gcs_prefix?`
- `drive_file_ids[]` (IDs or URLs ok)
- `content_mode_for_editors: 'text' | 'skip'` (default is 'text')

#### Steps

1. Normalize
2. For each Drive file
  - **Fetch metadata**; if shortcut, resovle to the target
  - **Decide how to get content**
    - If Editors type:
    - If non-Editors type:
      - Download bytes with `alt=media`; MIME = the file's Drive `mimeType`

3. On any error for a file
  - Push `{ drive_file_id, error_message, error_code }` to `failed` (parse HTTP status to `error_code` when possible)

4. Return
  - `uploaded[]`
  - `failed[]`
  - `summary { total, success, failed }`

#### Notes

- The action does not add Drive IDs into the GCS object path; names are "prefix + filename". If name collisions are a concern, add a timestamp or UUID in a later enhancement or use GCS preconditions as future variant.
- Editors are never uploaded as raw bytes unless they were exported to a textual format first.
