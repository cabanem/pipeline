Here’s a tight, do-this-next plan to get Drive/GCS permissions sane and prep the next connector iteration (Drive/GCS + Vertex). It’s split into (A) preflight fixes/checks, (B) connector changes, (C) minimal test matrix, and (D) a one-week iteration plan with clear acceptance criteria.

# A) Preflight: make the service account actually powerful enough

### 1) Identify the exact SA

* **Service account (SA) email:** `your-sa@YOUR_PROJECT.iam.gserviceaccount.com`
* Use only this SA in every step below. Consistency kills 403s.

### 2) Project-level roles (enable token + basic introspection)

Grant on **the GCP project that issues tokens**:

* `roles/iam.serviceAccountTokenCreator` (only if another SA needs to mint tokens on behalf of this one; otherwise skip)
* `roles/serviceusage.serviceUsageConsumer`
* `roles/iam.securityReviewer` (optional, helps debug “what do I have?”; drop later)

### 3) GCS permissions (bucket-scoped, not just project)

On **each** bucket you’ll touch:

* Prefer **UBLA** (Uniform Bucket-Level Access) ON.
* Grant SA:

  * **Write path:** `roles/storage.objectAdmin` on the bucket
  * **Read-only path:** `roles/storage.objectViewer` on the bucket
  * If you list buckets themselves: also `roles/storage.legacyBucketReader` or just `roles/storage.admin` on specific buckets if you want to avoid legacy roles.
* **Requester pays?**

  * Enable billing on the project.
  * Pass `userProject=<BILLING_PROJECT_ID>` on every JSON API call.
  * SA needs `serviceusage.serviceUsageConsumer` on the **billing** project.

### 4) Drive permissions (no domain-wide delegation)

Pick one of these patterns and stick to it:

**A. My Drive folder share (simple)**

* Create a top-level folder `connector-canary`.
* Share **that folder** with the SA as **Editor**.
* Put all test files inside this folder (or subfolders).

**B. Shared Drive (org-grade)**

* Add SA to the **Shared Drive** as **Content manager** (or higher).
* Ensure actions set `supportsAllDrives=true` and `includeItemsFromAllDrives=true`.

> Google-native files (Docs/Sheets) **must be exported** when downloading (use `files.export` with a concrete `mimeType`, not `files.get?...alt=media`).

### 5) Verify with commands (fast confidence)

```bash
# 0) Activate SA locally (only for verification; Workato will use JWT)
gcloud auth activate-service-account your-sa@YOUR_PROJECT.iam.gserviceaccount.com --key-file=sa.json
gcloud config set project YOUR_PROJECT

# 1) Can I see my bucket?
gsutil ls -p YOUR_PROJECT
gsutil ls gs://RAG_CANARY_BUCKET/

# 2) Can I write?
echo "probe" | gsutil cp - gs://RAG_CANARY_BUCKET/probes/$(date +%s).txt
gsutil rm gs://RAG_CANARY_BUCKET/probes/*.txt

# 3) Drive: can I see the shared folder?
# Get folder ID from Drive UI, then:
# List files within folder (replace FOLDER_ID)
curl -H "Authorization: Bearer $(gcloud auth print-access-token)" \
"https://www.googleapis.com/drive/v3/files?q='FOLDER_ID'+in+parents&supportsAllDrives=true&includeItemsFromAllDrives=true&fields=files(id,name,mimeType,driveId)"
```

If any of those fail, fix IAM/sharing **before** touching connector code.

---

# B) Connector iteration: high-impact changes

### 1) Centralize auth + scopes

* Make a single helper `auth.build_access_token!(connection, scopes:)` that:

  * Accepts a **scope set** (merge of Drive/GCS needs).
  * Issues JWT → exchanges at `https://oauth2.googleapis.com/token`.
  * Caches token until `exp-60s`.
* **Scopes**: default to superset; allow action-specific overrides:

  * Drive: `https://www.googleapis.com/auth/drive`
  * GCS: `https://www.googleapis.com/auth/devstorage.read_write`
  * Optional “safer” variants for read-only actions: `drive.readonly`, `devstorage.read_only`

### 2) Add a “permission probe” action (saves hours)

Create a lightweight action that returns:

* `whoami`: token’s `email` (from tokeninfo)
* `drive_access`: list of first 3 files in the configured canary folder / shared drive
* `gcs_access`: list of first 3 objects under `gs://<bucket>/probes/` (use `userProject` if set)
* `requester_pays_detected`: boolean from a 403 body match + retry success using `userProject`
* `supportsAllDrives` & `includeItemsFromAllDrives` values used

Use this as your first step in every recipe and in **connection test**.

### 3) Drive download correctness

* If `mimeType` **starts with** `application/vnd.google-apps` → use **`files.export`** with explicit export type:

  * Docs → `application/pdf` or `application/vnd.openxmlformats-officedocument.wordprocessingml.document`
  * Sheets → `text/csv` (per sheet) or `application/vnd.openxmlformats-officedocument.spreadsheetml.sheet`
* For binary/non-Google files → `files.get?alt=media`
* Always send `supportsAllDrives=true&includeItemsFromAllDrives=true`

### 4) GCS requester-pays safety

* If `connection.user_project` is set, append `userProject=<id>` to **every** JSON API call.
* Normalize 403s that mention “requester pays” with a clear hint in errors.

### 5) Connection test that actually catches 403s

* Step 1: Mint token; call `https://oauth2.googleapis.com/tokeninfo` (or introspect locally).
* Step 2: If `canary_bucket` is set, call `buckets.get` with `userProject` if present.
* Step 3: If `canary_drive_folder_id` is set, call a tiny Drive list using `supportsAllDrives=true`.
* Bubble a **single normalized error** that includes:

  * `auth_type`, `scopes`, `has_user_project`, `supportsAllDrives`, and friendly “known causes” link.

### 6) Vertex connector parity

* Ensure **same token helper** (shared module or duplicated carefully).
* Scopes: either `https://www.googleapis.com/auth/cloud-platform` or the specific Vertex/scoped set.
* Regional endpoints: `https://{REGION}-aiplatform.googleapis.com`
* Add a “model discovery probe” (lists top 5 models from publishers/models.list) — same telemetry, same token cache.

### 7) Telemetry envelope (keep it lean)

* `correlation_id`, `duration_ms`, `endpoint`, `status`, `hint` (if known 4xx class), and `user_project` flag.
* Never dump raw bodies unless `verbose_errors` true.

---

# C) Minimal test matrix (15 minutes to run)

**Auth/Token**

* [ ] Acquire token once; reuse for 50 minutes.
* [ ] Invalidate and confirm silent refresh on 401.

**GCS**

* [ ] `buckets.get` on canary (expect 200).
* [ ] `objects.list` with and without `userProject` (simulate requester-pays).
* [ ] Upload/download/delete `probes/<ts>.txt`.

**Drive**

* [ ] List files in `connector-canary` with `supportsAllDrives=true`.
* [ ] Download a binary (PDF) via `files.get?alt=media`.
* [ ] Export a Google Doc via `files.export` to DOCX.
* [ ] 404 path: request a file the SA cannot see → verify normalized error + hint.

**Vertex**

* [ ] List models (model garden).
* [ ] Simple `text.generate` or `responses:generate` call (depending on your chosen API), verify region routing.

---

# D) One-week iteration plan (with acceptance criteria)

### Day 1 — IAM & Sharing Lockdown

* Apply roles from **A2–A4** and set UBLA on canary bucket.
* Share Drive **folder or Shared Drive** with SA.
* **AC:** `gsutil cp` works; Drive list works with SA token.

### Day 2 — Connector Auth Core

* Implement `auth.build_access_token!(scopes:)` + token cache.
* Wire `userProject` propagation in all GCS calls.
* **AC:** Single token minted per hour across multiple actions; no “unregistered caller”.

### Day 3 — Permission Probe + Connection Test

* Add `permission_probe` action and use it in connection `test`.
* Normalize common 403s (requester pays, missing Drive share).
* **AC:** Test clearly says what’s wrong and how to fix, not just “403”.

### Day 4 — Drive Download/Export Correctness

* Implement Google-native export vs non-native alt=media logic.
* Force `supportsAllDrives/includeItemsFromAllDrives`.
* **AC:** Download of PDF + export of a Google Doc both succeed.

### Day 5 — Vertex Parity + Telemetry Trim

* Reuse token helper in Vertex connector; regionalize endpoints.
* Add model list probe.
* **AC:** Vertex calls succeed with same token pathway; correlation IDs appear in logs.

### Day 6 — Test Matrix Run + Fixes

* Run the matrix in section C; fix any regressions quickly.
* **AC:** All checks green, no 403s without a precise hint.

### Day 7 — Hardening & Docs

* Short README: “How to share Drive/Shared Drive,” “GCS requester-pays,” “Scopes,” “Common 403 table.”
* **AC:** A new teammate can connect in <15 minutes using the doc.

---

## Drop-in snippets (you can adapt as diffs)

### Token helper (Ruby pseudo-DSL inside connector)

```ruby
object_definitions['auth_token'] = { fields: [{ name: 'access_token' }, { name: 'expires_at' }] }

methods: {
  auth: {
    build_access_token!: lambda do |connection, scopes:|
      now = Time.now.to_i
      cache = connection[:_token_cache] || {}
      if cache['access_token'] && cache['expires_at'].to_i > now + 60
        next cache
      end

      sa = JSON.parse(connection['service_account_key_json'])
      payload = {
        iss: sa['client_email'],
        scope: Array(scopes).uniq.join(' '),
        aud: sa['token_uri'] || 'https://oauth2.googleapis.com/token',
        iat: now,
        exp: now + 3600
      }
      jwt = call(:jwt_sign_rs256, payload, sa['private_key'], sa['private_key_id'])

      resp = post('https://oauth2.googleapis.com/token')
               .payload(grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: jwt)
               .request_format_www_form_urlencoded
      token = resp['access_token']
      exp   = now + resp['expires_in'].to_i

      connection[:_token_cache] = { 'access_token' => token, 'expires_at' => exp }
      connection[:_token_cache]
    end
  }
}
```

### Applying `userProject` to GCS calls

```ruby
def gcs_params(connection)
  p = {}
  p[:userProject] = connection['user_project'] if connection['user_project'].present?
  p
end
```

### Drive list with All Drives flags

```ruby
get("https://www.googleapis.com/drive/v3/files")
  .params({
    q: "'#{input['folder_id']}' in parents",
    fields: 'files(id,name,mimeType,driveId)',
    supportsAllDrives: true,
    includeItemsFromAllDrives: true
  })
```

### Export Google Docs vs download binary

```ruby
if file['mimeType'].start_with?('application/vnd.google-apps')
  get("https://www.googleapis.com/drive/v3/files/#{file_id}/export")
    .params(mimeType: export_mime) # e.g., application/pdf
else
  get("https://www.googleapis.com/drive/v3/files/#{file_id}")
    .params(alt: 'media', supportsAllDrives: true, includeItemsFromAllDrives: true)
end
```

---

