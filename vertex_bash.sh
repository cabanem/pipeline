#!/usr/bin/env bash
set -euo pipefail

# ========= CONFIG (edit these) ============================================
PROJECT_ID="${PROJECT_ID:-your-project-id}"
LOCATION="${LOCATION:-us-east4}"                         # must be regional (not 'global')
CORPUS_ID="${CORPUS_ID:-hr-kb}"                          # new or existing ragCorpora/{CORPUS_ID}
CONNECTOR_SA="${CONNECTOR_SA:-connector-sa@${PROJECT_ID}.iam.gserviceaccount.com}"

# Import source (GCS):
GCS_URIS="${GCS_URIS:-gs://your-bucket/path/**}"         # comma-separated or single pattern/URI

# Optional: write import results to GCS
IMPORT_RESULTS_PREFIX="${IMPORT_RESULTS_PREFIX:-gs://your-logs-bucket/rag-imports/}"

# Toggle ANN rebuild on first bulk import
REBUILD_ANN="${REBUILD_ANN:-true}"                       # true|false

# ========= END CONFIG =====================================================

# Minimal checks
[[ -z "$PROJECT_ID" || -z "$LOCATION" || -z "$CORPUS_ID" || -z "$CONNECTOR_SA" ]] && {
  echo "Set PROJECT_ID, LOCATION, CORPUS_ID, CONNECTOR_SA"; exit 1; }

gcloud config set project "$PROJECT_ID" >/dev/null

PROJECT_NUMBER="$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")"
RAG_AGENT_SA="service-${PROJECT_NUMBER}@gcp-sa-vertex-rag.iam.gserviceaccount.com"
CORPUS_NAME="projects/${PROJECT_ID}/locations/${LOCATION}/ragCorpora/${CORPUS_ID}"

echo "==> Project: $PROJECT_ID  (#$PROJECT_NUMBER)"
echo "==> Location: $LOCATION"
echo "==> Corpus: $CORPUS_NAME"
echo "==> Connector SA: $CONNECTOR_SA"
echo "==> RAG Data Service Agent: $RAG_AGENT_SA"
echo

# -------------------------------------------------------------------------
# 1) Enable required APIs
# -------------------------------------------------------------------------
echo "==> Enabling APIs (Vertex AI, Storage)..."
gcloud services enable aiplatform.googleapis.com storage.googleapis.com

# -------------------------------------------------------------------------
# 2) IAM: allow your connector SA to use Vertex AI, and Storage (if needed)
# -------------------------------------------------------------------------
echo "==> Granting roles to connector SA..."
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${CONNECTOR_SA}" \
  --role="roles/aiplatform.user" >/dev/null

# Optional: if your connector SA needs to read from the GCS source directly (not required for import)
# gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET \
#   --member="serviceAccount:${CONNECTOR_SA}" \
#   --role="roles/storage.objectViewer" >/dev/null || true

# -------------------------------------------------------------------------
# 3) IAM: grant the *Vertex RAG Data Service Agent* read access to your GCS data
#         (this service agent fetches the files during ragFiles:import)
# -------------------------------------------------------------------------
echo "==> Granting Storage Object Viewer to RAG Data Service Agent on source buckets..."
# Extract unique buckets from GCS_URIS and bind viewer
IFS=',' read -r -a URI_ARR <<< "$GCS_URIS"
declare -A BUCKETS=()
for u in "${URI_ARR[@]}"; do
  b="$(echo "$u" | sed -E 's#^gs://([^/]+).*$#\1#')"
  BUCKETS["$b"]=1
done
for b in "${!BUCKETS[@]}"; do
  echo "   - gs://${b}"
  gcloud storage buckets add-iam-policy-binding "gs://${b}" \
    --member="serviceAccount:${RAG_AGENT_SA}" \
    --role="roles/storage.objectViewer" >/dev/null || true
done

# Also grant the agent write to IMPORT_RESULTS_PREFIX bucket (so it can write logs), if configured
if [[ -n "$IMPORT_RESULTS_PREFIX" ]]; then
  LOG_BUCKET="$(echo "$IMPORT_RESULTS_PREFIX" | sed -E 's#^gs://([^/]+).*$#\1#')"
  echo "==> Granting Storage Object Admin to RAG agent on ${LOG_BUCKET} (for import logs)..."
  gcloud storage buckets add-iam-policy-binding "gs://${LOG_BUCKET}" \
    --member="serviceAccount:${RAG_AGENT_SA}" \
    --role="roles/storage.objectAdmin" >/dev/null || true
fi

# -------------------------------------------------------------------------
# Helper: REST call with active account token
# -------------------------------------------------------------------------
api() {
  local method="$1"; shift
  local url="$1"; shift
  local body="${1:-}"
  local token; token="$(gcloud auth print-access-token)"
  if [[ -n "$body" ]]; then
    curl -sS -X "$method" \
      -H "Authorization: Bearer ${token}" \
      -H "Content-Type: application/json" \
      --data "$body" "$url"
  else
    curl -sS -X "$method" \
      -H "Authorization: Bearer ${token}" \
      -H "Content-Type: application/json" \
      "$url"
  fi
}

# -------------------------------------------------------------------------
# 4) Create corpus if it doesn't exist
# -------------------------------------------------------------------------
echo "==> Ensuring RAG corpus exists..."
GET_URL="https://${LOCATION}-aiplatform.googleapis.com/v1/${CORPUS_NAME}"
if api GET "$GET_URL" | grep -q "\"name\": \"${CORPUS_NAME}\""; then
  echo "   - Corpus exists: ${CORPUS_NAME}"
else
  echo "   - Creating corpus..."
  CREATE_URL="https://${LOCATION}-aiplatform.googleapis.com/v1/projects/${PROJECT_ID}/locations/${LOCATION}/ragCorpora"
  BODY="$(jq -n --arg name "$CORPUS_ID" --arg dn "$CORPUS_ID" \
    '{ ragCorpus: { displayName: $dn }, ragCorpusId: $name }')"
  api POST "$CREATE_URL" "$BODY" >/dev/null
  # Poll until GET returns
  for i in {1..20}; do
    sleep 2
    if api GET "$GET_URL" | grep -q "\"name\": \"${CORPUS_NAME}\""; then
      echo "   - Created: ${CORPUS_NAME}"
      break
    fi
    [[ $i -eq 20 ]] && { echo "   ! Timed out creating corpus"; exit 1; }
  done
fi

# -------------------------------------------------------------------------
# 5) Import files from GCS (ragFiles:import)
# -------------------------------------------------------------------------
echo "==> Importing files from GCS into corpus..."
# Build payload
# Split comma-separated URIs into array
GCS_ARRAY_JSON="$(printf '%s' "$GCS_URIS" | awk -F',' 'BEGIN{printf("[" )} {for(i=1;i<=NF;i++){gsub(/^[ \t]+|[ \t]+$/,"",$i); printf("%s\"%s\"", (i>1?",":""), $i)} } END{printf("]")}')"

IMPORT_URL="https://${LOCATION}-aiplatform.googleapis.com/v1/${CORPUS_NAME}:ragFiles:import"

REBUILD_JSON="$( [[ "$REBUILD_ANN" == "true" ]] && echo "true" || echo "false" )"

if [[ -n "$IMPORT_RESULTS_PREFIX" ]]; then
  BODY="$(jq -n \
    --argjson uris "$GCS_ARRAY_JSON" \
    --arg sink "$IMPORT_RESULTS_PREFIX" \
    --argjson rebuild "$REBUILD_JSON" '
    {
      importRagFilesConfig: {
        gcsSource: { uris: ($uris|fromjson) },
        rebuildAnnIndex: ($rebuild|tostring|test("true")),
        importResultGcsSink: { outputUriPrefix: $sink }
      }
    }')"
else
  BODY="$(jq -n \
    --argjson uris "$GCS_ARRAY_JSON" \
    --argjson rebuild "$REBUILD_JSON" '
    {
      importRagFilesConfig: {
        gcsSource: { uris: ($uris|fromjson) },
        rebuildAnnIndex: ($rebuild|tostring|test("true"))
      }
    }')"
fi

LRO="$(api POST "$IMPORT_URL" "$BODY")"
NAME="$(echo "$LRO" | jq -r '.name // empty')"
[[ -z "$NAME" ]] && { echo "   ! Import request failed:"; echo "$LRO"; exit 1; }
echo "   - LRO: $NAME"

# Poll the operation
echo -n "   - Waiting for import to complete"
for i in {1..120}; do
  sleep 5
  OP="$(api GET "https://${LOCATION}-aiplatform.googleapis.com/v1/${NAME}")"
  DONE="$(echo "$OP" | jq -r '.done // false')"
  if [[ "$DONE" == "true" ]]; then
    ERR="$(echo "$OP" | jq -r '.error.message // empty')"
    echo
    if [[ -n "$ERR" && "$ERR" != "null" ]]; then
      echo "   ! Import operation error: $ERR"
      exit 1
    fi
    echo "   - Import completed."
    break
  fi
  echo -n "."
  [[ $i -eq 120 ]] && { echo; echo "   ! Timed out waiting for import"; exit 1; }
done

# -------------------------------------------------------------------------
# 6) Final echo + next steps
# -------------------------------------------------------------------------
echo
echo "==> Done."
echo "Corpus: ${CORPUS_NAME}"
echo "RAG Agent SA has read on source buckets and write on ${IMPORT_RESULTS_PREFIX:-<none>}."
echo "Next: call retrieveContexts and feed to generateContent (your connector's rag_answer)."

# ---------------------------- OPTIONAL NOTES ------------------------------
# For Google Drive import instead of GCS:
#  - Share the Drive folder/files with: ${RAG_AGENT_SA} as Viewer.
#  - Replace the import BODY above with:
#    jq -n --arg folder "DRIVE_FOLDER_ID" --argjson rebuild "$REBUILD_JSON" '{
#      importRagFilesConfig: {
#        googleDriveSource: { folderId: $folder },
#        rebuildAnnIndex: ($rebuild|tostring|test("true"))
#      }
#    }'
# -------------------------------------------------------------------------
