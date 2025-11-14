#!/usr/bin/env bash
#
# sa-key-usage-audit.sh
#
# Usage:
#   ./sa-key-usage-audit.sh <PROJECT_ID> [OUTPUT_DIR]
#
# Output files (in OUTPUT_DIR, default "."):
#   - sa-emails.txt          : all service account emails in the project
#   - all-keys.csv           : all keys with metadata
#   - key-activity.json      : raw Policy Intelligence response
#   - key-last-used.csv      : KEY_ID -> LAST_AUTH_TIME
#   - keys-with-usage.csv    : joined view (keys + LAST_AUTH_TIME/no_recent_usage)
#
# Prereqs (once per project):
#   gcloud services enable iam.googleapis.com policyanalyzer.googleapis.com monitoring.googleapis.com
#   Roles: iam.serviceAccountViewer, policyanalyzer.activityAnalysisViewer (and monitoring.viewer if you want metrics)
#
# Notes:
#   - Uses Policy Intelligence activity type: serviceAccountKeyLastAuthentication
#   - key-activity.json is a top-level JSON ARRAY, so we use jq '.[] | .activity ...'

set -euo pipefail

#######################################
# Parse args
#######################################
if [[ "${1:-}" == "" ]]; then
  echo "Usage: $0 <PROJECT_ID> [OUTPUT_DIR]" >&2
  exit 1
fi

PROJECT_ID="$1"
OUTPUT_DIR="${2:-.}"

#######################################
# Basic checks
#######################################
if ! command -v gcloud >/dev/null 2>&1; then
  echo "ERROR: gcloud CLI not found in PATH" >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "ERROR: jq not found in PATH (Cloud Shell normally has this installed)" >&2
  exit 1
fi

mkdir -p "$OUTPUT_DIR"
cd "$OUTPUT_DIR"

echo "=== Service Account Key Usage Audit ==="
echo "Project: $PROJECT_ID"
echo "Output dir: $(pwd)"
echo

#######################################
# Configure project (doesn't modify anything else)
#######################################
echo "[1/4] Setting gcloud project..."
gcloud config set project "$PROJECT_ID" >/dev/null

#######################################
# List all service accounts
#######################################
echo "[2/4] Listing service accounts..."
gcloud iam service-accounts list \
  --project="$PROJECT_ID" \
  --format='value(email)' \
  > sa-emails.txt

if [[ ! -s sa-emails.txt ]]; then
  echo "No service accounts found in project $PROJECT_ID. Exiting."
  exit 0
fi

SA_COUNT=$(wc -l < sa-emails.txt | tr -d ' ')
echo "  Found $SA_COUNT service accounts."

#######################################
# List all keys for all service accounts
#######################################
echo "[3/4] Listing keys for each service account..."

# Init CSV with header
echo "PROJECT_ID,SA_EMAIL,KEY_ID,KEY_TYPE,CREATED_AT,EXPIRES_AT,DISABLED" > all-keys.csv

while read -r SA; do
  echo "  -> $SA"
  # May return 0 keys; that's fine
  gcloud iam service-accounts keys list \
    --project="$PROJECT_ID" \
    --iam-account="$SA" \
    --format='csv[no-heading](KEY_ID,KEY_TYPE,CREATED_AT,EXPIRES_AT,DISABLED)' \
  | awk -F, -v proj="$PROJECT_ID" -v sa="$SA" -v OFS=',' '
      NF {
        # NF guards against blank lines when an SA has no keys
        print proj, sa, $1, $2, $3, $4, $5
      }
    ' >> all-keys.csv
done < sa-emails.txt

KEY_ROWS=$(( $(wc -l < all-keys.csv) - 1 ))
echo "  Collected $KEY_ROWS keys (rows) into all-keys.csv."

#######################################
# Query Policy Intelligence for last key auth
#######################################
echo "[4/4] Querying Policy Intelligence for last key authentication activity..."
echo "  (activity type: serviceAccountKeyLastAuthentication)"

gcloud policy-intelligence query-activity \
  --project="$PROJECT_ID" \
  --activity-type=serviceAccountKeyLastAuthentication \
  --limit=1000 \
  --format=json \
  > key-activity.json

# Handle case where no activity is returned: key-activity.json == []
if jq -e 'length == 0' key-activity.json >/dev/null 2>&1; then
  echo "  No key authentication activity returned; marking all keys as no_recent_usage."
  echo "KEY_ID,LAST_AUTH_TIME" > key-last-used.csv
else
  {
    echo "KEY_ID,LAST_AUTH_TIME"
    jq -r '
      .[]                                  # top-level array
      | .activity as $a
      | $a.serviceAccountKey.fullResourceName as $name
      | $a.lastAuthenticatedTime as $ts
      | ($name | split("/keys/")[1]) + "," + $ts
    ' key-activity.json
  } > key-last-used.csv
fi

#######################################
# Join: all-keys.csv + key-last-used.csv -> keys-with-usage.csv
#######################################
echo
echo "Joining key metadata with last-used timestamps..."

awk -F, '
  NR==FNR {
    # First file: key-last-used.csv
    if (NR == 1) next         # skip header
    lu[$1] = $2               # KEY_ID -> LAST_AUTH_TIME
    next
  }
  FNR==1 {
    # Second file: all-keys.csv header
    print $0",LAST_AUTH_TIME"
    next
  }
  {
    key_id = $3               # PROJECT_ID,SA_EMAIL,KEY_ID,...
    last_used = (key_id in lu ? lu[key_id] : "no_recent_usage")
    print $0","last_used
  }
' key-last-used.csv all-keys.csv > keys-with-usage.csv

#######################################
# Summary
#######################################
echo
echo "=== Done ==="
echo "Generated files in: $(pwd)"
echo "  - sa-emails.txt"
echo "  - all-keys.csv"
echo "  - key-activity.json"
echo "  - key-last-used.csv"
echo "  - keys-with-usage.csv"
echo
echo "Open keys-with-usage.csv in Sheets/Excel and filter on:"
echo "  LAST_AUTH_TIME = no_recent_usage"
echo "to find candidate keys to disable/rotate/delete."
