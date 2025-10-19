#!/usr/bin/env bash
set -euo pipefail

PROJECT_ID="${1:?PROJECT_ID}"
REGION="${2:?REGION}"
INPUT_BUCKET="${3:?INPUT_BUCKET (name only)}"
OUTPUT_BUCKET="${4:?OUTPUT_BUCKET (name only)}"

# Comma-separated list of SA emails optional (else we list all)
CANDIDATES="${5:-}"

req_in_roles=("roles/storage.objectViewer" "roles/storage.objectAdmin" "roles/storage.admin" "roles/editor" "roles/owner")
req_out_roles=("roles/storage.objectCreator" "roles/storage.objectAdmin" "roles/storage.admin" "roles/editor" "roles/owner")

acct="$(gcloud auth list --filter=status:ACTIVE --format='value(account)')"
[[ -z "$acct" ]] && { echo "No active gcloud account"; exit 1; }

if [[ -z "$CANDIDATES" ]]; then
  mapfile -t SAs < <(gcloud iam service-accounts list --project "$PROJECT_ID" --format='value(email)')
else
  IFS=',' read -r -a SAs <<< "$CANDIDATES"
fi

in_json="$(gcloud storage buckets get-iam-policy "gs://${INPUT_BUCKET}" --format=json || echo '{}')"
out_json="$(gcloud storage buckets get-iam-policy "gs://${OUTPUT_BUCKET}" --format=json || echo '{}')"

csv="results.csv"; echo "sa_email,can_act_as,input_access,output_access,notes" > "$csv"
ndjson="results.ndjson"; : > "$ndjson"

has_any_role() {
  local json="$1" sa="$2"; shift 2
  local -a roles=("$@")
  for role in "${roles[@]}"; do
    if jq -e --arg r "$role" --arg sa "$sa" \
      '.bindings[]? | select(.role==$r) | .members[]? | select(.=="serviceAccount:"+$sa)' \
      <<<"$json" >/dev/null; then
      return 0
    fi
  done
  return 1
}

can_act_as() {
  local sapol="$1" principal="$2"
  # exact user or SA match
  if jq -e --arg p "user:$principal" \
        '.bindings[]? | select(.role=="roles/iam.serviceAccountUser" or .role=="roles/owner" or .role=="roles/editor") | .members[]? | select(.==$p)' \
        <<<"$sapol" >/dev/null; then
    echo "true"; return
  fi
  # maybe via group
  if jq -e \
        '.bindings[]? | select(.role=="roles/iam.serviceAccountUser" or .role=="roles/owner" or .role=="roles/editor") | any(.members[]?; startswith("group:"))' \
        <<<"$sapol" >/dev/null; then
    echo "maybe"; return
  fi
  echo "false"
}

for sa in "${SAs[@]}"; do
  [[ -z "$sa" ]] && continue
  sapol="$(gcloud iam service-accounts get-iam-policy "$sa" --format=json || echo '{}')"
  actas="$(can_act_as "$sapol" "$acct")"
  in_ok=false; out_ok=false
  has_any_role "$in_json"  "$sa" "${req_in_roles[@]}"  && in_ok=true
  has_any_role "$out_json" "$sa" "${req_out_roles[@]}" && out_ok=true

  notes=()
  [[ "$actas" == "maybe" ]] && notes+=("actAs:maybe(group)")
  [[ "$in_ok" == false ]] && notes+=("no_input_role")
  [[ "$out_ok" == false ]] && notes+=("no_output_role")

  printf '%s,%s,%s,%s,"%s"\n' "$sa" "$actas" "$in_ok" "$out_ok" "$(IFS=\|; echo "${notes[*]}")" >> "$csv"

  jq -n --arg sa "$sa" --arg act "$actas" --argjson in $([[ $in_ok == true ]] && echo true || echo false) \
        --argjson out $([[ $out_ok == true ]] && echo true || echo false) \
        --arg acct "$acct" --arg proj "$PROJECT_ID" --arg ts "$(date -u +%FT%TZ)" \
        '{sa_email:$sa, can_act_as:$act, input_access:$in, output_access:$out, account:$acct, project_id:$proj, checked_at:$ts}' \
        >> "$ndjson"
done

echo "Wrote $csv"
echo "Wrote $ndjson"
echo
echo "Top candidates:"
awk -F, '$2 ~ /True|true/ && $3 ~ /True|true/ && $4 ~ /True|true/ {print $1}' "$csv"
