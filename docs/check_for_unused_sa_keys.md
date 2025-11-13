# Guide: Finding Used and Unused Service Account Keys with Cloud Shell

## Why we’re doing this

Service account keys are long-lived credentials. Old or unused keys are risk magnets. Our goal:

* Enumerate all service account keys in a project.
* Determine when each key was last used.
* Identify candidates to disable/rotate/delete.

We’ll do this **entirely from Cloud Shell** using:

* `gcloud iam` to list keys.
* Policy Intelligence / Activity Analyzer for “last authentication” events. ([Google Cloud Documentation][1])
* (Optional) Cloud Monitoring metrics `iam.googleapis.com/service_account/key/authn_events_count` as a cross-check. ([P0 Security][2])

---

## 0. Setup and prerequisites

In Cloud Shell:

```bash
PROJECT_ID="your-project-id"
gcloud config set project "$PROJECT_ID"
```

Enable required APIs (once per project):

```bash
gcloud services enable \
  iam.googleapis.com \
  policyanalyzer.googleapis.com \
  monitoring.googleapis.com
```

You’ll need these IAM roles on the project:

* `roles/iam.serviceAccountViewer` – list service accounts and keys.
* `roles/policyanalyzer.activityAnalysisViewer` – query Activity Analyzer for last auth events. ([Google Cloud Documentation][1])
* `roles/monitoring.viewer` – query Monitoring metrics (optional).

---

## 1. List all service accounts and keys

### 1.1 Export all service account emails

```bash
gcloud iam service-accounts list \
  --project="$PROJECT_ID" \
  --format='value(email)' \
  > sa-emails.txt
```

### 1.2 Export all keys to CSV

This collects **all** keys (user-managed + system-managed) for each service account:

```bash
while read SA; do
  gcloud iam service-accounts keys list \
    --project="$PROJECT_ID" \
    --iam-account="$SA" \
    --format='csv[no-heading](KEY_ID,KEY_TYPE,CREATED_AT,EXPIRES_AT,DISABLED)'
done < sa-emails.txt \
| awk -F, -v OFS=',' -v proj="$PROJECT_ID" -v file="all-keys.csv" '
  NR==1 { print "PROJECT_ID","SA_EMAIL","KEY_ID","KEY_TYPE","CREATED_AT","EXPIRES_AT","DISABLED" > file }
  {
    # SA email is not in the row, so we pass via ENVIRON["SA"] is not trivial here.
    # We’ll instead re-run with SA in the loop so we can capture it.
  }
'
```

The above awk is a bit awkward, so here’s a simpler, more direct version:

```bash
> all-keys.csv
echo "PROJECT_ID,SA_EMAIL,KEY_ID,KEY_TYPE,CREATED_AT,EXPIRES_AT,DISABLED" >> all-keys.csv

while read SA; do
  gcloud iam service-accounts keys list \
    --project="$PROJECT_ID" \
    --iam-account="$SA" \
    --format='csv[no-heading](KEY_ID,KEY_TYPE,CREATED_AT,EXPIRES_AT,DISABLED)' \
  | awk -v proj="$PROJECT_ID" -v sa="$SA" -F, -v OFS=',' '
      { print proj, sa, $1, $2, $3, $4, $5 }
    ' >> all-keys.csv
done < sa-emails.txt
```

You now have `all-keys.csv` with one row per existing key.

---

## 2. Get “last used” data via Policy Intelligence

Activity Analyzer can list **last authentication times** for all service account keys in a project. ([Google Cloud Documentation][1])

```bash
gcloud policy-intelligence query-activity \
  --project="$PROJECT_ID" \
  --activity-type=serviceAccountKeyLastAuthentication \
  --limit=1000 \
  --format=json \
  > key-activity.json
```

The response contains entries with:

* `activity.lastAuthenticatedTime`
* `activity.serviceAccountKey.fullResourceName` (contains the key ID)

### 2.1 Extract KEY_ID → lastAuthenticatedTime

```bash
jq -r '
  .activities[]
  | .activity as $a
  | $a.serviceAccountKey.fullResourceName as $name
  | $a.lastAuthenticatedTime as $ts
  | ($name | split("/keys/")[1]) + "," + $ts
' key-activity.json > key-last-used.csv
```

`key-last-used.csv` will look like:

```text
KEY_ID,LAST_AUTH_TIME
1c65fca351d6abcdef,2025-10-10T13:00:00Z
...
```

Note: Only keys that have authenticated within the observation window appear here. If a key isn’t in this file, it has **no recorded authentication events in that period** (it may be unused or only used outside the data retention window).

---

## 3. Join “all keys” with “last used”

We want a single CSV with:

* Project, service account, key metadata.
* Last authentication time, or a sentinel like `no_recent_usage`.

### 3.1 Build a lookup map of last-used times

```bash
# Normalize header
sed -i '1s/^/KEY_ID,LAST_AUTH_TIME\n/' key-last-used.csv

# Build a simple map file (KEY_ID,LAST_AUTH_TIME)
cp key-last-used.csv key-last-used-map.csv
```

### 3.2 Merge on KEY_ID

```bash
awk -F, '
  NR==FNR {
    # First file: key-last-used-map.csv
    if (NR > 1) { lu[$1] = $2 }  # KEY_ID -> LAST_AUTH_TIME
    next
  }
  # Second file: all-keys.csv
  NR==1 {
    # print header + LAST_AUTH_TIME
    print $0",LAST_AUTH_TIME"
    next
  }
  {
    key_id = $3
    last_used = (key_id in lu ? lu[key_id] : "no_recent_usage")
    print $0","last_used
  }
' key-last-used-map.csv all-keys.csv > keys-with-usage.csv
```

`keys-with-usage.csv` now contains all keys plus when they were last observed authenticating.

---

## 4. Optional: cross-check a single key via Cloud Monitoring

Cloud Monitoring exposes the metric:

* `iam.googleapis.com/service_account/key/authn_events_count` – counts authentication events per key. ([P0 Security][2])

To spot-check one key:

```bash
PROJECT_ID="your-project-id"
KEY_ID="the-key-id"
START_TIME="2025-09-01T00:00:00Z"
END_TIME="2025-11-13T00:00:00Z"

ACCESS_TOKEN=$(gcloud auth print-access-token)

curl -s -X GET \
  -H "Authorization: Bearer ${ACCESS_TOKEN}" \
  -H "Content-Type: application/json" \
  "https://monitoring.googleapis.com/v3/projects/${PROJECT_ID}/timeSeries?\
filter=metric.type=\"iam.googleapis.com/service_account/key/authn_events_count\"%20AND%20metric.labels.key_id=\"${KEY_ID}\"&\
interval.startTime=${START_TIME}&interval.endTime=${END_TIME}"
```

* If you see timeSeries points: the key has auth events in that window.
* If response is empty: no auth events during that interval.

---

## 5. Interpreting results and cleanup strategy

### Things to keep in mind

* **Data retention**: Activity Analyzer and Monitoring keep data for a limited time (weeks to months). “no_recent_usage” usually means “not used within the retention window,” not “never used in history.” ([Google Cloud Documentation][1])
* **Disabled keys**: Disabled keys won’t generate new auth events, but historical events may still appear for some time. ([Google Cloud Documentation][1])
* **Usage semantics**: “Used” means “participated in an authentication event,” which includes some internal flows.

### Recommended safe cleanup flow

For each key:

1. Review `keys-with-usage.csv`:

   * Focus on keys with `LAST_AUTH_TIME = no_recent_usage`.
   * Also flag very old `CREATED_AT` dates.
2. Check with the owning team/app if the key is still needed.
3. **Stepwise remediation**:

   * **Phase 1: Disable** the key:

     ```bash
     gcloud iam service-accounts keys disable KEY_ID \
       --iam-account=SA_EMAIL \
       --project="$PROJECT_ID"
     ```
   * Wait a few days; watch for breakage.
   * **Phase 2: Delete** the key:

     ```bash
     gcloud iam service-accounts keys delete KEY_ID \
       --iam-account=SA_EMAIL \
       --project="$PROJECT_ID"
     ```
4. Long term: Prefer **Workload Identity Federation or short-lived credentials** instead of long-lived keys.

---

## 6. Quick summary for humans

* We **export** all service account keys in the project (`all-keys.csv`).
* We **query Policy Intelligence** for last authentication events per key (`key-activity.json` → `key-last-used.csv`).
* We **join** those into `keys-with-usage.csv` so every key has a `LAST_AUTH_TIME` or `no_recent_usage`.
* We **use that file** as the source of truth for reviewing, disabling, and deleting old or unused keys.

Hand this guide to anyone doing IAM hygiene and tell them this is the “vacuum cleaner” for legacy service account keys.

[1]: https://docs.cloud.google.com/policy-intelligence/docs/activity-analyzer-service-account-authentication "View recent usage for service accounts and keys"
[2]: https://www.p0.dev/blog/service-account-key-origins "Understanding Service Account Key Origins"
