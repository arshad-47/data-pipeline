#!/bin/bash
set -e

source /opt/config.env

FLINK_API="http://localhost:8081"
DOWNLOAD_DIR="/opt/flink/usrlib"

mkdir -p "$DOWNLOAD_DIR"

download_aws() {
    export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY"
    local DEST="${2:-$DOWNLOAD_DIR/$1}"

    aws s3 cp "s3://$AWS_BUCKET/$1" "$DEST" --region "$AWS_REGION"
}

download_azure() {
    URL="https://${AZURE_ACCOUNT}.blob.core.windows.net/${AZURE_CONTAINER}/$1"
    local DEST="${2:-$DOWNLOAD_DIR/$1}"

    curl -s -X GET \
        -H "Authorization: Bearer $AZURE_KEY" \
        -o "$DEST" \
        "$URL"
}

download_gcp() {
    export GOOGLE_APPLICATION_CREDENTIALS="$GCP_CREDENTIALS"
    local DEST="${2:-$DOWNLOAD_DIR/$1}"
    gsutil cp "gs://$GCP_BUCKET/$1" "$DEST"
}

download_oci() {
    local DEST="${2:-$DOWNLOAD_DIR/$1}"
    oci os object get \
      --namespace "$OCI_NAMESPACE" \
      --bucket-name "$OCI_BUCKET" \
      --name "$1" \
      --file "$DEST" \
      --region "$OCI_REGION" \
      --config-file "$OCI_CONFIG"
}

download_file() {
    case "$ENVIRONMENT" in
        aws) download_aws "$1" "$2" ;;
        azure) download_azure "$1" "$2" ;;
        gcp) download_gcp "$1" "$2" ;;
        oci) download_oci "$1" "$2" ;;
        *) echo "❌ Invalid environment"; exit 1 ;;
    esac
}

echo "🌍 Using cloud: $ENVIRONMENT"
echo "📥 Fetching jars-index.txt..."

# Download the index file depending on cloud
case "$ENVIRONMENT" in
  aws)
    export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY"
    export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY"
    aws s3 cp "s3://$AWS_BUCKET/jars-index.txt" /tmp/jars-index.txt --region "$AWS_REGION"
    ;;
  azure)
    curl -s -X GET \
      -H "Authorization: Bearer $AZURE_KEY" \
      "https://${AZURE_ACCOUNT}.blob.core.windows.net/${AZURE_CONTAINER}/jars-index.txt" \
      -o /tmp/jars-index.txt
    ;;
  gcp)
    export GOOGLE_APPLICATION_CREDENTIALS="$GCP_CREDENTIALS"
    gsutil cp "gs://$GCP_BUCKET/jars-index.txt" /tmp/jars-index.txt
    ;;
  oci)
    oci os object get \
      --namespace "$OCI_NAMESPACE" \
      --bucket-name "$OCI_BUCKET" \
      --name "jars-index.txt" \
      --file /tmp/jars-index.txt \
      --region "$OCI_REGION" \
      --config-file "$OCI_CONFIG"
    ;;
esac

echo "📃 JAR list:"
cat /tmp/jars-index.txt

echo "🚀 Submitting jobs to Flink..."

while read JAR; do
    [[ -z "$JAR" ]] && continue

    echo "⬇ Downloading: $JAR"
    download_file "$JAR"

    echo "📤 Uploading JAR to Flink..."
    UPLOAD=$(curl -s -X POST -H "Expect:" \
        -F "jarfile=@${DOWNLOAD_DIR}/${JAR}" \
        "$FLINK_API/jars/upload")

    JAR_ID=$(echo "$UPLOAD" | jq -r '.filename' | awk -F "/" '{print $NF}')
    echo "📎 Uploaded as: $JAR_ID"

    echo "▶️ Running job..."
    curl -s -X POST "$FLINK_API/jars/${JAR_ID}/run"

done < /tmp/jars-index.txt

echo "🎉 All multi-cloud jars submitted to Flink!"

##############################################################
# MULTI-CLOUD AUTO-MONITOR AND AUTO-RESTART FAILED JOBS
# Runs every 2 minutes
# Uses same ENVIRONMENT for cloud selection
##############################################################

FLINK_API="http://localhost:8081"

echo "🟢 Auto-monitor enabled for ENVIRONMENT = $ENVIRONMENT (checking every 2 minutes...)"

while true; do
    echo "⏳ Checking Flink job statuses..."

    JOBS_JSON=$(curl -s "$FLINK_API/jobs/overview")

    if [[ -z "$JOBS_JSON" ]]; then
        echo "❌ ERROR: Cannot reach JobManager API"
        sleep 120
        continue
    fi

    # FAILED jobs
    # We get the Job Name and Job ID
    FAILED_JOBS=$(echo "$JOBS_JSON" | jq -r '.jobs[] | select(.state == "FAILED") | "\(.name)|\(.jid)"')

    if [[ -z "$FAILED_JOBS" ]]; then
        echo "✅ Everything looks good. No restarts needed."
        sleep 120
        continue
    fi

    echo "$FAILED_JOBS" | while IFS="|" read -r JOB_NAME JID; do
        echo "🚨 Job '$JOB_NAME' ($JID) failed → Attempting restart…"

        # Find the JAR file that matches this Job Name
        # We assume the Job Name is a substring of the JAR name or vice versa
        # For simplicity, we look for the Job Name in the jars-index.txt
        # Or better, we look for which JAR in jars-index.txt contains the Job Name
        
        # Strategy: Iterate over known JARs and see if one matches the Job Name
        MATCHED_JAR=""
        
        # Normalize Job Name:
        # 1. Lowercase
        # 2. Remove 'streamjob' suffix
        # 3. Remove trailing 's' (to handle plural Job names like 'Observations' vs 'observation' JAR)
        CORE_NAME=$(echo "$JOB_NAME" | tr '[:upper:]' '[:lower:]' | sed 's/streamjob//g' | sed 's/s$//')
        
        echo "🔍 Looking for JAR matching core name: '$CORE_NAME' (derived from '$JOB_NAME')"

        while read JAR_ENTRY; do
             if echo "$JAR_ENTRY" | grep -iq "${CORE_NAME}"; then
                 MATCHED_JAR="$JAR_ENTRY"
                 break
             fi
        done < /tmp/jars-index.txt

        if [[ -z "$MATCHED_JAR" ]]; then
            echo "⚠️ Could not find a matching JAR for job '$JOB_NAME'. Skipping..."
            echo "📄 Available JARs in index:"
            cat /tmp/jars-index.txt
            continue
        fi

        echo "🔄 Found matching JAR: $MATCHED_JAR"
        
        LOCAL_JAR_PATH="$DOWNLOAD_DIR/$MATCHED_JAR"
        
        if [[ -f "$LOCAL_JAR_PATH" ]]; then
             echo "✅ JAR already exists locally at $LOCAL_JAR_PATH. Skipping download."
        else
             echo "🌩 Re-downloading JAR from: $ENVIRONMENT"
             download_file "$MATCHED_JAR" "$LOCAL_JAR_PATH"
             echo "📦 JAR downloaded to $LOCAL_JAR_PATH"
        fi

        echo "🚀 Uploading JAR to Flink for restart..."

        RESPONSE=$(curl -s -X POST -H "Expect:" \
            -F "jarfile=@$LOCAL_JAR_PATH" \
            "$FLINK_API/jars/upload")

        JAR_ID=$(echo "$RESPONSE" | jq -r '.filename' | awk -F"/" '{print $NF}')
        
        if [[ -z "$JAR_ID" || "$JAR_ID" == "null" ]]; then
             echo "❌ Upload failed. Response: $RESPONSE"
             continue
        fi

        echo "📌 New JAR ID: $JAR_ID"

        # Cancel the old failed job to clean up (optional, but good practice if it's still in FAILED state showing up)
        # FAILED jobs usually don't need cancelling, but we can just run the new one.

        curl -s -X POST "$FLINK_API/jars/$JAR_ID/run" > /dev/null

        echo "🎉 Job '$JOB_NAME' restarted successfully!"
        echo "-----------------------------------------------"
    done

    echo "⏱ Waiting 2 minutes..."
    sleep 120
done
