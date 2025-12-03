#!/bin/bash
set -e

# Load environment config
source ./config.env

#JOB_DIRS=(
#  #"/home/user-1/Documents/data-pipeline/stream-jobs/"
#  "/home/user-1/Documents/data-pipeline/metabase-jobs/"
#)


INDEX_FILE="/tmp/jars-index.txt"
echo "" > "$INDEX_FILE"

upload_aws() {
  export AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY"
  export AWS_SECRET_ACCESS_KEY="$AWS_SECRET_KEY"

  echo "🌩 Uploading to AWS S3..."
  aws s3 cp "$1" "s3://$AWS_BUCKET/$2" --region "$AWS_REGION"
}

upload_azure() {
  echo "🔵 Uploading to Azure Blob..."
  AZURE_URL="https://${AZURE_ACCOUNT}.blob.core.windows.net/${AZURE_CONTAINER}/$2"

  curl -X PUT -T "$1" \
    -H "x-ms-blob-type: BlockBlob" \
    -H "x-ms-version: 2020-04-08" \
    -H "Authorization: Bearer $AZURE_KEY" \
    "$AZURE_URL"
}

upload_gcp() {
  echo "🟢 Uploading to GCP..."

  export GOOGLE_APPLICATION_CREDENTIALS="$GCP_CREDENTIALS"
  gsutil cp "$1" "gs://$GCP_BUCKET/$2"
}

upload_oci() {
  echo "🟠 Uploading to OCI..."

  oci os object put \
    --namespace "$OCI_NAMESPACE" \
    --bucket-name "$OCI_BUCKET" \
    --name "$2" \
    --file "$1" \
    --region "$OCI_REGION" \
    --config-file "$OCI_CONFIG"
}

upload_file() {
  case "$ENVIRONMENT" in
    aws) upload_aws "$1" "$2" ;;
    azure) upload_azure "$1" "$2" ;;
    gcp) upload_gcp "$1" "$2" ;;
    oci) upload_oci "$1" "$2" ;;
    *) echo "❌ Unknown environment"; exit 1 ;;
  esac
}

echo "🌍 Selected cloud: $ENVIRONMENT"

for DIR in "${JOB_DIRS[@]}"; do
  find "$JAR" -name "*.jar" \
    ! -name "*original*" \
  | while read JAR; do

    BASE=$(basename "$JAR" .jar)
    TS=$(date +"%Y%m%d%H%M%S")
    RV="$GITHUB_RELEASE_VERSION"
    REMOTE="${BASE}-${RV}.jar"

    echo "⬆ Uploading $JAR → $REMOTE"
    upload_file "$JAR" "$REMOTE"

    echo "$REMOTE" >> "$INDEX_FILE"
  done
done

# Upload index file
case "$ENVIRONMENT" in
  aws) aws s3 cp "$INDEX_FILE" "s3://$AWS_BUCKET/jars-index.txt" --region "$AWS_REGION" ;;
  azure) upload_azure "$INDEX_FILE" "jars-index.txt" ;;
  gcp) gsutil cp "$INDEX_FILE" "gs://$GCP_BUCKET/jars-index.txt" ;;
  oci)
    oci os object put \
      --namespace "$OCI_NAMESPACE" \
      --bucket-name "$OCI_BUCKET" \
      --name "jars-index.txt" \
      --file "$INDEX_FILE"
    ;;
esac

echo "🎉 Multi-cloud upload complete!"
