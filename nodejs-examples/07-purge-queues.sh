#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-us-west-2}"
SQS_ENDPOINT_URL="${SQS_ENDPOINT_URL:-}"
MAIN_NAME="${MAIN_NAME:-demo-orders.fifo}"
DLQ_NAME="${DLQ_NAME:-demo-orders-dlq.fifo}"
QUEUE_URL="${QUEUE_URL:-}"
DLQ_URL="${DLQ_URL:-}"

AWS_CMD=(aws)
if [[ -n "${SQS_ENDPOINT_URL}" ]]; then
  AWS_CMD+=(--endpoint-url "${SQS_ENDPOINT_URL}")
  echo "Using local SQS endpoint: ${SQS_ENDPOINT_URL}"
fi

resolve_queue_url() {
  local queue_name="$1"
  local resolved_url

  if resolved_url=$("${AWS_CMD[@]}" sqs get-queue-url \
    --region "${REGION}" \
    --queue-name "${queue_name}" \
    --query QueueUrl --output text 2>/dev/null); then
    echo "${resolved_url}"
  else
    echo ""
  fi
}

purge_queue() {
  local label="$1"
  local url="$2"

  if [[ -z "${url}" ]]; then
    echo "Skipping ${label}: queue URL not found"
    return 0
  fi

  echo "Purging ${label}: ${url}"
  set +e
  output=$("${AWS_CMD[@]}" sqs purge-queue --region "${REGION}" --queue-url "${url}" 2>&1)
  exit_code=$?
  set -e

  if [[ ${exit_code} -eq 0 ]]; then
    echo "Purge requested for ${label}."
    return 0
  fi

  if echo "${output}" | grep -qi "PurgeQueueInProgress"; then
    echo "Purge already in progress for ${label}. Wait up to 60 seconds and retry if needed."
    return 0
  fi

  echo "Failed to purge ${label}."
  echo "${output}"
  return 1
}

if [[ -z "${QUEUE_URL}" ]]; then
  QUEUE_URL="$(resolve_queue_url "${MAIN_NAME}")"
fi

if [[ -z "${DLQ_URL}" ]]; then
  DLQ_URL="$(resolve_queue_url "${DLQ_NAME}")"
fi

purge_queue "main queue" "${QUEUE_URL}"
purge_queue "DLQ" "${DLQ_URL}"

echo "Done."
echo "Note: SQS purge can take up to 60 seconds to fully remove visible messages."
