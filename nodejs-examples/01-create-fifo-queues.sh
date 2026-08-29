#!/usr/bin/env bash
set -euo pipefail

REGION="${AWS_REGION:-us-west-2}"
MAIN_NAME="${MAIN_NAME:-demo-orders.fifo}"
DLQ_NAME="${DLQ_NAME:-demo-orders-dlq.fifo}"
SQS_ENDPOINT_URL="${SQS_ENDPOINT_URL:-}"

AWS_CMD=(aws)
if [[ -n "${SQS_ENDPOINT_URL}" ]]; then
  AWS_CMD+=(--endpoint-url "${SQS_ENDPOINT_URL}")
  echo "Using local SQS endpoint: ${SQS_ENDPOINT_URL}"
fi

echo "Creating DLQ: ${DLQ_NAME}"
"${AWS_CMD[@]}" sqs create-queue \
  --region "${REGION}" \
  --queue-name "${DLQ_NAME}" \
  --attributes "FifoQueue=true,VisibilityTimeout=90,ReceiveMessageWaitTimeSeconds=20,MessageRetentionPeriod=1209600"

DLQ_URL=$("${AWS_CMD[@]}" sqs get-queue-url --region "${REGION}" --queue-name "${DLQ_NAME}" --query QueueUrl --output text)
DLQ_ARN=$("${AWS_CMD[@]}" sqs get-queue-attributes --region "${REGION}" --queue-url "${DLQ_URL}" --attribute-names QueueArn --query Attributes.QueueArn --output text)

echo "Creating main FIFO queue: ${MAIN_NAME}"
"${AWS_CMD[@]}" sqs create-queue \
  --region "${REGION}" \
  --queue-name "${MAIN_NAME}" \
  --attributes "FifoQueue=true,ContentBasedDeduplication=true,VisibilityTimeout=90,ReceiveMessageWaitTimeSeconds=20,MessageRetentionPeriod=1209600"

MAIN_URL=$("${AWS_CMD[@]}" sqs get-queue-url --region "${REGION}" --queue-name "${MAIN_NAME}" --query QueueUrl --output text)

echo "Applying redrive policy to main queue"
ATTR_FILE="$(mktemp)"
cat > "${ATTR_FILE}" <<EOF
{"RedrivePolicy":"{\"deadLetterTargetArn\":\"${DLQ_ARN}\",\"maxReceiveCount\":\"3\"}"}
EOF
"${AWS_CMD[@]}" sqs set-queue-attributes \
  --region "${REGION}" \
  --queue-url "${MAIN_URL}" \
  --attributes "file://${ATTR_FILE}"

rm -f "${ATTR_FILE}"

echo "Done"
echo "MAIN_URL=${MAIN_URL}"
echo "DLQ_URL=${DLQ_URL}"
