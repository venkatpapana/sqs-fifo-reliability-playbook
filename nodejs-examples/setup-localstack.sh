#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")" && pwd)"
CONTAINER_NAME="${LOCALSTACK_CONTAINER_NAME:-sqs-fifo-localstack}"
LOCALSTACK_IMAGE="${LOCALSTACK_IMAGE:-localstack/localstack:3.0}"
SQS_ENDPOINT_URL="${SQS_ENDPOINT_URL:-http://localhost:4566}"
CONTAINER_CLI="${CONTAINER_CLI:-}"

export AWS_REGION="${AWS_REGION:-us-west-2}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export SQS_ENDPOINT_URL

MAIN_NAME="${MAIN_NAME:-demo-orders.fifo}"
DLQ_NAME="${DLQ_NAME:-demo-orders-dlq.fifo}"

if [[ -z "${CONTAINER_CLI}" ]]; then
  if command -v docker >/dev/null 2>&1; then
    CONTAINER_CLI="docker"
  elif command -v podman >/dev/null 2>&1; then
    CONTAINER_CLI="podman"
  else
    echo "Neither docker nor podman was found in PATH."
    echo "Install one of them or set CONTAINER_CLI to a compatible runtime binary."
    exit 1
  fi
fi

if ! command -v "${CONTAINER_CLI}" >/dev/null 2>&1; then
  echo "Container runtime '${CONTAINER_CLI}' was not found in PATH."
  exit 1
fi

if ! command -v aws >/dev/null 2>&1; then
  echo "aws CLI is required but not found in PATH."
  exit 1
fi

if "${CONTAINER_CLI}" ps --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
  echo "LocalStack container '${CONTAINER_NAME}' is already running."
else
  if "${CONTAINER_CLI}" ps -a --format '{{.Names}}' | grep -qx "${CONTAINER_NAME}"; then
    "${CONTAINER_CLI}" rm -f "${CONTAINER_NAME}" >/dev/null
  fi

  echo "Starting LocalStack container '${CONTAINER_NAME}' with ${CONTAINER_CLI}..."
  "${CONTAINER_CLI}" run -d \
    --name "${CONTAINER_NAME}" \
    -p 4566:4566 \
    -e SERVICES=sqs \
    "${LOCALSTACK_IMAGE}" >/dev/null
fi

echo "Waiting for LocalStack SQS endpoint..."
for i in {1..30}; do
  status="$(${CONTAINER_CLI} inspect --format '{{.State.Status}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
  if [[ "${status}" == "exited" || "${status}" == "dead" ]]; then
    exit_code="$(${CONTAINER_CLI} inspect --format '{{.State.ExitCode}}' "${CONTAINER_NAME}" 2>/dev/null || true)"
    echo "Container '${CONTAINER_NAME}' exited during startup (exit code: ${exit_code})."
    echo "Recent logs:"
    "${CONTAINER_CLI}" logs "${CONTAINER_NAME}" 2>/dev/null | tail -n 40 || true
    echo
    echo "Tip: If you see a license/auth error, use an OSS image tag (default is localstack/localstack:3.0)"
    echo "or provide LOCALSTACK_AUTH_TOKEN for licensed tags."
    exit 1
  fi

  if aws --endpoint-url "${SQS_ENDPOINT_URL}" sqs list-queues --region "${AWS_REGION}" >/dev/null 2>&1; then
    break
  fi
  if [[ "$i" -eq 30 ]]; then
    echo "LocalStack did not become ready in time."
    exit 1
  fi
  sleep 1
done

echo "Creating local FIFO queues..."
(
  cd "${ROOT_DIR}"
  MAIN_NAME="${MAIN_NAME}" \
  DLQ_NAME="${DLQ_NAME}" \
  AWS_REGION="${AWS_REGION}" \
  SQS_ENDPOINT_URL="${SQS_ENDPOINT_URL}" \
  bash 01-create-fifo-queues.sh
)

QUEUE_URL="$(aws --endpoint-url "${SQS_ENDPOINT_URL}" sqs get-queue-url \
  --region "${AWS_REGION}" \
  --queue-name "${MAIN_NAME}" \
  --query QueueUrl --output text)"

echo
echo "LocalStack is ready."
echo
echo "Run these commands in your current shell:"
echo
echo "export AWS_REGION=${AWS_REGION}"
echo "export AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID}"
echo "export AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY}"
echo "export SQS_ENDPOINT_URL=${SQS_ENDPOINT_URL}"
echo "export QUEUE_URL=${QUEUE_URL}"
echo
echo "Then run Node examples:"
echo "  npm run produce"
echo "  npm run consume"
echo "  npm run heartbeat"
echo "  npm run harness"
echo
echo "Then run Java examples (from ../java-examples):"
echo "  mvn -q -DskipTests compile"
echo "  mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.FifoProducerExample"
echo "  mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.LongPollConsumerExample"
echo "  mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.VisibilityHeartbeatExample"
