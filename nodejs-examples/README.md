# Node.js SQS FIFO examples

This folder contains Node.js examples for the same FIFO reliability patterns used in the article.

## Prerequisites

- Node.js 20+
- AWS credentials configured in your shell

For fully local offline mode (no AWS account dependency):

- Docker
- AWS CLI v2
- LocalStack container

Required environment variables:

- AWS_REGION (example: us-west-2)
- QUEUE_URL (FIFO queue URL)

Optional environment variables:

- SQS_ENDPOINT_URL (example: http://localhost:4566 for LocalStack)

## Fully local mode with LocalStack

One-command bootstrap (recommended):

```bash
npm run local:setup
```

This command:

- starts LocalStack SQS in Docker (or reuses running container)
- creates FIFO queue + DLQ
- prints `export` commands for your current shell
- prints ready-to-run Node and Java commands

After `npm run local:setup`, copy the printed `export` lines, then run examples.

Manual setup (if you prefer step-by-step):

Start LocalStack:

```bash
docker run --rm -it -p 4566:4566 -e SERVICES=sqs localstack/localstack:3.0
```

If you use Podman or want to override image/runtime in one command:

```bash
CONTAINER_CLI=podman LOCALSTACK_IMAGE=localstack/localstack:3.0 npm run local:setup
```

In another terminal, configure local env:

```bash
export AWS_REGION=us-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export SQS_ENDPOINT_URL=http://localhost:4566
```

Create local queues:

```bash
bash 01-create-fifo-queues.sh
```

Set queue URL from LocalStack:

```bash
export QUEUE_URL=$(aws --endpoint-url "$SQS_ENDPOINT_URL" sqs get-queue-url \
	--region "$AWS_REGION" \
	--queue-name demo-orders.fifo \
	--query QueueUrl --output text)
```

Then run the examples normally:

```bash
npm run purge
npm run produce
npm run consume
npm run heartbeat
npm run harness
```

## Install once

Run from this folder:

```bash
npm install
```

## Programs and what each one teaches

### 1) 01-create-fifo-queues.sh

Run:

```bash
bash 01-create-fifo-queues.sh
```

Purpose:

- Creates a FIFO queue and corresponding DLQ with sensible defaults for demos.

What it does:

- Creates DLQ first, then main queue.
- Configures `.fifo` naming, long polling defaults, and redrive policy wiring.

When to use:

- First-time setup in a sandbox account.
- Quick reset before running producer/consumer demos.

### 2) 02-producer.js

Run:

```bash
npm run produce
```

Purpose:

- Produces `ORDER_STATUS_UPDATED` events with explicit group and dedup IDs.

What it does:

- Builds `MessageGroupId` as `customerId#orderId`.
- Builds `MessageDeduplicationId` as `orderId#eventTimestamp`.
- Sends normal events plus one intentional duplicate (same dedup ID) to demonstrate suppression.

What to look for in output:

- Logs include `groupId`, `dedupId`, and `sequenceNumber`.
- Duplicate send is accepted by SQS, but not delivered again within the dedup window.

### 3) 03-consumer-long-poll.js

Run:

```bash
npm run consume
```

Purpose:

- Demonstrates long polling and batch delete behavior.

What it does:

- Calls `ReceiveMessage` with `WaitTimeSeconds=20` and `VisibilityTimeout=90`.
- Prints payload plus FIFO attributes (`MessageGroupId`, `MessageDeduplicationId`, `SequenceNumber`).
- Deletes consumed messages in one batch API call.

What to look for in output:

- `No messages this poll.` when queue is empty.
- Ordered sequence progression inside each message group.

### 4) 04-lambda-fifo-handler.js

Run (syntax/shape validation):

```bash
node --check 04-lambda-fifo-handler.js
```

Purpose:

- Reference Lambda handler for FIFO partial batch failure response.

What it does:

- Processes each record independently.
- Returns only failed record IDs in `batchItemFailures`.
- Ensures successful records are not retried due to one failing record.

How to use:

- Deploy as Lambda handler with SQS event source mapping.
- Replace `fakeHttpCall` with real idempotent business logic.

### 5) 05-visibility-heartbeat.js

Run:

```bash
npm run heartbeat
```

Purpose:

- Demonstrates visibility-heartbeat extension for long-running processing.

What it does:

- Receives one message with an initial visibility timeout.
- Sleeps to simulate long work.
- Repeatedly extends visibility via `ChangeMessageVisibility`.
- Deletes message after processing completes.

Why this matters:

- Prevents premature redelivery while processing is still in flight.
- Reduces retry noise and duplicate downstream side effects.

### 6) 06-end-to-end-harness.js

Run:

```bash
npm run harness
```

Purpose:

- End-to-end verification harness for ordering and dedup behavior.

What it does:

- Produces a controlled event set across multiple order streams.
- Intentionally sends one duplicate event.
- Consumes events with long polling.
- Verifies:
	- per-order stream ordering is preserved
	- duplicate event was suppressed

Exit behavior:

- Exit code `0`: ordering and dedup checks passed.
- Exit code `2`: one or more verification checks failed.

### 7) 07-purge-queues.sh

Run:

```bash
npm run purge
```

Purpose:

- Clears messages from both main FIFO queue and DLQ for repeatable demo runs.

What it does:

- Purges the main queue and DLQ using current shell env (`QUEUE_URL` / `DLQ_URL`) when set.
- If queue URLs are not set, resolves URLs by queue names (`MAIN_NAME` / `DLQ_NAME`).
- Supports local endpoint mode via `SQS_ENDPOINT_URL`.

Notes:

- SQS purge can take up to 60 seconds to fully clear visible messages.
- SQS may return `PurgeQueueInProgress` if called again within that window.

## Suggested execution order for learning

1. Run `01-create-fifo-queues.sh` (optional if queue already exists).
2. Run `npm run purge` for a clean queue baseline.
3. Run `npm run produce` to enqueue known events.
4. Run `npm run consume` to inspect ordering and message attributes.
5. Re-run producer, then run `npm run heartbeat` to observe visibility extension.
6. Run `npm run harness` for deterministic end-to-end verification.

## Operational notes

- These are educational examples, not full production services.
- In production, add:
	- persistent idempotency store keyed by event identity
	- structured logging and metrics
	- DLQ alarms and replay tooling
- With LocalStack mode enabled, scripts use the local SQS endpoint and never call AWS SQS.

## Java examples

See ../java-examples/README.md for Java 17 + Maven instructions.
