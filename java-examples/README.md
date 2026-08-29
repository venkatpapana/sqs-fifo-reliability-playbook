# Java SQS FIFO examples

This folder contains Java 17 examples for the same FIFO reliability patterns used in the article.

## Prerequisites

- Java 17+
- Maven 3.9+
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

One-command bootstrap from Node examples (recommended):

```bash
cd ../nodejs-examples
npm run local:setup
```

Copy the printed `export` lines into your shell, then run Java examples from this folder.

Manual setup (if you prefer step-by-step):

Start LocalStack:

```bash
docker run --rm -it -p 4566:4566 -e SERVICES=sqs localstack/localstack:3.0
```

Or use the one-command bootstrap with Podman:

```bash
cd ../nodejs-examples
CONTAINER_CLI=podman LOCALSTACK_IMAGE=localstack/localstack:3.0 npm run local:setup
```

In another terminal, configure local env:

```bash
export AWS_REGION=us-west-2
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export SQS_ENDPOINT_URL=http://localhost:4566
```

Create local queues from nodejs examples folder:

```bash
cd ../nodejs-examples
bash 01-create-fifo-queues.sh
```

Set queue URL from LocalStack:

```bash
export QUEUE_URL=$(aws --endpoint-url "$SQS_ENDPOINT_URL" sqs get-queue-url \
	--region "$AWS_REGION" \
	--queue-name demo-orders.fifo \
	--query QueueUrl --output text)
```

Return to this folder and run Java examples:

```bash
cd ../java-examples
mvn -q -DskipTests compile
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.FifoProducerExample
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.LongPollConsumerExample
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.VisibilityHeartbeatExample
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.EndToEndHarnessExample
```

## Compile once

Run from this folder:

```bash
mvn -q -DskipTests compile
```

## Programs and what each one teaches

### 1) FifoProducerExample

Class: `com.example.sqsfifo.FifoProducerExample`

Run:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.FifoProducerExample
```

Purpose:

- Produces `ORDER_STATUS_UPDATED` events.
- Demonstrates how to build stable FIFO keys for order status streams.

What it does:

- Builds `MessageGroupId` as `customerId#orderId` to preserve in-order updates per order stream.
- Builds `MessageDeduplicationId` as `orderId#eventTimestamp` so each business event instance is unique.
- Publishes the same six-message demo sequence as Node (`msg#1` through `msg#6`), including one intentional duplicate to illustrate FIFO dedup behavior.

What to look for in output:

- Two-line colored logs matching the Node producer format, including `msg`, scenario labels, and `group` / `dedup` / `seq` values.
- Duplicate send requests are accepted by SQS, but matching dedup IDs inside 5 minutes are suppressed from delivery.

### 2) LongPollConsumerExample

Class: `com.example.sqsfifo.LongPollConsumerExample`

Run:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.LongPollConsumerExample
```

Purpose:

- Demonstrates long polling (`waitTimeSeconds=20`) and batch delete.
- Shows how FIFO attributes can be inspected during consumption.

What it does:

- Calls `ReceiveMessage` with:
	- `maxNumberOfMessages(10)`
	- `waitTimeSeconds(20)`
	- `visibilityTimeout(90)`
- Prints message metadata including `MessageGroupId`, `MessageDeduplicationId`, and `SequenceNumber`.
- Deletes all received messages in one `DeleteMessageBatch` call.

What to look for in output:

- `No messages this poll.` when queue is empty.
- Two-line colored logs matching the Node consumer format, plus the same consume summary block for dedup and per-group order.

### 3) VisibilityHeartbeatExample

Class: `com.example.sqsfifo.VisibilityHeartbeatExample`

Run:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.VisibilityHeartbeatExample
```

Purpose:

- Demonstrates the visibility-heartbeat pattern for long-running processing.
- Reduces retry noise caused by visibility timeout expiring before work is done.

What it does:

- Receives one message with `visibilityTimeout(30)`.
- Simulates long work by sleeping.
- Extends visibility three times with `ChangeMessageVisibilityRequest`.
- Deletes message after processing completes.

What to look for in output:

- The same two-line colored message format as the Node heartbeat example.
- `HEARTBEAT-START`, repeated `HEARTBEAT-EXTEND`, then `HEARTBEAT-DONE` for the same in-flight event.

Why this matters:

- Without heartbeat extension, a long-running consumer can exceed visibility timeout and the same message becomes visible again.
- That redelivery creates "random retry noise" in logs and metrics, often mistaken for transient instability.
- Heartbeats keep ownership of in-flight work explicit until delete happens.

### 4) FifoLambdaPartialFailureHandler

Class: `com.example.sqsfifo.FifoLambdaPartialFailureHandler`

Purpose:

- Reference handler for Lambda SQS FIFO partial batch failures.
- Shows how to return only failed message IDs so successful records are not retried.

How to use:

- This class is for Lambda runtime integration, not standalone local execution.
- Adapt `processOne(...)` with idempotent business logic.
- Keep the failure list contract intact:
	- success -> do nothing
	- failure -> append `new SQSBatchResponse.BatchItemFailure(messageId)`

### 5) EndToEndHarnessExample

Class: `com.example.sqsfifo.EndToEndHarnessExample`

Run:

```bash
mvn -q exec:java -Dexec.mainClass=com.example.sqsfifo.EndToEndHarnessExample
```

Purpose:

- Runs the shared end-to-end verification flow in Java.
- Confirms ordered delivery, duplicate suppression, and progress logging in one deterministic run.

What it does:

- Produces a controlled event set across two order streams.
- Intentionally sends one duplicate event.
- Consumes matching messages with long polling and verifies ordering plus dedup behavior.

What to look for in output:

- Two-line colored send/receive logs matching the Node harness format.
- `[progress]` logs while polling so waiting is visible.
- Final summary showing `received: 5`, `orderingPreserved: true`, and `dedupSuppressedDuplicate: true` for a clean run.

## Suggested execution order for learning

1. Run `FifoProducerExample` to enqueue ordered events.
2. Run `LongPollConsumerExample` to observe group ordering and message attributes.
3. Re-run producer, then use `VisibilityHeartbeatExample` to see heartbeat extensions during long processing.
4. Run `EndToEndHarnessExample` to validate the full FIFO contract end to end.
5. Use `FifoLambdaPartialFailureHandler` as the template when wiring FIFO to Lambda in production.

## Operational notes

- These are educational examples, not full production apps.
- In production, add:
	- persistent idempotency store keyed by event identity
	- structured logging and metrics
	- DLQ alarms and replay tooling
- With SQS_ENDPOINT_URL set, Java examples automatically use local static credentials and the LocalStack SQS endpoint.
