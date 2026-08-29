const { SQSClient, SendMessageCommand } = require("@aws-sdk/client-sqs");

const region = process.env.AWS_REGION || "us-west-2";
const endpoint = process.env.SQS_ENDPOINT_URL;
const queueUrl = process.env.QUEUE_URL;
const useColor = process.stdout.isTTY && process.env.NO_COLOR !== "1";

const ANSI = {
  reset: "\x1b[0m",
  dim: "\x1b[2m",
  cyan: "\x1b[36m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  blue: "\x1b[34m",
  magenta: "\x1b[35m"
};

if (!queueUrl) {
  throw new Error("Set QUEUE_URL to your FIFO queue URL");
}

const sqs = new SQSClient(endpoint ? { region, endpoint } : { region });

function paint(text, color) {
  if (!useColor) {
    return text;
  }
  return `${color}${text}${ANSI.reset}`;
}

function buildOrderStatusEvent({ msg, customerId, orderId, status, version, eventTimestamp }) {
  const ts = eventTimestamp || new Date().toISOString();

  // Keep ordering per customer order stream.
  const messageGroupId = `${customerId}#${orderId}`;

  // Dedup identifies one event instance, not the whole order.
  const messageDeduplicationId = `${orderId}#${ts}`;

  return {
    body: {
      msg,
      schemaVersion: 2,
      eventType: "ORDER_STATUS_UPDATED",
      eventId: `evt-${orderId}-${version}`,
      customerId,
      orderId,
      status,
      version,
      eventTimestamp: ts
    },
    messageGroupId,
    messageDeduplicationId
  };
}

async function send(event) {
  const cmd = new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(event.body),
    MessageGroupId: event.messageGroupId,
    MessageDeduplicationId: event.messageDeduplicationId
  });

  const res = await sqs.send(cmd);
  return {
    messageId: res.MessageId,
    sequenceNumber: res.SequenceNumber,
    groupId: event.messageGroupId,
    dedupId: event.messageDeduplicationId
  };
}

function logSend(result, event, scenario, expected) {
  const { msg, customerId, orderId, version, status, eventTimestamp } = event.body;

  console.log(
    `${paint(`[${msg}]`, ANSI.magenta)} ${paint(`[${scenario}]`, ANSI.cyan)} ` +
    `${paint(`${customerId}/${orderId}`, ANSI.green)} ` +
    `${paint(`v${version}`, ANSI.blue)} ${paint(status, ANSI.yellow)} ` +
    `${paint("@", ANSI.dim)} ${eventTimestamp}`
  );
  console.log(
    `  ${paint(expected, ANSI.yellow)} ${paint("|", ANSI.dim)} ` +
    `${paint("group", ANSI.cyan)}=${result.groupId} ` +
    `${paint("dedup", ANSI.green)}=${result.dedupId} ` +
    `${paint("seq", ANSI.blue)}=${result.sequenceNumber}`
  );
}

async function main() {
  // Same group, increasing version -> ordered stream.
  const ts1 = new Date().toISOString();
  const ts2 = new Date(Date.now() + 1_000).toISOString();
  const ts3 = new Date(Date.now() + 2_000).toISOString();
  const ts4 = new Date(Date.now() + 3_000).toISOString();
  const ts5 = new Date(Date.now() + 4_000).toISOString();

  const e1 = buildOrderStatusEvent({ msg: "msg#1", customerId: "C1001", orderId: "A100", status: "PACKED", version: 1, eventTimestamp: ts1 });
  const e2 = buildOrderStatusEvent({ msg: "msg#2", customerId: "C1001", orderId: "A100", status: "SHIPPED", version: 2, eventTimestamp: ts2 });

  // Different group can process in parallel.
  const e3 = buildOrderStatusEvent({ msg: "msg#3", customerId: "C1001", orderId: "B200", status: "PACKED", version: 1, eventTimestamp: ts3 });
  const e4 = buildOrderStatusEvent({ msg: "msg#4", customerId: "C1001", orderId: "A100", status: "DELIVERED", version: 3, eventTimestamp: ts5 });

  logSend(
    await send(e1),
    e1,
    "ORDERING-STEP-1",
    "ordered stream start"
  );
  logSend(
    await send(e2),
    e2,
    "ORDERING-STEP-2",
    "same stream, processed after step 1"
  );
  logSend(
    await send(e3),
    e3,
    "PARALLEL-GROUP",
    "different stream, can run in parallel"
  );
  logSend(
    await send(e4),
    e4,
    "ORDERING-STEP-3",
    "same stream, processed after step 2"
  );

  // Duplicate within dedup window; SQS accepts request but suppresses delivery.
  const e2Duplicate = buildOrderStatusEvent({ msg: "msg#5", customerId: "C1001", orderId: "A100", status: "SHIPPED", version: 2, eventTimestamp: ts2 });
  const e5 = buildOrderStatusEvent({ msg: "msg#6", customerId: "C1002", orderId: "A100", status: "SHIPPED", version: 1, eventTimestamp: ts4 });
  logSend(
    await send(e2Duplicate),
    e2Duplicate,
    "DEDUP-RETRY",
    "same dedup as step 2, should be suppressed"
  );
  logSend(
    await send(e5),
    e5,
    "PARALLEL-GROUP-2",
    "same orderId, different customer => separate stream"
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
