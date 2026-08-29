const {
  SQSClient,
  ReceiveMessageCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageCommand
} = require("@aws-sdk/client-sqs");

const region = process.env.AWS_REGION || "us-west-2";
const endpoint = process.env.SQS_ENDPOINT_URL;
const queueUrl = process.env.QUEUE_URL;
if (!queueUrl) throw new Error("Set QUEUE_URL to your FIFO queue URL");

const sqs = new SQSClient(endpoint ? { region, endpoint } : { region });
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

function paint(text, color) {
  if (!useColor) {
    return text;
  }
  return `${color}${text}${ANSI.reset}`;
}

function parseBody(rawBody) {
  try {
    return JSON.parse(rawBody);
  } catch {
    return {};
  }
}

function logHeartbeatLine(msgTag, scenario, stream, version, status, eventTs, detail, attrs) {
  console.log(
    `${paint(`[${msgTag}]`, ANSI.magenta)} ${paint(`[${scenario}]`, ANSI.cyan)} ` +
    `${paint(stream, ANSI.green)} ${paint(version, ANSI.blue)} ${paint(status, ANSI.yellow)} ` +
    `${paint("@", ANSI.dim)} ${eventTs}`
  );
  console.log(
    `  ${paint(detail, ANSI.yellow)} ${paint("|", ANSI.dim)} ` +
    `${paint("group", ANSI.cyan)}=${attrs.groupId} ` +
    `${paint("dedup", ANSI.green)}=${attrs.dedupId} ` +
    `${paint("seq", ANSI.blue)}=${attrs.sequence}`
  );
}

async function main() {
  const recv = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 20,
      VisibilityTimeout: 30,
      AttributeNames: ["All"]
    })
  );

  const msg = (recv.Messages || [])[0];
  if (!msg) {
    console.log("No message available");
    return;
  }

  const body = parseBody(msg.Body);
  const msgTag = body.msg || "msg#?";
  const stream = `${body.customerId || "?"}/${body.orderId || "?"}`;
  const version = body.version !== undefined ? `v${body.version}` : "v?";
  const status = body.status || "PROCESSING";
  const eventTs = body.eventTimestamp || "-";
  const attrs = {
    groupId: msg.Attributes?.MessageGroupId || "-",
    dedupId: msg.Attributes?.MessageDeduplicationId || "-",
    sequence: msg.Attributes?.SequenceNumber || "-"
  };

  logHeartbeatLine(
    msgTag,
    "HEARTBEAT-START",
    stream,
    version,
    status,
    eventTs,
    `long task started (messageId=${msg.MessageId})`,
    attrs
  );

  // Heartbeat loop for long tasks.
  for (let i = 0; i < 3; i++) {
    await sleep(20_000);
    await sqs.send(
      new ChangeMessageVisibilityCommand({
        QueueUrl: queueUrl,
        ReceiptHandle: msg.ReceiptHandle,
        VisibilityTimeout: 30
      })
    );
    logHeartbeatLine(
      msgTag,
      "HEARTBEAT-EXTEND",
      stream,
      version,
      status,
      eventTs,
      `visibility extended ${i + 1} time(s)`,
      attrs
    );
  }

  await sqs.send(
    new DeleteMessageCommand({
      QueueUrl: queueUrl,
      ReceiptHandle: msg.ReceiptHandle
    })
  );

  logHeartbeatLine(
    msgTag,
    "HEARTBEAT-DONE",
    stream,
    version,
    status,
    eventTs,
    "completed and deleted",
    attrs
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
