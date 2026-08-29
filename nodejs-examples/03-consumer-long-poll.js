const {
  SQSClient,
  ReceiveMessageCommand,
  DeleteMessageBatchCommand
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
  } catch (err) {
    return { _rawBody: rawBody, _parseError: err.message };
  }
}

function logReceived(index, message, body) {
  const groupId = message.Attributes?.MessageGroupId || "-";
  const dedupId = message.Attributes?.MessageDeduplicationId || "-";
  const sequence = message.Attributes?.SequenceNumber || "-";
  const msgTag = body.msg || "msg#?";
  const stream = `${body.customerId || "?"}/${body.orderId || "?"}`;
  const version = body.version !== undefined ? `v${body.version}` : "v?";
  const status = body.status || "UNKNOWN";
  const eventTs = body.eventTimestamp || "-";

  console.log(
    `${paint(`[consume#${index}]`, ANSI.magenta)} ${paint(`[${msgTag}]`, ANSI.magenta)} ` +
    `${paint(stream, ANSI.green)} ${paint(version, ANSI.blue)} ${paint(status, ANSI.yellow)} ` +
    `${paint("@", ANSI.dim)} ${eventTs}`
  );
  console.log(
    `  ${paint("group", ANSI.cyan)}=${groupId} ` +
    `${paint("dedup", ANSI.green)}=${dedupId} ` +
    `${paint("seq", ANSI.blue)}=${sequence}`
  );
}

function printSummary(messages, parsedBodies) {
  const dedupSet = new Set();
  const perGroup = new Map();

  for (let i = 0; i < messages.length; i += 1) {
    const msg = messages[i];
    const body = parsedBodies[i];
    const groupId = msg.Attributes?.MessageGroupId || "-";
    const dedupId = msg.Attributes?.MessageDeduplicationId || "-";
    const msgTag = body.msg || `m${i + 1}`;
    const status = body.status || "UNKNOWN";
    const version = body.version !== undefined ? `v${body.version}` : "v?";

    dedupSet.add(dedupId);
    if (!perGroup.has(groupId)) {
      perGroup.set(groupId, []);
    }
    perGroup.get(groupId).push(`${msgTag}:${version}:${status}`);
  }

  const msgTags = parsedBodies.map((b) => b.msg).filter(Boolean);
  const hasDedupBase = msgTags.includes("msg#2");
  const hasDedupRetry = msgTags.includes("msg#5");
  let dedupResult = "not evaluated (msg#2/msg#5 not in this poll)";
  if (hasDedupBase && !hasDedupRetry) {
    dedupResult = "yes";
  } else if (hasDedupBase && hasDedupRetry) {
    dedupResult = "no (retry message also consumed)";
  }

  console.log("\n" + paint("Consume summary", ANSI.cyan));
  console.log(`- received: ${messages.length}`);
  console.log(`- unique dedup ids: ${dedupSet.size}`);
  console.log(`- dedup retry (msg#5) suppressed: ${dedupResult}`);
  console.log("- per-group order:");
  for (const [groupId, sequence] of perGroup.entries()) {
    console.log(`  ${groupId} -> ${sequence.join(" -> ")}`);
  }
}

async function pollOnce() {
  const receive = new ReceiveMessageCommand({
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 10,
    // Long polling: wait up to 20s for available messages.
    WaitTimeSeconds: 20,
    VisibilityTimeout: 90,
    AttributeNames: ["All"]
  });

  const result = await sqs.send(receive);
  const messages = result.Messages || [];

  if (messages.length === 0) {
    console.log("No messages this poll.");
    return;
  }

  const parsedBodies = messages.map((m) => parseBody(m.Body));

  for (let i = 0; i < messages.length; i += 1) {
    logReceived(i + 1, messages[i], parsedBodies[i]);
  }

  printSummary(messages, parsedBodies);

  const del = new DeleteMessageBatchCommand({
    QueueUrl: queueUrl,
    Entries: messages.map((m, i) => ({ Id: `msg-${i}`, ReceiptHandle: m.ReceiptHandle }))
  });

  await sqs.send(del);
  console.log(`Deleted ${messages.length} messages`);
}

pollOnce().catch((err) => {
  console.error(err);
  process.exit(1);
});
