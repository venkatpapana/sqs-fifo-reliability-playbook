const {
  SQSClient,
  SendMessageCommand,
  ReceiveMessageCommand,
  DeleteMessageBatchCommand,
  ChangeMessageVisibilityBatchCommand
} = require("@aws-sdk/client-sqs");

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
  throw new Error("Set QUEUE_URL to a dedicated FIFO demo queue URL");
}

const sqs = new SQSClient(endpoint ? { region, endpoint } : { region });

function paint(text, color) {
  if (!useColor) {
    return text;
  }
  return `${color}${text}${ANSI.reset}`;
}

function logFifoLine(event, scenario, detail, meta) {
  const msgTag = event.msg || "msg#?";
  const stream = `${event.customerId}/${event.orderId}`;
  const version = `v${event.version}`;

  console.log(
    `${paint(`[${msgTag}]`, ANSI.magenta)} ${paint(`[${scenario}]`, ANSI.cyan)} ` +
    `${paint(stream, ANSI.green)} ${paint(version, ANSI.blue)} ${paint(event.status, ANSI.yellow)} ` +
    `${paint("@", ANSI.dim)} ${event.eventTimestamp}`
  );
  console.log(
    `  ${paint(detail, ANSI.yellow)} ${paint("|", ANSI.dim)} ` +
    `${paint("group", ANSI.cyan)}=${meta.groupId} ` +
    `${paint("dedup", ANSI.green)}=${meta.dedupId} ` +
    `${paint("seq", ANSI.blue)}=${meta.sequence}`
  );
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function sendEvent({ msg, scenario, runId, customerId, orderId, status, version, eventTimestamp }) {
  const streamId = `${customerId}#${orderId}`;
  const groupId = `${runId}#${streamId}`;
  const dedupId = `${runId}#${orderId}#${eventTimestamp}`;

  const body = {
    msg,
    scenario,
    schemaVersion: 2,
    eventType: "ORDER_STATUS_UPDATED",
    runId,
    streamId,
    eventId: `${runId}-${orderId}-v${version}`,
    customerId,
    orderId,
    status,
    version,
    eventTimestamp
  };

  const cmd = new SendMessageCommand({
    QueueUrl: queueUrl,
    MessageBody: JSON.stringify(body),
    MessageGroupId: groupId,
    MessageDeduplicationId: dedupId
  });

  const res = await sqs.send(cmd);
  logFifoLine(
    body,
    scenario || "HARNESS-SEND",
    "published by harness",
    {
      groupId,
      dedupId,
      sequence: res.SequenceNumber
    }
  );
}

async function produceTraffic(runId) {
  const t1 = new Date().toISOString();
  const t2 = new Date(Date.now() + 1_000).toISOString();
  const t3 = new Date(Date.now() + 2_000).toISOString();
  const t4 = new Date(Date.now() + 3_000).toISOString();
  const t5 = new Date(Date.now() + 4_000).toISOString();

  const events = [
    { msg: "msg#1", scenario: "ORDERING-STEP-1", customerId: "C1001", orderId: "A100", status: "PACKED", version: 1, eventTimestamp: t1 },
    { msg: "msg#2", scenario: "PARALLEL-GROUP", customerId: "C1001", orderId: "B200", status: "PACKED", version: 1, eventTimestamp: t2 },
    { msg: "msg#3", scenario: "ORDERING-STEP-2", customerId: "C1001", orderId: "A100", status: "SHIPPED", version: 2, eventTimestamp: t3 },
    { msg: "msg#4", scenario: "DEDUP-RETRY", customerId: "C1001", orderId: "A100", status: "SHIPPED", version: 2, eventTimestamp: t3 },
    { msg: "msg#5", scenario: "PARALLEL-GROUP-2", customerId: "C1001", orderId: "B200", status: "SHIPPED", version: 2, eventTimestamp: t4 },
    { msg: "msg#6", scenario: "ORDERING-STEP-3", customerId: "C1001", orderId: "A100", status: "DELIVERED", version: 3, eventTimestamp: t5 }
  ];

  for (const event of events) {
    await sendEvent({ runId, ...event });
  }
}

function verifyOrdering(consumed) {
  const byGroup = new Map();
  for (const item of consumed) {
    if (!byGroup.has(item.streamId)) byGroup.set(item.streamId, []);
    byGroup.get(item.streamId).push(item.version);
  }

  let ok = true;
  for (const [streamId, versions] of byGroup.entries()) {
    const sorted = [...versions].sort((a, b) => a - b);
    const isOrdered = versions.every((v, i) => v === sorted[i]);
    if (!isOrdered) ok = false;
    console.log("order-stream", { streamId, versions, isOrdered });
  }

  return { ok, byGroup };
}

async function consumeAndVerify(runId) {
  const expectedUnique = 5;
  const consumed = [];
  const start = Date.now();
  const timeoutMs = 90_000;

  while (Date.now() - start < timeoutMs && consumed.length < expectedUnique) {
    const elapsedSeconds = Math.floor((Date.now() - start) / 1000);
    console.log(
      `${paint("[progress]", ANSI.cyan)} polling... received=${consumed.length}/${expectedUnique} ` +
      `elapsed=${elapsedSeconds}s timeout=${Math.floor(timeoutMs / 1000)}s`
    );

    const res = await sqs.send(new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 20,
      VisibilityTimeout: 90,
      AttributeNames: ["All"]
    }));

    const msgs = res.Messages || [];
    if (msgs.length === 0) {
      console.log(`${paint("[progress]", ANSI.cyan)} long poll returned 0 messages`);
      continue;
    }

    const toDelete = [];
    const toRelease = [];
    let matchedThisBatch = 0;
    let releasedThisBatch = 0;

    for (let i = 0; i < msgs.length; i++) {
      const m = msgs[i];
      let body;
      try {
        body = JSON.parse(m.Body || "{}");
      } catch {
        body = {};
      }

      if (body.runId === runId) {
        consumed.push({ streamId: body.streamId || `${body.customerId}#${body.orderId}`, version: body.version });
        matchedThisBatch += 1;
        logFifoLine(
          {
            msg: body.msg || "msg#?",
            customerId: body.customerId || "?",
            orderId: body.orderId || "?",
            version: body.version || "?",
            status: body.status || "UNKNOWN",
            eventTimestamp: body.eventTimestamp || "-"
          },
          body.scenario || "HARNESS-RECV",
          "received by harness",
          {
            groupId: m.Attributes?.MessageGroupId || "-",
            dedupId: m.Attributes?.MessageDeduplicationId || "-",
            sequence: m.Attributes?.SequenceNumber || "-"
          }
        );
        toDelete.push({ Id: `d-${i}`, ReceiptHandle: m.ReceiptHandle });
      } else {
        // Release unrelated message quickly so shared queues are not blocked by this demo.
        releasedThisBatch += 1;
        toRelease.push({ Id: `r-${i}`, ReceiptHandle: m.ReceiptHandle, VisibilityTimeout: 0 });
      }
    }

    if (toDelete.length > 0) {
      await sqs.send(new DeleteMessageBatchCommand({ QueueUrl: queueUrl, Entries: toDelete }));
    }

    if (toRelease.length > 0) {
      await sqs.send(new ChangeMessageVisibilityBatchCommand({ QueueUrl: queueUrl, Entries: toRelease }));
    }

    console.log(
      `${paint("[progress]", ANSI.cyan)} batch done: matched=${matchedThisBatch} released=${releasedThisBatch} ` +
      `total=${consumed.length}/${expectedUnique}`
    );

    await sleep(250);
  }

  console.log("summary", { expectedUnique, received: consumed.length, runId });

  const { ok, byGroup } = verifyOrdering(consumed);
  const dedupWorked = consumed.length === expectedUnique;

  console.log("verification", {
    orderingPreserved: ok,
    dedupSuppressedDuplicate: dedupWorked,
    groups: Object.fromEntries([...byGroup.entries()])
  });

  if (!ok || !dedupWorked) {
    process.exitCode = 2;
  }
}

async function main() {
  const runId = `run-${Date.now()}`;
  console.log("starting", { runId, queueUrl, region });

  await produceTraffic(runId);
  await consumeAndVerify(runId);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
