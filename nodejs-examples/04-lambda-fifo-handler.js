// Example Lambda handler for SQS FIFO with partial batch failures.
// Runtime: Node.js 20+ or 22+

exports.handler = async (event) => {
  const batchItemFailures = [];

  for (const record of event.Records) {
    try {
      const payload = JSON.parse(record.body);
      await processOne(payload);
    } catch (err) {
      console.error("Record failed", {
        messageId: record.messageId,
        err: err.message
      });

      // Only failed records are returned for retry.
      batchItemFailures.push({ itemIdentifier: record.messageId });
    }
  }

  return { batchItemFailures };
};

async function processOne(payload) {
  // Replace with idempotent business logic.
  // Example: conditional DB upsert using eventId/version key.

  if (payload.eventType !== "ORDER_STATUS_UPDATED") {
    throw new Error(`Unsupported event type: ${payload.eventType}`);
  }

  // Simulate external API call.
  await fakeHttpCall(payload);
}

async function fakeHttpCall(payload) {
  if (payload.orderId === "POISON-ORDER") {
    throw new Error("Simulated downstream failure");
  }

  return {
    statusCode: 200,
    orderId: payload.orderId
  };
}
