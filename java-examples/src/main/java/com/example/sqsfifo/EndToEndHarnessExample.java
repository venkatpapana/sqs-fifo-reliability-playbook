package com.example.sqsfifo;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequest;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.net.URI;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class EndToEndHarnessExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String regionName = envOrDefault("AWS_REGION", "us-west-2");
        String queueUrl = requireEnv("QUEUE_URL");
        String runId = "run-" + System.currentTimeMillis();

        System.out.println("starting { runId: '" + runId + "', queueUrl: '" + queueUrl + "', region: '" + regionName + "' }");

        try (SqsClient sqs = buildSqsClient(regionName)) {
            produceTraffic(sqs, queueUrl, runId);
            consumeAndVerify(sqs, queueUrl, runId);
        }
    }

    private static void produceTraffic(SqsClient sqs, String queueUrl, String runId) throws Exception {
        String t1 = Instant.now().toString();
        String t2 = Instant.now().plusSeconds(1).toString();
        String t3 = Instant.now().plusSeconds(2).toString();
        String t4 = Instant.now().plusSeconds(3).toString();
        String t5 = Instant.now().plusSeconds(4).toString();

        List<Map<String, Object>> events = List.of(
                event("msg#1", "ORDERING-STEP-1", runId, "C1001", "A100", "PACKED", 1, t1),
                event("msg#2", "PARALLEL-GROUP", runId, "C1001", "B200", "PACKED", 1, t2),
                event("msg#3", "ORDERING-STEP-2", runId, "C1001", "A100", "SHIPPED", 2, t3),
                event("msg#4", "DEDUP-RETRY", runId, "C1001", "A100", "SHIPPED", 2, t3),
                event("msg#5", "PARALLEL-GROUP-2", runId, "C1001", "B200", "SHIPPED", 2, t4),
                event("msg#6", "ORDERING-STEP-3", runId, "C1001", "A100", "DELIVERED", 3, t5)
        );

        for (Map<String, Object> body : events) {
            sendEvent(sqs, queueUrl, body);
        }
    }

    private static Map<String, Object> event(
            String msg,
            String scenario,
            String runId,
            String customerId,
            String orderId,
            String status,
            int version,
            String eventTimestamp) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("msg", msg);
        body.put("scenario", scenario);
        body.put("schemaVersion", 2);
        body.put("eventType", "ORDER_STATUS_UPDATED");
        body.put("runId", runId);
        body.put("streamId", customerId + "#" + orderId);
        body.put("eventId", runId + "-" + orderId + "-v" + version);
        body.put("customerId", customerId);
        body.put("orderId", orderId);
        body.put("status", status);
        body.put("version", version);
        body.put("eventTimestamp", eventTimestamp);
        return body;
    }

    private static void sendEvent(SqsClient sqs, String queueUrl, Map<String, Object> body) throws Exception {
        String runId = ExampleCliFormatter.stringValue(body, "runId", "run-?");
        String msg = ExampleCliFormatter.stringValue(body, "msg", "msg#?");
        String scenario = ExampleCliFormatter.stringValue(body, "scenario", "HARNESS-SEND");
        String customerId = ExampleCliFormatter.stringValue(body, "customerId", "?");
        String orderId = ExampleCliFormatter.stringValue(body, "orderId", "?");
        String status = ExampleCliFormatter.stringValue(body, "status", "UNKNOWN");
        int version = ExampleCliFormatter.intValue(body, "version", -1);
        String eventTimestamp = ExampleCliFormatter.stringValue(body, "eventTimestamp", "-");
        String streamId = customerId + "#" + orderId;
        String groupId = runId + "#" + streamId;
        String dedupId = runId + "#" + orderId + "#" + eventTimestamp;

        String payload = MAPPER.writeValueAsString(body);
        SendMessageResponse res = sqs.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(payload)
                .messageGroupId(groupId)
                .messageDeduplicationId(dedupId)
                .build());

        ExampleCliFormatter.printFifoLine(
                "",
                msg,
                scenario,
                customerId,
                orderId,
                version,
                status,
                eventTimestamp,
                "published by harness",
                groupId,
                dedupId,
                res.sequenceNumber());
    }

    private static void consumeAndVerify(SqsClient sqs, String queueUrl, String runId) throws Exception {
        int expectedUnique = 5;
        List<Map<String, Object>> consumed = new ArrayList<>();
        long start = System.currentTimeMillis();
        long timeoutMs = 90_000;

        while (System.currentTimeMillis() - start < timeoutMs && consumed.size() < expectedUnique) {
            long elapsedSeconds = (System.currentTimeMillis() - start) / 1000;
            System.out.println("[progress] polling... received=" + consumed.size() + "/" + expectedUnique
                    + " elapsed=" + elapsedSeconds + "s timeout=" + (timeoutMs / 1000) + "s");

            ReceiveMessageResponse response = sqs.receiveMessage(ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .visibilityTimeout(90)
                    .attributeNamesWithStrings("All")
                    .build());

            List<Message> messages = response.messages();
            if (messages == null || messages.isEmpty()) {
                System.out.println("[progress] long poll returned 0 messages");
                continue;
            }

            List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>();
            List<ChangeMessageVisibilityBatchRequestEntry> releaseEntries = new ArrayList<>();
            int matchedThisBatch = 0;
            int releasedThisBatch = 0;

            for (int i = 0; i < messages.size(); i++) {
                Message message = messages.get(i);
                Map<String, Object> payload = MAPPER.readValue(message.body(), Map.class);
                String messageRunId = ExampleCliFormatter.stringValue(payload, "runId", "");
                String msgTag = ExampleCliFormatter.stringValue(payload, "msg", "msg#?");
                String scenario = ExampleCliFormatter.stringValue(payload, "scenario", "HARNESS-RECV");
                String customerId = ExampleCliFormatter.stringValue(payload, "customerId", "?");
                String orderId = ExampleCliFormatter.stringValue(payload, "orderId", "?");
                int version = ExampleCliFormatter.intValue(payload, "version", -1);
                String status = ExampleCliFormatter.stringValue(payload, "status", "UNKNOWN");
                String eventTimestamp = ExampleCliFormatter.stringValue(payload, "eventTimestamp", "-");
                String groupId = message.attributesAsStrings().getOrDefault("MessageGroupId", "-");
                String dedupId = message.attributesAsStrings().getOrDefault("MessageDeduplicationId", "-");
                String sequence = message.attributesAsStrings().getOrDefault("SequenceNumber", "-");

                if (runId.equals(messageRunId)) {
                    consumed.add(Map.of(
                            "streamId", ExampleCliFormatter.stringValue(payload, "streamId", customerId + "#" + orderId),
                            "version", version
                    ));
                    matchedThisBatch += 1;
                    ExampleCliFormatter.printFifoLine(
                            "consume#" + (consumed.size()),
                            msgTag,
                            scenario,
                            customerId,
                            orderId,
                            version,
                            status,
                            eventTimestamp,
                            "received by harness",
                            groupId,
                            dedupId,
                            sequence);
                    deleteEntries.add(DeleteMessageBatchRequestEntry.builder()
                            .id("d-" + i)
                            .receiptHandle(message.receiptHandle())
                            .build());
                } else {
                    releasedThisBatch += 1;
                    releaseEntries.add(ChangeMessageVisibilityBatchRequestEntry.builder()
                            .id("r-" + i)
                            .receiptHandle(message.receiptHandle())
                            .visibilityTimeout(0)
                            .build());
                }
            }

            if (!deleteEntries.isEmpty()) {
                sqs.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(deleteEntries)
                        .build());
            }

            if (!releaseEntries.isEmpty()) {
                sqs.changeMessageVisibilityBatch(ChangeMessageVisibilityBatchRequest.builder()
                        .queueUrl(queueUrl)
                        .entries(releaseEntries)
                        .build());
            }

            System.out.println("[progress] batch done: matched=" + matchedThisBatch
                    + " released=" + releasedThisBatch
                    + " total=" + consumed.size() + "/" + expectedUnique);
        }

        System.out.println();
        System.out.println("summary { expectedUnique: " + expectedUnique + ", received: " + consumed.size()
                + ", runId: '" + runId + "' }");

        VerificationResult result = verifyOrdering(consumed);
        boolean dedupWorked = consumed.size() == expectedUnique;

        System.out.println("verification {");
        System.out.println("  orderingPreserved: " + result.orderingPreserved + ",");
        System.out.println("  dedupSuppressedDuplicate: " + dedupWorked + ",");
        System.out.println("  groups: " + result.groups);
        System.out.println("}");

        if (!result.orderingPreserved || !dedupWorked) {
            System.exit(2);
        }
    }

    private static VerificationResult verifyOrdering(List<Map<String, Object>> consumed) {
        Map<String, List<Integer>> byGroup = new TreeMap<>();
        for (Map<String, Object> item : consumed) {
            String streamId = String.valueOf(item.getOrDefault("streamId", "unknown"));
            int version = ((Number) item.getOrDefault("version", -1)).intValue();
            byGroup.computeIfAbsent(streamId, ignored -> new ArrayList<>()).add(version);
        }

        boolean ok = true;
        for (Map.Entry<String, List<Integer>> entry : byGroup.entrySet()) {
            List<Integer> versions = entry.getValue();
            List<Integer> sorted = new ArrayList<>(versions);
            sorted.sort(Integer::compareTo);
            boolean isOrdered = versions.equals(sorted);
            ok = ok && isOrdered;
            System.out.println("order-stream { streamId: '" + entry.getKey() + "', versions: " + versions
                    + ", isOrdered: " + isOrdered + " }");
        }

        return new VerificationResult(ok, byGroup);
    }

    private static String requireEnv(String key) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing required env var: " + key);
        }
        return value;
    }

    private static SqsClient buildSqsClient(String regionName) {
        SqsClientBuilder builder = SqsClient.builder().region(Region.of(regionName));
        String endpoint = System.getenv("SQS_ENDPOINT_URL");

        if (endpoint != null && !endpoint.isBlank()) {
            builder.endpointOverride(URI.create(endpoint));
            builder.credentialsProvider(StaticCredentialsProvider.create(
                    AwsBasicCredentials.create("test", "test")));
            System.out.println("Using local SQS endpoint: " + endpoint);
        }

        return builder.build();
    }

    private static String envOrDefault(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value;
    }

    private record VerificationResult(boolean orderingPreserved, Map<String, List<Integer>> groups) {
    }
}
