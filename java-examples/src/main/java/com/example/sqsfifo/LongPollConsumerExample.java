package com.example.sqsfifo;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class LongPollConsumerExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String regionName = envOrDefault("AWS_REGION", "us-west-2");
        String queueUrl = requireEnv("QUEUE_URL");

        try (SqsClient sqs = buildSqsClient(regionName)) {
            ReceiveMessageRequest req = ReceiveMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20)
                    .visibilityTimeout(90)
                    .attributeNamesWithStrings("All")
                    .build();

            ReceiveMessageResponse response = sqs.receiveMessage(req);
            List<Message> messages = response.messages();

            if (messages == null || messages.isEmpty()) {
                System.out.println("No messages this poll.");
                return;
            }

            List<DeleteMessageBatchRequestEntry> deleteEntries = new ArrayList<>();
            List<Map<String, Object>> payloads = new ArrayList<>();
            Set<String> dedupIds = new LinkedHashSet<>();
            Map<String, List<String>> perGroup = new TreeMap<>();

            int idx = 0;
            for (Message message : messages) {
                Map<String, Object> payload = MAPPER.readValue(message.body(), Map.class);
                payloads.add(payload);
                String groupId = message.attributesAsStrings().getOrDefault("MessageGroupId", "-");
                String dedupId = message.attributesAsStrings().getOrDefault("MessageDeduplicationId", "-");
                String sequence = message.attributesAsStrings().getOrDefault("SequenceNumber", "-");
                String msgTag = ExampleCliFormatter.stringValue(payload, "msg", "msg#?");
                String customerId = ExampleCliFormatter.stringValue(payload, "customerId", "?");
                String orderId = ExampleCliFormatter.stringValue(payload, "orderId", "?");
                int version = ExampleCliFormatter.intValue(payload, "version", -1);
                String status = ExampleCliFormatter.stringValue(payload, "status", "UNKNOWN");
                String eventTimestamp = ExampleCliFormatter.stringValue(payload, "eventTimestamp", "-");

                ExampleCliFormatter.printFifoLine(
                        "consume#" + (idx + 1),
                        msgTag,
                        ExampleCliFormatter.stringValue(payload, "scenario", "CONSUME"),
                        customerId,
                        orderId,
                        version,
                        status,
                        eventTimestamp,
                        "received by consumer",
                        groupId,
                        dedupId,
                        sequence);

                dedupIds.add(dedupId);
                perGroup.computeIfAbsent(groupId, ignored -> new ArrayList<>())
                        .add(msgTag + ":v" + version + ":" + status);

                deleteEntries.add(DeleteMessageBatchRequestEntry.builder()
                        .id("msg-" + idx++)
                        .receiptHandle(message.receiptHandle())
                        .build());
            }

            List<String> msgTags = payloads.stream()
                    .map(payload -> ExampleCliFormatter.stringValue(payload, "msg", ""))
                    .filter(tag -> !tag.isBlank())
                    .toList();
            boolean hasDedupBase = msgTags.contains("msg#2");
            boolean hasDedupRetry = msgTags.contains("msg#5");
            String dedupResult = "not evaluated (msg#2/msg#5 not in this poll)";
            if (hasDedupBase && !hasDedupRetry) {
                dedupResult = "yes";
            } else if (hasDedupBase) {
                dedupResult = "no (retry message also consumed)";
            }

            System.out.println();
            System.out.println(ExampleCliFormatter.color("Consume summary", "\u001B[36m"));
            System.out.println("- received: " + messages.size());
            System.out.println("- unique dedup ids: " + dedupIds.size());
            System.out.println("- dedup retry (msg#5) suppressed: " + dedupResult);
            System.out.println("- per-group order:");
            for (Map.Entry<String, List<String>> entry : perGroup.entrySet()) {
                System.out.println("  " + entry.getKey() + " -> " + String.join(" -> ", entry.getValue()));
            }

            sqs.deleteMessageBatch(DeleteMessageBatchRequest.builder()
                    .queueUrl(queueUrl)
                    .entries(deleteEntries)
                    .build());

            System.out.println("Deleted " + messages.size() + " messages");
        }
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
}
