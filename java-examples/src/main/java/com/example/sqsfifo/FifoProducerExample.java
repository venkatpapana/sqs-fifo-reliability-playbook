package com.example.sqsfifo;

import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageResponse;

import java.net.URI;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class FifoProducerExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String regionName = envOrDefault("AWS_REGION", "us-west-2");
        String queueUrl = requireEnv("QUEUE_URL");

        try (SqsClient sqs = buildSqsClient(regionName)) {
        String ts1 = Instant.now().toString();
        String ts2 = Instant.now().plusSeconds(1).toString();
        String ts3 = Instant.now().plusSeconds(2).toString();
        String ts4 = Instant.now().plusSeconds(3).toString();
        String ts5 = Instant.now().plusSeconds(4).toString();

        sendOrderStatusEvent(sqs, queueUrl, "msg#1", "ORDERING-STEP-1", "ordered stream start",
            "C1001", "A100", "PACKED", 1, ts1);
        sendOrderStatusEvent(sqs, queueUrl, "msg#2", "ORDERING-STEP-2", "same stream, processed after step 1",
            "C1001", "A100", "SHIPPED", 2, ts2);
        sendOrderStatusEvent(sqs, queueUrl, "msg#3", "PARALLEL-GROUP", "different stream, can run in parallel",
            "C1001", "B200", "PACKED", 1, ts3);
        sendOrderStatusEvent(sqs, queueUrl, "msg#4", "ORDERING-STEP-3", "same stream, processed after step 2",
            "C1001", "A100", "DELIVERED", 3, ts5);

        sendOrderStatusEvent(sqs, queueUrl, "msg#5", "DEDUP-RETRY", "same dedup as step 2, should be suppressed",
            "C1001", "A100", "SHIPPED", 2, ts2);
        sendOrderStatusEvent(sqs, queueUrl, "msg#6", "PARALLEL-GROUP-2", "same orderId, different customer => separate stream",
            "C1002", "A100", "SHIPPED", 1, ts4);
        }
    }

    private static void sendOrderStatusEvent(
            SqsClient sqs,
            String queueUrl,
        String msg,
        String scenario,
        String expected,
            String customerId,
            String orderId,
        String status,
        int version,
        String eventTimestamp) throws Exception {
        String groupId = customerId + "#" + orderId;
        String dedupId = orderId + "#" + eventTimestamp;

        Map<String, Object> body = new LinkedHashMap<>();
    body.put("msg", msg);
    body.put("schemaVersion", 2);
        body.put("eventType", "ORDER_STATUS_UPDATED");
    body.put("eventId", "evt-" + orderId + "-" + version);
        body.put("eventTimestamp", eventTimestamp);
        body.put("customerId", customerId);
        body.put("orderId", orderId);
        body.put("status", status);
    body.put("version", version);

        String payload = MAPPER.writeValueAsString(body);

        SendMessageRequest req = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(payload)
                .messageGroupId(groupId)
                .messageDeduplicationId(dedupId)
                .build();

        SendMessageResponse res = sqs.sendMessage(req);
            ExampleCliFormatter.printFifoLine(
                "",
                msg,
                scenario,
                customerId,
                orderId,
                version,
                status,
                eventTimestamp,
                expected,
                groupId,
                dedupId,
                res.sequenceNumber());
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
