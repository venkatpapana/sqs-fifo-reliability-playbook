package com.example.sqsfifo;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;
import software.amazon.awssdk.services.sqs.model.ChangeMessageVisibilityRequest;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageResponse;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class VisibilityHeartbeatExample {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws Exception {
        String regionName = envOrDefault("AWS_REGION", "us-west-2");
        String queueUrl = requireEnv("QUEUE_URL");

        try (SqsClient sqs = buildSqsClient(regionName)) {
            ReceiveMessageResponse response = sqs.receiveMessage(
                    ReceiveMessageRequest.builder()
                            .queueUrl(queueUrl)
                            .maxNumberOfMessages(1)
                            .waitTimeSeconds(20)
                            .visibilityTimeout(30)
                            .attributeNamesWithStrings("All")
                            .build());

            List<Message> messages = response.messages();
            if (messages == null || messages.isEmpty()) {
                System.out.println("No message available.");
                return;
            }

            Message message = messages.get(0);
                Map<String, Object> payload = MAPPER.readValue(message.body(), new TypeReference<>() {
                });
                String msgTag = ExampleCliFormatter.stringValue(payload, "msg", "msg#?");
                String customerId = ExampleCliFormatter.stringValue(payload, "customerId", "?");
                String orderId = ExampleCliFormatter.stringValue(payload, "orderId", "?");
                int version = ExampleCliFormatter.intValue(payload, "version", -1);
                String status = ExampleCliFormatter.stringValue(payload, "status", "PROCESSING");
                String eventTimestamp = ExampleCliFormatter.stringValue(payload, "eventTimestamp", "-");
                String groupId = message.attributesAsStrings().getOrDefault("MessageGroupId", "-");
                String dedupId = message.attributesAsStrings().getOrDefault("MessageDeduplicationId", "-");
                String sequence = message.attributesAsStrings().getOrDefault("SequenceNumber", "-");

                ExampleCliFormatter.printFifoLine(
                    "",
                    msgTag,
                    "HEARTBEAT-START",
                    customerId,
                    orderId,
                    version,
                    status,
                    eventTimestamp,
                    "long task started (messageId=" + message.messageId() + ")",
                    groupId,
                    dedupId,
                    sequence);

            for (int i = 1; i <= 3; i++) {
                Thread.sleep(Duration.ofSeconds(20).toMillis());
                sqs.changeMessageVisibility(ChangeMessageVisibilityRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .visibilityTimeout(30)
                        .build());
                ExampleCliFormatter.printFifoLine(
                    "",
                    msgTag,
                    "HEARTBEAT-EXTEND",
                    customerId,
                    orderId,
                    version,
                    status,
                    eventTimestamp,
                    "visibility extended " + i + " time(s)",
                    groupId,
                    dedupId,
                    sequence);
            }

            sqs.deleteMessage(DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build());
                ExampleCliFormatter.printFifoLine(
                    "",
                    msgTag,
                    "HEARTBEAT-DONE",
                    customerId,
                    orderId,
                    version,
                    status,
                    eventTimestamp,
                    "completed and deleted",
                    groupId,
                    dedupId,
                    sequence);
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
