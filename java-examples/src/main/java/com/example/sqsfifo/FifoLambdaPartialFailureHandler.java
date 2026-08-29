package com.example.sqsfifo;

import com.amazonaws.services.lambda.runtime.events.SQSEvent;
import com.amazonaws.services.lambda.runtime.events.SQSBatchResponse;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;

public class FifoLambdaPartialFailureHandler {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public SQSBatchResponse handleRequest(SQSEvent event) {
        List<SQSBatchResponse.BatchItemFailure> failures = new ArrayList<>();

        for (SQSEvent.SQSMessage message : event.getRecords()) {
            try {
                JsonNode payload = MAPPER.readTree(message.getBody());
                processOne(payload);
            } catch (Exception ex) {
                System.err.println("Record failed messageId=" + message.getMessageId() + " error=" + ex.getMessage());
                failures.add(new SQSBatchResponse.BatchItemFailure(message.getMessageId()));
            }
        }

        return new SQSBatchResponse(failures);
    }

    private void processOne(JsonNode payload) {
        String eventType = payload.path("eventType").asText();
        if (!"ORDER_STATUS_UPDATED".equals(eventType)) {
            throw new IllegalArgumentException("Unsupported eventType: " + eventType);
        }

        String orderId = payload.path("orderId").asText();
        if ("POISON-ORDER".equals(orderId)) {
            throw new RuntimeException("Simulated downstream failure");
        }

        // Insert idempotent business logic here.
    }
}
