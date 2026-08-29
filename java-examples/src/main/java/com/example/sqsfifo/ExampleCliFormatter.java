package com.example.sqsfifo;

import java.util.Map;

final class ExampleCliFormatter {
    private static final boolean USE_COLOR = System.console() != null
            && !"1".equals(System.getenv("NO_COLOR"));

    private static final String RESET = "\u001B[0m";
    private static final String DIM = "\u001B[2m";
    private static final String CYAN = "\u001B[36m";
    private static final String GREEN = "\u001B[32m";
    private static final String YELLOW = "\u001B[33m";
    private static final String BLUE = "\u001B[34m";
    private static final String MAGENTA = "\u001B[35m";

    private ExampleCliFormatter() {
    }

    static void printFifoLine(
            String prefix,
            String msgTag,
            String scenario,
            String customerId,
            String orderId,
            int version,
            String status,
            String eventTimestamp,
            String detail,
            String groupId,
            String dedupId,
            String sequence) {
        String linePrefix = prefix == null || prefix.isBlank() ? "" : color("[" + prefix + "]", MAGENTA) + " ";
        System.out.println(
                linePrefix
                        + color("[" + msgTag + "]", MAGENTA) + " "
                        + color("[" + scenario + "]", CYAN) + " "
                        + color(customerId + "/" + orderId, GREEN) + " "
                        + color("v" + version, BLUE) + " "
                        + color(status, YELLOW) + " "
                        + color("@", DIM) + " "
                        + eventTimestamp);
        System.out.println(
                "  " + color(detail, YELLOW) + " " + color("|", DIM) + " "
                        + color("group", CYAN) + "=" + groupId + " "
                        + color("dedup", GREEN) + "=" + dedupId + " "
                        + color("seq", BLUE) + "=" + sequence);
    }

    static String stringValue(Map<String, Object> payload, String key, String fallback) {
        Object value = payload.get(key);
        return value == null ? fallback : String.valueOf(value);
    }

    static int intValue(Map<String, Object> payload, String key, int fallback) {
        Object value = payload.get(key);
        if (value instanceof Number number) {
            return number.intValue();
        }
        if (value instanceof String text) {
            try {
                return Integer.parseInt(text);
            } catch (NumberFormatException ignored) {
                return fallback;
            }
        }
        return fallback;
    }

    static String color(String text, String ansiCode) {
        if (!USE_COLOR) {
            return text;
        }
        return ansiCode + text + RESET;
    }
}