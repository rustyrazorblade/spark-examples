package com.rustyrazorblade.easydblab.spark;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;

/**
 * The values the cassandra-easy-stress loader prints with {@code --workload.mode=EXPECTED
 * --workload.expectedOutput=<file>}: one JSON line per table, with its row count, partition count,
 * checksum, and named values.
 */
public final class ExpectedValues {

    private final Map<String, JsonNode> tables = new HashMap<>();

    private ExpectedValues() {
    }

    /** An empty set: every reference is unknown. */
    public static ExpectedValues none() {
        return new ExpectedValues();
    }

    /** Parses the JSON lines of an EXPECTED output file.  For a table with several states, the first one wins. */
    public static ExpectedValues parse(String jsonLines) throws IOException {
        ExpectedValues out = new ExpectedValues();
        ObjectMapper mapper = new ObjectMapper();
        for (String line : jsonLines.split("\r?\n")) {
            if (line.trim().isEmpty()) {
                continue;
            }
            JsonNode node = mapper.readTree(line);
            out.tables.putIfAbsent(node.get("table").asText(), node);
        }
        return out;
    }

    /**
     * Resolves an expected value: a number, or {@code <table>.rows}, {@code <table>.partitions},
     * {@code <table>.checksum}, or {@code <table>.values.<name>}.  Returns null when the reference
     * names a table or value the file does not have.
     */
    public String resolve(String reference) {
        if (isNumber(reference)) {
            return reference;
        }
        int dot = reference.indexOf('.');
        if (dot < 0) {
            throw new IllegalArgumentException("expected value '" + reference + "' is not a number or <table>.<field>");
        }
        JsonNode table = tables.get(reference.substring(0, dot));
        if (table == null) {
            return null;
        }
        String field = reference.substring(dot + 1);
        if (field.startsWith("values.")) {
            JsonNode v = table.get("values").get(field.substring("values.".length()));
            return v == null ? null : v.asText();
        }
        JsonNode v = table.get(field);
        return v == null ? null : v.asText();
    }

    /** Compares an expected and an actual value: as numbers when both are numbers, else as text. */
    public static boolean matches(String expected, String actual) {
        if (expected == null || actual == null) {
            return false;
        }
        if (isNumber(expected) && isNumber(actual)) {
            return new BigDecimal(expected).compareTo(new BigDecimal(actual)) == 0;
        }
        return expected.equals(actual);
    }

    static boolean isNumber(String s) {
        try {
            new BigDecimal(s);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }
}
