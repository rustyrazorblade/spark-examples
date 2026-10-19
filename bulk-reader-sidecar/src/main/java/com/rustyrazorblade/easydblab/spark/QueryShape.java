package com.rustyrazorblade.easydblab.spark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * One query shape of the benchmark: a Spark SQL statement over the dataset's tables, how its
 * result is consumed, and the values it must produce.
 *
 * <p>A shapes file holds shapes one after another.  Each starts with a {@code -- shape: <id>}
 * line; the {@code --} lines after it set its properties, and the other lines are its SQL, with
 * an optional trailing semicolon:
 *
 * <pre>
 * -- shape: Q4_0.1
 * -- tables: fact
 * -- sink: checksum
 * -- expect: rows = fact.values.rows with bucket &lt; 10
 * SELECT * FROM fact WHERE bucket &lt; 10;
 * </pre>
 *
 * <ul>
 *   <li>{@code tables}: the tables to read for this shape, comma separated.  Each is read
 *       through Sidecar and registered as a view of the same name.  Without the line, the names
 *       after {@code FROM} and {@code JOIN} are used.</li>
 *   <li>{@code sink}: {@code collect} (the default) brings the result rows to the driver and
 *       reports the first of them; {@code checksum} reads every result row on the executors and
 *       reports only the row count, checksum, and column sums.  Use {@code checksum} for large
 *       results.</li>
 *   <li>{@code expect}: {@code <metric> = <expected>}, repeatable.  The metric is {@code rows},
 *       {@code checksum}, {@code sum(<column>)}, or a column name, which needs a one-row result.
 *       The expected value is a number, or a reference into the loader's EXPECTED output:
 *       {@code <table>.rows}, {@code <table>.partitions}, {@code <table>.checksum}, or
 *       {@code <table>.values.<name>}.</li>
 * </ul>
 */
public final class QueryShape {

    /** How the result of a shape is consumed. */
    public enum Sink { COLLECT, CHECKSUM }

    /** One expected value of a shape. */
    public static final class Expectation {
        public final String metric;
        public final String expected;

        Expectation(String metric, String expected) {
            this.metric = metric;
            this.expected = expected;
        }
    }

    private static final Pattern TABLE_REFERENCE =
        Pattern.compile("\\b(?:from|join)\\s+([A-Za-z_][A-Za-z0-9_]*)", Pattern.CASE_INSENSITIVE);

    public final String id;
    public final String sql;
    public final List<String> tables;
    public final Sink sink;
    public final List<Expectation> expectations;

    QueryShape(String id, String sql, List<String> tables, Sink sink, List<Expectation> expectations) {
        this.id = id;
        this.sql = sql;
        this.tables = Collections.unmodifiableList(tables);
        this.sink = sink;
        this.expectations = Collections.unmodifiableList(expectations);
    }

    /** Parses every shape in {@code text}, in file order. */
    public static List<QueryShape> parseAll(String text) {
        List<QueryShape> shapes = new ArrayList<>();
        Builder current = null;
        int lineNumber = 0;
        for (String raw : text.split("\r?\n", -1)) {
            lineNumber++;
            String line = raw.trim();
            if (line.startsWith("--")) {
                String directive = line.substring(2).trim();
                int colon = directive.indexOf(':');
                String key = colon < 0 ? "" : directive.substring(0, colon).trim().toLowerCase(Locale.ROOT);
                String value = colon < 0 ? "" : directive.substring(colon + 1).trim();
                if (key.equals("shape")) {
                    if (current != null) {
                        shapes.add(current.build());
                    }
                    current = new Builder(value, lineNumber);
                } else if (current != null) {
                    current.directive(key, value, lineNumber);
                }
            } else if (!line.isEmpty()) {
                if (current == null) {
                    throw new IllegalArgumentException("line " + lineNumber + ": SQL before the first '-- shape:' line");
                }
                current.sql.append(raw).append('\n');
            }
        }
        if (current != null) {
            shapes.add(current.build());
        }
        Set<String> ids = new LinkedHashSet<>();
        for (QueryShape s : shapes) {
            if (!ids.add(s.id)) {
                throw new IllegalArgumentException("shape " + s.id + " is defined twice");
            }
        }
        return shapes;
    }

    private static final class Builder {
        final String id;
        final int line;
        final StringBuilder sql = new StringBuilder();
        List<String> tables;
        Sink sink = Sink.COLLECT;
        final List<Expectation> expectations = new ArrayList<>();

        Builder(String id, int line) {
            if (id.isEmpty()) {
                throw new IllegalArgumentException("line " + line + ": '-- shape:' needs an id");
            }
            this.id = id;
            this.line = line;
        }

        void directive(String key, String value, int lineNumber) {
            switch (key) {
                case "tables":
                    tables = new ArrayList<>();
                    for (String t : value.split(",")) {
                        if (!t.trim().isEmpty()) {
                            tables.add(t.trim());
                        }
                    }
                    break;
                case "sink":
                    try {
                        sink = Sink.valueOf(value.toUpperCase(Locale.ROOT));
                    } catch (IllegalArgumentException e) {
                        throw new IllegalArgumentException("line " + lineNumber + ": unknown sink '" + value + "'");
                    }
                    break;
                case "expect":
                    int eq = value.indexOf('=');
                    if (eq <= 0 || eq == value.length() - 1) {
                        throw new IllegalArgumentException("line " + lineNumber + ": expect needs '<metric> = <expected>'");
                    }
                    expectations.add(new Expectation(value.substring(0, eq).trim(), value.substring(eq + 1).trim()));
                    break;
                default:
                    // Any other "--" line is a comment.
                    break;
            }
        }

        QueryShape build() {
            String text = sql.toString().trim();
            if (text.endsWith(";")) {
                text = text.substring(0, text.length() - 1).trim();
            }
            if (text.isEmpty()) {
                throw new IllegalArgumentException("shape " + id + " (line " + line + ") has no SQL");
            }
            List<String> t = tables;
            if (t == null) {
                Set<String> found = new LinkedHashSet<>();
                Matcher m = TABLE_REFERENCE.matcher(text);
                while (m.find()) {
                    found.add(m.group(1));
                }
                t = new ArrayList<>(found);
            }
            if (t.isEmpty()) {
                throw new IllegalArgumentException("shape " + id + " reads no table");
            }
            return new QueryShape(id, text, t, sink, expectations);
        }
    }
}
