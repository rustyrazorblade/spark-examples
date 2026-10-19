package com.rustyrazorblade.easydblab.spark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.cassandra.spark.KryoRegister;
import org.apache.cassandra.spark.bulkwriter.BulkSparkConf;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.SparkPlan;
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec;
import org.apache.spark.sql.execution.adaptive.QueryStageExec;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeSet;

/**
 * Bulk-read benchmark: reads the cassandra-easy-stress {@code BulkReadDataset} tables through
 * Cassandra Sidecar with the Cassandra Analytics bulk reader, and times Spark SQL query shapes
 * over them.
 *
 * <p>Every run of a shape reads its tables anew: one {@code load()} per table, which takes a new
 * Sidecar snapshot and reads the ring and schema, then the query.  Each run prints one JSON line
 * to stdout (and to {@code spark.easydblab.read.output} if set) with the shape, size, consistency
 * level, snapshot time, plan time, execution time, total time, result rows, checksum, column
 * sums, the values of small results, the checks against the loader's EXPECTED output, and the
 * executor task metrics.  The job exits with status 1 if any run failed or any check did not match.
 *
 * <p>Usage:
 * <pre>
 *   spark-submit \
 *     --conf spark.easydblab.contactPoints=host1,host2,host3 \
 *     --conf spark.easydblab.keyspace=bench_1m \
 *     --conf spark.easydblab.localDc=us-west-2 \
 *     --conf spark.easydblab.read.expected=s3://bucket/expected-1m.jsonl \
 *     --class com.rustyrazorblade.easydblab.spark.BulkReadBenchmark \
 *     bulk-reader-sidecar.jar [shape ids...]
 * </pre>
 * See {@link BulkReadConfig} for every setting and {@link QueryShape} for the shapes file format.
 */
public class BulkReadBenchmark {

    static final String DATA_SOURCE = "org.apache.cassandra.spark.sparksql.CassandraDataSource";
    static final String DEFAULT_SHAPES = "bulk-read-shapes.sql";
    private static final int PRINTED_ROWS = 20;

    private final ObjectMapper mapper = new ObjectMapper();
    private final List<String> lines = new ArrayList<>();
    private int failures;

    public static void main(String[] args) {
        int status;
        try {
            status = new BulkReadBenchmark().run(args);
        } catch (Exception e) {
            System.err.println("Error during bulk read benchmark: " + e.getMessage());
            e.printStackTrace();
            status = 1;
        }
        System.exit(status);
    }

    int run(String[] args) throws Exception {
        SparkConf conf = new SparkConf(true).setAppName("BulkReadBenchmark");
        // The bulk reader's executor JVM options for JDK 17+, and its Kryo registrations.
        BulkSparkConf.setupSparkConf(conf, false);
        KryoRegister.setup(conf);

        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();
        BulkReadConfig config = null;
        try {
            config = BulkReadConfig.load(spark.sparkContext().conf(), args);
            List<QueryShape> shapes = selectShapes(config, readShapes(spark, config));
            ExpectedValues expected = config.expectedFile == null
                ? ExpectedValues.none()
                : ExpectedValues.parse(readText(spark, config.expectedFile));

            if (!checkJvms(spark, config)) {
                return 1;
            }

            RunMetrics metrics = new RunMetrics();
            spark.sparkContext().addSparkListener(metrics);
            for (String consistency : config.consistencyLevels) {
                for (QueryShape shape : shapes) {
                    for (int i = 0; i < config.warmups + config.runs; i++) {
                        boolean warmup = i < config.warmups;
                        int run = warmup ? i + 1 : i - config.warmups + 1;
                        boolean ok = runOnce(spark, config, shape, consistency, warmup, run, expected, metrics);
                        if (!ok && runFailedHard) {
                            break;
                        }
                    }
                }
            }
            return failures == 0 ? 0 : 1;
        } finally {
            if (config != null && config.outputFile != null) {
                writeText(spark, config.outputFile, String.join("\n", lines) + "\n");
            }
            spark.stop();
        }
    }

    // Set when a run threw, so the remaining runs of that shape are skipped.
    private boolean runFailedHard;

    private boolean runOnce(SparkSession spark, BulkReadConfig config, QueryShape shape, String consistency,
                            boolean warmup, int run, ExpectedValues expected, RunMetrics metrics) {
        runFailedHard = false;
        String group = snapshotName(shape.id, consistency, warmup, run);
        ObjectNode out = mapper.createObjectNode();
        out.put("type", "run");
        out.put("shape", shape.id);
        out.put("size", config.size);
        out.put("keyspace", config.keyspace);
        out.put("consistency", consistency);
        out.put("warmup", warmup);
        out.put("run", run);
        out.put("sink", shape.sink.name().toLowerCase(Locale.ROOT));
        out.put("started_at", Instant.now().toString());
        out.put("spark_parallelism", spark.sparkContext().defaultParallelism());

        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        jsc.setJobGroup(group, shape.id, false);
        long start = System.nanoTime();
        try {
            ObjectNode snapshots = out.putObject("snapshot_ms_by_table");
            for (String table : shape.tables) {
                long t = System.nanoTime();
                Map<String, String> options =
                    config.readOptions(table, consistency, group, spark.sparkContext().defaultParallelism());
                spark.read().format(DATA_SOURCE).options(options).load().createOrReplaceTempView(table);
                snapshots.put(table, millis(t));
            }
            long snapshotMs = millis(start);

            long planStart = System.nanoTime();
            Dataset<Row> query = spark.sql(shape.sql);
            query.queryExecution().executedPlan();
            long planMs = millis(planStart);

            long execStart = System.nanoTime();
            String[] columns = query.columns();
            ResultSummary summary;
            List<Row> collected = null;
            if (shape.sink == QueryShape.Sink.CHECKSUM) {
                summary = query.javaRDD()
                    .mapPartitions(it -> Collections.singletonList(new ResultSummary(columns).addAll(it)).iterator())
                    .reduce(ResultSummary::merge);
            } else {
                collected = query.collectAsList();
                summary = new ResultSummary(columns).addAll(collected.iterator());
            }
            long execMs = millis(execStart);

            out.put("snapshot_ms", snapshotMs);
            out.put("plan_ms", planMs);
            out.put("exec_ms", execMs);
            out.put("total_ms", millis(start));
            out.put("rows", summary.rows());
            out.put("checksum", summary.checksum());
            ObjectNode sums = out.putObject("sums");
            summary.sums().forEach(sums::put);
            if (collected != null) {
                ArrayNode values = out.putArray("values");
                for (Row row : collected.subList(0, Math.min(PRINTED_ROWS, collected.size()))) {
                    ObjectNode r = values.addObject();
                    for (int c = 0; c < columns.length; c++) {
                        putValue(r, columns[c], row.get(c));
                    }
                }
            }
            out.set("plan_nodes", mapper.valueToTree(planNodes(query)));
            boolean matched = check(out.putArray("checks"), shape, summary, collected, columns, expected);
            out.put("status", matched ? "ok" : "mismatch");
            if (!matched) {
                failures++;
            }
            return matched;
        } catch (Throwable e) {
            out.put("total_ms", millis(start));
            out.put("status", "error");
            out.put("error", String.valueOf(e));
            System.err.println("Shape " + shape.id + " failed:");
            e.printStackTrace();
            failures++;
            runFailedHard = true;
            return false;
        } finally {
            jsc.clearJobGroup();
            waitForListeners(spark);
            out.set("executor", mapper.valueToTree(metrics.take(group)));
            emit(out);
        }
    }

    private boolean check(ArrayNode checks, QueryShape shape, ResultSummary summary, List<Row> collected,
                          String[] columns, ExpectedValues expected) {
        boolean all = true;
        for (QueryShape.Expectation e : shape.expectations) {
            ObjectNode c = checks.addObject();
            c.put("metric", e.metric);
            c.put("reference", e.expected);
            String want = expected.resolve(e.expected);
            String got = actual(e.metric, summary, collected, columns);
            c.put("expected", want);
            c.put("actual", got);
            if (want == null) {
                // No EXPECTED file, or it lacks this value: reported, not judged.
                c.put("result", "no expected value");
                continue;
            }
            boolean ok = ExpectedValues.matches(want, got);
            c.put("result", ok ? "match" : "MISMATCH");
            all &= ok;
        }
        return all;
    }

    private static String actual(String metric, ResultSummary summary, List<Row> collected, String[] columns) {
        if (metric.equals("rows")) {
            return Long.toString(summary.rows());
        }
        if (metric.equals("checksum")) {
            return summary.checksum();
        }
        if (metric.startsWith("sum(") && metric.endsWith(")")) {
            Long sum = summary.sums().get(metric.substring(4, metric.length() - 1).trim());
            return sum == null ? null : sum.toString();
        }
        if (collected == null || collected.size() != 1) {
            return null;
        }
        for (int c = 0; c < columns.length; c++) {
            if (columns[c].equals(metric)) {
                return text(collected.get(0).get(c));
            }
        }
        return null;
    }

    private boolean checkJvms(SparkSession spark, BulkReadConfig config) {
        String driver = JvmInfo.describe();
        int partitions = Math.max(2, spark.sparkContext().defaultParallelism() * 2);
        List<String> executors = spark.range(0, partitions, 1, partitions).javaRDD()
            .mapPartitions(it -> Collections.singletonList(JvmInfo.describe()).iterator())
            .distinct()
            .collect();
        ObjectNode out = mapper.createObjectNode();
        out.put("type", "jvm");
        out.put("driver", driver);
        out.set("executors", mapper.valueToTree(new TreeSet<>(executors)));
        boolean ok = JvmInfo.isGenerationalZgc(driver) && executors.stream().allMatch(JvmInfo::isGenerationalZgc);
        out.put("generational_zgc", ok);
        emit(out);
        if (!ok && config.requireGenerationalZgc) {
            System.err.println("The driver and every executor must run generational ZGC (-XX:+UseZGC, plus "
                + "-XX:+ZGenerational on JDK 21).  Set " + BulkReadConfig.PREFIX
                + "requireGenerationalZgc=false to run anyway.");
            return false;
        }
        return true;
    }

    // Names of the physical plan nodes, after adaptive execution chose the final plan.
    private static TreeSet<String> planNodes(Dataset<Row> query) {
        TreeSet<String> names = new TreeSet<>();
        collectNodes(query.queryExecution().executedPlan(), names);
        return names;
    }

    private static void collectNodes(SparkPlan plan, TreeSet<String> names) {
        if (plan instanceof AdaptiveSparkPlanExec) {
            collectNodes(((AdaptiveSparkPlanExec) plan).executedPlan(), names);
            return;
        }
        if (plan instanceof QueryStageExec) {
            collectNodes(((QueryStageExec) plan).plan(), names);
            return;
        }
        names.add(plan.nodeName());
        scala.collection.Iterator<SparkPlan> it = plan.children().iterator();
        while (it.hasNext()) {
            collectNodes(it.next(), names);
        }
    }

    private static void waitForListeners(SparkSession spark) {
        try {
            spark.sparkContext().listenerBus().waitUntilEmpty();
        } catch (Exception e) {
            // Metrics may then miss the last tasks of the run; the timings are unaffected.
        }
    }

    private List<QueryShape> selectShapes(BulkReadConfig config, List<QueryShape> all) {
        if (config.shapeIds.isEmpty()) {
            return all;
        }
        Map<String, QueryShape> byId = new LinkedHashMap<>();
        all.forEach(s -> byId.put(s.id, s));
        List<QueryShape> out = new ArrayList<>();
        for (String id : config.shapeIds) {
            QueryShape s = byId.get(id);
            if (s == null) {
                throw new IllegalArgumentException("Unknown shape '" + id + "'.  Shapes: " + byId.keySet());
            }
            out.add(s);
        }
        return out;
    }

    private static List<QueryShape> readShapes(SparkSession spark, BulkReadConfig config) throws IOException {
        if (config.shapesFile != null) {
            return QueryShape.parseAll(readText(spark, config.shapesFile));
        }
        try (InputStream in = BulkReadBenchmark.class.getClassLoader().getResourceAsStream(DEFAULT_SHAPES)) {
            if (in == null) {
                throw new IllegalStateException(DEFAULT_SHAPES + " is missing from the jar");
            }
            return QueryShape.parseAll(new String(in.readAllBytes(), StandardCharsets.UTF_8));
        }
    }

    private static String readText(SparkSession spark, String path) throws IOException {
        Path p = new Path(path);
        FileSystem fs = FileSystem.get(URI.create(path), spark.sparkContext().hadoopConfiguration());
        try (FSDataInputStream in = fs.open(p); ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            in.transferTo(out);
            return out.toString(StandardCharsets.UTF_8);
        }
    }

    private static void writeText(SparkSession spark, String path, String text) {
        try {
            FileSystem fs = FileSystem.get(URI.create(path), spark.sparkContext().hadoopConfiguration());
            try (FSDataOutputStream out = fs.create(new Path(path), true)) {
                out.write(text.getBytes(StandardCharsets.UTF_8));
            }
            System.out.println("Wrote " + path);
        } catch (IOException e) {
            System.err.println("Could not write " + path + ": " + e);
        }
    }

    private void emit(ObjectNode node) {
        String line;
        try {
            line = mapper.writeValueAsString(node);
        } catch (IOException e) {
            line = "{\"type\":\"error\",\"error\":\"" + e + "\"}";
        }
        lines.add(line);
        System.out.println(line);
        System.out.flush();
    }

    // Sidecar snapshot names: letters, digits, and underscores.
    static String snapshotName(String shape, String consistency, boolean warmup, int run) {
        String name = "bench_" + shape + "_" + consistency + "_" + (warmup ? "w" : "r") + run + "_"
            + System.currentTimeMillis();
        return name.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9_]", "_");
    }

    private static void putValue(ObjectNode node, String name, Object v) {
        if (v == null) {
            node.putNull(name);
        } else if (v instanceof Long) {
            node.put(name, (Long) v);
        } else if (v instanceof Integer) {
            node.put(name, (Integer) v);
        } else {
            node.put(name, text(v));
        }
    }

    private static String text(Object v) {
        if (v == null) {
            return null;
        }
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).stripTrailingZeros().toPlainString();
        }
        if (v instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) v).toInstant().toString();
        }
        return v.toString();
    }

    private static long millis(long startNanos) {
        return (System.nanoTime() - startNanos) / 1_000_000;
    }
}
