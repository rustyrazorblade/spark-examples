package com.rustyrazorblade.easydblab.spark;

import org.apache.spark.SparkConf;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Settings of {@link BulkReadBenchmark}, all passed as Spark properties ({@code --conf}).
 *
 * <p>Required:
 * <ul>
 *   <li>{@code spark.easydblab.contactPoints}: comma-separated Sidecar hosts (the Cassandra nodes)</li>
 *   <li>{@code spark.easydblab.keyspace}: the keyspace the dataset was loaded into</li>
 *   <li>{@code spark.easydblab.localDc}: the datacenter to read from</li>
 * </ul>
 * Optional ({@code spark.easydblab.read.*}):
 * <ul>
 *   <li>{@code sidecarPort}: Sidecar port (default 9043)</li>
 *   <li>{@code consistency}: read consistency levels, comma separated; every shape runs at each
 *       (default {@code ONE}; for example {@code ONE,LOCAL_QUORUM})</li>
 *   <li>{@code createSnapshot}: take a new Sidecar snapshot for every read (default true).  With
 *       false, {@code snapshotName} names an existing snapshot to read.</li>
 *   <li>{@code snapshotName}: the snapshot to read when {@code createSnapshot} is false</li>
 *   <li>{@code clearSnapshotStrategy}: passed to the bulk reader (default: its own default)</li>
 *   <li>{@code shapes}: shape ids to run, comma separated (default: every shape in the file).
 *       Program arguments, when given, replace this list.</li>
 *   <li>{@code shapesFile}: a shapes file (local or Hadoop path, such as {@code s3://}); default is
 *       the bundled {@code bulk-read-shapes.sql}</li>
 *   <li>{@code warmups}: discarded runs per shape and consistency level (default 1)</li>
 *   <li>{@code runs}: timed runs per shape and consistency level (default 5)</li>
 *   <li>{@code size}: a label for the dataset size in the output (default: the keyspace)</li>
 *   <li>{@code expected}: the loader's EXPECTED JSON lines file (local or Hadoop path)</li>
 *   <li>{@code output}: a file (local or Hadoop path) to write the JSON result lines to, as well as stdout</li>
 *   <li>{@code numCores}, {@code defaultParallelism}: passed to the bulk reader, which sizes its
 *       Spark partitions from them (default: Spark's default parallelism)</li>
 *   <li>{@code requireGenerationalZgc}: fail unless the driver and every executor run
 *       generational ZGC (default true)</li>
 *   <li>{@code option.<name>}: any other bulk reader option, passed through as is</li>
 * </ul>
 */
public final class BulkReadConfig {

    public static final String PROP_CONTACT_POINTS = "spark.easydblab.contactPoints";
    public static final String PROP_KEYSPACE = "spark.easydblab.keyspace";
    public static final String PROP_LOCAL_DC = "spark.easydblab.localDc";
    public static final String PREFIX = "spark.easydblab.read.";
    public static final String OPTION_PREFIX = PREFIX + "option.";

    public final String contactPoints;
    public final String keyspace;
    public final String localDc;
    public final int sidecarPort;
    public final List<String> consistencyLevels;
    public final boolean createSnapshot;
    public final String snapshotName;
    public final String clearSnapshotStrategy;
    public final List<String> shapeIds;
    public final String shapesFile;
    public final int warmups;
    public final int runs;
    public final String size;
    public final String expectedFile;
    public final String outputFile;
    public final Integer numCores;
    public final Integer defaultParallelism;
    public final boolean requireGenerationalZgc;
    public final Map<String, String> extraOptions;

    private BulkReadConfig(SparkConf conf, String[] args) {
        contactPoints = required(conf, PROP_CONTACT_POINTS);
        keyspace = required(conf, PROP_KEYSPACE);
        localDc = required(conf, PROP_LOCAL_DC);
        sidecarPort = intProp(conf, PREFIX + "sidecarPort", 9043);
        consistencyLevels = list(conf.get(PREFIX + "consistency", "ONE").toUpperCase(Locale.ROOT));
        createSnapshot = boolProp(conf, PREFIX + "createSnapshot", true);
        snapshotName = conf.get(PREFIX + "snapshotName", null);
        if (!createSnapshot && snapshotName == null) {
            throw new IllegalArgumentException(PREFIX + "createSnapshot=false needs " + PREFIX + "snapshotName");
        }
        clearSnapshotStrategy = conf.get(PREFIX + "clearSnapshotStrategy", null);
        shapeIds = args.length > 0 ? Arrays.asList(args) : list(conf.get(PREFIX + "shapes", ""));
        shapesFile = conf.get(PREFIX + "shapesFile", null);
        warmups = intProp(conf, PREFIX + "warmups", 1);
        runs = intProp(conf, PREFIX + "runs", 5);
        if (warmups < 0 || runs < 1) {
            throw new IllegalArgumentException(PREFIX + "warmups must be >= 0 and " + PREFIX + "runs >= 1");
        }
        size = conf.get(PREFIX + "size", keyspace);
        expectedFile = conf.get(PREFIX + "expected", null);
        outputFile = conf.get(PREFIX + "output", null);
        numCores = conf.contains(PREFIX + "numCores") ? intProp(conf, PREFIX + "numCores", 0) : null;
        defaultParallelism = conf.contains(PREFIX + "defaultParallelism")
            ? intProp(conf, PREFIX + "defaultParallelism", 0) : null;
        requireGenerationalZgc = boolProp(conf, PREFIX + "requireGenerationalZgc", true);
        Map<String, String> extra = new LinkedHashMap<>();
        for (Tuple2<String, String> e : conf.getAllWithPrefix(OPTION_PREFIX)) {
            extra.put(e._1(), e._2());
        }
        extraOptions = Collections.unmodifiableMap(extra);
    }

    /** Reads the settings from {@code conf}; program arguments, if any, are the shape ids to run. */
    public static BulkReadConfig load(SparkConf conf, String[] args) {
        return new BulkReadConfig(conf, args);
    }

    /**
     * Returns the bulk reader options for one read of {@code table} at {@code consistency}.
     * {@code snapshot} names the new snapshot when {@link #createSnapshot} is set.
     */
    public Map<String, String> readOptions(String table, String consistency, String snapshot,
                                           int sparkParallelism) {
        Map<String, String> o = new LinkedHashMap<>();
        o.put("sidecar_contact_points", contactPoints);
        o.put("sidecar_port", Integer.toString(sidecarPort));
        o.put("keyspace", keyspace);
        o.put("table", table);
        o.put("dc", localDc);
        o.put("consistencyLevel", consistency);
        o.put("createSnapshot", Boolean.toString(createSnapshot));
        o.put("snapshotName", createSnapshot ? snapshot : snapshotName);
        if (clearSnapshotStrategy != null) {
            o.put("clearSnapshotStrategy", clearSnapshotStrategy);
        }
        o.put("numCores", Integer.toString(numCores != null ? numCores : sparkParallelism));
        o.put("defaultParallelism", Integer.toString(defaultParallelism != null ? defaultParallelism : sparkParallelism));
        o.putAll(extraOptions);
        return o;
    }

    private static String required(SparkConf conf, String key) {
        if (!conf.contains(key)) {
            throw new IllegalArgumentException("Required Spark property not set: " + key + "\n\n"
                + "Required: --conf " + PROP_CONTACT_POINTS + "=<sidecar hosts> --conf " + PROP_KEYSPACE
                + "=<keyspace> --conf " + PROP_LOCAL_DC + "=<datacenter>\n"
                + "See the BulkReadConfig javadoc for the optional " + PREFIX + "* properties.");
        }
        return conf.get(key);
    }

    private static List<String> list(String value) {
        List<String> out = new ArrayList<>();
        for (String s : value.split(",")) {
            if (!s.trim().isEmpty()) {
                out.add(s.trim());
            }
        }
        return out;
    }

    private static int intProp(SparkConf conf, String key, int defaultValue) {
        if (!conf.contains(key)) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(conf.get(key).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid value for " + key + ": '" + conf.get(key) + "'. Expected an integer.", e);
        }
    }

    private static boolean boolProp(SparkConf conf, String key, boolean defaultValue) {
        if (!conf.contains(key)) {
            return defaultValue;
        }
        String v = conf.get(key).trim();
        if (v.equalsIgnoreCase("true")) {
            return true;
        }
        if (v.equalsIgnoreCase("false")) {
            return false;
        }
        throw new IllegalArgumentException("Invalid value for " + key + ": '" + v + "'. Expected 'true' or 'false'.");
    }
}
