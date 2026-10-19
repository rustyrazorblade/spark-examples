package com.rustyrazorblade.easydblab.spark;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the checksum against the values cassandra-easy-stress prints with
 * {@code BulkReadDataset --workload.mode=EXPECTED --populate 10k --threads 10} (100K fact rows,
 * seed 1, every other parameter at its default).  The rows are rebuilt here the way the loader
 * builds them, then given the Java types the Spark bulk reader returns.
 */
class ResultSummaryTest {

    private static final long GOLDEN_GAMMA = 0x9e3779b97f4a7c15L;
    private static final long BASE_TIME_MILLIS = 1_767_225_600_000L;
    private static final long ROWS = 100_000;
    private static final long REF_ROWS = 10_000;
    private static final long DIM2_KEY_SPACE = 1112;  // ceil(1000 / 0.9)

    // Port of DatasetHashes for seed 1.
    private static final long BASE = RowHash.mix64(1L);

    private static long of(int stream, long index) {
        return RowHash.mix64(BASE + RowHash.mix64((stream + 1L) * GOLDEN_GAMMA) + index * GOLDEN_GAMMA);
    }

    private static long below(int stream, long index, long bound) {
        return Math.floorMod(of(stream, index), bound);
    }

    private static double unit(int stream, long index) {
        return (of(stream, index) >>> 11) * (1.0 / (1L << 53));
    }

    private static long group(int stream, long index, long cardinality) {
        return Math.min(cardinality - 1, Math.max(0, (long) (unit(stream, index) * cardinality)));
    }

    @Test
    void factChecksumMatchesTheLoader() {
        String[] columns = {"pk", "event_time", "row_id", "region", "i32", "i64", "bucket", "name", "amount", "uid",
            "ref_id", "g_low", "g_mid", "g_high", "dim2_id", "tags", "attrs"};
        ResultSummary summary = new ResultSummary(columns);
        for (long i = 0; i < ROWS; i++) {
            summary.add(factRow(i));
        }
        assertThat(summary.rows()).isEqualTo(100_000);
        assertThat(summary.checksum()).isEqualTo("85e40666aebd4f6e");
        assertThat(summary.sums().get("i64")).isEqualTo(50005083734591L);
    }

    @Test
    void dimensionChecksumsMatchTheLoader() {
        ResultSummary dim = new ResultSummary(new String[] {"g_mid", "name", "category", "weight"});
        for (int i = 0; i < 10_000; i++) {
            dim.add(RowFactory.create(i, "dim-" + i, (int) below(201, i, 100), (int) below(202, i, 1000)));
        }
        assertThat(dim.checksum()).isEqualTo("fdac75890241e7aa");

        ResultSummary dim2 = new ResultSummary(new String[] {"dim2_id", "label", "tier", "score"});
        for (int i = 0; i < 1000; i++) {
            dim2.add(RowFactory.create(i, "dim2-" + i, (int) below(401, i, 10), (int) below(402, i, 1000)));
        }
        assertThat(dim2.checksum()).isEqualTo("a33236c29291b316");
    }

    @Test
    void checksumDoesNotDependOnRowOrColumnOrder() {
        ResultSummary a = new ResultSummary(new String[] {"x", "y"});
        a.add(RowFactory.create(1L, "a"));
        a.add(RowFactory.create(2L, "b"));
        ResultSummary b = new ResultSummary(new String[] {"y", "x"});
        b.add(RowFactory.create("b", 2L));
        ResultSummary c = new ResultSummary(new String[] {"y", "x"});
        c.add(RowFactory.create("a", 1L));
        assertThat(b.merge(c).checksum()).isEqualTo(a.checksum());
        assertThat(a.sums()).containsEntry("x", 3L).doesNotContainKey("y");
    }

    // A fact row as the bulk reader returns it: decimals at Spark's scale 19, uuids as strings,
    // timestamps as java.sql.Timestamp, collections as Scala collections.
    private static Row factRow(long i) {
        long pk = unit(1, i) < 0.01 ? below(2, i, 10) : 10 + i / 100;
        int tagCount = 1 + (int) below(14, i, 3);
        TreeSet<Integer> tags = new TreeSet<>();
        for (int k = 0; k < tagCount; k++) {
            tags.add((int) below(20 + k, i, 100));
        }
        int attrCount = 1 + (int) below(30, i, 3);
        Map<String, Integer> attrs = new TreeMap<>();
        for (int k = 0; k < attrCount; k++) {
            attrs.put("k" + below(40 + k, i, 10), (int) below(50 + k, i, 1000));
        }
        List<Integer> tagList = new ArrayList<>(tags);
        return RowFactory.create(
            pk,
            new Timestamp(BASE_TIME_MILLIS + i * 1000),
            i,
            "region-" + Math.floorMod(pk, 50L),
            (int) of(3, i),
            below(4, i, 1_000_000_000L),
            (int) below(5, i, 10_000),
            "name-" + below(6, i, 1_000_000),
            BigDecimal.valueOf(below(7, i, 10_000_000), 2).setScale(19, RoundingMode.UNNECESSARY),
            new UUID(of(8, i), of(9, i)).toString(),
            below(10, i, REF_ROWS),
            (int) group(11, i, 10),
            (int) group(12, i, 10_000),
            group(13, i, 10_000_000),
            (int) below(15, i, DIM2_KEY_SPACE),
            scala.jdk.javaapi.CollectionConverters.asScala(tagList).toSeq(),
            scala.jdk.javaapi.CollectionConverters.asScala(attrs));
    }
}
