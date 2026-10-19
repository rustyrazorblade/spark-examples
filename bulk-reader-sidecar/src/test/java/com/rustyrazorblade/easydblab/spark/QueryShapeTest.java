package com.rustyrazorblade.easydblab.spark;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryShapeTest {

    private static final String EXPECTED_100K =
        "{\"table\":\"fact\",\"state\":\"as loaded\",\"rows\":100000,\"partitions\":1010,"
            + "\"checksum\":\"85e40666aebd4f6e\",\"values\":{\"count(*)\":\"100000\",\"sum(i64)\":\"50005083734591\","
            + "\"rows with bucket < 10\":\"117\",\"rows joining fact_dim and fact_dim2\":\"89955\"}}\n"
            + "{\"table\":\"fact_dim\",\"state\":\"as loaded\",\"rows\":10000,\"partitions\":10000,"
            + "\"checksum\":\"fdac75890241e7aa\",\"values\":{}}\n";

    @Test
    void bundledShapesParse() throws IOException {
        List<QueryShape> shapes = QueryShape.parseAll(bundled());
        assertThat(shapes.stream().map(s -> s.id).collect(Collectors.toList())).containsExactly(
            "Q1", "Q2", "Q3", "Q4_0.1", "Q4_1", "Q4_10", "Q4_50", "Q6", "Q7_low", "Q7_mid", "Q7_high", "Q8",
            "Q9", "Q10", "Q11", "Q15", "Q19");
        QueryShape q2 = shapes.get(1);
        assertThat(q2.sink).isEqualTo(QueryShape.Sink.CHECKSUM);
        assertThat(q2.sql).isEqualTo("SELECT * FROM fact");
        assertThat(q2.expectations).extracting(e -> e.metric).containsExactly("rows", "checksum", "sum(i64)");
        QueryShape q19 = shapes.get(shapes.size() - 1);
        assertThat(q19.tables).containsExactly("fact", "fact_dim", "fact_dim2");
        assertThat(q19.sink).isEqualTo(QueryShape.Sink.COLLECT);
    }

    @Test
    void everyBundledExpectationResolves() throws IOException {
        ExpectedValues expected = ExpectedValues.parse(EXPECTED_100K);
        assertThat(expected.resolve("fact.rows")).isEqualTo("100000");
        assertThat(expected.resolve("fact.checksum")).isEqualTo("85e40666aebd4f6e");
        assertThat(expected.resolve("fact.values.rows with bucket < 10")).isEqualTo("117");
        assertThat(expected.resolve("fact.values.rows joining fact_dim and fact_dim2")).isEqualTo("89955");
        assertThat(expected.resolve("100")).isEqualTo("100");
        assertThat(expected.resolve("fact_ref.rows")).isNull();
        assertThat(ExpectedValues.matches("100000", "100000.0")).isTrue();
        assertThat(ExpectedValues.matches("85e40666aebd4f6e", "85e40666aebd4f6f")).isFalse();
    }

    @Test
    void tablesComeFromTheSqlWithoutATablesLine() {
        List<QueryShape> shapes = QueryShape.parseAll(
            "-- shape: J\nSELECT count(*) FROM fact f\n  join fact_ref r ON f.ref_id = r.ref_id;\n");
        assertThat(shapes.get(0).tables).containsExactly("fact", "fact_ref");
    }

    @Test
    void badShapesAreRejected() {
        assertThatThrownBy(() -> QueryShape.parseAll("SELECT 1"))
            .hasMessageContaining("before the first");
        assertThatThrownBy(() -> QueryShape.parseAll("-- shape: A\n-- sink: nowhere\nSELECT 1 FROM t"))
            .hasMessageContaining("unknown sink");
        assertThatThrownBy(() -> QueryShape.parseAll("-- shape: A\nSELECT 1 FROM t\n-- shape: A\nSELECT 2 FROM t"))
            .hasMessageContaining("defined twice");
        assertThatThrownBy(() -> QueryShape.parseAll("-- shape: A\n-- expect: rows\nSELECT 1 FROM t"))
            .hasMessageContaining("expect needs");
    }

    private static String bundled() throws IOException {
        try (InputStream in = QueryShapeTest.class.getClassLoader().getResourceAsStream(BulkReadBenchmark.DEFAULT_SHAPES)) {
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
