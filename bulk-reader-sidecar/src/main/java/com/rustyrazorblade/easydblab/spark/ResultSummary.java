package com.rustyrazorblade.easydblab.spark;

import com.rustyrazorblade.easydblab.spark.CqlValueEncoder.CqlType;
import org.apache.spark.sql.Row;

import java.io.Serializable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * What a query shape's result held: the row count, the order-independent checksum of the rows,
 * and the sum of every integral column.  Summaries of disjoint row sets merge in any order, so
 * each Spark partition builds its own and the driver adds them up.
 */
public final class ResultSummary implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String[] columns;
    private final long[] nameHashes;
    private final CqlType[] types;
    private final long[] sums;
    private final boolean[] integral;
    private long rows;
    private long checksum;

    /** Starts an empty summary of rows with these column names. */
    public ResultSummary(String[] columns) {
        this.columns = columns.clone();
        this.nameHashes = new long[columns.length];
        this.types = new CqlType[columns.length];
        this.sums = new long[columns.length];
        this.integral = new boolean[columns.length];
        for (int i = 0; i < columns.length; i++) {
            nameHashes[i] = RowHash.nameHash(columns[i]);
            types[i] = DatasetSchema.typeOf(columns[i]);
            integral[i] = true;
        }
    }

    /** Adds one row; its values must be in the order of the column names. */
    public void add(Row row) {
        rows++;
        long cellSum = 0;
        for (int i = 0; i < columns.length; i++) {
            Object v = row.get(i);
            cellSum += RowHash.cell(nameHashes[i], CqlValueEncoder.encode(v, types[i]));
            if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
                sums[i] += ((Number) v).longValue();
            } else if (v != null) {
                integral[i] = false;
            }
        }
        checksum += RowHash.row(cellSum);
    }

    /** Adds every row of {@code it}, then returns this summary. */
    public ResultSummary addAll(Iterator<Row> it) {
        while (it.hasNext()) {
            add(it.next());
        }
        return this;
    }

    /** Adds the rows {@code other} summarized; both must have the same columns. */
    public ResultSummary merge(ResultSummary other) {
        rows += other.rows;
        checksum += other.checksum;
        for (int i = 0; i < columns.length; i++) {
            sums[i] += other.sums[i];
            integral[i] &= other.integral[i];
        }
        return this;
    }

    public long rows() {
        return rows;
    }

    /** The checksum as 16 hex digits. */
    public String checksum() {
        return RowHash.format(checksum);
    }

    /** The sum of each column whose values are all integers, by column name. */
    public Map<String, Long> sums() {
        Map<String, Long> out = new LinkedHashMap<>();
        for (int i = 0; i < columns.length; i++) {
            if (integral[i]) {
                out.put(columns[i], sums[i]);
            }
        }
        return out;
    }
}
