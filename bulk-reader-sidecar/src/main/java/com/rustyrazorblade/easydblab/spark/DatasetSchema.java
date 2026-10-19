package com.rustyrazorblade.easydblab.spark;

import com.rustyrazorblade.easydblab.spark.CqlValueEncoder.CqlType;

import java.util.HashMap;
import java.util.Map;

/**
 * The CQL types of the cassandra-easy-stress {@code BulkReadDataset} STANDARD tables
 * ({@code bulkread/DatasetTables.kt}), for the columns Spark cannot encode back to CQL bytes by
 * itself.  A column that is not listed is encoded from its Spark value.
 *
 * <p>Column names are unique across the tables with the same type, except where noted, so one map
 * serves every table and every query result that keeps a column's name.
 */
public final class DatasetSchema {

    private static final Map<String, CqlType> TYPES = new HashMap<>();

    static {
        // fact
        TYPES.put("pk", CqlType.of("bigint"));
        TYPES.put("event_time", CqlType.of("timestamp"));
        TYPES.put("row_id", CqlType.of("bigint"));
        TYPES.put("region", CqlType.of("text"));
        TYPES.put("i32", CqlType.of("int"));
        TYPES.put("i64", CqlType.of("bigint"));
        TYPES.put("bucket", CqlType.of("int"));
        TYPES.put("name", CqlType.of("text"));
        // The loader writes every amount with scale 2; Spark reads it back with its own scale.
        TYPES.put("amount", CqlType.decimal(2));
        TYPES.put("uid", CqlType.of("uuid"));
        TYPES.put("ref_id", CqlType.of("bigint"));
        TYPES.put("g_low", CqlType.of("int"));
        TYPES.put("g_mid", CqlType.of("int"));
        TYPES.put("g_high", CqlType.of("bigint"));
        TYPES.put("dim2_id", CqlType.of("int"));
        TYPES.put("tags", CqlType.set(CqlType.of("int")));
        TYPES.put("attrs", CqlType.map(CqlType.of("text"), CqlType.of("int")));
        // fact_ref (label, score, created), fact_dim (name, category, weight), fact_dim2 (label, tier, score)
        TYPES.put("label", CqlType.of("text"));
        TYPES.put("score", CqlType.of("int"));
        TYPES.put("created", CqlType.of("timestamp"));
        TYPES.put("category", CqlType.of("int"));
        TYPES.put("weight", CqlType.of("int"));
        TYPES.put("tier", CqlType.of("int"));
    }

    private DatasetSchema() {
    }

    /** Returns the CQL type of the dataset column {@code name}, or null when it is not a dataset column. */
    public static CqlType typeOf(String name) {
        return TYPES.get(name);
    }
}
