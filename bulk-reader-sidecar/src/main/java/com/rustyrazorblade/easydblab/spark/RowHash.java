package com.rustyrazorblade.easydblab.spark;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * The row hash and checksum of cassandra-easy-stress {@code BulkReadDataset} and {@code FullTableScan}.
 *
 * <p>A row hash combines each column name with the column's value in the CQL native protocol
 * encoding, so it does not depend on column order.  A checksum is the wrapping sum of row hashes,
 * so it does not depend on row order either.  A Spark read of a table therefore reports the same
 * checksum as the loader's EXPECTED mode and as a paged CQL scan.
 *
 * <p>This is a port of {@code RowHasher}, {@code hashBytes} and {@code mix64} in
 * cassandra-easy-stress ({@code bulkread/RowChecksum.kt} and {@code bulkread/SplitMix.kt}).
 */
public final class RowHash {

    private static final long FNV_OFFSET = 0xcbf29ce484222325L;
    private static final long FNV_PRIME = 0x100000001b3L;
    private static final long NULL_HASH = 0x5bd1e9955bd1e995L;
    private static final long CELL_PRIME = 0x100000001b3L;

    private RowHash() {
    }

    /** Returns the hash a column named {@code name} contributes before mixing in its value. */
    public static long nameHash(String name) {
        return hashBytes(ByteBuffer.wrap(name.getBytes(StandardCharsets.UTF_8)));
    }

    /** Returns the hash of one cell: the column's name hash and its encoded value, or null for no value. */
    public static long cell(long nameHash, ByteBuffer value) {
        return mix64(nameHash * CELL_PRIME + hashBytes(value));
    }

    /** Returns the row hash from the sum of its cell hashes. */
    public static long row(long cellSum) {
        return mix64(cellSum);
    }

    /** Formats a checksum as 16 lowercase hex digits. */
    public static String format(long checksum) {
        return String.format("%016x", checksum);
    }

    /** FNV-1a over the remaining bytes, read without moving the buffer position, then mixed. */
    static long hashBytes(ByteBuffer value) {
        if (value == null) {
            return NULL_HASH;
        }
        long h = FNV_OFFSET;
        for (int i = value.position(); i < value.limit(); i++) {
            h = (h ^ (value.get(i) & 0xffL)) * FNV_PRIME;
        }
        return mix64(h + value.remaining());
    }

    /** The SplitMix64 finalizer. */
    static long mix64(long z) {
        long x = z;
        x = (x ^ (x >>> 30)) * 0xbf58476d1ce4e5b9L;
        x = (x ^ (x >>> 27)) * 0x94d049bb133111ebL;
        return x ^ (x >>> 31);
    }
}
