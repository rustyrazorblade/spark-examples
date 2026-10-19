package com.rustyrazorblade.easydblab.spark;

import java.io.Serializable;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Encodes a value read by Spark into its CQL native protocol bytes, the encoding the row hash of
 * cassandra-easy-stress is defined on.
 *
 * <p>Spark loses some CQL type information: a {@code uuid} arrives as a string, and a
 * {@code decimal} arrives with Spark's scale instead of its own.  A {@link CqlType} supplies what
 * is missing.  Without one, the encoding follows the Spark value's Java class.
 */
public final class CqlValueEncoder {

    private CqlValueEncoder() {
    }

    /** A CQL column type, with the decimal scale of the stored values for {@code decimal}. */
    public static final class CqlType implements Serializable {
        private static final long serialVersionUID = 1L;

        final String name;
        final int decimalScale;
        final CqlType element;
        final CqlType value;

        private CqlType(String name, int decimalScale, CqlType element, CqlType value) {
            this.name = name;
            this.decimalScale = decimalScale;
            this.element = element;
            this.value = value;
        }

        /** A scalar type such as {@code bigint}, {@code text}, {@code uuid}, or {@code timestamp}. */
        public static CqlType of(String name) {
            return new CqlType(name, -1, null, null);
        }

        /** A {@code decimal} whose stored values all have {@code scale}. */
        public static CqlType decimal(int scale) {
            return new CqlType("decimal", scale, null, null);
        }

        /** A {@code set} (frozen or not) of {@code element}. */
        public static CqlType set(CqlType element) {
            return new CqlType("set", -1, element, null);
        }

        /** A {@code list} (frozen or not) of {@code element}. */
        public static CqlType list(CqlType element) {
            return new CqlType("list", -1, element, null);
        }

        /** A {@code map} (frozen or not) from {@code key} to {@code value}. */
        public static CqlType map(CqlType key, CqlType value) {
            return new CqlType("map", -1, key, value);
        }
    }

    /** Returns the CQL encoding of {@code v}, or null for a null value.  {@code type} may be null. */
    public static ByteBuffer encode(Object v, CqlType type) {
        if (v == null) {
            return null;
        }
        String name = type == null ? null : type.name;
        if (name != null) {
            switch (name) {
                case "uuid":
                case "timeuuid":
                    return uuid(v instanceof UUID ? (UUID) v : UUID.fromString(v.toString()));
                case "decimal":
                    return decimal(((BigDecimal) v), type.decimalScale);
                case "bigint":
                case "counter":
                    return ByteBuffer.allocate(8).putLong(0, ((Number) v).longValue());
                case "int":
                    return ByteBuffer.allocate(4).putInt(0, ((Number) v).intValue());
                case "smallint":
                    return ByteBuffer.allocate(2).putShort(0, ((Number) v).shortValue());
                case "tinyint":
                    return ByteBuffer.wrap(new byte[] {((Number) v).byteValue()});
                case "set":
                case "list":
                    return collection(elements(v), type.element, "set".equals(name));
                case "map":
                    return map(v, type.element, type.value);
                default:
                    break;
            }
        }
        return byClass(v);
    }

    private static ByteBuffer byClass(Object v) {
        if (v instanceof Long) {
            return ByteBuffer.allocate(8).putLong(0, (Long) v);
        }
        if (v instanceof Integer) {
            return ByteBuffer.allocate(4).putInt(0, (Integer) v);
        }
        if (v instanceof Short) {
            return ByteBuffer.allocate(2).putShort(0, (Short) v);
        }
        if (v instanceof Byte) {
            return ByteBuffer.wrap(new byte[] {(Byte) v});
        }
        if (v instanceof String) {
            return ByteBuffer.wrap(((String) v).getBytes(StandardCharsets.UTF_8));
        }
        if (v instanceof Timestamp) {
            return ByteBuffer.allocate(8).putLong(0, ((Timestamp) v).getTime());
        }
        if (v instanceof Instant) {
            return ByteBuffer.allocate(8).putLong(0, ((Instant) v).toEpochMilli());
        }
        if (v instanceof BigDecimal) {
            return decimal((BigDecimal) v, -1);
        }
        if (v instanceof Double) {
            return ByteBuffer.allocate(8).putDouble(0, (Double) v);
        }
        if (v instanceof Float) {
            return ByteBuffer.allocate(4).putFloat(0, (Float) v);
        }
        if (v instanceof Boolean) {
            return ByteBuffer.wrap(new byte[] {(byte) ((Boolean) v ? 1 : 0)});
        }
        if (v instanceof byte[]) {
            return ByteBuffer.wrap((byte[]) v);
        }
        if (v instanceof Date) {
            return date(((Date) v).toLocalDate());
        }
        if (v instanceof LocalDate) {
            return date((LocalDate) v);
        }
        if (v instanceof scala.collection.Map || v instanceof Map) {
            return map(v, null, null);
        }
        if (v instanceof scala.collection.Iterable || v instanceof Collection) {
            return collection(elements(v), null, false);
        }
        return ByteBuffer.wrap(v.toString().getBytes(StandardCharsets.UTF_8));
    }

    private static ByteBuffer uuid(UUID u) {
        return ByteBuffer.allocate(16).putLong(0, u.getMostSignificantBits()).putLong(8, u.getLeastSignificantBits());
    }

    // CQL decimal: a 4-byte scale, then the unscaled value in two's complement.
    private static ByteBuffer decimal(BigDecimal d, int scale) {
        BigDecimal value = scale >= 0 ? d.setScale(scale, RoundingMode.UNNECESSARY) : d.stripTrailingZeros();
        byte[] unscaled = value.unscaledValue().toByteArray();
        ByteBuffer out = ByteBuffer.allocate(4 + unscaled.length);
        out.putInt(value.scale()).put(unscaled).flip();
        return out;
    }

    // CQL date: days since the epoch, shifted by 2^31, as an unsigned 4-byte value.
    private static ByteBuffer date(LocalDate d) {
        long days = d.toEpochDay() + (1L << 31);
        return ByteBuffer.allocate(4).putInt(0, (int) days);
    }

    private static List<Object> elements(Object v) {
        if (v instanceof scala.collection.Iterable) {
            List<Object> out = new ArrayList<>();
            scala.collection.Iterator<?> it = ((scala.collection.Iterable<?>) v).iterator();
            while (it.hasNext()) {
                out.add(it.next());
            }
            return out;
        }
        return new ArrayList<>((Collection<?>) v);
    }

    // A set is stored sorted by Cassandra's comparator; a list keeps its order.
    private static ByteBuffer collection(List<Object> items, CqlType elementType, boolean sorted) {
        List<ByteBuffer> encoded = new ArrayList<>(items.size());
        for (Object item : items) {
            encoded.add(encode(item, elementType));
        }
        if (sorted) {
            encoded.sort(cassandraOrder(elementType));
        }
        return serialize(encoded, null);
    }

    private static ByteBuffer map(Object v, CqlType keyType, CqlType valueType) {
        List<ByteBuffer[]> entries = new ArrayList<>();
        if (v instanceof scala.collection.Map) {
            scala.collection.Iterator<? extends scala.Tuple2<?, ?>> it = ((scala.collection.Map<?, ?>) v).iterator();
            while (it.hasNext()) {
                scala.Tuple2<?, ?> e = it.next();
                entries.add(new ByteBuffer[] {encode(e._1(), keyType), encode(e._2(), valueType)});
            }
        } else {
            for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                entries.add(new ByteBuffer[] {encode(e.getKey(), keyType), encode(e.getValue(), valueType)});
            }
        }
        Comparator<ByteBuffer> keyOrder = cassandraOrder(keyType);
        entries.sort((a, b) -> keyOrder.compare(a[0], b[0]));
        List<ByteBuffer> flat = new ArrayList<>(entries.size() * 2);
        for (ByteBuffer[] e : entries) {
            flat.add(e[0]);
            flat.add(e[1]);
        }
        return serialize(flat, entries.size());
    }

    // Native protocol v3+ collection: a 4-byte element count, then each element as [4-byte length][bytes].
    private static ByteBuffer serialize(List<ByteBuffer> values, Integer count) {
        int size = 4;
        for (ByteBuffer b : values) {
            size += 4 + b.remaining();
        }
        ByteBuffer out = ByteBuffer.allocate(size);
        out.putInt(count == null ? values.size() : count);
        for (ByteBuffer b : values) {
            out.putInt(b.remaining()).put(b.duplicate());
        }
        out.flip();
        return out;
    }

    // Cassandra sorts int, bigint, smallint and tinyint as signed numbers, and text and blobs as unsigned bytes.
    private static Comparator<ByteBuffer> cassandraOrder(CqlType type) {
        String name = type == null ? "" : type.name;
        switch (name) {
            case "int":
                return Comparator.comparingInt(b -> b.getInt(b.position()));
            case "bigint":
                return Comparator.comparingLong(b -> b.getLong(b.position()));
            case "smallint":
                return Comparator.comparingInt(b -> b.getShort(b.position()));
            case "tinyint":
                return Comparator.comparingInt(b -> b.get(b.position()));
            default:
                return CqlValueEncoder::compareUnsigned;
        }
    }

    private static int compareUnsigned(ByteBuffer a, ByteBuffer b) {
        int n = Math.min(a.remaining(), b.remaining());
        for (int i = 0; i < n; i++) {
            int c = Integer.compare(a.get(a.position() + i) & 0xff, b.get(b.position() + i) & 0xff);
            if (c != 0) {
                return c;
            }
        }
        return Integer.compare(a.remaining(), b.remaining());
    }
}
