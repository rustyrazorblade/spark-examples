package com.rustyrazorblade.easydblab.spark;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.List;

/** The JVM a driver or executor runs on: host, Java version, garbage collector, and -XX flags. */
public final class JvmInfo {

    private JvmInfo() {
    }

    /** The garbage collector bean that only generational ZGC registers. */
    static final String GENERATIONAL_ZGC_BEAN = "ZGC Major Cycles";

    /** Returns "host|java version|gc bean names|-XX flags" for this JVM, '|' separated, lists ',' separated. */
    public static String describe() {
        String host;
        try {
            host = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            host = "unknown";
        }
        List<String> gcs = new ArrayList<>();
        for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
            gcs.add(gc.getName());
        }
        List<String> flags = new ArrayList<>();
        for (String arg : ManagementFactory.getRuntimeMXBean().getInputArguments()) {
            if (arg.startsWith("-XX:") && (arg.contains("GC") || arg.contains("ZGenerational") || arg.contains("Heap"))) {
                flags.add(arg);
            }
        }
        return host + "|" + System.getProperty("java.version") + "|" + String.join(",", gcs) + "|" + String.join(",", flags);
    }

    /** Whether a {@link #describe()} string shows generational ZGC. */
    public static boolean isGenerationalZgc(String description) {
        String[] parts = description.split("\\|", -1);
        return parts.length > 2 && parts[2].contains(GENERATIONAL_ZGC_BEAN);
    }
}
