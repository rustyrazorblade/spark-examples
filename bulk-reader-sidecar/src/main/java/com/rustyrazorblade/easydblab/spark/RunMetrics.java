package com.rustyrazorblade.easydblab.spark;

import org.apache.spark.executor.TaskMetrics;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerTaskEnd;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Adds up the task metrics of the Spark jobs started under one job group, so each run reports the
 * executor time it cost: task count, run time, CPU time, GC time, and shuffle bytes.
 */
public final class RunMetrics extends SparkListener {

    private static final String JOB_GROUP = "spark.jobGroup.id";

    private final Map<Integer, String> stageGroups = new ConcurrentHashMap<>();
    private final Map<String, long[]> totals = new ConcurrentHashMap<>();

    // Index of each total in the long[] kept per group.
    private static final int TASKS = 0;
    private static final int RUN_MS = 1;
    private static final int CPU_NS = 2;
    private static final int GC_MS = 3;
    private static final int SHUFFLE_READ = 4;
    private static final int SHUFFLE_WRITE = 5;
    private static final int RESULT_BYTES = 6;
    private static final int SPILL_BYTES = 7;
    private static final int FAILED_TASKS = 8;

    @Override
    public void onJobStart(SparkListenerJobStart jobStart) {
        String group = jobStart.properties() == null ? null : jobStart.properties().getProperty(JOB_GROUP);
        if (group == null) {
            return;
        }
        scala.collection.Iterator<Object> it = jobStart.stageIds().iterator();
        while (it.hasNext()) {
            stageGroups.put((Integer) it.next(), group);
        }
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd taskEnd) {
        String group = stageGroups.get(taskEnd.stageId());
        if (group == null) {
            return;
        }
        long[] t = totals.computeIfAbsent(group, g -> new long[9]);
        TaskMetrics m = taskEnd.taskMetrics();
        synchronized (t) {
            t[TASKS]++;
            if (!taskEnd.taskInfo().successful()) {
                t[FAILED_TASKS]++;
            }
            if (m != null) {
                t[RUN_MS] += m.executorRunTime();
                t[CPU_NS] += m.executorCpuTime();
                t[GC_MS] += m.jvmGCTime();
                t[SHUFFLE_READ] += m.shuffleReadMetrics().totalBytesRead();
                t[SHUFFLE_WRITE] += m.shuffleWriteMetrics().bytesWritten();
                t[RESULT_BYTES] += m.resultSize();
                t[SPILL_BYTES] += m.diskBytesSpilled();
            }
        }
    }

    /** Returns and forgets the totals of job group {@code group}. */
    public Map<String, Long> take(String group) {
        long[] t = totals.remove(group);
        stageGroups.values().removeIf(group::equals);
        if (t == null) {
            t = new long[9];
        }
        Map<String, Long> out = new LinkedHashMap<>();
        synchronized (t) {
            out.put("tasks", t[TASKS]);
            out.put("failed_tasks", t[FAILED_TASKS]);
            out.put("executor_run_ms", t[RUN_MS]);
            out.put("executor_cpu_ms", t[CPU_NS] / 1_000_000);
            out.put("executor_gc_ms", t[GC_MS]);
            out.put("shuffle_read_bytes", t[SHUFFLE_READ]);
            out.put("shuffle_write_bytes", t[SHUFFLE_WRITE]);
            out.put("result_bytes", t[RESULT_BYTES]);
            out.put("disk_spill_bytes", t[SPILL_BYTES]);
        }
        return out;
    }
}
