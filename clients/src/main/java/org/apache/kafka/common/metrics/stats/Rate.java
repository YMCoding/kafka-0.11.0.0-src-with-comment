/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.metrics.stats;

import org.apache.kafka.common.metrics.MeasurableStat;
import org.apache.kafka.common.metrics.MetricConfig;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;


/**
 * The rate of the given quantity. By default this is the total observed over a set of samples from a sampled statistic
 * divided by the elapsed time over the sample windows. Alternative {@link SampledStat} implementations can be provided,
 * however, to record the rate of occurrences (e.g. the count of values measured over the time interval) or other such
 * values.
 */
// 用于记录比率，每分钟创建连接的个数
public class Rate implements MeasurableStat {

    protected final TimeUnit unit;
    protected final SampledStat stat;

    public Rate() {
        this(TimeUnit.SECONDS);
    }

    public Rate(TimeUnit unit) {
        this(unit, new SampledTotal());
    }

    public Rate(SampledStat stat) {
        this(TimeUnit.SECONDS, stat);
    }

    public Rate(TimeUnit unit, SampledStat stat) {
        this.stat = stat;
        this.unit = unit;
    }

    public String unitName() {
        return unit.name().substring(0, unit.name().length() - 2).toLowerCase(Locale.ROOT);
    }

    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        // 委托给其中封装的SampledStat对象
        this.stat.record(config, value, timeMs);
    }
    // 获取其stat字段的记录值并进行相应计算得到的最终比率
    @Override
    public double measure(MetricConfig config, long now) {
        double value = stat.measure(config, now);
        return value / convert(windowSize(config, now));
    }

    public long windowSize(MetricConfig config, long now) {
        // purge old samples before we compute the window size
        // 重置所有已经过期的sample,不参与总时间的计算
        stat.purgeObsoleteSamples(config, now);

        /*
         * Here we check the total amount of time elapsed since the oldest non-obsolete window.
         * This give the total windowSize of the batch which is the time used for Rate computation.
         * However, there is an issue if we do not have sufficient data for e.g. if only 1 second has elapsed in a 30 second
         * window, the measured rate will be very high.
         * Hence we assume that the elapsed time is always N-1 complete windows plus whatever fraction of the final window is complete.
         *
         * Note that we could simply count the amount of time elapsed in the current window and add n-1 windows to get the total time,
         * but this approach does not account for sleeps. SampledStat only creates samples whenever record is called,
         * if no record is called for a period of time that time is not accounted for in windowSize and produces incorrect results.
         */
        // 获得最旧的sample对象，从而获得总时间
        long totalElapsedTimeMs = now - stat.oldest(now).lastWindowMs;
        // Check how many full windows of data we have currently retained
        // 有多少完整的时间窗口，就有多少完成的Sample
        int numFullWindows = (int) (totalElapsedTimeMs / config.timeWindowMs());
        int minFullWindows = config.samples() - 1;

        // If the available windows are less than the minimum required, add the difference to the totalElapsedTime
        // 对总时间进行补偿
        if (numFullWindows < minFullWindows)
            totalElapsedTimeMs += (minFullWindows - numFullWindows) * config.timeWindowMs();

        return totalElapsedTimeMs;
    }

    private double convert(long timeMs) {
        switch (unit) {
            case NANOSECONDS:
                return timeMs * 1000.0 * 1000.0;
            case MICROSECONDS:
                return timeMs * 1000.0;
            case MILLISECONDS:
                return timeMs;
            case SECONDS:
                return timeMs / 1000.0;
            case MINUTES:
                return timeMs / (60.0 * 1000.0);
            case HOURS:
                return timeMs / (60.0 * 60.0 * 1000.0);
            case DAYS:
                return timeMs / (24.0 * 60.0 * 60.0 * 1000.0);
            default:
                throw new IllegalStateException("Unknown unit: " + unit);
        }
    }

    public static class SampledTotal extends SampledStat {

        public SampledTotal() {
            super(0.0d);
        }

        @Override
        protected void update(Sample sample, MetricConfig config, double value, long timeMs) {
            sample.value += value;
        }

        @Override
        public double combine(List<Sample> samples, MetricConfig config, long now) {
            double total = 0.0;
            for (Sample sample : samples)
                total += sample.value;
            return total;
        }

    }
}
