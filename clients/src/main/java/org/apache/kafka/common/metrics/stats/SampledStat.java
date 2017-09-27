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

import java.util.ArrayList;
import java.util.List;

/**
 * A SampledStat records a single scalar value measured over one or more samples. Each sample is recorded over a
 * configurable window. The window can be defined by number of events or elapsed time (or both, if both are given the
 * window is complete when <i>either</i> the event count or elapsed time criterion is met).
 * <p>
 * All the samples are combined to produce the measurement. When a window is complete the oldest sample is cleared and
 * recycled to begin recording the next sample.
 * 
 * Subclasses of this class define different statistics measured using this basic pattern.
 */
// 标示一个抽样的度量
// 有多个Sample 通过多个Sample完成对一个值得度量
public abstract class SampledStat implements MeasurableStat {

    private double initialValue; // 指定样本的初始值
    private int current = 0; // 当前使用的Sample的下标
    protected List<Sample> samples; // 保存多个取样

    public SampledStat(double initialValue) {
        this.initialValue = initialValue;
        this.samples = new ArrayList<Sample>(2);
    }

    // 根据时间窗口和事件数使用合适的Sample对象进行记录
    @Override
    public void record(MetricConfig config, double value, long timeMs) {
        // 当前sample
        Sample sample = current(timeMs);
        // 检测是否完成了取样
        if (sample.isComplete(timeMs, config))
            sample = advance(config, timeMs);
        // 更新Sample
        update(sample, config, value, timeMs);
        // 增加事件树
        sample.eventCount += 1;
    }

    // 根据配置指定的Sample数量决定创建新sample还是使用之前的sample对象
    private Sample advance(MetricConfig config, long timeMs) {
        this.current = (this.current + 1) % config.samples();
        // 创建新sample对象
        if (this.current >= samples.size()) {
            Sample sample = newSample(timeMs);
            this.samples.add(sample);
            return sample;
        } else {
            // 重用之前的smple对象
            Sample sample = current(timeMs);
            sample.reset(timeMs);
            return sample;
        }
    }

    protected Sample newSample(long timeMs) {
        return new Sample(this.initialValue, timeMs);
    }

    @Override
    public double measure(MetricConfig config, long now) {
        // 将过去的sample重置
        purgeObsoleteSamples(config, now);
        // 完成计算
        return combine(this.samples, config, now);
    }

    public Sample current(long timeMs) {
        if (samples.size() == 0)
            this.samples.add(newSample(timeMs));
        return this.samples.get(this.current);
    }

    public Sample oldest(long now) {
        if (samples.size() == 0)
            this.samples.add(newSample(now));
        Sample oldest = this.samples.get(0);
        for (int i = 1; i < this.samples.size(); i++) {
            Sample curr = this.samples.get(i);
            if (curr.lastWindowMs < oldest.lastWindowMs)
                oldest = curr;
        }
        return oldest;
    }

    protected abstract void update(Sample sample, MetricConfig config, double value, long timeMs);

    public abstract double combine(List<Sample> samples, MetricConfig config, long now);

    /* Timeout any windows that have expired in the absence of any events */
    protected void purgeObsoleteSamples(MetricConfig config, long now) {
        // 计算过期时长
        long expireAge = config.samples() * config.timeWindowMs();
        for (Sample sample : samples) {
            if (now - sample.lastWindowMs >= expireAge)
                // 检测到sample过期，则将其重置
                sample.reset(now);
        }
    }

    protected static class Sample {
        public double initialValue; // 指定样本的初始值
        public long eventCount; // 记录当前样本的事件树
        public long lastWindowMs; // 记录当前样本的时间窗口开始的时间戳
        public double value; // 记录样本的值

        public Sample(double initialValue, long now) {
            this.initialValue = initialValue;
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }

        public void reset(long now) {
            this.eventCount = 0;
            this.lastWindowMs = now;
            this.value = initialValue;
        }
        // 检测eventCount和lastWindows决定当前样本是否已经取样完成
        public boolean isComplete(long timeMs, MetricConfig config) {
            return timeMs - lastWindowMs >= config.timeWindowMs() || eventCount >= config.eventWindow();
        }
    }

}
