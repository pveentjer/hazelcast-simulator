/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hazelcast.simulator.worker.metronome;

import com.hazelcast.simulator.utils.Preconditions;

import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class MetronomeBuilder {

    private MetronomeType type = MetronomeType.BUSY_SPINNING;
    private boolean accountForCoordinatedOmission = true;
    private long intervalNanos;

    public MetronomeBuilder() {
    }

    public MetronomeBuilder withMetronomeType(MetronomeType type) {
        this.type = Preconditions.checkNotNull(type, "type cant be null");
        return this;
    }

    public MetronomeBuilder withRatePerSecond(double ratePerSecond) {
        if (ratePerSecond <= 0) {
            throw new IllegalArgumentException("ratePerSecond can't");
        }

        long intervalMicros = Math.round(TimeUnit.MICROSECONDS.toNanos(1) / ratePerSecond);
        return withIntervalUs(intervalMicros);
    }

    public MetronomeBuilder withIntervalUs(long intervalUs) {
        if (intervalUs < 0) {
            throw new IllegalArgumentException("intervalUs can't be smaller than 0, but was:" + intervalUs);
        }
        this.intervalNanos = MICROSECONDS.toNanos(intervalUs);
        return this;
    }

    public MetronomeBuilder withAccountForCoordinatedOmission(boolean accountForCoordinatedOmission) {
        this.accountForCoordinatedOmission = accountForCoordinatedOmission;
        return this;
    }

    public Metronome build() {
        switch (type) {
            case NOP:
                return new EmptyMetronome();
            case BUSY_SPINNING:
                return new BusySpinningMetronome(intervalNanos, accountForCoordinatedOmission);
            case SLEEPING:
                return new SleepingMetronome(intervalNanos, accountForCoordinatedOmission);
            default:
                throw new IllegalStateException("Unrecognized metronomeType:" + type);
        }
    }

    public Class<? extends Metronome> getMetronomeClass() {
        return build().getClass();
    }
}
