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

import static com.hazelcast.simulator.worker.metronome.MetronomeType.SLEEPING;
import static java.lang.Math.round;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class MetronomeFactory {

    private static final Metronome EMPTY_METRONOME = new EmptyMetronome();

    private MetronomeFactory() {
    }

    /**
     * Creates a {@link Metronome} instance with a fixed millisecond interval of type {@link MetronomeType#SLEEPING}.
     *
     * @param intervalMs wait interval in milliseconds
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedIntervalMs(int intervalMs) {
        return withFixedIntervalMs(intervalMs, SLEEPING);
    }

    /**
     * Creates a {@link Metronome} instance with a fixed frequency in Hz of type {@link MetronomeType#SLEEPING}.
     * <p>
     * If the frequency is 0 Hz the method {@link Metronome#waitForNext()} will have no delay.
     *
     * @param frequency frequency in Hz
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedFrequency(float frequency) {
        return withFixedFrequency(frequency, SLEEPING);
    }

    /**
     * Creates a {@link Metronome} instance with a fixed millisecond interval.
     *
     * @param intervalMs wait interval in milliseconds
     * @param type       {@link MetronomeType} to create
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedIntervalMs(int intervalMs, MetronomeType type) {
        if (intervalMs == 0) {
            return EMPTY_METRONOME;
        }
        switch (type) {
            case BUSY_SPINNING:
                return new BusySpinningMetronome(MILLISECONDS.toNanos(intervalMs), true);
            case SLEEPING:
                return new SleepingMetronome(MILLISECONDS.toNanos(intervalMs), true);
            default:
                return EMPTY_METRONOME;
        }
    }

    /**
     * Creates a {@link Metronome} instance with a fixed frequency in Hz.
     * <p>
     * If the frequency is 0 Hz the method {@link Metronome#waitForNext()} will have no delay.
     *
     * @param frequency frequency in Hz
     * @param type      {@link MetronomeType} to create
     * @return a {@link Metronome} instance
     */
    public static Metronome withFixedFrequency(float frequency, MetronomeType type) {
        if (frequency == 0) {
            return EMPTY_METRONOME;
        }

        long intervalNanos = round((double) SECONDS.toNanos(1) / frequency);
        switch (type) {
            case BUSY_SPINNING:
                return new BusySpinningMetronome(intervalNanos, true);
            case SLEEPING:
                return new SleepingMetronome(intervalNanos, true);
            default:
                return EMPTY_METRONOME;
        }
    }
}
