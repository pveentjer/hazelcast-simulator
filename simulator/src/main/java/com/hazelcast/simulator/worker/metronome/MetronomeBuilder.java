package com.hazelcast.simulator.worker.metronome;

import com.hazelcast.simulator.utils.Preconditions;

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
        return this;
    }

    public MetronomeBuilder withIntervalUs(long intervalUs) {
        if (intervalUs < 0) {
            throw new IllegalArgumentException("intervalUs can't be smaller than 0, but was:" + intervalUs);
        }
        this.intervalNanos = MICROSECONDS.toNanos(intervalUs);
        return this;
    }

    public MetronomeBuilder withDelayUs(long delayUs) {
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
