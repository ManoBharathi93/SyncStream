package com.syncstream.retry;

import java.util.function.DoubleSupplier;

public final class ExponentialBackoffStrategy implements BackoffStrategy {
    private final DoubleSupplier randomDouble;

    public ExponentialBackoffStrategy() {
        this(Math::random);
    }

    public ExponentialBackoffStrategy(DoubleSupplier randomDouble) {
        this.randomDouble = randomDouble;
    }

    @Override
    public long nextDelayMs(int attempt, RetryPolicy policy) {
        if (attempt < 1) {
            throw new IllegalArgumentException("attempt must be >= 1");
        }

        long exponential = safeMultiply(policy.baseDelayMs(), powerOfTwo(attempt - 1));
        long capped = Math.min(exponential, policy.maxDelayMs());

        if (policy.jitterFactor() == 0.0) {
            return capped;
        }

        double jitter = policy.jitterFactor();
        double minFactor = 1.0 - jitter;
        double maxFactor = 1.0 + jitter;
        double factor = minFactor + (maxFactor - minFactor) * randomDouble.getAsDouble();
        long withJitter = (long) Math.floor(capped * factor);

        if (withJitter < 1L) {
            return 1L;
        }
        return Math.min(withJitter, policy.maxDelayMs());
    }

    private long powerOfTwo(int exponent) {
        if (exponent >= 62) {
            return Long.MAX_VALUE;
        }
        return 1L << exponent;
    }

    private long safeMultiply(long a, long b) {
        if (a <= 0 || b <= 0) {
            return 0L;
        }
        if (a > Long.MAX_VALUE / b) {
            return Long.MAX_VALUE;
        }
        return a * b;
    }
}
