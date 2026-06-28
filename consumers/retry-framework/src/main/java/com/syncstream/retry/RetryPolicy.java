package com.syncstream.retry;

public final class RetryPolicy {
    private final int maxAttempts;
    private final long baseDelayMs;
    private final long maxDelayMs;
    private final double jitterFactor;

    public RetryPolicy(int maxAttempts, long baseDelayMs, long maxDelayMs, double jitterFactor) {
        if (maxAttempts < 1) {
            throw new IllegalArgumentException("maxAttempts must be >= 1");
        }
        if (baseDelayMs < 1) {
            throw new IllegalArgumentException("baseDelayMs must be >= 1");
        }
        if (maxDelayMs < baseDelayMs) {
            throw new IllegalArgumentException("maxDelayMs must be >= baseDelayMs");
        }
        if (jitterFactor < 0.0 || jitterFactor > 1.0) {
            throw new IllegalArgumentException("jitterFactor must be between 0.0 and 1.0");
        }

        this.maxAttempts = maxAttempts;
        this.baseDelayMs = baseDelayMs;
        this.maxDelayMs = maxDelayMs;
        this.jitterFactor = jitterFactor;
    }

    public static RetryPolicy defaults() {
        return new RetryPolicy(3, 300L, 5_000L, 0.20);
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    public long baseDelayMs() {
        return baseDelayMs;
    }

    public long maxDelayMs() {
        return maxDelayMs;
    }

    public double jitterFactor() {
        return jitterFactor;
    }
}
