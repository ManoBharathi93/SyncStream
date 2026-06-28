package com.syncstream.retry;

public interface BackoffStrategy {
    long nextDelayMs(int attempt, RetryPolicy policy);
}
