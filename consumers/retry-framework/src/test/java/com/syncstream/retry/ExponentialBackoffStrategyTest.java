package com.syncstream.retry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExponentialBackoffStrategyTest {

    @Test
    void shouldIncreaseExponentiallyWithoutJitter() {
        RetryPolicy policy = new RetryPolicy(5, 100L, 10_000L, 0.0);
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy();

        assertEquals(100L, strategy.nextDelayMs(1, policy));
        assertEquals(200L, strategy.nextDelayMs(2, policy));
        assertEquals(400L, strategy.nextDelayMs(3, policy));
        assertEquals(800L, strategy.nextDelayMs(4, policy));
    }

    @Test
    void shouldCapAtMaxDelay() {
        RetryPolicy policy = new RetryPolicy(6, 300L, 1_000L, 0.0);
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy();

        assertEquals(300L, strategy.nextDelayMs(1, policy));
        assertEquals(600L, strategy.nextDelayMs(2, policy));
        assertEquals(1_000L, strategy.nextDelayMs(3, policy));
        assertEquals(1_000L, strategy.nextDelayMs(6, policy));
    }

    @Test
    void shouldApplyDeterministicJitterRange() {
        RetryPolicy policy = new RetryPolicy(3, 1_000L, 5_000L, 0.20);

        ExponentialBackoffStrategy minJitter = new ExponentialBackoffStrategy(() -> 0.0);
        ExponentialBackoffStrategy maxJitter = new ExponentialBackoffStrategy(() -> 1.0);

        long min = minJitter.nextDelayMs(1, policy);
        long max = maxJitter.nextDelayMs(1, policy);

        assertEquals(800L, min);
        assertEquals(1_200L, max);
    }

    @Test
    void shouldRejectInvalidAttempt() {
        RetryPolicy policy = RetryPolicy.defaults();
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy();

        assertThrows(IllegalArgumentException.class, () -> strategy.nextDelayMs(0, policy));
    }

    @Test
    void jitteredDelayShouldStillBePositiveAndBounded() {
        RetryPolicy policy = new RetryPolicy(3, 100L, 500L, 0.5);
        ExponentialBackoffStrategy strategy = new ExponentialBackoffStrategy(() -> 0.25);

        long delay = strategy.nextDelayMs(2, policy);
        assertTrue(delay >= 1L);
        assertTrue(delay <= 500L);
    }
}
