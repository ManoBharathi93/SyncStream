package com.syncstream.retry;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RetryPolicyTest {

    @Test
    void defaultsShouldMatchExpectedPolicy() {
        RetryPolicy defaults = RetryPolicy.defaults();

        assertEquals(3, defaults.maxAttempts());
        assertEquals(300L, defaults.baseDelayMs());
        assertEquals(5_000L, defaults.maxDelayMs());
        assertEquals(0.20, defaults.jitterFactor(), 0.0001);
    }

    @Test
    void constructorShouldRejectInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(0, 100, 1000, 0.2));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(3, 0, 1000, 0.2));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(3, 500, 100, 0.2));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(3, 100, 1000, -0.1));
        assertThrows(IllegalArgumentException.class, () -> new RetryPolicy(3, 100, 1000, 1.1));
    }
}
