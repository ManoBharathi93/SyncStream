package com.syncstream.retry;

import org.junit.jupiter.api.Test;

import java.net.ConnectException;
import java.net.SocketTimeoutException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DefaultFailureClassifierTest {

    @Test
    void shouldClassifyConnectionFailuresAsRetryable() {
        DefaultFailureClassifier classifier = new DefaultFailureClassifier();

        assertEquals(FailureCategory.RETRYABLE, classifier.classify(new ConnectException("connection refused")));
        assertEquals(FailureCategory.RETRYABLE, classifier.classify(new SocketTimeoutException("read timeout")));
    }

    @Test
    void shouldClassifyInputAndStateErrorsAsNonRetryable() {
        DefaultFailureClassifier classifier = new DefaultFailureClassifier();

        assertEquals(FailureCategory.NON_RETRYABLE, classifier.classify(new IllegalArgumentException("bad payload")));
        assertEquals(FailureCategory.NON_RETRYABLE, classifier.classify(new NumberFormatException("NaN")));
        assertEquals(FailureCategory.NON_RETRYABLE, classifier.classify(new IllegalStateException("invalid state")));
    }

    @Test
    void shouldClassifyJsonParseSignalsAsNonRetryable() {
        DefaultFailureClassifier classifier = new DefaultFailureClassifier();

        assertEquals(FailureCategory.NON_RETRYABLE, classifier.classify(new FakeJsonParseException("bad json")));
        assertEquals(FailureCategory.NON_RETRYABLE, classifier.classify(new FakeJsonMappingException("bad schema")));
    }

    @Test
    void shouldWalkCauseChain() {
        DefaultFailureClassifier classifier = new DefaultFailureClassifier();
        RuntimeException wrapped = new RuntimeException("wrapper", new ConnectException("refused"));

        assertEquals(FailureCategory.RETRYABLE, classifier.classify(wrapped));
    }

    @Test
    void unknownFailuresShouldDefaultToRetryable() {
        DefaultFailureClassifier classifier = new DefaultFailureClassifier();

        assertEquals(FailureCategory.RETRYABLE, classifier.classify(new RuntimeException("unknown")));
        assertTrue(classifier.shouldRetry(new RuntimeException("unknown")));
    }

    @Test
    void shouldRejectNullFailure() {
        DefaultFailureClassifier classifier = new DefaultFailureClassifier();

        assertThrows(IllegalArgumentException.class, () -> classifier.classify(null));
    }

    private static final class FakeJsonParseException extends Exception {
        FakeJsonParseException(String message) {
            super(message);
        }
    }

    private static final class FakeJsonMappingException extends Exception {
        FakeJsonMappingException(String message) {
            super(message);
        }
    }
}
