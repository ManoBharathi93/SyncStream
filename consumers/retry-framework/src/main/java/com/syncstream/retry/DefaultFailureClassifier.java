package com.syncstream.retry;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public final class DefaultFailureClassifier implements FailureClassifier {
    private static final Set<Class<?>> NON_RETRYABLE_TYPES = new HashSet<Class<?>>(Arrays.<Class<?>>asList(
        IllegalArgumentException.class,
        NumberFormatException.class,
        ClassCastException.class,
        UnsupportedOperationException.class,
        IllegalStateException.class,
        InterruptedException.class
    ));

    private static final Set<Class<?>> RETRYABLE_TYPES = new HashSet<Class<?>>(Arrays.<Class<?>>asList(
        ConnectException.class,
        SocketTimeoutException.class,
        TimeoutException.class
    ));

    @Override
    public FailureCategory classify(Throwable failure) {
        if (failure == null) {
            throw new IllegalArgumentException("failure must not be null");
        }

        Throwable current = failure;
        while (current != null) {
            if (isNonRetryable(current)) {
                return FailureCategory.NON_RETRYABLE;
            }
            if (isRetryable(current)) {
                return FailureCategory.RETRYABLE;
            }
            current = current.getCause();
        }

        return FailureCategory.RETRYABLE;
    }

    private boolean isNonRetryable(Throwable failure) {
        for (Class<?> nonRetryableType : NON_RETRYABLE_TYPES) {
            if (nonRetryableType.isAssignableFrom(failure.getClass())) {
                return true;
            }
        }

        String simpleName = failure.getClass().getSimpleName();
        // Keep Jackson out of compile deps while still classifying malformed payload errors fast.
        return simpleName.contains("JsonParseException")
            || simpleName.contains("JsonMappingException")
            || simpleName.contains("MismatchedInputException");
    }

    private boolean isRetryable(Throwable failure) {
        for (Class<?> retryableType : RETRYABLE_TYPES) {
            if (retryableType.isAssignableFrom(failure.getClass())) {
                return true;
            }
        }
        return false;
    }
}
