package com.syncstream.retry;

public interface FailureClassifier {
    FailureCategory classify(Throwable failure);

    default boolean shouldRetry(Throwable failure) {
        return classify(failure) == FailureCategory.RETRYABLE;
    }
}
