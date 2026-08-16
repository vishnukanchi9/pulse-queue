package com.vishnukanchi.pulsequeue;

import java.time.Duration;

public final class RetryPolicy {
    private RetryPolicy() { }
    public static Duration backoff(int attempt) {
        long seconds = Math.min(300, 1L << Math.min(19, Math.max(0, attempt - 1)));
        return Duration.ofSeconds(seconds);
    }
}
