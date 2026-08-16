package com.vishnukanchi.pulsequeue;

import static org.junit.jupiter.api.Assertions.assertEquals;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RetryPolicyTest {
    @Test void backsOffExponentiallyAndCapsAtFiveMinutes() {
        assertEquals(Duration.ofSeconds(1), RetryPolicy.backoff(1));
        assertEquals(Duration.ofSeconds(8), RetryPolicy.backoff(4));
        assertEquals(Duration.ofSeconds(300), RetryPolicy.backoff(20));
    }
}
