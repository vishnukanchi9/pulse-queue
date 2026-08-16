package com.vishnukanchi.pulsequeue;

import org.springframework.stereotype.Component;

@Component
public class JobProcessor {
    public void process(Job job) {
        // A deterministic failure hook makes retry/DLQ behavior demonstrable without an external vendor.
        if (job.getPayload().contains("\"simulateFailure\":true")) throw new IllegalStateException("Simulated downstream failure");
    }
}
