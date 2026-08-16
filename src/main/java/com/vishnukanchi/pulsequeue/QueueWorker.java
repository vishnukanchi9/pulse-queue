package com.vishnukanchi.pulsequeue;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.Set;
import java.util.UUID;

@Component
public class QueueWorker {
    private final QueueGateway queue;
    private final JobService jobs;
    private final boolean enabled;
    public QueueWorker(QueueGateway queue, JobService jobs, @Value("${queue.worker-enabled:true}") boolean enabled) { this.queue = queue; this.jobs = jobs; this.enabled = enabled; }
    @Scheduled(fixedDelayString = "${queue.poll-ms:250}")
    public void pollReadyQueue() { if (!enabled) return; String value = queue.poll(); if (value != null) jobs.execute(UUID.fromString(value)); }
    @Scheduled(fixedDelayString = "${queue.retry-poll-ms:1000}")
    public void promoteDueRetries() { if (!enabled) return; Set<String> due = queue.dueRetries(System.currentTimeMillis()); if (due == null) return; for (String value : due) { UUID id = UUID.fromString(value); if (jobs.markQueued(id)) queue.enqueue(id); queue.removeRetry(value); } }
}
