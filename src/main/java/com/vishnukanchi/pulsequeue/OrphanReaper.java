package com.vishnukanchi.pulsequeue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Closes the gap between PostgreSQL and Redis.
 *
 * <p>{@code JobService.submit} commits the job row and then pushes its id to Redis. There is no
 * transaction spanning the two, so a Redis outage or a crash in between leaves a job that is
 * durable and invisible - it would sit in {@code QUEUED} forever with no worker aware of it.
 *
 * <p>This sweep finds those rows and publishes them again. A job only counts as stranded once it
 * has been sitting in {@code QUEUED} longer than the grace period, which has to comfortably exceed
 * the worker poll interval or the reaper would race healthy jobs that are simply waiting their
 * turn.
 */
@Component
public class OrphanReaper {

    private static final Logger log = LoggerFactory.getLogger(OrphanReaper.class);

    private final JobService jobs;
    private final boolean enabled;
    private final Duration grace;

    public OrphanReaper(
            JobService jobs,
            @Value("${queue.worker-enabled:true}") boolean enabled,
            @Value("${queue.orphan-grace-ms:30000}") long graceMillis) {
        this.jobs = jobs;
        this.enabled = enabled;
        this.grace = Duration.ofMillis(graceMillis);
    }

    @Scheduled(fixedDelayString = "${queue.orphan-sweep-ms:15000}")
    public void republishStrandedJobs() {
        if (!enabled) {
            return;
        }
        List<UUID> republished = jobs.republishStale(Instant.now().minus(grace));
        if (!republished.isEmpty()) {
            // Worth logging loudly: a non-empty sweep means Redis and PostgreSQL diverged, which is
            // the symptom of a real outage rather than routine housekeeping.
            log.warn("republished {} stranded job(s) missing from the ready queue: {}",
                    republished.size(), republished);
        }
    }
}
