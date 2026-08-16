package com.vishnukanchi.pulsequeue;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class JobService {

    private final JobRepository jobs;
    private final QueueGateway queue;
    private final JobProcessor processor;

    public JobService(JobRepository jobs, QueueGateway queue, JobProcessor processor) {
        this.jobs = jobs;
        this.queue = queue;
        this.processor = processor;
    }

    /**
     * Persist a job, then publish its id.
     *
     * <p>These are two systems with no transaction between them, which is the classic dual-write:
     * if Redis is unreachable, or the process dies after the commit and before the push, the job is
     * durable but invisible. Rather than pretend otherwise, the write order is chosen so the
     * failure is recoverable - PostgreSQL is the system of record, Redis is only a delivery hint,
     * and {@link OrphanReaper} re-publishes anything left behind.
     *
     * <p>Doing it the other way round would be unrecoverable: an id in Redis with no row behind it
     * is a job nobody can reconstruct.
     */
    public Job submit(String queueName, String payload, int maxAttempts) {
        Job job = jobs.save(new Job(queueName, payload, maxAttempts));
        try {
            queue.enqueue(job.getId());
        } catch (RuntimeException publishFailure) {
            // Swallowed on purpose. The job is already durable, and the reaper will publish it
            // shortly; failing the caller here would suggest the submission was lost when it
            // was not.
            return job;
        }
        return job;
    }

    /**
     * Take ownership of a job, or decline if someone else already has.
     *
     * <p>The row lock is what makes execution exactly-once even though delivery is at-least-once.
     * The status filter alone is not enough: two workers can both read {@code QUEUED} before either
     * writes.
     */
    @Transactional
    public Optional<Job> claim(UUID id) {
        return jobs.findByIdForUpdate(id)
                .filter(job -> job.getStatus() == JobStatus.QUEUED)
                .map(
                        job -> {
                            job.claim();
                            return job;
                        });
    }

    /**
     * Run a job end to end inside one transaction.
     *
     * <p>The lock taken by {@code claim} is held until this returns, which is deliberate: it is
     * what stops a duplicate delivery from running the payload a second time. The cost is that a
     * slow payload holds a row lock for its duration, which would matter for long-running work.
     */
    @Transactional
    public void execute(UUID id) {
        Optional<Job> claimed = claim(id);
        if (claimed.isEmpty()) {
            return;
        }
        try {
            processor.process(claimed.get());
            claimed.get().succeed();
        } catch (Exception error) {
            settleFailure(claimed.get(), error.getMessage());
        }
    }

    /** Decide between another attempt and the dead-letter queue. */
    private void settleFailure(Job job, String error) {
        if (job.getAttempts() >= job.getMaxAttempts()) {
            job.deadLetter(error);
            queue.deadLetter(job.getId());
            return;
        }
        Instant retryAt = Instant.now().plus(RetryPolicy.backoff(job.getAttempts()));
        job.retryAt(retryAt, error);
        queue.scheduleRetry(job.getId(), retryAt.toEpochMilli());
    }

    @Transactional
    public boolean markQueued(UUID id) {
        return jobs.findByIdForUpdate(id)
                .filter(job -> job.getStatus() == JobStatus.RETRY_SCHEDULED)
                .map(
                        job -> {
                            job.requeue();
                            return true;
                        })
                .orElse(false);
    }

    /**
     * Re-publish jobs that are durable in PostgreSQL but absent from Redis.
     *
     * <p>Republishing something that is in fact still queued is harmless - the claim lock collapses
     * the duplicate - so this does not try to check Redis membership first, which would be an O(n)
     * scan of the ready list on every sweep.
     */
    @Transactional(readOnly = true)
    public List<UUID> republishStale(Instant staleBefore) {
        List<Job> stranded = jobs.findStale(JobStatus.QUEUED, staleBefore);
        stranded.forEach(job -> queue.enqueue(job.getId()));
        return stranded.stream().map(Job::getId).toList();
    }
}
