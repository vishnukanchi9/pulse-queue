package com.vishnukanchi.pulsequeue;

import jakarta.transaction.Transactional;
import org.springframework.stereotype.Service;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Service
public class JobService {
    private final JobRepository jobs;
    private final QueueGateway queue;
    private final JobProcessor processor;
    public JobService(JobRepository jobs, QueueGateway queue, JobProcessor processor) { this.jobs = jobs; this.queue = queue; this.processor = processor; }
    public Job submit(String queueName, String payload, int maxAttempts) { Job job = jobs.save(new Job(queueName, payload, maxAttempts)); queue.enqueue(job.getId()); return job; }
    @Transactional
    public Optional<Job> claim(UUID id) { return jobs.findById(id).filter(job -> job.getStatus() == JobStatus.QUEUED).map(job -> { job.claim(); return job; }); }
    @Transactional
    public void execute(UUID id) {
        Optional<Job> claimed = claim(id); if (claimed.isEmpty()) return;
        try { processor.process(claimed.get()); succeed(id); } catch (Exception error) { fail(id, error.getMessage()); }
    }
    @Transactional
    public void succeed(UUID id) { jobs.findById(id).ifPresent(Job::succeed); }
    @Transactional
    public void fail(UUID id, String error) { jobs.findById(id).ifPresent(job -> { if (job.getAttempts() >= job.getMaxAttempts()) { job.deadLetter(error); queue.deadLetter(id); } else { Instant retryAt = Instant.now().plus(RetryPolicy.backoff(job.getAttempts())); job.retryAt(retryAt, error); queue.scheduleRetry(id, retryAt.toEpochMilli()); } }); }
    @Transactional
    public boolean markQueued(UUID id) { return jobs.findById(id).filter(job -> job.getStatus() == JobStatus.RETRY_SCHEDULED).map(job -> { job.requeue(); return true; }).orElse(false); }
}
