package com.vishnukanchi.pulsequeue;

import jakarta.validation.Valid;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/jobs")
public class JobController {
    private final JobService service;
    private final JobRepository jobs;
    public JobController(JobService service, JobRepository jobs) { this.service = service; this.jobs = jobs; }
    @PostMapping @ResponseStatus(HttpStatus.ACCEPTED)
    public JobView submit(@Valid @RequestBody SubmitJob request) { return JobView.from(service.submit(request.queueName(), request.payload(), request.maxAttempts())); }
    @GetMapping("/{id}") public JobView get(@PathVariable UUID id) { return jobs.findById(id).map(JobView::from).orElseThrow(() -> new JobNotFound(id)); }
    @GetMapping public List<JobView> list() { return jobs.findAll().stream().map(JobView::from).toList(); }
    @GetMapping("/stats")
    public Map<JobStatus, Long> stats() {
        return Map.of(
            JobStatus.QUEUED, jobs.countByStatus(JobStatus.QUEUED),
            JobStatus.PROCESSING, jobs.countByStatus(JobStatus.PROCESSING),
            JobStatus.SUCCEEDED, jobs.countByStatus(JobStatus.SUCCEEDED),
            JobStatus.RETRY_SCHEDULED, jobs.countByStatus(JobStatus.RETRY_SCHEDULED),
            JobStatus.DEAD_LETTER, jobs.countByStatus(JobStatus.DEAD_LETTER)
        );
    }
    public record SubmitJob(@NotBlank String queueName, @NotBlank String payload, @Min(1) @Max(20) int maxAttempts) { }
    public record JobView(UUID id, String queueName, String payload, JobStatus status, int attempts, int maxAttempts, Instant nextAttemptAt, String lastError, Instant createdAt, Instant completedAt) { static JobView from(Job job) { return new JobView(job.getId(), job.getQueueName(), job.getPayload(), job.getStatus(), job.getAttempts(), job.getMaxAttempts(), job.getNextAttemptAt(), job.getLastError(), job.getCreatedAt(), job.getCompletedAt()); } }
    @ResponseStatus(HttpStatus.NOT_FOUND) static class JobNotFound extends RuntimeException { JobNotFound(UUID id) { super("Job %s was not found".formatted(id)); } }
}
