package com.vishnukanchi.pulsequeue;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "jobs")
public class Job {
    @Id private UUID id;
    @Column(name = "queue_name", nullable = false) private String queueName;
    @Column(nullable = false, columnDefinition = "text") private String payload;
    @Enumerated(EnumType.STRING) @Column(nullable = false) private JobStatus status;
    @Column(nullable = false) private int attempts;
    @Column(name = "max_attempts", nullable = false) private int maxAttempts;
    @Column(name = "next_attempt_at") private Instant nextAttemptAt;
    @Column(name = "last_error", columnDefinition = "text") private String lastError;
    @Column(name = "created_at", nullable = false) private Instant createdAt;
    @Column(name = "updated_at", nullable = false) private Instant updatedAt;
    @Column(name = "completed_at") private Instant completedAt;
    protected Job() { }
    public Job(String queueName, String payload, int maxAttempts) { id = UUID.randomUUID(); this.queueName = queueName; this.payload = payload; this.maxAttempts = maxAttempts; status = JobStatus.QUEUED; createdAt = updatedAt = Instant.now(); }
    public UUID getId() { return id; } public String getQueueName() { return queueName; } public String getPayload() { return payload; } public JobStatus getStatus() { return status; } public int getAttempts() { return attempts; } public int getMaxAttempts() { return maxAttempts; } public Instant getNextAttemptAt() { return nextAttemptAt; } public String getLastError() { return lastError; } public Instant getCreatedAt() { return createdAt; } public Instant getCompletedAt() { return completedAt; }
    public void claim() { status = JobStatus.PROCESSING; attempts++; updatedAt = Instant.now(); }
    public void requeue() { status = JobStatus.QUEUED; nextAttemptAt = null; updatedAt = Instant.now(); }
    public void succeed() { status = JobStatus.SUCCEEDED; completedAt = updatedAt = Instant.now(); nextAttemptAt = null; }
    public void retryAt(Instant when, String error) { status = JobStatus.RETRY_SCHEDULED; nextAttemptAt = when; lastError = error; updatedAt = Instant.now(); }
    public void deadLetter(String error) { status = JobStatus.DEAD_LETTER; lastError = error; updatedAt = Instant.now(); nextAttemptAt = null; }
}
