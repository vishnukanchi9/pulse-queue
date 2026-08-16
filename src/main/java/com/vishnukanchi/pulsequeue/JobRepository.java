package com.vishnukanchi.pulsequeue;

import jakarta.persistence.LockModeType;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface JobRepository extends JpaRepository<Job, UUID> {

    long countByStatus(JobStatus status);

    /**
     * Read a job with a pessimistic write lock, so only one worker can claim it.
     *
     * <p>Without the lock, two workers can both read status {@code QUEUED} before either commits
     * and both run the payload. That was invisible while a single scheduled thread polled Redis and
     * {@code rightPop} handed each id out once - but the orphan reaper deliberately re-delivers ids,
     * and a second instance would too, so delivery is at-least-once and the claim has to be the
     * thing that makes execution exactly-once.
     */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select j from Job j where j.id = :id")
    Optional<Job> findByIdForUpdate(@Param("id") UUID id);

    /**
     * Jobs stranded in {@code QUEUED} with nothing in Redis to pick them up.
     *
     * <p>{@code submit} writes to PostgreSQL and then to Redis. Those are two systems and there is
     * no transaction spanning them, so a Redis outage or a crash between the two leaves a durable
     * job row that no worker will ever see. This query is how they are found again.
     */
    @Query("select j from Job j where j.status = :status and j.updatedAt < :staleBefore")
    List<Job> findStale(@Param("status") JobStatus status, @Param("staleBefore") Instant staleBefore);
}
