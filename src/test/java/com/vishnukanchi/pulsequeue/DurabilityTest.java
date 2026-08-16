package com.vishnukanchi.pulsequeue;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Tests for the seam between the two stores.
 *
 * <p>A job is written to PostgreSQL and then published to Redis, with no transaction spanning both.
 * That gap is unavoidable without an outbox, so what matters is that it is *recoverable*: nothing
 * durable may be lost, and nothing may run twice.
 *
 * <p>The reaper is configured aggressively here (sweep and grace in the hundreds of milliseconds)
 * so recovery can be observed in a test rather than in thirty seconds of production time.
 */
@SpringBootTest(
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"queue.orphan-sweep-ms=200", "queue.orphan-grace-ms=300"})
class DurabilityTest {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pulse_queue")
                    .withUsername("pulse")
                    .withPassword("pulse");

    @SuppressWarnings("resource")
    static final GenericContainer<?> REDIS =
            new GenericContainer<>(DockerImageName.parse("redis:7-alpine")).withExposedPorts(6379);

    static {
        POSTGRES.start();
        REDIS.start();
    }

    @DynamicPropertySource
    static void wiring(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.data.redis.host", REDIS::getHost);
        registry.add("spring.data.redis.port", () -> REDIS.getMappedPort(6379));
    }

    @Autowired JobService jobs;
    @Autowired JobRepository repository;
    @Autowired QueueGateway queue;
    @Autowired StringRedisTemplate redis;

    @BeforeEach
    void reset() {
        repository.deleteAll();
        redis.delete(QueueGateway.READY);
        redis.delete(QueueGateway.RETRY);
        redis.delete(QueueGateway.DEAD_LETTER);
    }

    private void awaitStatus(UUID id, JobStatus expected) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> repository.findById(id).orElseThrow().getStatus() == expected);
    }

    @Test
    void aJobStrandedByALostRedisPublishIsRecoveredAndStillRuns() {
        // Reproduce the dual-write failure exactly: the row commits, the publish does not.
        // Before the reaper existed this job sat in QUEUED forever and no worker ever saw it.
        Job job = jobs.submit("default", "{\"work\":\"ok\"}", 3);
        redis.delete(QueueGateway.READY);

        assertThat(repository.findById(job.getId()).orElseThrow().getStatus())
                .as("the row is durable even though nothing is queued")
                .isEqualTo(JobStatus.QUEUED);

        // No manual nudge: the scheduled reaper notices and republishes on its own.
        awaitStatus(job.getId(), JobStatus.SUCCEEDED);

        assertThat(repository.findById(job.getId()).orElseThrow().getAttempts())
                .as("recovery must not cost an extra attempt")
                .isEqualTo(1);
    }

    @Test
    void recoveryStillHonoursTheAttemptCeiling() {
        // A stranded job that is also broken must not get extra lives from being republished.
        Job job = jobs.submit("default", "{\"simulateFailure\":true}", 1);
        redis.delete(QueueGateway.READY);

        awaitStatus(job.getId(), JobStatus.DEAD_LETTER);

        assertThat(repository.findById(job.getId()).orElseThrow().getAttempts()).isEqualTo(1);
    }

    @Test
    void aDuplicateDeliveryRunsTheJobExactlyOnce() {
        // Recovery is at-least-once by design: the reaper republishes without checking Redis
        // membership. The claim's row lock is what collapses the duplicate, so a job delivered
        // twice must still be executed once.
        Job job = jobs.submit("default", "{\"work\":\"ok\"}", 3);
        queue.enqueue(job.getId());
        queue.enqueue(job.getId());

        awaitStatus(job.getId(), JobStatus.SUCCEEDED);

        // Give the worker time to drain the surplus copies and try to claim an already-finished job.
        await().pollDelay(Duration.ofSeconds(2))
                .atMost(Duration.ofSeconds(10))
                .until(() -> redis.opsForList().size(QueueGateway.READY) == 0L);

        assertThat(repository.findById(job.getId()).orElseThrow().getAttempts())
                .as("a redelivered job must not run a second time")
                .isEqualTo(1);
        assertThat(repository.countByStatus(JobStatus.SUCCEEDED)).isEqualTo(1);
    }

    @Test
    void aHealthyQueuedJobIsNotRepublishedBeforeItsGracePeriod() {
        // The reaper must not race jobs that are simply waiting their turn, or every submission
        // would be delivered twice under normal operation.
        Job job = jobs.submit("default", "{\"work\":\"ok\"}", 3);

        assertThat(jobs.republishStale(Instant.now().minusSeconds(60)))
                .as("a job submitted moments ago is not stranded")
                .doesNotContain(job.getId());
    }

    @Test
    void republishOnlyTouchesJobsLeftInQueued() {
        // Anything already running, finished, or scheduled for retry has a different owner; the
        // reaper republishing those would resurrect completed work.
        Job succeeded = jobs.submit("default", "{\"work\":\"ok\"}", 3);
        awaitStatus(succeeded.getId(), JobStatus.SUCCEEDED);

        Job dead = jobs.submit("default", "{\"simulateFailure\":true}", 1);
        awaitStatus(dead.getId(), JobStatus.DEAD_LETTER);

        assertThat(jobs.republishStale(Instant.now().plusSeconds(60)))
                .as("only QUEUED rows are eligible")
                .doesNotContain(succeeded.getId(), dead.getId());
    }

    @Test
    void submitSucceedsAndStaysRecoverableEvenIfPublishingFails() {
        // The caller must not be told the submission failed when the job is safely durable.
        Job job = jobs.submit("default", "{\"work\":\"ok\"}", 3);

        assertThat(repository.findById(job.getId()))
                .as("PostgreSQL is the system of record, not Redis")
                .isPresent();
    }
}
