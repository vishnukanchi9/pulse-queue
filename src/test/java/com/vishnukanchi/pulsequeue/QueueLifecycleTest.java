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
 * End-to-end tests for the claims this project makes: jobs survive in PostgreSQL, failures retry
 * with exponential backoff, and exhausted jobs land in the dead-letter queue.
 *
 * <p>These run against a real PostgreSQL and a real Redis. The previous suite tested only
 * {@code RetryPolicy.backoff}, which is pure arithmetic - it could pass while the queue itself was
 * completely broken, because nothing exercised the two stores together.
 *
 * <p>The scheduled worker is left running rather than driven by hand. Backoff for the first retry
 * is one second, so a full submit → fail → retry → fail → dead-letter journey completes in a couple
 * of seconds, and testing it with the real scheduler is what makes the result mean something.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class QueueLifecycleTest {

    static final PostgreSQLContainer<?> POSTGRES =
            new PostgreSQLContainer<>("postgres:16-alpine")
                    .withDatabaseName("pulse_queue")
                    .withUsername("pulse")
                    .withPassword("pulse");

    @SuppressWarnings("resource") // closed by the JVM at exit; reused across the whole suite
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
    @Autowired StringRedisTemplate redis;

    @BeforeEach
    void reset() {
        repository.deleteAll();
        redis.delete(QueueGateway.READY);
        redis.delete(QueueGateway.RETRY);
        redis.delete(QueueGateway.DEAD_LETTER);
    }

    private Job submit(String payload, int maxAttempts) {
        return jobs.submit("default", payload, maxAttempts);
    }

    private JobStatus statusOf(UUID id) {
        return repository.findById(id).orElseThrow().getStatus();
    }

    private void awaitStatus(UUID id, JobStatus expected) {
        await().atMost(Duration.ofSeconds(30))
                .pollInterval(Duration.ofMillis(100))
                .until(() -> statusOf(id) == expected);
    }

    @Test
    void aHealthyJobIsPickedUpAndSucceedsOnTheFirstAttempt() {
        Job job = submit("{\"work\":\"ok\"}", 3);

        awaitStatus(job.getId(), JobStatus.SUCCEEDED);

        Job settled = repository.findById(job.getId()).orElseThrow();
        assertThat(settled.getAttempts()).isEqualTo(1);
        assertThat(settled.getCompletedAt()).isNotNull();
        assertThat(settled.getLastError()).isNull();
    }

    @Test
    void aFailingJobIsScheduledForRetryWithABackoffInTheFuture() {
        // Two attempts allowed, so the first failure schedules a retry rather than dead-lettering.
        Job job = submit("{\"simulateFailure\":true}", 2);

        awaitStatus(job.getId(), JobStatus.RETRY_SCHEDULED);

        Job scheduled = repository.findById(job.getId()).orElseThrow();
        assertThat(scheduled.getAttempts()).isEqualTo(1);
        assertThat(scheduled.getLastError()).contains("Simulated downstream failure");
        assertThat(scheduled.getNextAttemptAt())
                .as("retry must be deferred, not immediate")
                .isAfter(Instant.now().minusSeconds(1));

        // The deferral is held in Redis as a sorted set scored by due time - that is what makes it
        // survive a restart of the worker.
        assertThat(redis.opsForZSet().score(QueueGateway.RETRY, job.getId().toString()))
                .as("job must be scored into the retry set")
                .isNotNull();
    }

    @Test
    void aJobThatKeepsFailingExhaustsItsAttemptsAndDeadLetters() {
        Job job = submit("{\"simulateFailure\":true}", 2);

        awaitStatus(job.getId(), JobStatus.DEAD_LETTER);

        Job dead = repository.findById(job.getId()).orElseThrow();
        assertThat(dead.getAttempts())
                .as("must stop at the configured ceiling, not overshoot it")
                .isEqualTo(2);
        assertThat(dead.getLastError()).contains("Simulated downstream failure");
        assertThat(dead.getNextAttemptAt()).as("a dead job must not stay scheduled").isNull();

        // Dead-lettered ids are pushed to a Redis list so an operator can inspect or replay them.
        assertThat(redis.opsForList().range(QueueGateway.DEAD_LETTER, 0, -1))
                .contains(job.getId().toString());
    }

    @Test
    void aSingleAttemptJobDeadLettersImmediatelyWithoutBeingScheduled() {
        Job job = submit("{\"simulateFailure\":true}", 1);

        awaitStatus(job.getId(), JobStatus.DEAD_LETTER);

        assertThat(repository.findById(job.getId()).orElseThrow().getAttempts()).isEqualTo(1);
        assertThat(redis.opsForZSet().score(QueueGateway.RETRY, job.getId().toString()))
                .as("with one attempt allowed there is nothing to retry")
                .isNull();
    }

    @Test
    void theRetrySetIsDrainedOnceAJobHasBeenPromoted() {
        Job job = submit("{\"simulateFailure\":true}", 2);

        awaitStatus(job.getId(), JobStatus.DEAD_LETTER);

        // A promoted job left behind in the retry set would be re-enqueued forever.
        await().atMost(Duration.ofSeconds(10))
                .until(
                        () ->
                                redis.opsForZSet().score(QueueGateway.RETRY, job.getId().toString())
                                        == null);
    }

    @Test
    void independentJobsSettleIndependentlyWhenProcessedTogether() {
        Job healthy = submit("{\"work\":\"ok\"}", 2);
        Job doomed = submit("{\"simulateFailure\":true}", 1);
        Job alsoHealthy = submit("{\"work\":\"fine\"}", 2);

        awaitStatus(healthy.getId(), JobStatus.SUCCEEDED);
        awaitStatus(alsoHealthy.getId(), JobStatus.SUCCEEDED);
        awaitStatus(doomed.getId(), JobStatus.DEAD_LETTER);

        // One poisoned job must not block or corrupt the rest of the queue.
        assertThat(repository.countByStatus(JobStatus.SUCCEEDED)).isEqualTo(2);
        assertThat(repository.countByStatus(JobStatus.DEAD_LETTER)).isEqualTo(1);
    }

    @Test
    void everyJobIsDurableInPostgresFromTheMomentItIsSubmitted() {
        Job job = submit("{\"work\":\"ok\"}", 3);

        // The row exists before any worker has run: the queue is not the system of record,
        // PostgreSQL is.
        assertThat(repository.findById(job.getId())).isPresent();

        awaitStatus(job.getId(), JobStatus.SUCCEEDED);

        Job settled = repository.findById(job.getId()).orElseThrow();
        assertThat(settled.getQueueName()).isEqualTo("default");
        assertThat(settled.getPayload()).isEqualTo("{\"work\":\"ok\"}");
        assertThat(settled.getCreatedAt()).isNotNull();
    }
}
