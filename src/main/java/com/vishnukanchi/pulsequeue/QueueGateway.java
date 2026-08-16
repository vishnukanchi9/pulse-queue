package com.vishnukanchi.pulsequeue;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import java.util.UUID;

@Component
public class QueueGateway {
    public static final String READY = "pulse:ready";
    public static final String RETRY = "pulse:retry";
    public static final String DEAD_LETTER = "pulse:dead-letter";
    private final StringRedisTemplate redis;
    public QueueGateway(StringRedisTemplate redis) { this.redis = redis; }
    public void enqueue(UUID id) { redis.opsForList().leftPush(READY, id.toString()); }
    public String poll() { return redis.opsForList().rightPop(READY); }
    public void scheduleRetry(UUID id, long epochMillis) { redis.opsForZSet().add(RETRY, id.toString(), epochMillis); }
    public void deadLetter(UUID id) { redis.opsForList().leftPush(DEAD_LETTER, id.toString()); }
    public java.util.Set<String> dueRetries(long now) { return redis.opsForZSet().rangeByScore(RETRY, 0, now); }
    public void removeRetry(String id) { redis.opsForZSet().remove(RETRY, id); }
}
