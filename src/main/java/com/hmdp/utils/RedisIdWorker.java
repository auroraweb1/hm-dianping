package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import java.time.Instant;


@Component
public class RedisIdWorker {
    // 改成“秒”级时间戳：2022-01-01 00:00:00 UTC
    private static final long BEGIN_TIMESTAMP = 1640995200L;
    private static final int COUNT_BITS = 32;

    private final StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix) {
        // 1) 秒级时间戳
        long nowSeconds = Instant.now().getEpochSecond();
        long timestamp = nowSeconds - BEGIN_TIMESTAMP;

        // 2) 当天自增序列（按业务前缀隔离）
        String date = DateTimeFormatter.ofPattern("yyyy:MM:dd")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now());
        long count = stringRedisTemplate.opsForValue()
                .increment("icr:" + keyPrefix + ":" + date);

        // 3) 拼接：高位时间戳 + 低位序列号
        return (timestamp << COUNT_BITS) | count;
    }
}
