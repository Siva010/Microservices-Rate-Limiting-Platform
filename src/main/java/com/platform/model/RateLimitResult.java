package com.platform.model;

public record RateLimitResult(
        boolean allowed,
        long remainingTokens,
        int burstCapacity,
        int replenishRate
) {
    public static RateLimitResult denied(int burstCapacity, int replenishRate) {
        return new RateLimitResult(false, 0, burstCapacity, replenishRate);
    }

    public static RateLimitResult redisUnavailable(boolean failOpen, int burstCapacity, int replenishRate) {
        return new RateLimitResult(failOpen, failOpen ? burstCapacity : 0, burstCapacity, replenishRate);
    }

    public long retryAfterSeconds() {
        if (allowed || replenishRate <= 0) {
            return 0;
        }
        return Math.max(1, (long) Math.ceil(1.0 / replenishRate));
    }
}