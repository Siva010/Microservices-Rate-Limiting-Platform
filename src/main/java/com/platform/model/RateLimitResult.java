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
        // Conservative minimum wait: on deny path remainingTokens is typically 0 (Lua returns filled before subtract).
        // Value is a lower-bound estimate; clients should treat it as "at least this long".
        long needed = Math.max(1L, 1 - remainingTokens);
        double secondsPerNeeded = needed / (double) replenishRate;
        return Math.max(1L, (long) Math.ceil(secondsPerNeeded));
    }
}