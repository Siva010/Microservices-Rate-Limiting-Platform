# Microservices Rate Limiting Platform

A high-performance API Gateway and distributed rate limiting platform built with Java, Spring Boot, Redis, and Kafka.

## Architecture & Workflow

### 1. The Gateway Entry Point
When a user sends an HTTP request (e.g., to `http://localhost:8080/api/get`), it hits our **Spring Cloud Gateway**. The gateway acts as the single entry point for all traffic. It checks its routing rules (defined in `application.yml`) and sees that paths starting with `/api/**` should be routed to our dummy downstream service (`httpbin.org`). 

### 2. The Custom Rate Limiter Filter
Before the request is forwarded, it must pass through a chain of filters. The very first one is our **CustomRateLimiter**.
- The filter extracts a unique identifier for the user. In our current implementation, it uses the user's IP Address as the key.
- It asks the **RateLimiterService**: *"Is this IP allowed to make a request right now?"* 

### 3. The Redis Token Bucket (The Core Logic)
To determine if the request is allowed, the gateway needs to do this blazingly fast without blocking other requests (which is why we use WebFlux). It sends a single command to **Redis** to execute a highly optimized **Lua Script** (`request_rate_limiter.lua`). 

This script implements the **Token Bucket Algorithm**:
- Imagine a bucket for every user's IP address that holds a maximum number of tokens (the **Burst Capacity** = 20).
- Tokens are constantly added to this bucket at a fixed rate (the **Replenish Rate** = 10 tokens per second).
- When a request arrives, the script tries to take 1 token out of the bucket.
- **If a token is available:** The script removes the token, updates the bucket in Redis, and tells the Java application `allowed = true`.
- **If the bucket is empty:** The script tells the Java application `allowed = false`.

*Why use a Lua script?* Because the script runs atomically inside the Redis engine. This guarantees there are no race conditions when 10,000 parallel requests hit the server, and it avoids the network latency of making multiple back-and-forth calls to Redis to read and write token counts.

### 4. Handling The Result
Once the Gateway gets the answer back from Redis:
- **If Allowed:** The request is passed to the next filter in the chain (the Circuit Breaker) and then routed to the downstream microservice.
- **If Denied:** Two things happen immediately:
  1. The Gateway blocks the request and instantly returns an `HTTP 429 Too Many Requests` status to the user.
  2. The `AlertService` is triggered, which asynchronously publishes an alert message containing the Route ID and the User's IP to our **Kafka topic** (`rate-limit-alerts`). This allows any monitoring system tapped into Kafka to detect potential DDoS attacks or heavy users in real-time.

### 5. The Circuit Breaker (Resilience4j)
If the request was allowed, it still has to be sent to the downstream microservice. But what if the downstream service is crashing or timing out? 
- Our gateway tracks failures using **Resilience4j**. If the failure rate goes above a defined threshold (e.g., 50% of requests fail), the Circuit Breaker trips and goes into an **Open state**.
- Instead of making the user wait for a timeout or sending more traffic to an already broken service, the gateway instantly shorts the connection and routes the user to our **FallbackController** (`/fallback`), safely returning an `HTTP 503 Service Unavailable` message. 

In summary: It provides extremely high-throughput protection at the edge of your network, ensuring your downstream microservices are never overloaded by spikes in traffic or damaged by cascading failures!

## Running the Application

You can spin up the entire infrastructure and your application by executing:
```bash
docker-compose up --build -d
```
This will start:
- **Kafka (KRaft)** on port `9092` — no Zookeeper required
- **Redis** on port `6379`
- **Mock API** (local demo downstream)
- **API Gateway** on port `8080`
