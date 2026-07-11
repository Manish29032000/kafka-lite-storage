# Kafka-Lite (Distributed Message Broker)

Building a lightweight, high-performance distributed message broker from scratch using Java 21. This project demonstrates low-level system design, disk I/O optimization, and custom network protocols without relying on heavy frameworks like Spring Boot.

## 🏗️ Architecture & Roadmap

This project is being built in 4 phases over 8 weeks:

- [x] **Phase 1: The Storage Engine (NIO)**
    - Implemented an append-only commit log using Java `FileChannel`.
    - Built an in-memory index (`ConcurrentSkipListMap`) for `O(log N)` offset lookups.
    - Optimized for zero-allocation sequential reads and thread-safe positional writes.
    - *Benchmark: Reached 55+ MB/s (214k+ msgs/sec) single-threaded write throughput.*
- [ ] **Phase 2: The Network & Transport Layer**
    - Expose Produce/Fetch internal APIs.
    - Implement raw Netty TCP server with Zero-Copy (`transferTo`) reads.
- [ ] **Phase 3: Coordination & State**
    - Static consumer groups and partition assignment.
    - Offset management via internal `__consumer_offsets` topic.
- [ ] **Phase 4: Hardening & Presentation**
    - Single-writer concurrency model.
    - Crash recovery and Dockerization.

## 🚀 Tech Stack
* **Language:** Java 21
* **Storage:** Java NIO, Memory-Mapped Files
* **Network:** (Upcoming) Java Netty
* **Data Integrity:** CRC32 Checksums