# Kafka-Lite (Distributed Message Broker)

Building a lightweight, high-performance distributed message broker from scratch using Java 21. This project demonstrates low-level system design, disk I/O optimization, and custom network protocols without relying on heavy frameworks like Spring Boot.

## 🏗️ Architecture & Roadmap

This project is being built in 4 phases over 8 weeks:

- [x] **Phase 1: The Storage Engine (NIO)**
    - Implemented an append-only commit log using Java `FileChannel`.
    - Built an in-memory index (`ConcurrentSkipListMap`) for `O(log N)` offset lookups.
    - Optimized for zero-allocation sequential reads and thread-safe positional writes.
    - *Benchmark: Reached 55+ MB/s (214k+ msgs/sec) single-threaded write throughput.*
- [x] **Phase 2: The Network & Transport Layer**
    - Built a custom Length-Prefixed Binary TCP Protocol.
    - Implemented a high-performance async Netty server (Boss/Worker EventLoops).
    - Achieved **Zero-Copy Reads** bypassing the JVM heap via OS-level `sendfile` (`FileChannel.transferTo()`).
- [x] **Phase 3: Coordination & State**
    - Implemented `ConsumerCoordinator` for O(1) offset lookups.
    - Built internal `__consumer_offsets` log for durable state tracking across broker crashes.
    - Stateful consumer clients capable of resuming from their last checkpoint.
- [ ] **Phase 4: Hardening & Presentation (In Progress)**
    - Packaging and Dockerization.
    - Architecture diagrams and final documentation.

## 🚀 Tech Stack
* **Language:** Java 21
* **Storage:** Java NIO, Memory-Mapped Files
* **Network:** Java Netty (Custom TCP Protocol)
* **Data Integrity:** CRC32 Checksums