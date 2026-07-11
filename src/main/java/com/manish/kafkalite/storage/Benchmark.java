package com.manish.kafkalite.storage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

public class Benchmark {

    public static void main(String[] args) throws Exception {
        String filePath = "data/benchmark-partition.log";
        Path path = Paths.get(filePath);

        // Clean up previous runs so we always start fresh
        Files.deleteIfExists(path);

        // Benchmark Configuration
        int messageCount = 1_000_000;
        int payloadSizeBytes = 256;

        // Create a dummy payload filled with '1's
        byte[] payload = new byte[payloadSizeBytes];
        Arrays.fill(payload, (byte) 1);

        System.out.println("---- Starting Kafka-Lite Storage Benchmark ----");
        System.out.println("Messages to write: " + messageCount);
        System.out.println("Payload size:      " + payloadSizeBytes + " bytes");
        System.out.println("Warming up JVM & writing to disk...\n");

        long startTime = System.currentTimeMillis();

        // Run the benchmark
        try (LogSegment log = new LogSegment(filePath)) {
            for (int i = 0; i < messageCount; i++) {
                log.append(payload);
            }
        }

        long endTime = System.currentTimeMillis();
        long durationMs = endTime - startTime;
        double durationSec = durationMs / 1000.0;

        // Metrics Calculation
        // Every message has a 16-byte header + the payload
        long totalBytesWritten = (long) messageCount * (16 + payloadSizeBytes);
        double totalMbWritten = totalBytesWritten / (1024.0 * 1024.0);

        double mbPerSecond = totalMbWritten / durationSec;
        double messagesPerSecond = messageCount / durationSec;

        System.out.println("---- Benchmark Results ----");
        System.out.printf("Time Taken:        %.3f seconds\n", durationSec);
        System.out.printf("Total Data:        %.2f MB\n", totalMbWritten);
        System.out.printf("Throughput:        %.2f MB/s\n", mbPerSecond);
        System.out.printf("Message Rate:      %.2f msgs/s\n", messagesPerSecond);
    }
}