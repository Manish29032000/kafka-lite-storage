package com.manish.kafkalite.network;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class NetworkBenchmark {

    public static void main(String[] args) throws InterruptedException {
        int totalRequests = 500;
        int concurrentThreads = 50;

        ExecutorService executor = Executors.newFixedThreadPool(concurrentThreads);
        CountDownLatch latch = new CountDownLatch(totalRequests);
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failCount = new AtomicInteger(0);

        HttpClient client = HttpClient.newBuilder().build();

        System.out.println("---- Starting Network Concurrency Test ----");
        System.out.println("Firing " + totalRequests + " requests across " + concurrentThreads + " threads...");

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < totalRequests; i++) {
            final int msgIndex = i;
            executor.submit(() -> {
                try {
                    String payload = "Concurrent-Message-" + msgIndex;
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:8080/produce"))
                            .POST(HttpRequest.BodyPublishers.ofString(payload))
                            .header("Content-Type", "text/plain")
                            .build();

                    HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());

                    if (response.statusCode() == 200) {
                        successCount.incrementAndGet();
                    } else {
                        failCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failCount.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        // Wait for all threads to finish
        latch.await();
        long endTime = System.currentTimeMillis();

        System.out.println("---- Test Complete ----");
        System.out.println("Time taken: " + (endTime - startTime) + "ms");
        System.out.println("Successful Appends: " + successCount.get());
        System.out.println("Failed Appends: " + failCount.get());

        executor.shutdown();
    }
}