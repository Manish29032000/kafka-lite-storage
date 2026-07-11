package com.manish.kafkalite.network;

import com.manish.kafkalite.storage.LogSegment;
import com.manish.kafkalite.storage.Message;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;

public class BrokerServer {

    private final LogSegment logSegment;
    private final HttpServer server;

    // Now the storage engine evolves from a local file writer into an actual distributed component

    public BrokerServer(int port, String logFilePath) throws IOException {
        // 1. Initialize Storage Engine
        this.logSegment = new LogSegment(logFilePath);

        // 2. Initialize HTTP Server
        this.server = HttpServer.create(new InetSocketAddress(port), 0);

        // 3. Define the Endpoints
        this.server.createContext("/produce", this::handleProduce);
        this.server.createContext("/fetch", this::handleFetch);

        // 4. Assign a Thread Pool for concurrent connections
        this.server.setExecutor(Executors.newFixedThreadPool(10));
    }

    public void start() {
        server.start();
        System.out.println("🚀 Kafka-Lite Broker listening on port " + server.getAddress().getPort());
    }

    /**
     * POST /produce
     * Body: Raw message bytes
     */
    private void handleProduce(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1); // Method Not Allowed
            return;
        }

        try (InputStream is = exchange.getRequestBody()) {
            byte[] payload = is.readAllBytes();

            // Append to disk!
            long offset = logSegment.append(payload);

            // Respond with the assigned offset
            String response = String.format("{\"offset\": %d}", offset);
            byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
            exchange.sendResponseHeaders(500, -1);
        }
    }

    /**
     * GET /fetch?offset=X&max=Y
     */
    private void handleFetch(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            exchange.sendResponseHeaders(405, -1);
            return;
        }

        try {
            // Default parameters
            long offset = 0;
            int max = 10;

            // Basic Query Param Parsing
            String query = exchange.getRequestURI().getQuery();
            if (query != null) {
                for (String param : query.split("&")) {
                    String[] pair = param.split("=");
                    if (pair.length == 2) {
                        if (pair[0].equals("offset")) offset = Long.parseLong(pair[1]);
                        if (pair[0].equals("max")) max = Integer.parseInt(pair[1]);
                    }
                }
            }

            // Read from disk!
            List<Message> messages = logSegment.read(offset, max);

            // Manual JSON construction to avoid external Jackson dependencies
            StringBuilder jsonBuilder = new StringBuilder("[");
            for (int i = 0; i < messages.size(); i++) {
                Message msg = messages.get(i);
                String payloadStr = new String(msg.getPayload(), StandardCharsets.UTF_8);
                // Escape quotes if necessary for strict JSON, simplified here:
                jsonBuilder.append(String.format("{\"offset\": %d, \"payload\": \"%s\"}", msg.getOffset(), payloadStr));
                if (i < messages.size() - 1) jsonBuilder.append(",");
            }
            jsonBuilder.append("]");

            byte[] responseBytes = jsonBuilder.toString().getBytes(StandardCharsets.UTF_8);

            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, responseBytes.length);

            try (OutputStream os = exchange.getResponseBody()) {
                os.write(responseBytes);
            }
        } catch (Exception e) {
            e.printStackTrace();
            exchange.sendResponseHeaders(500, -1);
        }
    }

    public static void main(String[] args) throws IOException {
        BrokerServer broker = new BrokerServer(8080, "data/network-partition.log");
        broker.start();
    }
}