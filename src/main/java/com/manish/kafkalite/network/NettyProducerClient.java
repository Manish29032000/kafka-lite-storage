package com.manish.kafkalite.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

// This script manually crafts the exact byte sequence we defined in our protocol ([API Key] + [Length] + [Payload])
// and sends it over a raw TCP socket

public class NettyProducerClient {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9092;

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            System.out.println("🔌 Connected to Kafka-Lite Netty Broker at " + host + ":" + port);

            // 1. Prepare our message
            String message = "Hello, High-Performance TCP World!";
            byte[] payload = message.getBytes(StandardCharsets.UTF_8);

            // 2. ENCODE & SEND THE PRODUCE REQUEST
            System.out.println("Sending Produce Request: '" + message + "'");
            out.writeShort(1);              // API Key: 1 (Produce)
            out.writeInt(payload.length);   // Payload Length
            out.write(payload);             // The actual message bytes
            out.flush();                    // Force the bytes over the network

            // 3. READ & DECODE THE PRODUCE RESPONSE
            // Our server responds with: [API Key (short)] + [Length (int)] + [Offset (long)]
            short responseApiKey = in.readShort();
            int responseLength = in.readInt();
            long assignedOffset = in.readLong();

            System.out.println("✅ Received Produce Response:");
            System.out.println("   - API Key: " + responseApiKey);
            System.out.println("   - Length: " + responseLength + " bytes");
            System.out.println("   - Assigned Offset: " + assignedOffset);

        } catch (Exception e) {
            System.err.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}