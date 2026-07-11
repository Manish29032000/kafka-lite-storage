package com.manish.kafkalite.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class NettyConsumerClient {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9092;

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            System.out.println("🔌 Connected to Kafka-Lite Netty Broker at " + host + ":" + port);

            // 1. ENCODE & SEND THE FETCH REQUEST (Starting at Offset 0)
            long fetchOffset = 0;
            System.out.println("Sending Fetch Request for offset: " + fetchOffset);
            out.writeShort(2);              // API Key: 2 (Fetch)
            out.writeInt(8);                // Payload Length (8 bytes for a long)
            out.writeLong(fetchOffset);     // The requested offset
            out.flush();

            // 2. READ THE HEADER (Sent by the JVM)
            short responseApiKey = in.readShort();
            int responseLength = in.readInt();

            System.out.println("✅ Received Fetch Response Header:");
            System.out.println("   - API Key: " + responseApiKey);
            System.out.println("   - Payload Length: " + responseLength + " bytes");

            if (responseLength == 0) {
                System.out.println("No messages found at this offset.");
                return;
            }

            // 3. READ THE ZERO-COPY RAW BYTES (Sent by the OS)
            System.out.println("\n📥 Decoding Zero-Copy Stream:");
            int bytesRead = 0;

            while (bytesRead < responseLength) {
                long msgOffset = in.readLong();
                int msgLength = in.readInt();
                int crc = in.readInt();

                // Read the exact payload length
                byte[] payload = new byte[msgLength];
                in.readFully(payload);

                String messageStr = new String(payload, StandardCharsets.UTF_8);
                System.out.printf("   -> [Offset: %d] %s\n", msgOffset, messageStr);

                // Track bytes read: 8 (offset) + 4 (length) + 4 (crc) + payload size
                bytesRead += (16 + msgLength);
            }

        } catch (Exception e) {
            System.err.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}