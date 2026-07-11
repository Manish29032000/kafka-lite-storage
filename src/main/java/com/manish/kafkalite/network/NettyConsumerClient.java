package com.manish.kafkalite.network;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class NettyConsumerClient {

    public static void main(String[] args) {
        String host = "localhost";
        int port = 9092;

        // Define our Consumer Group
        String groupId = "payments-service";
        byte[] groupBytes = groupId.getBytes(StandardCharsets.UTF_8);

        try (Socket socket = new Socket(host, port);
             DataOutputStream out = new DataOutputStream(socket.getOutputStream());
             DataInputStream in = new DataInputStream(socket.getInputStream())) {

            System.out.println("🔌 Connected to Kafka-Lite Broker as group: [" + groupId + "]");

            // ==========================================
            // STEP 1: OFFSET FETCH (Where did I leave off?)
            // ==========================================
            System.out.println("\n[Step 1] Fetching saved offset for group...");
            out.writeShort(4); // API Key 4 (Offset Fetch)
            out.writeInt(2 + groupBytes.length); // Payload length (short + bytes)
            out.writeShort(groupBytes.length);
            out.write(groupBytes);
            out.flush();

            short fetchResApiKey = in.readShort();
            int fetchResLen = in.readInt();
            long startingOffset = in.readLong();
            System.out.println("✅ Broker says we should start at offset: " + startingOffset);


            // ==========================================
            // STEP 2: FETCH MESSAGES (Read Zero-Copy Stream)
            // ==========================================
            System.out.println("\n[Step 2] Requesting messages starting at offset: " + startingOffset);
            out.writeShort(2); // API Key 2 (Fetch)
            out.writeInt(8);   // Payload Length (8 bytes for a long)
            out.writeLong(startingOffset);
            out.flush();

            short msgResApiKey = in.readShort();
            int msgResLen = in.readInt();

            long lastReadOffset = -1;

            if (msgResLen == 0) {
                System.out.println("   No new messages found at this time.");
            } else {
                System.out.println("📥 Decoding Zero-Copy Stream:");
                int bytesRead = 0;

                while (bytesRead < msgResLen) {
                    long msgOffset = in.readLong();
                    int msgLength = in.readInt();
                    int crc = in.readInt();

                    byte[] payload = new byte[msgLength];
                    in.readFully(payload);

                    String messageStr = new String(payload, StandardCharsets.UTF_8);
                    System.out.printf("   -> [Offset: %d] %s\n", msgOffset, messageStr);

                    lastReadOffset = msgOffset; // Keep track of the highest offset we processed
                    bytesRead += (16 + msgLength);
                }
            }


            // ==========================================
            // STEP 3: OFFSET COMMIT (Save our progress)
            // ==========================================
            if (lastReadOffset != -1) {
                // We commit the NEXT offset we want to read, so we don't re-read the last one
                long nextOffsetToCommit = lastReadOffset + 1;
                System.out.println("\n[Step 3] Committing new offset: " + nextOffsetToCommit);

                out.writeShort(3); // API Key 3 (Offset Commit)
                // Payload length = string short (2) + string bytes (N) + long (8)
                out.writeInt(2 + groupBytes.length + 8);
                out.writeShort(groupBytes.length);
                out.write(groupBytes);
                out.writeLong(nextOffsetToCommit);
                out.flush();

                short commitResApiKey = in.readShort();
                int commitResLen = in.readInt();
                boolean success = in.readBoolean();

                System.out.println("✅ Offset Commit Status: " + (success ? "SUCCESS" : "FAILED"));
            } else {
                System.out.println("\n[Step 3] No new messages read, skipping offset commit.");
            }

        } catch (Exception e) {
            System.err.println("❌ Connection failed: " + e.getMessage());
            e.printStackTrace();
        }
    }
}