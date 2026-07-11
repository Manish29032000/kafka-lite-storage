package com.manish.kafkalite.coordination;

import com.manish.kafkalite.storage.LogSegment;
import com.manish.kafkalite.storage.Message;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/*
This class does two things:
    It maintains an ultra-fast ConcurrentHashMap in memory for instant offset lookups.

    Every time an offset is committed, it writes it to a durable .log file so the state survives a broker crash.
    When the broker starts up, it replays this log to rebuild the memory cache.
 */

public class ConsumerCoordinator {

    // In-memory cache for ultra-fast O(1) lookups: GroupID -> Offset
    private final ConcurrentHashMap<String, Long> offsetCache = new ConcurrentHashMap<>();

    // Durable storage so we don't lose offsets if the broker crashes
    private final LogSegment offsetLog;

    public ConsumerCoordinator(String offsetLogPath) throws IOException {
        this.offsetLog = new LogSegment(offsetLogPath);
        recoverOffsetsFromDisk();
    }

    /**
     * Saves the consumer group's progress.
     */
    public void commitOffset(String groupId, long offset) throws IOException {
        // 1. Update the fast in-memory cache
        offsetCache.put(groupId, offset);

        // 2. Persist to disk (Format: "groupId:offset")
        String payload = groupId + ":" + offset;
        offsetLog.append(payload.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Retrieves the last committed offset for a group.
     */
    public long getOffset(String groupId) {
        // Return the saved offset, or 0 if this is a brand new consumer group
        return offsetCache.getOrDefault(groupId, 0L);
    }

    /**
     * Runs on broker startup. Replays the internal log to rebuild the cache.
     */
    private void recoverOffsetsFromDisk() throws IOException {
        System.out.println("🔄 Recovering consumer offsets from disk...");
        long currentReadOffset = 0;

        while (true) {
            // Read messages in batches of 100
            List<Message> messages = offsetLog.read(currentReadOffset, 100);
            if (messages.isEmpty()) break;

            for (Message msg : messages) {
                String payload = new String(msg.getPayload(), StandardCharsets.UTF_8);
                String[] parts = payload.split(":");
                if (parts.length == 2) {
                    // Overwrite the cache with the newest offset for this group
                    offsetCache.put(parts[0], Long.parseLong(parts[1]));
                }
                currentReadOffset = msg.getOffset() + 1;
            }
        }
        System.out.println("✅ Offset recovery complete. Tracking " + offsetCache.size() + " consumer groups.");
    }
}