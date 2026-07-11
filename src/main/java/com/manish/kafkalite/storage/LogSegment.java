package com.manish.kafkalite.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.zip.CRC32;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentSkipListMap;

/*
Writes messages to disk, and Reads messages from disk
Responsibilities of LogSegment(heart of the system)
    Open a .log file
    Append messages (using FileChannel)
    Maintain write position
    Read messages sequentially
 */
public class LogSegment implements AutoCloseable {

    private final FileChannel fileChannel;                  // talks to the file (read/write)
    private final MessageSerializer serializer;            // converts message ⇄ bytes
    private final OffsetManager offsetManager;            // gives message numbers (0,1,2...)

    private static final int HEADER_SIZE = 16;
    private final ConcurrentSkipListMap<Long, Long> index = new ConcurrentSkipListMap<>();
    /**
     * Initializes the log segment by opening/creating the log file.
     */
    public LogSegment(String filePath) throws IOException {
        Path path = Paths.get(filePath);

        // Ensure directory exists
        Files.createDirectories(path.getParent());     // path.getParent() -> return data
                                                        // If data/ folder doesn’t exist → it creates it
        // Open file -> Create file if not exists, Allow reading, Allow writing
        this.fileChannel = FileChannel.open(
                path,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.READ
        );                                               // If file doesn’t exist → create it,
                                                         // If file exists → reuse it

        this.serializer = new MessageSerializer();

        long nextOffset = recoverNextOffset();
        this.offsetManager = new OffsetManager(nextOffset);

        // Ensure pointer is at end AFTER recovery
        fileChannel.position(fileChannel.size());
    }

    /**
     * Appends a new message to the log file.
     *
     * Steps:
     * 1. Generate next offset
     * 2. Serialize message
     * 3. Write to file sequentially
     *
     * @param payload message data
     * @return assigned offset
     */
    public synchronized long append(byte[] payload) throws IOException {
        long offset = offsetManager.nextOffset();
        Message message = new Message(offset, payload);
        byte[] bytes = serializer.serialize(message);
        ByteBuffer buffer = ByteBuffer.wrap(bytes);

        // Capture position before writing
        long writePosition = fileChannel.position();

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);
        }

        // Add to index!
        index.put(offset, writePosition);

//        fileChannel.force(false);         // commenting this code, take 5 to 10 minutes to finish
        return offset;
    }                                              // the exact position (ID) of the message in the log

    /**
     * Fetches a batch of messages starting from a specific offset.
     *
     * @param startingOffset The offset to start reading from.
     * @param maxMessages The maximum number of messages to return.
     * @return A list of valid messages.
     */
    /**
     * Fetches a batch of messages starting from a specific offset.
     * Uses positional reads to ensure thread safety with concurrent writers.
     */
    public List<Message> read(long startingOffset, int maxMessages) throws IOException {
        List<Message> messages = new ArrayList<>();

        Map.Entry<Long, Long> entry = index.ceilingEntry(startingOffset);
        if (entry == null) {
            return messages;
        }

        // Keep track of our local read position instead of moving the global file pointer
        long currentReadPosition = entry.getValue();
        ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

        for (int i = 0; i < maxMessages; i++) {
            headerBuffer.clear();

            // THREAD-SAFE POSITIONAL READ
            int bytesRead = fileChannel.read(headerBuffer, currentReadPosition);
            if (bytesRead < HEADER_SIZE) break; // EOF

            currentReadPosition += HEADER_SIZE; // Advance local pointer
            headerBuffer.flip();

            long offset = headerBuffer.getLong();
            int size = headerBuffer.getInt();
            int crc = headerBuffer.getInt();

            ByteBuffer payloadBuffer = ByteBuffer.allocate(size);
            int totalRead = 0;

            while (payloadBuffer.hasRemaining()) {
                // THREAD-SAFE POSITIONAL READ
                int read = fileChannel.read(payloadBuffer, currentReadPosition);
                if (read <= 0) break;
                totalRead += read;
                currentReadPosition += read; // Advance local pointer
            }

            if (totalRead < size) break; // Corrupt/Partial

            payloadBuffer.flip();

            CRC32 crc32 = new CRC32();
            crc32.update(payloadBuffer.array());
            if (crc != (int) crc32.getValue()) {
                System.out.println("Corrupt message detected. Stopping read.");
                break;
            }

            messages.add(new Message(offset, payloadBuffer.array()));
        }

        return messages;
    }

    private long recoverNextOffset() throws IOException {
        if (fileChannel.size() == 0) return 0;

        fileChannel.position(0);
        long lastOffset = -1;
        ByteBuffer header = ByteBuffer.allocate(HEADER_SIZE);

        while (true) {
            long currentPosition = fileChannel.position(); // Capture position BEFORE reading

            header.clear();
            int read = fileChannel.read(header);
            if (read < HEADER_SIZE) break;

            header.flip();
            long offset = header.getLong();
            int size = header.getInt();
            header.getInt(); // skip CRC

            // Populate the index!
            index.put(offset, currentPosition);

            fileChannel.position(fileChannel.position() + size); // Skip payload
            lastOffset = offset;
        }

        return lastOffset + 1;
    }

    /**
     * Closes the underlying file channel.
     * Must be called to release OS file resources.
     */
    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}

/*
| Method     | Meaning                     |
| ---------- | --------------------------- |
| `.clear()` | “Prepare for writing again” |
| `.flip()`  | “Prepare for reading”       |

 */