package com.manish.kafkalite.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;

/*
Writes messages to disk, and Reads messages from disk
Responsibilities of LogSegment(heart of the system)
    Open a .log file
    Append messages (using FileChannel)
    Maintain write position
    Read messages sequentially
 */
public class LogSegment {

    private final FileChannel fileChannel;                  // talks to the file (read/write)
    private final MessageSerializer serializer;            // converts message ⇄ bytes
    private final OffsetManager offsetManager;            // gives message numbers (0,1,2...)

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
        this.offsetManager = new OffsetManager();
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
    public synchronized long append(byte[] payload) throws IOException {      // Write one(synchronized) message to file

        long offset = offsetManager.nextOffset();   // get next offset i.e msg-0 → offset 0, msg-1 → offset 1

        Message message = new Message(offset, payload);   // Create Message

        byte[] bytes = serializer.serialize(message);   // Serialize it([offset][size][crc][payload])

        ByteBuffer buffer = ByteBuffer.wrap(bytes);    // Prepare for writing to file

        while (buffer.hasRemaining()) {
            fileChannel.write(buffer);                  // Write to file i.e. Put bytes into file (at the end)
        }

        return offset;                             // message ID(After submit a form, system gives us a receipt number)
    }                                              // the exact position (ID) of the message in the log

    /**
     * Reads all messages sequentially from the log file.
     */
    public void readAll() throws IOException {

        fileChannel.position(0);           // Go to beginning i.e. Start reading from beginning of file

        // Create header buffer
        ByteBuffer headerBuffer = ByteBuffer.allocate(16); // offset + size + crc(offset (8) + size (4) + crc (4) = 16 bytes)

        while (true) {                     // Keep reading until file ends
            headerBuffer.clear();         // resets buffer -> position = 0, limit = capacity (Buffer is empty now, ready for new data)

            int bytesRead = fileChannel.read(headerBuffer);    // Read header (16 bytes)

            if (bytesRead < 16) {                              // If less than 16 → stop (end of file)
                break; // end of file
            }

            headerBuffer.flip();                              // switch to read mode

            // Extract fields(header)
            long offset = headerBuffer.getLong();
            int size = headerBuffer.getInt();
            int crc = headerBuffer.getInt();

            // Read payload
            ByteBuffer payloadBuffer = ByteBuffer.allocate(size);
            fileChannel.read(payloadBuffer);                            // Read actual message data

            payloadBuffer.flip();                       // flip → read mode i.e. switch from writing to reading

            // Rebuild full message buffer
            ByteBuffer fullBuffer = ByteBuffer.allocate(16 + size);    // Reconstruct original format:[offset][size][crc][payload]

            // Rebuild full message i.e. Recreate original byte structure
            fullBuffer.putLong(offset);
            fullBuffer.putInt(size);
            fullBuffer.putInt(crc);
            fullBuffer.put(payloadBuffer.array());

            fullBuffer.flip();

            // Convert back to Message object
            Message message = serializer.deserialize(fullBuffer);

            System.out.println("Offset: " + message.getOffset() +
                    ", Payload: " + new String(message.getPayload()));
        }
    }
}

/*
| Method     | Meaning                     |
| ---------- | --------------------------- |
| `.clear()` | “Prepare for writing again” |
| `.flip()`  | “Prepare for reading”       |

 */