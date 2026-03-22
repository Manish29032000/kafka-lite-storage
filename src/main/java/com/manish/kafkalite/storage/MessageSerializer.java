package com.manish.kafkalite.storage;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/** MessageSerializer packs and unpacks messages into a safe binary format for disk storage.
 * Responsible for converting Message objects to and from a binary format i.e. Converts Message(Java world) ⇄ Bytes(disk world) (so we can store in file)
 *
 * Binary Layout:
 * | Offset (8 bytes) | Size (4 bytes) | CRC (4 bytes) | Payload (N bytes) |
 *
 * This format ensures:
 * - Efficient sequential disk writes
 * - Data integrity via CRC validation
 */
public class MessageSerializer {
    private static final int HEADER_SIZE = 8 + 4 + 4;            // 8 (offset) + 4 (size) + 4 (crc) = 16 bytes
                                                                // This is the fixed part of every message
    /**
     * Serializes a Message object into a byte array following the defined binary format.
     *
     * Steps:
     * 1. Extract payload
     * 2. Compute CRC checksum for data integrity
     * 3. Allocate buffer for full message
     * 4. Write fields sequentially into buffer
     *
     * @param message Message object to serialize
     * @return byte array representing the serialized message
     */
    public byte[] serialize(Message message) {
        byte payload[] = message.getPayload();         // "msg-1" like data
        int size = payload.length;                     // how big the message is

        // Compute CRC checksum(Create a checksum so we know if data gets corrupted)
        CRC32 crc32 = new CRC32();
        crc32.update(payload);                             // Create checksum to detect corruption
        int crc = (int) crc32.getValue();

        // Allocate buffer for full message(Create a box big enough to hold everything)
        ByteBuffer buffer = ByteBuffer.allocate(HEADER_SIZE + size);

        // Pack everything(data) into bytes
        buffer.putLong(message.getOffset()); // 8 bytes
        buffer.putInt(size);                 // 4 bytes
        buffer.putInt(crc);                  // 4 bytes
        buffer.put(payload);                 // N bytes

        return buffer.array();         // Return the final packed bytes
    }

    /**
     * Deserializes a ByteBuffer into a Message object i.e Take bytes and rebuild Message
     *
     * Steps:
     * 1. Read offset, size, and CRC from buffer
     * 2. Extract payload bytes
     * 3. Recompute CRC and validate integrity
     * 4. Construct Message object
     *
     * @param buffer ByteBuffer containing serialized message
     * @return deserialized Message object
     * @throws RuntimeException if CRC validation fails (data corruption)
     */
    public Message deserialize(ByteBuffer buffer) {
        long offset = buffer.getLong();                              // message number
        int size = buffer.getInt();                                 // how many bytes to read next
        int crc = buffer.getInt();                                 // original checksum

        byte[] payload = new byte[size];
        buffer.get(payload);                                       // actual data

        // Validate CRC
        CRC32 crc32 = new CRC32();
        crc32.update(payload);
        int computedCrc = (int) crc32.getValue();

        if (crc != computedCrc) {                                        // If values don’t match → data is broken
            throw new RuntimeException("CRC mismatch! Data corrupted.");
        }

        return new Message(offset, payload);                        // rebuild original object
    }
}