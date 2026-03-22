package com.manish.kafkalite.storage;

/*
    “One record in the commit log”
     Offset → used for indexing and Payload → actual message
                 offset → message number (0,1,2...)
                 payload → actual data (msg-1, msg-2...)
     * Represents a single record in the append-only log.
     *
     * Each message has:
     * - A unique offset (monotonically increasing)
     * - A payload (actual data)
     *
     * This is the fundamental unit of storage in the Kafka-lite system.
*/

public class Message {

    private final long offset;
    private final byte payload[];

    /**
     * Constructing a Message with a given offset and payload(Create a message with a number and data).
     *
     * @param offset  Unique identifier for the message
     * @param payload Actual message data in bytes
     */

    public Message(long offset, byte payload[]) {
        this.offset = offset;
        this.payload = payload;
    }

    /**
     * @return the offset of this message i.e. the message number
     */
    public long getOffset() {
        return offset;
    }

    /**
     * @return the payload (raw bytes) of this message i.e. the actual data
     */
    public byte[] getPayload() {
        return payload;
    }
}