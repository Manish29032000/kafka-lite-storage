package com.manish.kafkalite.network.protocol;

public class Responses {
    public interface Response {}

    public record ProduceResponse(long assignedOffset) implements Response {}

    public record FetchHeader(int payloadLength) implements Response {}

    // Sent back after a successful OffsetCommit
    public record OffsetCommitResponse(boolean success) implements Response {}

    // Sent back to tell the consumer where to start
    public record OffsetFetchResponse(long offset) implements Response {}
}