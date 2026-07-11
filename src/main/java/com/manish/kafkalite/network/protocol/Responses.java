package com.manish.kafkalite.network.protocol;

public class Responses {
    // 1. Add this marker interface
    public interface Response {}

    // 2. Add 'implements Response' to both records
    public record ProduceResponse(long assignedOffset) implements Response {}

    public record FetchHeader(int payloadLength) implements Response {}
}