package com.manish.kafkalite.network.protocol;

public class Requests {
    // API Key 1
    public record ProduceRequest(byte[] payload) {}

    // API Key 2
    public record FetchRequest(long startingOffset) {}

    // API Key 3
    public record OffsetCommitRequest(String groupId, long offset) {}

    // API Key 4
    public record OffsetFetchRequest(String groupId) {}
}