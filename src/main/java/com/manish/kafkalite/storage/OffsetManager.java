package com.manish.kafkalite.storage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing offsets i.e. for generating unique increasing numbers
 */
public class OffsetManager {

    private final AtomicLong offset;

    public OffsetManager(long initialOffset) {
        this.offset = new AtomicLong(initialOffset);
    }

    public long nextOffset() {
        return offset.getAndIncrement();
    }
}