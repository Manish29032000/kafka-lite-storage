package com.manish.kafkalite.storage;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Generates monotonically increasing offsets i.e. for generating unique increasing numbers
 */
public class OffsetManager {

    private final AtomicLong offset = new AtomicLong(0);            // Starts from 0

    public long nextOffset() {
        return offset.getAndIncrement();                        // Give next number(starts from 0 -> 1 -> 2) and increase
    }
}