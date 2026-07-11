package com.manish.kafkalite.storage;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class Main {
    public static void main(String[] args) throws Exception {
        try (LogSegment log = new LogSegment("data/partition-0.log")) {

            System.out.println("---- Writing 15 Messages ----");
            for (int i = 0; i < 15; i++) {
                log.append(("msg-" + i).getBytes(StandardCharsets.UTF_8));
            }

            System.out.println("\n---- Reading 3 Messages starting at Offset 5 ----");
            List<Message> batch1 = log.read(5, 3);
            for (Message m : batch1) {
                System.out.println("Offset: " + m.getOffset() + " | Payload: " + new String(m.getPayload(), StandardCharsets.UTF_8));
            }

            System.out.println("\n---- Reading 5 Messages starting at Offset 12 ----");
            List<Message> batch2 = log.read(12, 5);
            for (Message m : batch2) {
                System.out.println("Offset: " + m.getOffset() + " | Payload: " + new String(m.getPayload(), StandardCharsets.UTF_8));
            }
        }
    }
}