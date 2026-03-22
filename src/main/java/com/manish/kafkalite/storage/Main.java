package com.manish.kafkalite.storage;

public class Main {

    public static void main(String[] args) throws Exception {

        LogSegment log = new LogSegment("data/partition-0.log");                // Creating a log file handler
                                                                                  // use a file called partition-0.log inside a folder called data to store messages.
        // Write messages                                                         // "data/partition-0.log" becomes a Path, and Java converts it into a file location
        for (int i = 0; i < 10; i++) {
            log.append(("msg-" + i).getBytes());                  // Writing messages one by one
        }

        System.out.println("---- Reading ----");

        // Read messages
        log.readAll();                                         // Reads everything back
    }
}