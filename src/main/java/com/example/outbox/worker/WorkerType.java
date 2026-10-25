package com.example.outbox.worker;

public enum WorkerType {
    POLLING("Polling"),
    LISTEN_NOTIFY("LISTEN/NOTIFY"),
    CDC("CDC (WAL)");

    private final String displayName;

    WorkerType(String displayName) { this.displayName = displayName; }

    public String getDisplayName() { return displayName; }
}
