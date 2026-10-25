package com.example.outbox.worker;

import org.springframework.context.SmartLifecycle;

public interface OutboxWorker extends SmartLifecycle {
    WorkerType getType();
    String getState();
    String description();
    boolean isEnabled();
    void setEnabled(boolean enabled);
}
