package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;

public final class PollingWorker extends AbstractOutboxWorker {
    public PollingWorker(OutboxRunner runner, OutboxStore store, boolean enabled,
                         long pollIntervalMillis, long reconnectMillis) {
        super(runner, store, enabled, pollIntervalMillis, reconnectMillis);
    }

    @Override public WorkerType getType() { return WorkerType.POLLING; }
    @Override protected boolean usesRetryTimer() { return false; }
    @Override protected String idleState() { return "WAITING_FOR_POLL"; }
}
