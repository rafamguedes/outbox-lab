package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import com.example.outbox.service.OrderProcessor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class OutboxRunner {
    private static final Logger log = LoggerFactory.getLogger(OutboxRunner.class);
    private final OutboxStore store;
    private final OrderProcessor processor;
    private final int retrySeconds;
    private final int maxAttempts;

    public OutboxRunner(OutboxStore store, OrderProcessor processor,
                        @Value("${outbox.worker.retry-seconds:12}") int retrySeconds,
                        @Value("${outbox.worker.max-attempts:2}") int maxAttempts) {
        this.store = store;
        this.processor = processor;
        this.retrySeconds = retrySeconds;
        if (maxAttempts < 1) throw new IllegalArgumentException("Max attempts must be positive.");
        this.maxAttempts = maxAttempts;
    }

    @Transactional
    public boolean processNext() {
        var next = store.lockNextEvent();
        if (next.isEmpty()) return false;
        var event = next.get();
        try {
            processor.process(event);
        } catch (RuntimeException e) {
            // O savepoint do processor já reverteu qualquer mudança no pedido.
            String error = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
            if (event.attempts() + 1 >= maxAttempts) {
                store.failEvent(event.id(), error);
            } else {
                store.retryEvent(event.id(), error, retrySeconds);
            }
            log.warn("Event {} failed on attempt {}: {}", event.id(), event.attempts() + 1, error);
            return true;
        }
        // O pedido e a conclusão do evento são confirmados no mesmo commit.
        store.completeEvent(event.id());
        log.debug("Event {} processed; order {} completed; attempt {}",
                event.id(), event.orderId(), event.attempts() + 1);
        return true;
    }
}
