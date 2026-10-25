package com.example.outbox.service;

import com.example.outbox.repository.OutboxStore;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class EventRetryService {
    private final OutboxStore store;

    public EventRetryService(OutboxStore store) { this.store = store; }

    @Transactional
    public boolean retryFailedEvent(UUID eventId) {
        // O NOTIFY ocorre no commit. A requisição apenas recoloca na fila;
        // o worker executa uma tentativa adicional com o mesmo identificador.
        return store.requeueFailedEvent(eventId);
    }
}
