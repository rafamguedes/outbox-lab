package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class WorkerConfiguration {
    @Bean
    public OutboxWorker outboxWorker(OutboxRunner runner, OutboxStore store, JdbcConnectionDetails connectionDetails,
            @Value("${outbox.worker.type:LISTEN_NOTIFY}") WorkerType type,
            @Value("${outbox.worker.enabled:true}") boolean enabled,
            @Value("${outbox.worker.poll-interval-ms:1000}") long pollMillis,
            @Value("${outbox.worker.recovery-ms:60000}") long recoveryMillis,
            @Value("${outbox.worker.reconnect-ms:2000}") long reconnectMillis,
            @Value("${outbox.worker.cdc.slot-name:outbox_demo_slot}") String slotName,
            @Value("${outbox.worker.cdc.publication-name:outbox_demo_publication}") String publicationName) {
        // Existe somente um bean ativo. A escolha não muda o processamento de negócio.
        return switch (type) {
            case POLLING -> new PollingWorker(runner, store, enabled, pollMillis, reconnectMillis);
            case LISTEN_NOTIFY -> new ListenNotifyWorker(runner, store, connectionDetails, enabled, recoveryMillis, reconnectMillis);
            case CDC -> new CdcWorker(runner, store, connectionDetails, enabled, recoveryMillis, reconnectMillis, slotName, publicationName);
        };
    }
}
