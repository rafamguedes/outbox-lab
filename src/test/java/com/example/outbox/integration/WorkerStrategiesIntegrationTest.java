package com.example.outbox.integration;

import com.example.outbox.dto.CreateOrderRequest;
import com.example.outbox.repository.OutboxStore;
import com.example.outbox.service.EventRetryService;
import com.example.outbox.service.OrderService;
import com.example.outbox.worker.CdcWorker;
import com.example.outbox.worker.JdbcSignalWorker;
import com.example.outbox.worker.ListenNotifyWorker;
import com.example.outbox.worker.OutboxRunner;
import com.example.outbox.worker.OutboxWorker;
import com.example.outbox.worker.PollingWorker;
import com.example.outbox.worker.WorkerType;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.Mockito.*;

@SpringBootTest(properties = "outbox.worker.enabled=false")
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class WorkerStrategiesIntegrationTest {
    @Container @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4",
                    "-c", "max_wal_senders=4", "-c", "max_slot_wal_keep_size=64MB", "-c", "max_worker_processes=0");
    @Autowired OrderService service;
    @Autowired EventRetryService retryService;
    @Autowired OutboxRunner runner;
    @Autowired JdbcTemplate jdbc;
    @Autowired JdbcConnectionDetails details;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired ApplicationContext context;
    @MockitoSpyBean OutboxStore store;
    private final List<OutboxWorker> workers = new ArrayList<>();
    private final List<String> slots = new ArrayList<>();
    private final List<String> publications = new ArrayList<>();

    @BeforeEach
    void clean() {
        jdbc.execute("TRUNCATE outbox, orders");
        // Os testes destas estratégias não podem passar por causa do LISTEN/NOTIFY.
        jdbc.execute("ALTER TABLE outbox DISABLE TRIGGER outbox_insert_notify");
        jdbc.execute("ALTER TABLE outbox DISABLE TRIGGER outbox_schedule_notify");
    }

    @AfterEach
    void stopAndRemoveTestSlots() {
        workers.forEach(OutboxWorker::stop);
        for (String slot : slots) {
            await().atMost(Duration.ofSeconds(6)).until(() -> Boolean.FALSE.equals(jdbc.queryForObject(
                    "SELECT COALESCE(bool_or(active),false) FROM pg_replication_slots WHERE slot_name=?", Boolean.class, slot)));
            if (jdbc.queryForObject("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name=?", Long.class, slot) > 0) {
                jdbc.queryForList("SELECT pg_drop_replication_slot(?)", slot);
            }
        }
        for (String publication : publications) jdbc.execute("DROP PUBLICATION IF EXISTS " + publication);
        jdbc.execute("ALTER TABLE outbox ENABLE TRIGGER outbox_insert_notify");
        jdbc.execute("ALTER TABLE outbox ENABLE TRIGGER outbox_schedule_notify");
    }

    @AfterAll
    static void stopDefaultWorker(@Autowired OutboxWorker worker) { worker.stop(); }

    @Test
    void defaultConfigurationCreatesOneInterfaceBeanAndSelectsListenNotify() {
        assertThat(context.getBeansOfType(OutboxWorker.class)).hasSize(1);
        assertThat(context.getBean(OutboxWorker.class)).isExactlyInstanceOf(ListenNotifyWorker.class);
        assertThat(context.getBean(OutboxWorker.class).getType()).isEqualTo(WorkerType.LISTEN_NOTIFY);
    }

    @Test
    void pollingFindsOrdersWithoutNotificationsAndPauseSuppressesQueries() {
        var worker = pollingWorker();
        worker.start();
        await().atMost(Duration.ofSeconds(3)).until(() -> lockCalls() >= 2);
        var order = service.create(new CreateOrderRequest("Polling without notifications", 1, false));
        awaitProcessed(order.id(), 5);
        worker.setEnabled(false);
        // Espera o término de qualquer tentativa em andamento antes de contar consultas.
        await().pollDelay(Duration.ofMillis(150)).atMost(Duration.ofSeconds(2))
                .until(() -> worker.getState().equals("PAUSED"));
        long calls = lockCalls();
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(lockCalls()).isEqualTo(calls));
        var paused = service.create(new CreateOrderRequest("Received while paused", 1, false));
        assertThat(store.findOrder(paused.id()).orElseThrow().status()).isEqualTo("RECEIVED");
        worker.setEnabled(true);
        awaitProcessed(paused.id(), 5);
        verify(store, never()).nextAvailableDelayMillis();
    }

    @Test
    void pollingAutomaticallyRetriesWithoutAnyNotification() {
        var worker = pollingWorker();
        worker.start();
        var order = service.create(new CreateOrderRequest("Polling retry", 1, true));
        awaitProcessed(order.id(), 20);
        assertThat(store.events().getFirst().attempts()).isEqualTo(2);
    }

    @Test
    void cdcWakesOnlyAfterCommitWithoutTriggersAndAcknowledgesWal() {
        var worker = cdcWorker(200);
        worker.start();
        awaitQuiet(worker);
        String initialLsn = jdbc.queryForObject("SELECT confirmed_flush_lsn::text FROM pg_replication_slots WHERE slot_name=?", String.class, slots.getFirst());
        long calls = lockCalls();
        new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            var rolledBack = service.create(new CreateOrderRequest("Uncommitted WAL", 1, false));
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() -> {
                assertThat(lockCalls()).isEqualTo(calls);
                // O Awaitility usa outra thread/conexão: a linha ainda não confirmada é invisível.
                assertThat(store.findOrder(rolledBack.id())).isEmpty();
            });
            assertThat(store.findOrder(rolledBack.id()).orElseThrow().status()).isEqualTo("RECEIVED");
            tx.setRollbackOnly();
        });
        assertThat(store.countOrders()).isZero();
        jdbc.execute("CREATE TABLE unrelated_writes (id INTEGER)");
        try {
            jdbc.update("INSERT INTO unrelated_writes VALUES (1), (2)");
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                    .untilAsserted(() -> assertThat(lockCalls()).isEqualTo(calls));
        } finally { jdbc.execute("DROP TABLE unrelated_writes"); }
        var committed = service.create(new CreateOrderRequest("Committed WAL", 1, false));
        awaitProcessed(committed.id(), 5);
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> assertThat(jdbc.queryForObject(
                "SELECT pg_wal_lsn_diff(confirmed_flush_lsn, ?::pg_lsn) > 0 FROM pg_replication_slots WHERE slot_name=?",
                Boolean.class, initialLsn, slots.getFirst())).isTrue());
        assertThat(store.events().getFirst().attempts()).isEqualTo(1);
    }

    @Test
    void cdcRecoversPreexistingOrdersAndReusesTheSlotAfterRestart() {
        var before = service.create(new CreateOrderRequest("Before CDC startup", 1, false));
        var worker = cdcWorker(200);
        worker.start();
        awaitProcessed(before.id(), 10);
        awaitQuiet(worker);
        int originalPid = worker.getSourceBackendPid();
        worker.stop();
        var stopped = service.create(new CreateOrderRequest("While CDC stopped", 1, false));
        worker.start();
        awaitProcessed(stopped.id(), 10);
        awaitQuiet(worker);
        assertThat(worker.getSourceBackendPid()).isNotEqualTo(originalPid);
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM pg_replication_slots WHERE slot_name=?", Long.class, slots.getFirst())).isEqualTo(1);
        assertThat(store.events()).allMatch(event -> event.attempts() == 1);
    }

    @Test
    void cdcReconnectFindsOrdersCommittedDuringDisconnection() {
        var worker = cdcWorker(1500);
        worker.start();
        awaitQuiet(worker);
        int originalPid = worker.getSourceBackendPid();
        assertThat(jdbc.queryForObject("SELECT pg_terminate_backend(?)", Boolean.class, originalPid)).isTrue();
        await().atMost(Duration.ofSeconds(5)).until(() -> !worker.isSourceConnected());
        var order = service.create(new CreateOrderRequest("Disconnected CDC", 1, false));
        awaitProcessed(order.id(), 10);
        awaitQuiet(worker);
        assertThat(worker.getSourceBackendPid()).isNotEqualTo(originalPid);
    }

    @Test
    void cdcRetryTimerExhaustsBudgetAndManualRecoveryUsesWalWithoutTriggers() {
        var worker = cdcWorker(200);
        worker.start();
        awaitQuiet(worker);
        var order = service.create(new CreateOrderRequest("CDC permanent failure", 1, true, 2));
        await().atMost(Duration.ofSeconds(20)).untilAsserted(() -> assertThat(store.countFailures()).isEqualTo(1));
        var failed = store.failedEvents(25, 0).getFirst();
        assertThat(failed.attempts()).isEqualTo(2);
        awaitQuiet(worker);
        long calls = lockCalls();
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2))
                .untilAsserted(() -> assertThat(lockCalls()).isEqualTo(calls));
        assertThat(retryService.retryFailedEvent(failed.id())).isTrue();
        awaitProcessed(order.id(), 5);
        assertThat(store.processedEvents(25, 0).getFirst().attempts()).isEqualTo(3);
    }

    @Test
    void cdcRefusesReplicaWalInsteadOfSilentlyPretendingToCaptureChanges() {
        try (var replica = new PostgreSQLContainer<>("postgres:17-alpine")) {
            replica.start();
            JdbcConnectionDetails replicaDetails = new JdbcConnectionDetails() {
                public String getJdbcUrl() { return replica.getJdbcUrl(); }
                public String getUsername() { return replica.getUsername(); }
                public String getPassword() { return replica.getPassword(); }
            };
            var worker = new CdcWorker(runner, store, replicaDetails, true, 60000, 200, "replica_slot", "replica_publication");
            workers.add(worker);
            assertThatThrownBy(worker::start).isInstanceOf(IllegalStateException.class).hasMessageContaining("wal_level=logical");
            assertThat(worker.isRunning()).isFalse();
        }
    }

    private PollingWorker pollingWorker() {
        var worker = new PollingWorker(runner, store, true, 100, 200);
        workers.add(worker);
        return worker;
    }

    private CdcWorker cdcWorker(long reconnectMillis) {
        String suffix = UUID.randomUUID().toString().replace("-", "");
        String slot = "test_slot_" + suffix;
        String publication = "test_pub_" + suffix;
        slots.add(slot);
        publications.add(publication);
        var worker = new CdcWorker(runner, store, details, true, 60000, reconnectMillis, slot, publication);
        workers.add(worker);
        return worker;
    }

    private void awaitProcessed(UUID orderId, int seconds) {
        await().atMost(Duration.ofSeconds(seconds)).untilAsserted(() ->
                assertThat(store.findOrder(orderId).orElseThrow().status()).isEqualTo("PROCESSED"));
    }

    private void awaitQuiet(JdbcSignalWorker worker) {
        AtomicLong previous = new AtomicLong(lockCalls());
        await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(10)).until(() -> {
            long current = lockCalls();
            long last = previous.getAndSet(current);
            return worker.isSourceConnected() && worker.getState().equals("WAITING_FOR_SIGNAL") && current == last;
        });
    }

    private long lockCalls() {
        return mockingDetails(store).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("lockNextEvent")).count();
    }
}
