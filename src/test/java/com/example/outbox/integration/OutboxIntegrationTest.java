package com.example.outbox.integration;

import com.example.outbox.domain.Order;
import com.example.outbox.domain.OutboxEvent;
import com.example.outbox.dto.CreateOrderRequest;
import com.example.outbox.service.OrderService;
import com.example.outbox.service.EventRetryService;
import com.example.outbox.repository.OutboxStore;
import com.example.outbox.worker.OutboxRunner;
import com.example.outbox.worker.OutboxWorker;
import com.example.outbox.worker.ListenNotifyWorker;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.postgresql.PGConnection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.JdbcConnectionDetails;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import static org.assertj.core.api.Assertions.*;
import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest(properties = "outbox.worker.enabled=false")
@AutoConfigureMockMvc
@Testcontainers
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OutboxIntegrationTest {
    @Container @ServiceConnection
    static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:17-alpine")
            .withCommand("postgres", "-c", "wal_level=logical", "-c", "max_replication_slots=4",
                    "-c", "max_wal_senders=4", "-c", "max_slot_wal_keep_size=64MB");
    @Autowired OrderService service;
    @Autowired EventRetryService retryService;
    @Autowired OutboxRunner runner;
    @Autowired JdbcTemplate jdbc;
    @Autowired MockMvc mvc;
    @Autowired PlatformTransactionManager transactionManager;
    @Autowired JdbcConnectionDetails connectionDetails;
    @MockitoSpyBean OutboxStore store;

    @BeforeEach
    void clean() { jdbc.execute("TRUNCATE outbox, orders"); }

    @AfterAll
    static void stopApplicationWorker(@Autowired OutboxWorker worker) {
        // Encerra o LISTEN antes que o Testcontainers remova o banco de teste.
        worker.stop();
    }

    @Test
    void storesOrderAndEventAtomically() {
        var order = service.create(new CreateOrderRequest(" Keyboard ", 2, false));
        assertThat(store.findOrder(order.id())).get().extracting(Order::status).isEqualTo("RECEIVED");
        assertThat(store.findOrder(order.id()).orElseThrow().product()).isEqualTo("Keyboard");
        assertThat(store.events()).singleElement().satisfies(event -> {
            assertThat(event.orderId()).isEqualTo(order.id());
            assertThat(event.type()).isEqualTo("OrderCreated");
            assertThat(event.status()).isEqualTo("PENDING");
            assertThat(event.payload()).contains("Keyboard", order.id().toString());
            assertThat(event.attempts()).isZero();
        });
    }

    @Test
    void failedEventInsertRollsBackOrder() {
        doThrow(new IllegalStateException("Event insert failed")).when(store)
                .insertEvent(any(UUID.class), any(UUID.class), anyString(), anyInt(), any(OffsetDateTime.class));
        assertThatThrownBy(() -> service.create(new CreateOrderRequest("Mouse", 1, false)))
                .isInstanceOf(IllegalStateException.class);
        assertThat(store.countOrders()).isZero();
        assertThat(store.events()).isEmpty();
    }

    @Test
    void completesOrderAndEventAndHandlesAnEmptyQueue() {
        var order = service.create(new CreateOrderRequest("Monitor", 1, false));
        assertThat(runner.processNext()).isTrue();
        assertThat(store.findOrder(order.id())).get().extracting(Order::status).isEqualTo("PROCESSED");
        assertThat(store.events()).singleElement().satisfies(event -> {
            assertThat(event.status()).isEqualTo("PROCESSED");
            assertThat(event.attempts()).isEqualTo(1);
            assertThat(event.processedAt()).isNotNull();
        });
        assertThat(runner.processNext()).isFalse();
    }

    @Test
    void failureRollsBackOrderAndSchedulesRetry() {
        var order = service.create(new CreateOrderRequest("Camera", 1, true));
        assertThat(runner.processNext()).isTrue();
        assertThat(store.findOrder(order.id())).get().satisfies(current -> {
            assertThat(current.status()).isEqualTo("RECEIVED");
            assertThat(current.processedAt()).isNull();
        });
        assertThat(store.events().getFirst().status()).isEqualTo("RETRY");
        assertThat(store.events().getFirst().lastError()).contains("Simulated failure");
        assertThat(runner.processNext()).isFalse();
        jdbc.update("UPDATE outbox SET available_at = CURRENT_TIMESTAMP");
        assertThat(runner.processNext()).isTrue();
        assertThat(store.events().getFirst().attempts()).isEqualTo(2);
        assertThat(store.events().getFirst().lastError()).isNull();
        assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("PROCESSED");
    }

    @Test
    void failedEventCompletionRollsBackTheWholeTransaction() {
        var order = service.create(new CreateOrderRequest("Headset", 1, false));
        doThrow(new IllegalStateException("Completion failed")).when(store).completeEvent(any(UUID.class));
        assertThatThrownBy(runner::processNext).isInstanceOf(IllegalStateException.class);
        assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("RECEIVED");
        assertThat(store.events().getFirst().status()).isEqualTo("PENDING");
        assertThat(store.events().getFirst().attempts()).isZero();
    }

    @Test
    void repeatedEventDoesNotRepeatTheOrderEffect() {
        var order = service.create(new CreateOrderRequest("Cable", 1, false));
        runner.processNext();
        OffsetDateTime processedAt = store.findOrder(order.id()).orElseThrow().processedAt();
        jdbc.update("UPDATE outbox SET status='PENDING', available_at=CURRENT_TIMESTAMP");
        runner.processNext();
        assertThat(store.findOrder(order.id()).orElseThrow().processedAt()).isEqualTo(processedAt);
        assertThat(store.events().getFirst().attempts()).isEqualTo(2);
    }

    @Test
    void anotherWorkerSkipsTheLockedEvent() {
        service.create(new CreateOrderRequest("First", 1, false));
        service.create(new CreateOrderRequest("Second", 1, false));
        new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            var locked = store.lockNextEvent().orElseThrow();
            try {
                assertThat(CompletableFuture.supplyAsync(runner::processNext).get(10, TimeUnit.SECONDS)).isTrue();
            } catch (Exception e) { throw new AssertionError("Second worker must not wait for the locked event", e); }
            assertThat(store.events().stream().filter(event -> event.id().equals(locked.id()))
                    .findFirst().orElseThrow().status()).isEqualTo("PENDING");
            assertThat(store.countProcessed()).isEqualTo(1);
        });
    }

    @Test
    void apiUsesEnglishFieldsAndValidatesRequests() throws Exception {
        mvc.perform(post("/orders").contentType("application/json")
                        .content("{\"product\":\"Keyboard\",\"quantity\":2}"))
                .andExpect(status().isCreated()).andExpect(header().exists("Location"))
                .andExpect(jsonPath("$.product").value("Keyboard"))
                .andExpect(jsonPath("$.status").value("RECEIVED"));
        mvc.perform(post("/orders").contentType("application/json")
                        .content("{\"product\":\"\",\"quantity\":0}")).andExpect(status().isBadRequest());
        mvc.perform(get("/orders/" + UUID.randomUUID())).andExpect(status().isNotFound());
        assertThat(store.countOrders()).isEqualTo(1);
    }

    @Test
    void dashboardSeparatesProcessedAndPermanentFailuresAndEscapesPayloads() throws Exception {
        var failedEvent = createFailedEvent("<script>alert(1)</script>");
        service.create(new CreateOrderRequest("Healthy processed event", 1, false));
        assertThat(runner.processNext()).isTrue();
        var processedEvent = store.processedEvents(25, 0).getFirst();
        service.create(new CreateOrderRequest("Pending event must stay hidden", 1, false));
        mvc.perform(get("/")).andExpect(status().isOk())
                .andExpect(model().attribute("failedEvents", java.util.List.of(failedEvent)))
                .andExpect(model().attribute("processedEvents", java.util.List.of(processedEvent)))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Permanent failures")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Processed events")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("&lt;script&gt;")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Simulated failure")))
                .andExpect(content().string(org.hamcrest.Matchers.containsString("Healthy processed event")))
                .andExpect(content().string(org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("Pending event must stay hidden"))))
                .andExpect(content().string(org.hamcrest.Matchers.not(
                        org.hamcrest.Matchers.containsString("<script>alert(1)</script>"))));
        mvc.perform(get("/dashboard/data")).andExpect(status().isOk())
                .andExpect(content().string(org.hamcrest.Matchers.containsString("id=\"data\"")));
        mvc.perform(post("/dashboard/orders").param("product", "").param("quantity", "0"))
                .andExpect(status().isNotFound());
        mvc.perform(post("/dashboard/process")).andExpect(status().isNotFound());
        mvc.perform(post("/dashboard/worker").param("enabled", "false")).andExpect(status().isNotFound());
    }

    @Test
    void failedEventsAreFilteredBeforePaginationAndInvalidPagesAreClamped() throws Exception {
        for (int index = 0; index < 30; index++) {
            createFailedEvent("Failure " + index);
        }
        // Eventos recentes sem falha não podem esconder as falhas mais antigas.
        for (int index = 0; index < 55; index++) {
            service.create(new CreateOrderRequest("New processed " + index, 1, false));
            assertThat(runner.processNext()).isTrue();
        }
        var firstPage = store.failedEvents(25, 0);
        var secondPage = store.failedEvents(25, 25);
        assertThat(firstPage).hasSize(25).allMatch(event -> event.status().equals("FAILED"));
        assertThat(secondPage).hasSize(5).allMatch(event -> event.status().equals("FAILED"));
        assertThat(firstPage).doesNotContainAnyElementsOf(secondPage);
        mvc.perform(get("/").param("failedPage", "0")).andExpect(status().isOk())
                .andExpect(model().attribute("failedEvents", firstPage))
                .andExpect(model().attribute("failedPage", 1L)).andExpect(model().attribute("failedPageCount", 2L))
                .andExpect(model().attribute("failureCount", 30L));
        mvc.perform(get("/dashboard/data").param("failedPage", "999").param("processedPage", "999"))
                .andExpect(status().isOk()).andExpect(model().attribute("failedEvents", secondPage))
                .andExpect(model().attribute("processedEvents", store.processedEvents(25, 50)))
                .andExpect(model().attribute("failedPage", 2L)).andExpect(model().attribute("processedPage", 3L))
                .andExpect(model().attribute("processedCount", 55L));
    }

    @Test
    void automaticRetryExhaustionStopsPollingAndManualRecoveryWakesWorker() {
        ListenNotifyWorker worker = startWorker(60000, 500);
        try {
            awaitIdle(worker);
            var order = service.create(new CreateOrderRequest("Exhaust automatic retries", 1, true, 2));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(store.countFailures()).isEqualTo(1));
            var failed = store.failedEvents(25, 0).getFirst();
            assertThat(failed.attempts()).isEqualTo(2);
            assertThat(store.countPending()).isZero();
            assertThat(store.countRetries()).isZero();
            assertThat(store.nextAvailableDelayMillis()).isEmpty();
            assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("RECEIVED");
            awaitIdle(worker);
            long calls = lockCalls();
            await().during(Duration.ofMillis(1400)).atMost(Duration.ofSeconds(3))
                    .untilAsserted(() -> assertThat(lockCalls()).isEqualTo(calls));
            assertThat(retryService.retryFailedEvent(failed.id())).isTrue();
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("PROCESSED"));
            assertThat(store.events().getFirst().attempts()).isEqualTo(3);
        } finally { worker.stop(); }
    }

    @Test
    void manualRetryQueuesOnlyTheSelectedPermanentFailureAndPreservesHistory() throws Exception {
        var selected = createFailedEvent("Selected permanent failure");
        var other = createFailedEvent("Other permanent failure");
        try (Connection connection = openTestListener()) {
            mvc.perform(post("/dashboard/events/" + selected.id() + "/retry"))
                    .andExpect(status().is3xxRedirection())
                    .andExpect(redirectedUrl("/?failedPage=1&processedPage=1"))
                    .andExpect(flash().attribute("message", org.hamcrest.Matchers.containsString("Manual retry queued")));
            assertThat(notificationCount(connection.unwrap(PGConnection.class), 2000)).isEqualTo(1);
        }
        var queued = store.events().stream().filter(event -> event.id().equals(selected.id())).findFirst().orElseThrow();
        assertThat(queued.status()).isEqualTo("PENDING");
        assertThat(queued.attempts()).isEqualTo(2);
        assertThat(queued.lastError()).isEqualTo(selected.lastError());
        assertThat(queued.payload()).isEqualTo(selected.payload());
        assertThat(queued.createdAt()).isEqualTo(selected.createdAt());
        assertThat(store.failedEvents(25, 0)).containsExactly(other);
        assertThat(retryService.retryFailedEvent(selected.id())).isFalse();
        assertThat(runner.processNext()).isTrue();
        assertThat(runner.processNext()).isFalse();
        assertThat(store.findOrder(selected.orderId()).orElseThrow().status()).isEqualTo("PROCESSED");
        assertThat(store.processedEvents(25, 0)).singleElement().satisfies(event -> {
            assertThat(event.id()).isEqualTo(selected.id());
            assertThat(event.attempts()).isEqualTo(3);
            assertThat(event.lastError()).isNull();
        });
    }

    @Test
    void manualRetryRejectsPendingRetryingProcessedAndMissingEvents() throws Exception {
        service.create(new CreateOrderRequest("Already processed", 1, false));
        assertThat(runner.processNext()).isTrue();
        service.create(new CreateOrderRequest("Automatic retry", 1, true));
        assertThat(runner.processNext()).isTrue();
        service.create(new CreateOrderRequest("New pending", 1, false));
        var original = store.events();
        for (var event : original) {
            mvc.perform(post("/dashboard/events/" + event.id() + "/retry"))
                    .andExpect(status().is3xxRedirection())
                    .andExpect(flash().attribute("message", org.hamcrest.Matchers.containsString("No retry was queued")));
        }
        mvc.perform(post("/dashboard/events/" + UUID.randomUUID() + "/retry"))
                .andExpect(flash().attribute("message", org.hamcrest.Matchers.containsString("No retry was queued")));
        assertThat(store.events()).isEqualTo(original);
        mvc.perform(get("/dashboard/events/" + original.getFirst().id() + "/retry"))
                .andExpect(status().isMethodNotAllowed());
        mvc.perform(get("/")).andExpect(model().attribute("retryCount", 1L))
                .andExpect(content().string(org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("retry-form"))));
    }

    @Test
    void failedManualAttemptStaysPermanentWithoutRestartingAutomaticBudget() {
        var failed = createFailedEvent("Fails three times", 3);
        assertThat(retryService.retryFailedEvent(failed.id())).isTrue();
        assertThat(runner.processNext()).isTrue();
        var failedAgain = store.failedEvents(25, 0).getFirst();
        assertThat(failedAgain.attempts()).isEqualTo(3);
        assertThat(failedAgain.failuresRemaining()).isZero();
        assertThat(store.nextAvailableDelayMillis()).isEmpty();
        assertThat(runner.processNext()).isFalse();
        assertThat(retryService.retryFailedEvent(failed.id())).isTrue();
        assertThat(runner.processNext()).isTrue();
        assertThat(store.processedEvents(25, 0).getFirst().attempts()).isEqualTo(4);
    }

    @Test
    void concurrentManualRetryRequestsQueueExactlyOneAdditionalAttempt() throws Exception {
        var failed = createFailedEvent("Concurrent manual retry");
        var first = CompletableFuture.supplyAsync(() -> retryService.retryFailedEvent(failed.id()));
        var second = CompletableFuture.supplyAsync(() -> retryService.retryFailedEvent(failed.id()));
        assertThat(java.util.List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS)))
                .containsExactlyInAnyOrder(true, false);
        assertThat(store.countPending()).isEqualTo(1);
        assertThat(store.events().getFirst().attempts()).isEqualTo(2);
        assertThat(runner.processNext()).isTrue();
        assertThat(runner.processNext()).isFalse();
        assertThat(store.processedEvents(25, 0).getFirst().attempts()).isEqualTo(3);
    }

    @Test
    void rolledBackManualRecoveryKeepsPermanentStateAndDoesNotNotify() throws Exception {
        var failed = createFailedEvent("Rollback manual recovery");
        try (Connection connection = openTestListener()) {
            var pg = connection.unwrap(PGConnection.class);
            new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                assertThat(retryService.retryFailedEvent(failed.id())).isTrue();
                assertThat(notificationCount(pg, 150)).isZero();
                tx.setRollbackOnly();
            });
            assertThat(notificationCount(pg, 150)).isZero();
        }
        assertThat(store.failedEvents(25, 0)).containsExactly(failed);
        assertThat(store.countPending()).isZero();
    }

    @Test
    void permanentFailureWriteErrorRollsBackTheProcessingTransaction() {
        var order = service.create(new CreateOrderRequest("Failure persistence", 1, true, 2));
        assertThat(runner.processNext()).isTrue();
        jdbc.update("UPDATE outbox SET available_at=clock_timestamp() WHERE order_id=?", order.id());
        var before = store.events().getFirst();
        doThrow(new IllegalStateException("Cannot persist permanent failure")).when(store).failEvent(any(), anyString());
        assertThatThrownBy(runner::processNext).isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot persist permanent failure");
        assertThat(store.events()).containsExactly(before);
        assertThat(store.findOrder(order.id()).orElseThrow().processedAt()).isNull();
    }

    @Test
    void apiValidatesFailureAttemptCountAndPreservesFirstFailureDefault() throws Exception {
        mvc.perform(post("/orders").contentType("application/json")
                .content("{\"product\":\"Default failure\",\"quantity\":1,\"simulateFailure\":true}"))
                .andExpect(status().isCreated());
        assertThat(store.events().getFirst().failuresRemaining()).isEqualTo(1);
        mvc.perform(post("/orders").contentType("application/json")
                .content("{\"product\":\"Two failures\",\"quantity\":1,\"simulateFailure\":true,\"failureAttempts\":2}"))
                .andExpect(status().isCreated());
        assertThat(store.events().getFirst().failuresRemaining()).isEqualTo(2);
        mvc.perform(post("/orders").contentType("application/json")
                .content("{\"product\":\"No failure\",\"quantity\":1,\"failureAttempts\":3}"))
                .andExpect(status().isCreated());
        assertThat(store.events().getFirst().failuresRemaining()).isZero();
        for (int invalid : new int[]{-1, 101}) {
            mvc.perform(post("/orders").contentType("application/json")
                    .content("{\"product\":\"Invalid\",\"quantity\":1,\"failureAttempts\":" + invalid + "}"))
                    .andExpect(status().isBadRequest());
        }
        assertThat(store.countOrders()).isEqualTo(3);
    }

    private OutboxEvent createFailedEvent(String product) {
        return createFailedEvent(product, 2);
    }

    private OutboxEvent createFailedEvent(String product, int failureAttempts) {
        var order = service.create(new CreateOrderRequest(product, 1, true, failureAttempts));
        assertThat(runner.processNext()).isTrue();
        // O timer real tem teste próprio; aqui antecipamos a segunda tentativa da fixture.
        jdbc.update("UPDATE outbox SET available_at=clock_timestamp() WHERE order_id=?", order.id());
        assertThat(runner.processNext()).isTrue();
        return store.failedEvents(25, 0).stream().filter(event -> event.orderId().equals(order.id()))
                .findFirst().orElseThrow();
    }

    @Test
    void batchEndpointsAndTablesHaveBeenRemoved() throws Exception {
        mvc.perform(post("/lotes").contentType("application/json")
                .content("{\"total\":100,\"produto\":\"Removed\"}")).andExpect(status().isNotFound());
        mvc.perform(post("/dashboard/batches")).andExpect(status().isNotFound());
        assertThat(jdbc.queryForObject("SELECT to_regclass('public.lotes')::text", String.class)).isNull();
        mvc.perform(get("/")).andExpect(content().string(
                org.hamcrest.Matchers.not(org.hamcrest.Matchers.containsString("batch-card"))));
    }

    @Test
    void notificationIsDeliveredOnlyAfterCommitAndNeverAfterRollback() throws Exception {
        try (Connection connection = openTestListener()) {
            PGConnection pg = connection.unwrap(PGConnection.class);
            new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                service.create(new CreateOrderRequest("Rolled back", 1, false));
                assertThat(notificationCount(pg, 150)).isZero();
                tx.setRollbackOnly();
            });
            assertThat(notificationCount(pg, 150)).isZero();
            assertThat(store.countOrders()).isZero();
            service.create(new CreateOrderRequest("Committed", 1, false));
            assertThat(notificationCount(pg, 2000)).isEqualTo(1);
        }
    }

    @Test
    void notificationWakesWorkerAndIdleWorkerDoesNotPollEverySecond() {
        ListenNotifyWorker worker = startWorker(60000, 500);
        try {
            awaitIdle(worker);
            var order = service.create(new CreateOrderRequest("Signal", 1, false));
            awaitProcessed(order.id());
            awaitIdle(worker);
            long calls = lockCalls();
            // A recuperação é de 60 s: a fila vazia não deve ser consultada a cada 1 s.
            await().during(Duration.ofMillis(1400)).atMost(Duration.ofSeconds(3)).untilAsserted(() ->
                    assertThat(lockCalls()).isEqualTo(calls));
        } finally { worker.stop(); }
    }

    @Test
    void initialRecoveryFindsEventsStoredBeforeListenerConnected() {
        var order = service.create(new CreateOrderRequest("Before listener", 1, false));
        ListenNotifyWorker worker = startWorker(60000, 500);
        try { awaitProcessed(order.id()); } finally { worker.stop(); }
    }

    @Test
    void singleNotificationDrainsMultipleProcessingChunks() throws Exception {
        try (Connection connection = openTestListener()) {
            PGConnection pg = connection.unwrap(PGConnection.class);
            new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
                for (int index = 1; index <= 200; index++) {
                    service.create(new CreateOrderRequest("Chunk test " + index, 1, false));
                }
            });
            // O PostgreSQL agrupa sinais idênticos emitidos na mesma transação.
            assertThat(notificationCount(pg, 2000)).isEqualTo(1);
        }
        ListenNotifyWorker worker = startWorker(60000, 500);
        try {
            await().atMost(Duration.ofSeconds(25)).untilAsserted(() ->
                    assertThat(store.countProcessed()).isEqualTo(200));
            assertThat(store.countPending()).isZero();
        } finally { worker.stop(); }
    }

    @Test
    void retryTimerWakesWorkerWithoutAnotherOrder() {
        ListenNotifyWorker worker = startWorker(60000, 500);
        try {
            awaitIdle(worker);
            var order = service.create(new CreateOrderRequest("Timed retry", 1, true));
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThat(store.events().getFirst().status()).isEqualTo("RETRY"));
            await().atMost(Duration.ofSeconds(20)).untilAsserted(() ->
                    assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("PROCESSED"));
            assertThat(store.events().getFirst().attempts()).isEqualTo(2);
        } finally { worker.stop(); }
    }

    @Test
    void reconnectRecoversEventsCreatedDuringDisconnection() {
        ListenNotifyWorker worker = startWorker(60000, 1500);
        try {
            awaitIdle(worker);
            int oldPid = worker.getSourceBackendPid();
            assertThat(jdbc.queryForObject("SELECT pg_terminate_backend(?)", Boolean.class, oldPid)).isTrue();
            await().atMost(Duration.ofSeconds(5)).until(() -> !worker.isSourceConnected());
            var order = service.create(new CreateOrderRequest("During disconnect", 1, false));
            await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
                assertThat(worker.isSourceConnected()).isTrue();
                assertThat(worker.getSourceBackendPid()).isNotEqualTo(oldPid);
                assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("PROCESSED");
            });
        } finally { worker.stop(); }
    }

    @Test
    void resumeRecoversEventsReceivedWhilePaused() {
        ListenNotifyWorker worker = startWorker(60000, 500);
        try {
            awaitIdle(worker);
            worker.setEnabled(false);
            var order = service.create(new CreateOrderRequest("Paused", 1, false));
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(2)).untilAsserted(() ->
                    assertThat(store.findOrder(order.id()).orElseThrow().status()).isEqualTo("RECEIVED"));
            worker.setEnabled(true);
            awaitProcessed(order.id());
        } finally { worker.stop(); }
    }

    @Test
    void periodicRecoveryFindsAnEventWithoutNotification() {
        ListenNotifyWorker worker = startWorker(2000, 200);
        try {
            var order = service.create(new CreateOrderRequest("Periodic recovery", 1, false));
            awaitProcessed(order.id());
            awaitIdle(worker);
            // Aguardar o worker ocioso evita que um sinal antigo processe a alteração
            // e faça o teste passar mesmo se a recuperação periódica estiver quebrada.
            await().during(Duration.ofMillis(300)).atMost(Duration.ofSeconds(4))
                    .until(() -> worker.getState().equals("WAITING_FOR_SIGNAL"));
            jdbc.update("UPDATE outbox SET status='PENDING' WHERE order_id=?", order.id());
            await().atMost(Duration.ofSeconds(5)).untilAsserted(() -> {
                assertThat(store.events().getFirst().status()).isEqualTo("PROCESSED");
                assertThat(store.events().getFirst().attempts()).isEqualTo(2);
            });
        } finally { worker.stop(); }
    }

    @Test
    void infrastructureFailureIsRetriedWithoutAnotherSignal() {
        var order = service.create(new CreateOrderRequest("Infrastructure retry", 1, false));
        doThrow(new IllegalStateException("Transient failure")).doCallRealMethod().when(store).lockNextEvent();
        ListenNotifyWorker worker = startWorker(60000, 500);
        try { awaitProcessed(order.id()); } finally { worker.stop(); }
    }

    @Test
    void legacyMigrationPreservesOrderEventIdsAndConvertsPendingPayload() throws Exception {
        String schema = "legacy_migration_test";
        UUID orderId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        jdbc.execute("CREATE SCHEMA " + schema);
        try {
            Flyway.configure().dataSource(connectionDetails.getJdbcUrl(), connectionDetails.getUsername(),
                    connectionDetails.getPassword()).schemas(schema).defaultSchema(schema).target("3").load().migrate();
            try (Connection connection = DriverManager.getConnection(connectionDetails.getJdbcUrl(),
                    connectionDetails.getUsername(), connectionDetails.getPassword())) {
                connection.setSchema(schema);
                try (var statement = connection.prepareStatement("""
                        INSERT INTO pedidos (id,produto,quantidade,status,created_at)
                        VALUES (?, 'Legacy keyboard', 2, 'RECEBIDO', CURRENT_TIMESTAMP)
                        """)) { statement.setObject(1, orderId); statement.executeUpdate(); }
                try (var statement = connection.prepareStatement("""
                        INSERT INTO outbox (id,pedido_id,tipo,payload,created_at,available_at)
                        VALUES (?,?,'PedidoCriado',jsonb_build_object('pedidoId',?::text,
                            'produto','Legacy keyboard','quantidade',2),CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)
                        """)) {
                    statement.setObject(1, eventId); statement.setObject(2, orderId);
                    statement.setString(3, orderId.toString()); statement.executeUpdate();
                }
                Flyway.configure().dataSource(connectionDetails.getJdbcUrl(), connectionDetails.getUsername(),
                        connectionDetails.getPassword()).schemas(schema).defaultSchema(schema).load().migrate();
                try (var statement = connection.createStatement();
                     var rows = statement.executeQuery("""
                             SELECT e.id,e.order_id,e.status,e.type,e.payload->>'orderId' AS payload_order,
                                 e.payload->>'product' AS product,o.quantity
                             FROM outbox e JOIN orders o ON o.id=e.order_id
                             """)) {
                    assertThat(rows.next()).isTrue();
                    assertThat(rows.getObject("id", UUID.class)).isEqualTo(eventId);
                    assertThat(rows.getObject("order_id", UUID.class)).isEqualTo(orderId);
                    assertThat(rows.getString("status")).isEqualTo("PENDING");
                    assertThat(rows.getString("type")).isEqualTo("OrderCreated");
                    assertThat(rows.getString("payload_order")).isEqualTo(orderId.toString());
                    assertThat(rows.getString("product")).isEqualTo("Legacy keyboard");
                    assertThat(rows.getInt("quantity")).isEqualTo(2);
                    assertThat(rows.next()).isFalse();
                }
            }
        } finally { jdbc.execute("DROP SCHEMA " + schema + " CASCADE"); }
    }

    @Test
    void eventIsImmediatelyAvailableEvenWhenApplicationClockIsAhead() {
        UUID orderId = UUID.randomUUID();
        OffsetDateTime applicationTime = OffsetDateTime.now().plusDays(1);
        String payload = "{\"orderId\":\"" + orderId + "\",\"product\":\"Clock test\",\"quantity\":1}";
        new TransactionTemplate(transactionManager).executeWithoutResult(tx -> {
            store.insertOrder(orderId, new CreateOrderRequest("Clock test", 1, false), applicationTime);
            store.insertEvent(UUID.randomUUID(), orderId, payload, 0, applicationTime);
        });
        assertThat(runner.processNext()).isTrue();
        assertThat(store.findOrder(orderId).orElseThrow().status()).isEqualTo("PROCESSED");
    }

    private void awaitProcessed(UUID id) {
        await().atMost(Duration.ofSeconds(10)).untilAsserted(() ->
                assertThat(store.findOrder(id).orElseThrow().status()).isEqualTo("PROCESSED"));
    }
    private ListenNotifyWorker startWorker(long recoveryMillis, long reconnectMillis) {
        var worker = new ListenNotifyWorker(runner, store, connectionDetails, true, recoveryMillis, reconnectMillis);
        worker.start();
        return worker;
    }
    private void awaitIdle(ListenNotifyWorker worker) {
        await().atMost(Duration.ofSeconds(10)).until(() -> worker.isSourceConnected()
                && worker.getState().equals("WAITING_FOR_SIGNAL") && lockCalls() > 0);
    }
    private long lockCalls() {
        return mockingDetails(store).getInvocations().stream()
                .filter(invocation -> invocation.getMethod().getName().equals("lockNextEvent")).count();
    }
    private Connection openTestListener() throws SQLException {
        Connection connection = DriverManager.getConnection(connectionDetails.getJdbcUrl(),
                connectionDetails.getUsername(), connectionDetails.getPassword());
        try (var statement = connection.createStatement()) { statement.execute("LISTEN outbox_events"); }
        return connection;
    }
    private int notificationCount(PGConnection pg, int timeoutMillis) {
        try {
            var notifications = pg.getNotifications(timeoutMillis);
            return notifications == null ? 0 : notifications.length;
        } catch (SQLException e) { throw new IllegalStateException(e); }
    }
}
