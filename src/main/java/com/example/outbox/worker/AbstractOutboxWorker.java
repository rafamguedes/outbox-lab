package com.example.outbox.worker;

import com.example.outbox.repository.OutboxStore;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractOutboxWorker implements OutboxWorker {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    private final OutboxRunner runner;
    private final OutboxStore store;
    private final AtomicBoolean enabled;
    private final long idleMillis;
    protected final long reconnectMillis;
    // Os sinais são agrupados: nunca há uma tarefa em memória para cada evento.
    private final ArrayBlockingQueue<Boolean> wakeups = new ArrayBlockingQueue<>(1);
    private volatile boolean running;
    private volatile String state = "STOPPED";
    private volatile Thread processingThread;
    private volatile Thread sourceThread;

    protected AbstractOutboxWorker(OutboxRunner runner, OutboxStore store, boolean enabled,
                                  long idleMillis, long reconnectMillis) {
        if (idleMillis < 1 || reconnectMillis < 1) throw new IllegalArgumentException("Worker intervals must be positive.");
        this.runner = runner;
        this.store = store;
        this.enabled = new AtomicBoolean(enabled);
        this.idleMillis = idleMillis;
        this.reconnectMillis = reconnectMillis;
    }

    @Override
    public synchronized void start() {
        if (running) return;
        beforeStart();
        wakeups.clear();
        running = true;
        state = "STARTING";
        String prefix = "outbox-" + getType().name().toLowerCase(java.util.Locale.ROOT);
        processingThread = Thread.ofPlatform().daemon().name(prefix + "-processing").unstarted(this::processLoop);
        if (hasSignalSource()) sourceThread = Thread.ofPlatform().daemon().name(prefix + "-source").unstarted(this::runSignalSource);
        processingThread.start();
        if (hasSignalSource()) sourceThread.start();
        signal();
    }

    protected void beforeStart() {}
    protected boolean hasSignalSource() { return false; }
    protected boolean usesRetryTimer() { return true; }
    protected boolean sourceAvailable() { return true; }
    protected void runSignalSource() {}
    protected void closeSignalSource() {}
    protected String idleState() { return "WAITING_FOR_SIGNAL"; }
    protected final void signal() { wakeups.offer(Boolean.TRUE); }

    protected final boolean sourceActive() {
        return running && Thread.currentThread() == sourceThread && !Thread.currentThread().isInterrupted();
    }

    protected final boolean currentSourceThread() { return Thread.currentThread() == sourceThread; }

    private boolean processingActive() {
        return running && Thread.currentThread() == processingThread && !Thread.currentThread().isInterrupted();
    }

    private void processLoop() {
        long waitMillis = idleMillis;
        String waitingState = idleState();
        while (processingActive()) {
            try {
                if (enabled.get()) {
                    state = waitingState;
                    wakeups.poll(waitMillis, TimeUnit.MILLISECONDS);
                } else {
                    state = "PAUSED";
                    wakeups.take();
                }
                if (!processingActive()) break;
                if (!enabled.get()) continue;
                state = "PROCESSING";
                drain();
                if (!processingActive() || !enabled.get()) continue;
                waitMillis = idleMillis;
                waitingState = idleState();
                // O polling já terá outra consulta em breve; não precisa consultar o timer.
                if (usesRetryTimer()) {
                    var next = store.nextAvailableDelayMillis();
                    if (next.isPresent()) {
                        long delay = next.get();
                        waitMillis = Math.min(idleMillis, Math.max(1000, delay));
                        waitingState = delay > 0 ? "WAITING_FOR_RETRY" : "WAITING_FOR_AVAILABILITY";
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (RuntimeException e) {
                log.error("Processing failed; scheduling recovery", e);
                waitMillis = Math.min(idleMillis, reconnectMillis);
                waitingState = "RECOVERING";
            }
        }
        if (Thread.currentThread() == processingThread) state = "STOPPED";
    }

    private void drain() {
        boolean empty = false;
        while (processingActive() && enabled.get() && !empty) {
            int attempted = 0;
            long deadline = System.nanoTime() + 500_000_000L;
            for (int i = 0; i < 100 && processingActive() && enabled.get() && System.nanoTime() < deadline; i++) {
                if (!runner.processNext()) { empty = true; break; }
                attempted++;
            }
            if (attempted > 0) log.info("Worker executed {} attempt(s) in this chunk", attempted);
            // Cada evento mantém sua transação e outros blocos continuam sem outro sinal.
            if (!empty) Thread.yield();
        }
    }

    @Override public boolean isEnabled() { return enabled.get(); }
    @Override public void setEnabled(boolean value) { enabled.set(value); signal(); }
    @Override public boolean isRunning() { return running; }
    @Override public String getState() { return !running ? "STOPPED" : enabled.get() ? state : "PAUSED"; }

    @Override
    public String description() {
        String prefix = getType().getDisplayName() + " · ";
        if (!running) return prefix + "Stopped";
        if (!enabled.get()) return prefix + "Automatic processing paused";
        if (!sourceAvailable()) return prefix + "Source reconnecting; safety recovery enabled";
        return prefix + switch (state) {
            case "PROCESSING" -> "Processing events";
            case "WAITING_FOR_RETRY" -> "Waiting for the retry time";
            case "WAITING_FOR_AVAILABILITY" -> "Waiting for events held by another transaction";
            case "WAITING_FOR_POLL" -> "Waiting for the next database poll";
            case "RECOVERING" -> "Recovering after a failure";
            default -> "Waiting for a signal";
        };
    }

    @Override
    public synchronized void stop() {
        running = false;
        signal();
        if (processingThread != null) processingThread.interrupt();
        if (sourceThread != null) sourceThread.interrupt();
        closeSignalSource();
        join(processingThread);
        join(sourceThread);
        state = "STOPPED";
    }

    private void join(Thread thread) {
        if (thread == null || thread == Thread.currentThread()) return;
        try { thread.join(6000); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
