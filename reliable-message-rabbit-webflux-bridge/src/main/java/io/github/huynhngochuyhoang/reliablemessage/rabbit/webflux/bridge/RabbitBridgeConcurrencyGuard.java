package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitBridgeConcurrencyGuard {

    private final Semaphore permits;

    public RabbitBridgeConcurrencyGuard(RabbitWebFluxBridgeProperties.Bridge bridge) {
        this(bridge.getMaxConcurrency());
    }

    public RabbitBridgeConcurrencyGuard(int maxConcurrency) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be greater than 0");
        }
        this.permits = new Semaphore(maxConcurrency);
    }

    public Future<?> submit(ExecutorService executor, Runnable task) {
        Objects.requireNonNull(task, "task");
        return submit(executor, () -> {
            task.run();
            return null;
        });
    }

    public <T> Future<T> submit(ExecutorService executor, Callable<T> task) {
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(task, "task");
        acquire();
        GuardedFutureTask<T> futureTask = new GuardedFutureTask<>(task, permits);
        try {
            executor.execute(futureTask);
            return futureTask;
        } catch (RejectedExecutionException exception) {
            futureTask.releasePermit();
            throw new RabbitBridgeRejectedException("Rabbit bridge executor rejected work", exception);
        } catch (RuntimeException exception) {
            futureTask.releasePermit();
            throw exception;
        } catch (Error error) {
            futureTask.releasePermit();
            throw error;
        }
    }

    private void acquire() {
        if (!permits.tryAcquire()) {
            throw new RabbitBridgeRejectedException("Rabbit bridge concurrency limit reached");
        }
    }

    private static final class GuardedFutureTask<T> extends FutureTask<T> {
        private final Semaphore permits;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();

        private GuardedFutureTask(Callable<T> callable, Semaphore permits) {
            super(callable);
            this.permits = permits;
        }

        @Override
        public void run() {
            started.set(true);
            try {
                super.run();
            } finally {
                releasePermit();
            }
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            boolean cancelled = super.cancel(mayInterruptIfRunning);
            if (cancelled && !started.get()) {
                releasePermit();
            }
            return cancelled;
        }

        private void releasePermit() {
            if (released.compareAndSet(false, true)) {
                permits.release();
            }
        }
    }
}
