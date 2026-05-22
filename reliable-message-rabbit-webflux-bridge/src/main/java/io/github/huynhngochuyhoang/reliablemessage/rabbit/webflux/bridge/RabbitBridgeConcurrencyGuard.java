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
        return submitFuture(executor, task);
    }

    public <T> CompletableFuture<T> submitFuture(ExecutorService executor, Callable<T> task) {
        Objects.requireNonNull(executor, "executor");
        Objects.requireNonNull(task, "task");
        acquire();
        GuardedCompletableFuture<T> future = new GuardedCompletableFuture<>(task, permits);
        try {
            executor.execute(future);
            return future;
        } catch (RejectedExecutionException exception) {
            future.releasePermit();
            throw new RabbitBridgeRejectedException("Rabbit bridge executor rejected work", exception);
        } catch (RuntimeException | Error exception) {
            future.releasePermit();
            throw exception;
        }
    }

    private void acquire() {
        if (!permits.tryAcquire()) {
            throw new RabbitBridgeRejectedException("Rabbit bridge concurrency limit reached");
        }
    }

    private static final class GuardedCompletableFuture<T> extends CompletableFuture<T> implements Runnable {
        private final Callable<T> callable;
        private final Semaphore permits;
        private final AtomicBoolean started = new AtomicBoolean();
        private final AtomicBoolean released = new AtomicBoolean();

        private GuardedCompletableFuture(Callable<T> callable, Semaphore permits) {
            this.callable = callable;
            this.permits = permits;
        }

        @Override
        public void run() {
            started.set(true);
            if (isCancelled()) {
                releasePermit();
                return;
            }

            try {
                T result = callable.call();
                releasePermit();
                complete(result);
            } catch (Throwable error) {
                releasePermit();
                completeExceptionally(error);
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
