package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class RabbitBridgeConcurrencyGuard {

    private final Semaphore permits;

    public RabbitBridgeConcurrencyGuard(RabbitWebFluxBridgeProperties.Bridge bridge) {
        this(bridge.getMaxConcurrency());
    }

    public RabbitBridgeConcurrencyGuard(int maxConcurrency) {
        this.permits = new Semaphore(maxConcurrency);
    }

    public Future<?> submit(ExecutorService executor, Runnable task) {
        return submit(executor, () -> {
            task.run();
            return null;
        });
    }

    public <T> Future<T> submit(ExecutorService executor, Callable<T> task) {
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
        }
    }

    private void acquire() {
        if (!permits.tryAcquire()) {
            throw new RabbitBridgeRejectedException("Rabbit bridge concurrency limit reached");
        }
    }

    private static final class GuardedFutureTask<T> extends FutureTask<T> {
        private final Semaphore permits;
        private final AtomicBoolean released = new AtomicBoolean();

        private GuardedFutureTask(Callable<T> callable, Semaphore permits) {
            super(callable);
            this.permits = permits;
        }

        @Override
        protected void done() {
            releasePermit();
        }

        private void releasePermit() {
            if (released.compareAndSet(false, true)) {
                permits.release();
            }
        }
    }
}
