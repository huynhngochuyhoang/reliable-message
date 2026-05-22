package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class VirtualThreadRabbitBridgeExecutorProvider implements RabbitBridgeExecutorProvider {

    private final ExecutorService executor;

    public VirtualThreadRabbitBridgeExecutorProvider(RabbitWebFluxBridgeProperties.Bridge bridge) {
        ThreadFactory threadFactory = Thread.ofVirtual()
                .name("reliable-message-rabbit-bridge-virtual-", 1)
                .factory();
        ExecutorService delegate = Executors.newThreadPerTaskExecutor(threadFactory);
        this.executor = new BoundedVirtualThreadExecutorService(delegate, bridge.getMaxConcurrency());
    }

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void close() {
        RabbitBridgeExecutorShutdown.close(executor);
    }

    private static final class BoundedVirtualThreadExecutorService extends AbstractExecutorService {
        private final ExecutorService delegate;
        private final Semaphore permits;

        private BoundedVirtualThreadExecutorService(ExecutorService delegate, int maxConcurrency) {
            this.delegate = delegate;
            this.permits = new Semaphore(maxConcurrency);
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public List<Runnable> shutdownNow() {
            return delegate.shutdownNow();
        }

        @Override
        public boolean isShutdown() {
            return delegate.isShutdown();
        }

        @Override
        public boolean isTerminated() {
            return delegate.isTerminated();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            return delegate.awaitTermination(timeout, unit);
        }

        @Override
        public void execute(Runnable command) {
            Objects.requireNonNull(command, "command");
            acquire();
            try {
                delegate.execute(() -> {
                    try {
                        command.run();
                    } finally {
                        permits.release();
                    }
                });
            } catch (RejectedExecutionException exception) {
                permits.release();
                throw new RabbitBridgeRejectedException("Rabbit bridge virtual executor rejected work", exception);
            } catch (RuntimeException | Error exception) {
                permits.release();
                throw exception;
            }
        }

        @Override
        public Future<?> submit(Runnable task) {
            Objects.requireNonNull(task, "task");
            return submit(Executors.callable(task, null));
        }

        @Override
        public <T> Future<T> submit(Runnable task, T result) {
            Objects.requireNonNull(task, "task");
            return submit(Executors.callable(task, result));
        }

        @Override
        public <T> Future<T> submit(Callable<T> task) {
            Objects.requireNonNull(task, "task");
            acquire();
            PermitReleasingFutureTask<T> futureTask = new PermitReleasingFutureTask<>(task, permits);
            try {
                delegate.execute(futureTask);
                return futureTask;
            } catch (RejectedExecutionException exception) {
                futureTask.releasePermit();
                throw new RabbitBridgeRejectedException("Rabbit bridge virtual executor rejected work", exception);
            } catch (RuntimeException | Error exception) {
                futureTask.releasePermit();
                throw exception;
            }
        }

        private void acquire() {
            if (!permits.tryAcquire()) {
                throw new RabbitBridgeRejectedException("Rabbit bridge virtual executor concurrency limit reached");
            }
        }
    }

    private static final class PermitReleasingFutureTask<T> extends FutureTask<T> {
        private final Permit permit;
        private final AtomicBoolean started = new AtomicBoolean();

        private PermitReleasingFutureTask(Callable<T> callable, Semaphore permits) {
            this(callable, new Permit(permits));
        }

        private PermitReleasingFutureTask(Callable<T> callable, Permit permit) {
            super(() -> {
                try {
                    return callable.call();
                } finally {
                    permit.release();
                }
            });
            this.permit = permit;
        }

        @Override
        public void run() {
            started.set(true);
            try {
                super.run();
            } finally {
                if (isCancelled()) {
                    releasePermit();
                }
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
            permit.release();
        }
    }

    private static final class Permit {
        private final Semaphore permits;
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit(Semaphore permits) {
            this.permits = permits;
        }

        private void release() {
            if (released.compareAndSet(false, true)) {
                permits.release();
            }
        }
    }
}
