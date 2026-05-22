package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.List;
import java.util.concurrent.*;

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
            if (!permits.tryAcquire()) {
                throw new RabbitBridgeRejectedException("Rabbit bridge virtual executor concurrency limit reached");
            }
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
    }
}
