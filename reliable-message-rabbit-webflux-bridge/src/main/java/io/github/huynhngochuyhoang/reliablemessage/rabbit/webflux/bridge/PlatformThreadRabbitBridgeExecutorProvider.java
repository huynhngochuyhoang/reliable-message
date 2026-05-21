package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PlatformThreadRabbitBridgeExecutorProvider implements RabbitBridgeExecutorProvider {

    private final ExecutorService executor;

    public PlatformThreadRabbitBridgeExecutorProvider(RabbitWebFluxBridgeProperties.Bridge bridge) {
        int workerThreads = bridge.getWorkerThreads();
        this.executor = new ThreadPoolExecutor(
                workerThreads,
                workerThreads,
                0L,
                TimeUnit.MILLISECONDS,
                workQueue(bridge.getQueueCapacity()),
                threadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void close() {
        executor.shutdownNow();
    }

    private static BlockingQueue<Runnable> workQueue(int queueCapacity) {
        return queueCapacity == 0 ? new SynchronousQueue<>() : new ArrayBlockingQueue<>(queueCapacity);
    }

    private static ThreadFactory threadFactory() {
        AtomicInteger counter = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "reliable-message-rabbit-bridge-platform-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }
}
