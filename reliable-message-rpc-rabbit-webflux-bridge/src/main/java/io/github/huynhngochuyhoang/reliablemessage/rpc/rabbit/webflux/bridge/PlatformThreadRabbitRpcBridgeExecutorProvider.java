package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PlatformThreadRabbitRpcBridgeExecutorProvider implements RabbitRpcBridgeExecutorProvider {

    private final ThreadPoolExecutor executor;
    private final Scheduler scheduler;

    public PlatformThreadRabbitRpcBridgeExecutorProvider(RabbitRpcWebFluxBridgeProperties properties) {
        this.executor = new ThreadPoolExecutor(
                properties.getExecutorThreads(),
                properties.getExecutorThreads(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getExecutorQueueCapacity()),
                threadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        );
        this.scheduler = Schedulers.fromExecutor(executor);
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
    }

    @Override
    public void close() {
        scheduler.dispose();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private static ThreadFactory threadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "rabbit-rpc-bridge-" + sequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }
}
