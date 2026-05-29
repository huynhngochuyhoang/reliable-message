package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class PlatformThreadRabbitRpcBridgeExecutorProvider extends AbstractRabbitRpcBridgeExecutorProvider {

    public PlatformThreadRabbitRpcBridgeExecutorProvider(RabbitRpcWebFluxBridgeProperties properties) {
        super(new ThreadPoolExecutor(
                properties.getExecutorThreads(),
                properties.getExecutorThreads(),
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(properties.getExecutorQueueCapacity()),
                threadFactory(),
                new ThreadPoolExecutor.AbortPolicy()
        ), properties.getMaxConcurrency());
    }

    private static ThreadFactory threadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return task -> {
            Thread thread = new Thread(task, "rabbit-rpc-bridge-platform-" + sequence.incrementAndGet());
            thread.setDaemon(false);
            return thread;
        };
    }
}
