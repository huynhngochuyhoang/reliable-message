package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class VirtualThreadRabbitBridgeExecutorProvider implements RabbitBridgeExecutorProvider {

    private final ExecutorService executor;

    public VirtualThreadRabbitBridgeExecutorProvider(RabbitWebFluxBridgeProperties.Bridge bridge) {
        ThreadFactory threadFactory = Thread.ofVirtual()
                .name("reliable-message-rabbit-bridge-vt-", 0)
                .factory();

        this.executor = Executors.newThreadPerTaskExecutor(threadFactory);
    }

    @Override
    public ExecutorService getExecutor() {
        return executor;
    }

    @Override
    public void close() {
        RabbitBridgeExecutorShutdown.close(executor);
    }
}
