package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class VirtualThreadRabbitBridgeExecutorProvider implements RabbitBridgeExecutorProvider {

    private final ExecutorService executor;

    public VirtualThreadRabbitBridgeExecutorProvider(RabbitWebFluxBridgeProperties.Bridge bridge) {
        this.executor = Executors.newVirtualThreadPerTaskExecutor();
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
