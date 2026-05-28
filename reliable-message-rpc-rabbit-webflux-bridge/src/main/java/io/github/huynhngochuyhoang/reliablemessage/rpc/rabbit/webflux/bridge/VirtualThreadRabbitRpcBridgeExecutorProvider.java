package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class VirtualThreadRabbitRpcBridgeExecutorProvider extends AbstractRabbitRpcBridgeExecutorProvider {

    public VirtualThreadRabbitRpcBridgeExecutorProvider(RabbitRpcWebFluxBridgeProperties properties) {
        super(virtualExecutor(), properties.getMaxConcurrency());
    }

    private static ExecutorService virtualExecutor() {
        return Executors.newThreadPerTaskExecutor(threadFactory());
    }

    private static ThreadFactory threadFactory() {
        AtomicInteger sequence = new AtomicInteger();
        return Thread.ofVirtual()
                .name("rabbit-rpc-bridge-virtual-", sequence.getAndIncrement())
                .factory();
    }
}
