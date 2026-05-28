package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import reactor.core.scheduler.Scheduler;

public interface RabbitRpcBridgeExecutorProvider extends AutoCloseable {

    Scheduler scheduler();

    @Override
    void close();
}
