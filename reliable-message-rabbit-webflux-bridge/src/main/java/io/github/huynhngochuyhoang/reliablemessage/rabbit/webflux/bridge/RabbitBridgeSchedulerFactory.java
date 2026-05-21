package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public final class RabbitBridgeSchedulerFactory {

    private RabbitBridgeSchedulerFactory() {
    }

    public static Scheduler create(RabbitBridgeExecutorProvider executorProvider) {
        return Schedulers.fromExecutorService(executorProvider.getExecutor(), "reliable-message-rabbit-bridge");
    }
}
