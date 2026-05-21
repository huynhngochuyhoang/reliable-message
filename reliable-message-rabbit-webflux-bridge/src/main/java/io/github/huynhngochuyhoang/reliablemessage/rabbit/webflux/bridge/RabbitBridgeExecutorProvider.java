package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.ExecutorService;

public interface RabbitBridgeExecutorProvider extends AutoCloseable {

    ExecutorService getExecutor();

    @Override
    void close();
}
