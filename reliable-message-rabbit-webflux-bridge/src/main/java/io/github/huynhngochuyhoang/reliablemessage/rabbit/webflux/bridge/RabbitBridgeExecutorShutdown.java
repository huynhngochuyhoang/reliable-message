package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

final class RabbitBridgeExecutorShutdown {

    private static final long AWAIT_SECONDS = 30;

    private RabbitBridgeExecutorShutdown() {
    }

    static void close(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(AWAIT_SECONDS, TimeUnit.SECONDS)) {
                executor.shutdownNow();
                executor.awaitTermination(AWAIT_SECONDS, TimeUnit.SECONDS);
            }
        } catch (InterruptedException exception) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
