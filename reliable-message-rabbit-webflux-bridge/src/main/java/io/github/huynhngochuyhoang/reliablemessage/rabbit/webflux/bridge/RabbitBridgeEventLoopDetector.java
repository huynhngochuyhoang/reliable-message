package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import java.util.List;
import java.util.Locale;

public final class RabbitBridgeEventLoopDetector {

    private static final List<String> EVENT_LOOP_THREAD_PATTERNS = List.of(
            "reactor-http-nio",
            "reactor-http-epoll",
            "reactor-http-kqueue",
            "reactor-tcp-nio",
            "reactor-tcp-epoll",
            "reactor-tcp-kqueue",
            "nioeventloop",
            "epolleventloop",
            "kqueueeventloop"
    );

    public boolean isEventLoopThread(Thread thread) {
        return thread != null && isEventLoopThreadName(thread.getName());
    }

    boolean isEventLoopThreadName(String threadName) {
        if (threadName == null || threadName.isBlank()) {
            return false;
        }
        String normalizedThreadName = threadName.toLowerCase(Locale.ROOT);
        return EVENT_LOOP_THREAD_PATTERNS.stream().anyMatch(normalizedThreadName::contains);
    }
}
