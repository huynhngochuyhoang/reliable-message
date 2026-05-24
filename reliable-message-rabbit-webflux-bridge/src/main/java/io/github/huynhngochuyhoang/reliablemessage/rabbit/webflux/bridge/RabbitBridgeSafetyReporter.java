package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FunctionalInterface
public interface RabbitBridgeSafetyReporter {

    void eventLoopPublishDetected(String threadName);

    static RabbitBridgeSafetyReporter logging() {
        return LoggingRabbitBridgeSafetyReporter.INSTANCE;
    }

    static RabbitBridgeSafetyReporter noop() {
        return threadName -> { };
    }

    final class LoggingRabbitBridgeSafetyReporter implements RabbitBridgeSafetyReporter {
        private static final LoggingRabbitBridgeSafetyReporter INSTANCE = new LoggingRabbitBridgeSafetyReporter();
        private static final Logger LOGGER = LoggerFactory.getLogger(ReactiveRabbitBridgePublisher.class);

        private LoggingRabbitBridgeSafetyReporter() {
        }

        @Override
        public void eventLoopPublishDetected(String threadName) {
            LOGGER.warn(
                    "Rabbit WebFlux blocking bridge publish was called from event-loop-style thread '{}'; "
                            + "RabbitTemplate work remains offloaded to the bridge executor",
                    threadName
            );
        }
    }
}
