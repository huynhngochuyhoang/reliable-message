package io.github.huynhngochuyhoang.reliablemessage.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.webflux.autoconfigure.WebFluxReliableMessageAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.time.Instant;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ReactiveReliableListenerRegistrarTest {

    @Test
    void discoversReactiveReliableListeners() {
        new ApplicationContextRunner()
                .withUserConfiguration(ValidListenerConfig.class)
                .withConfiguration(AutoConfigurations.of(WebFluxReliableMessageAutoConfiguration.class))
                .run(context -> {
                    ReactiveReliableListenerRegistrar registrar = context.getBean(ReactiveReliableListenerRegistrar.class);

                    assertEquals(1, registrar.endpoints().size());
                    ReactiveReliableListenerEndpoint endpoint = registrar.endpoints().getFirst();
                    assertEquals("testListener", endpoint.beanName());
                    assertEquals("order.created", endpoint.eventName());
                    assertEquals(OrderCreated.class, endpoint.payloadType());
                });
    }

    @Test
    void rejectsBlockingVoidListeners() {
        new ApplicationContextRunner()
                .withUserConfiguration(BlockingListenerConfig.class)
                .withConfiguration(AutoConfigurations.of(WebFluxReliableMessageAutoConfiguration.class))
                .run(context -> {
                    assertNotNull(context.getStartupFailure());
                    assertInstanceOf(IllegalStateException.class, context.getStartupFailure());
                });
    }

    @Test
    void invokesMonoVoidHandler() {
        new ApplicationContextRunner()
                .withUserConfiguration(ValidListenerConfig.class)
                .withConfiguration(AutoConfigurations.of(WebFluxReliableMessageAutoConfiguration.class))
                .run(context -> {
                    ReactiveReliableListenerEndpoint endpoint = context.getBean(ReactiveReliableListenerRegistrar.class)
                            .endpoints()
                            .getFirst();
                    ReactiveReliableListenerMethodInvoker invoker = context.getBean(ReactiveReliableListenerMethodInvoker.class);

                    StepVerifier.create(invoker.invoke(endpoint, message()))
                            .verifyComplete();
                });
    }

    @Test
    void invokesPrivateHandlerMethod() throws Exception {
        PrivateListener listener = new PrivateListener();
        Method method = PrivateListener.class.getDeclaredMethod("handle", ReliableMessage.class);
        ReactiveReliableListenerEndpoint endpoint = new ReactiveReliableListenerEndpoint(
                "privateListener",
                listener,
                method,
                "order.created",
                OrderCreated.class
        );

        StepVerifier.create(new ReactiveReliableListenerMethodInvoker().invoke(endpoint, message()))
                .verifyComplete();
    }

    @Configuration(proxyBeanMethods = false)
    static class ValidListenerConfig {

        @Bean
        TestListener testListener() {
            return new TestListener();
        }
    }

    @Configuration(proxyBeanMethods = false)
    static class BlockingListenerConfig {

        @Bean
        BlockingListener blockingListener() {
            return new BlockingListener();
        }
    }

    static final class TestListener {

        @ReactiveReliableListener("order.created")
        Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
    }

    static final class PrivateListener {

        @ReactiveReliableListener("order.created")
        private Mono<Void> handle(ReliableMessage<OrderCreated> message) {
            return Mono.empty();
        }
    }

    static final class BlockingListener {

        @ReactiveReliableListener("order.created")
        void handle(ReliableMessage<OrderCreated> message) {
        }
    }

    record OrderCreated(String orderId) {
    }

    private static ReliableMessage<OrderCreated> message() {
        return new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.now(),
                Map.of(),
                new OrderCreated("order-1")
        );
    }
}
