package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.ReactiveOutboxFlushScheduler;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;
import io.github.huynhngochuyhoang.reliablemessage.rpc.webflux.ReactiveRpcContext;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliablePublisher;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = RabbitRpcWebFluxBrokerBackedSmokeTest.SampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "message.reliability.rpc.rabbit.webflux.enabled=true",
                "message.reliability.rpc.rabbit.webflux.exchange=s4.rpc.exchange",
                "message.reliability.rpc.rabbit.webflux.default-timeout=300ms",
                "message.reliability.rpc.rabbit.webflux.executor-mode=platform",
                "message.reliability.rpc.rabbit.webflux.executor-threads=1",
                "message.reliability.rpc.rabbit.webflux.executor-queue-capacity=1",
                "message.reliability.rpc.rabbit.webflux.max-concurrency=1",
                "message.reliability.rpc.rabbit.webflux.max-attempts=2",
                "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.r2dbc.R2dbcAutoConfiguration,io.github.huynhngochuyhoang.reliablemessage.outbox.r2dbc.R2dbcOutboxAutoConfiguration"
        }
)
class RabbitRpcWebFluxBrokerBackedSmokeTest {

    @Container
    static final RabbitMQContainer rabbit = new RabbitMQContainer("rabbitmq:3.13-alpine");

    @DynamicPropertySource
    static void rabbitProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.rabbitmq.host", rabbit::getHost);
        registry.add("spring.rabbitmq.port", rabbit::getAmqpPort);
        registry.add("spring.rabbitmq.username", rabbit::getAdminUsername);
        registry.add("spring.rabbitmq.password", rabbit::getAdminPassword);
    }

    @LocalServerPort
    int port;

    @Autowired
    ReactiveRabbitRpcClient rpcClient;

    @Autowired
    ResponderProbe responderProbe;

    @Autowired
    MeterRegistry meterRegistry;

    @Autowired
    ConfigurableApplicationContext context;

    @Test
    void webFluxEndpointCompletesRequestReplyThroughAutoConfiguredRpcClient() throws Exception {
        WebTestClient.bindToServer()
                .baseUrl("http://localhost:" + port)
                .build()
                .get()
                .uri("/rpc/orders/{orderId}", "order-1")
                .exchange()
                .expectStatus().isOk()
                .expectBody()
                .jsonPath("$.id").isEqualTo("order-1")
                .jsonPath("$.status").isEqualTo("PAID");

        assertTrue(responderProbe.awaitRoute("orders.lookup", Duration.ofSeconds(10)));
        assertEquals("controller-correlation", responderProbe.lastHeader(RpcHeaders.CORRELATION_ID));
        assertEquals("controller-request", responderProbe.lastHeader(RpcHeaders.REQUEST_ID));
        assertNotNull(responderProbe.controllerThread());
        assertTrue(responderProbe.controllerThread().contains("http"), responderProbe.controllerThread());
        assertMetric("rpc_rabbit_requests_total", "orders.lookup", "request", 1.0);
        assertMetric("rpc_rabbit_success_total", "orders.lookup", "success", 1.0);
        assertDuration("orders.lookup", "success");
    }

    @Test
    void genericParameterizedRepliesRoundTripThroughBroker() {
        ParameterizedTypeReference<List<OrderResponse>> listType = new ParameterizedTypeReference<>() {
        };
        StepVerifier.create(rpcClient.request("orders.list", new OrderLookupRequest("order-list"), listType, RpcOptions.raw()))
                .expectNext(List.of(new OrderResponse("order-1", "PAID"), new OrderResponse("order-2", "CREATED")))
                .verifyComplete();

        ParameterizedTypeReference<Map<String, OrderResponse>> mapType = new ParameterizedTypeReference<>() {
        };
        StepVerifier.create(rpcClient.request("orders.map", new OrderLookupRequest("order-map"), mapType, RpcOptions.raw()))
                .expectNext(Map.of("order-1", new OrderResponse("order-1", "PAID")))
                .verifyComplete();
    }

    @Test
    void envelopeSuccessAndRemoteErrorRoundTripThroughBroker() {
        StepVerifier.create(rpcClient.request(
                        "orders.envelope-success",
                        new OrderLookupRequest("order-1"),
                        OrderResponse.class,
                        RpcOptions.envelope()
                ))
                .expectNext(new OrderResponse("order-1", "PAID"))
                .verifyComplete();

        StepVerifier.create(rpcClient.request(
                        "orders.error",
                        new OrderLookupRequest("missing"),
                        OrderResponse.class,
                        RpcOptions.envelope()
                ))
                .expectErrorSatisfies(error -> {
                    assertInstanceOf(RabbitRpcRemoteException.class, error);
                    RabbitRpcRemoteException remote = (RabbitRpcRemoteException) error;
                    assertEquals("ORDER_NOT_FOUND", remote.getErrorCode());
                    assertEquals("Order missing", remote.getErrorMessage());
                    assertEquals("NotFound", remote.getErrorType());
                })
                .verify();

        assertMetric("rpc_rabbit_failed_total", "orders.error", "remote_error", 1.0);
    }

    @Test
    void timeoutIsCallerVisibleAndRecordsMetrics() {
        StepVerifier.create(rpcClient.request("orders.timeout", new OrderLookupRequest("slow"), OrderResponse.class))
                .expectErrorSatisfies(error -> assertTrue(error instanceof java.util.concurrent.TimeoutException))
                .verify();

        assertMetric("rpc_rabbit_timeout_total", "orders.timeout", "timeout", 1.0);
        assertMetric("rpc_rabbit_failed_total", "orders.timeout", "timeout", 1.0);
    }

    @Test
    void retryTimeoutUsesDistinctAmqpCorrelationIdsAndPreservesLogicalHeaders() throws Exception {
        int baseline = responderProbe.routeCount("orders.retry-timeout");
        RpcContext rpcContext = RpcContext.builder()
                .correlationId("retry-correlation")
                .requestId("retry-request")
                .build();

        StepVerifier.create(rpcClient.request("orders.retry-timeout", new OrderLookupRequest("retry-order"), OrderResponse.class)
                        .contextWrite(ReactiveRpcContext.write(rpcContext)))
                .expectNext(new OrderResponse("retry-order", "PAID"))
                .verifyComplete();

        assertTrue(responderProbe.awaitRouteCount("orders.retry-timeout", baseline + 2, Duration.ofSeconds(10)));
        List<Message> attempts = responderProbe.messages("orders.retry-timeout").subList(baseline, baseline + 2);
        assertNotEquals(
                attempts.get(0).getMessageProperties().getCorrelationId(),
                attempts.get(1).getMessageProperties().getCorrelationId()
        );
        for (Message attempt : attempts) {
            assertEquals("retry-correlation", attempt.getMessageProperties().getHeader(RpcHeaders.CORRELATION_ID));
            assertEquals("retry-request", attempt.getMessageProperties().getHeader(RpcHeaders.REQUEST_ID));
        }
        assertMetric("rpc_rabbit_retry_total", "orders.retry-timeout", "retry", 1.0);
        assertMetric("rpc_rabbit_timeout_total", "orders.retry-timeout", "timeout", 1.0);
    }

    @Test
    void bulkheadRejectsSecondRequestWhileFirstRequestIsInFlight() throws Exception {
        int baseline = responderProbe.routeCount("orders.timeout");
        Disposable first = rpcClient.request(
                        "orders.timeout",
                        new OrderLookupRequest("slow"),
                        OrderResponse.class,
                        RpcOptions.raw().withTimeout(Duration.ofSeconds(5))
                )
                .subscribe();

        assertTrue(responderProbe.awaitRouteCount("orders.timeout", baseline + 1, Duration.ofSeconds(10)));
        StepVerifier.create(rpcClient.request("orders.lookup", new OrderLookupRequest("order-2"), OrderResponse.class))
                .expectError(RabbitRpcBridgeRejectedException.class)
                .verify();
        first.dispose();

        StepVerifier.create(Mono.delay(Duration.ofMillis(100))
                        .then(rpcClient.request(
                                "orders.lookup",
                                new OrderLookupRequest("after-cancel"),
                                OrderResponse.class,
                                RpcOptions.raw().withTimeout(Duration.ofSeconds(2))
                        )))
                .expectNext(new OrderResponse("after-cancel", "PAID"))
                .verifyComplete();

        assertMetric("rpc_rabbit_bulkhead_rejected_total", "orders.lookup", "bulkhead_rejected", 1.0);
    }

    @Test
    void rpcContextDoesNotCreateEventOrOutboxBeans() {
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(ReactiveReliablePublisher.class));
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(ReactiveOutboxFlushScheduler.class));
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean("reactiveRabbitBridgeListenerRegistrar"));
    }

    private void assertMetric(String name, String route, String status, double expected) {
        assertTrue(meterRegistry.find(name)
                .tag("runtime", "webflux")
                .tag("transport", "rabbit")
                .tag("executor_mode", "platform")
                .tag("route", route)
                .tag("status", status)
                .counter()
                .count() >= expected);
    }

    private void assertDuration(String route, String status) {
        assertTrue(meterRegistry.find("rpc_rabbit_duration")
                .tag("runtime", "webflux")
                .tag("transport", "rabbit")
                .tag("executor_mode", "platform")
                .tag("route", route)
                .tag("status", status)
                .timer()
                .count() > 0);
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class SampleApplication {

        @Bean
        ConnectionFactory connectionFactory() {
            CachingConnectionFactory connectionFactory = new CachingConnectionFactory(rabbit.getHost(), rabbit.getAmqpPort());
            connectionFactory.setUsername(rabbit.getAdminUsername());
            connectionFactory.setPassword(rabbit.getAdminPassword());
            return connectionFactory;
        }

        @Bean
        Jackson2JsonMessageConverter rabbitRpcMessageConverter(ObjectMapper objectMapper) {
            return new Jackson2JsonMessageConverter(objectMapper);
        }

        @Bean
        RabbitTemplate rabbitTemplate(ConnectionFactory connectionFactory, Jackson2JsonMessageConverter converter) {
            RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
            rabbitTemplate.setMessageConverter(converter);
            return rabbitTemplate;
        }

        @Bean
        AsyncRabbitTemplate asyncRabbitTemplate(RabbitTemplate rabbitTemplate) {
            AsyncRabbitTemplate asyncRabbitTemplate = new AsyncRabbitTemplate(rabbitTemplate);
            asyncRabbitTemplate.setReceiveTimeout(10_000);
            return asyncRabbitTemplate;
        }

        @Bean
        RabbitAdmin rabbitAdmin(ConnectionFactory connectionFactory) {
            return new RabbitAdmin(connectionFactory);
        }

        @Bean
        DirectExchange rpcExchange() {
            return new DirectExchange("s4.rpc.exchange", true, false);
        }

        @Bean
        Queue rpcQueue() {
            return QueueBuilder.durable("s4.rpc.orders").build();
        }

        @Bean
        Binding lookupBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.lookup");
        }

        @Bean
        Binding listBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.list");
        }

        @Bean
        Binding mapBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.map");
        }

        @Bean
        Binding envelopeSuccessBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.envelope-success");
        }

        @Bean
        Binding errorBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.error");
        }

        @Bean
        Binding timeoutBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.timeout");
        }

        @Bean
        Binding retryTimeoutBinding(Queue rpcQueue, DirectExchange rpcExchange) {
            return BindingBuilder.bind(rpcQueue).to(rpcExchange).with("orders.retry-timeout");
        }

        @Bean
        SimpleMessageListenerContainer rpcResponderContainer(
                ConnectionFactory connectionFactory,
                RabbitTemplate rabbitTemplate,
                ResponderProbe responderProbe
        ) {
            SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
            container.setQueueNames("s4.rpc.orders");
            container.setConcurrentConsumers(1);
            container.setMessageListener((ChannelAwareMessageListener) (message, channel) -> {
                String route = message.getMessageProperties().getReceivedRoutingKey();
                int routeCount = responderProbe.record(route, message);
                if ("orders.timeout".equals(route)) {
                    return;
                }
                if ("orders.retry-timeout".equals(route) && routeCount == 1) {
                    return;
                }
                Object reply = switch (route) {
                    case "orders.list" -> List.of(
                            new OrderResponse("order-1", "PAID"),
                            new OrderResponse("order-2", "CREATED")
                    );
                    case "orders.map" -> Map.of("order-1", new OrderResponse("order-1", "PAID"));
                    case "orders.envelope-success" -> RpcResponseEnvelope.success(new OrderResponse("order-1", "PAID"));
                    case "orders.error" -> RpcResponseEnvelope.error("ORDER_NOT_FOUND", "Order missing", "NotFound");
                    default -> new OrderResponse(requestOrderId(message, rabbitTemplate), "PAID");
                };
                String replyTo = message.getMessageProperties().getReplyTo();
                String correlationId = message.getMessageProperties().getCorrelationId();
                rabbitTemplate.convertAndSend("", replyTo, reply, replyMessage -> {
                    replyMessage.getMessageProperties().setCorrelationId(correlationId);
                    return replyMessage;
                });
            });
            return container;
        }

        @Bean
        ResponderProbe responderProbe() {
            return new ResponderProbe();
        }

        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        SampleRpcController sampleRpcController(ReactiveRabbitRpcClient rpcClient, ResponderProbe responderProbe) {
            return new SampleRpcController(rpcClient, responderProbe);
        }

        private static String requestOrderId(Message message, RabbitTemplate rabbitTemplate) {
            Object request = rabbitTemplate.getMessageConverter().fromMessage(message);
            if (request instanceof OrderLookupRequest lookupRequest) {
                return lookupRequest.orderId();
            }
            return "order-1";
        }
    }

    @RestController
    static class SampleRpcController {
        private final ReactiveRabbitRpcClient rpcClient;
        private final ResponderProbe responderProbe;

        SampleRpcController(ReactiveRabbitRpcClient rpcClient, ResponderProbe responderProbe) {
            this.rpcClient = rpcClient;
            this.responderProbe = responderProbe;
        }

        @GetMapping("/rpc/orders/{orderId}")
        Mono<ResponseEntity<OrderResponse>> lookup(@PathVariable("orderId") String orderId) {
            responderProbe.controllerThread(Thread.currentThread().getName());
            RpcContext rpcContext = RpcContext.builder()
                    .correlationId("controller-correlation")
                    .requestId("controller-request")
                    .build();
            return rpcClient.request("orders.lookup", new OrderLookupRequest(orderId), OrderResponse.class)
                    .map(ResponseEntity::ok)
                    .contextWrite(ReactiveRpcContext.write(rpcContext));
        }
    }

    static final class ResponderProbe {
        private final Map<String, AtomicInteger> routeCounts = new java.util.concurrent.ConcurrentHashMap<>();
        private final Map<String, CopyOnWriteArrayList<Message>> routeMessages = new java.util.concurrent.ConcurrentHashMap<>();
        private final AtomicReference<Message> lastMessage = new AtomicReference<>();
        private final AtomicReference<String> controllerThread = new AtomicReference<>();
        private final AtomicInteger totalRequests = new AtomicInteger();

        int record(String route, Message message) {
            totalRequests.incrementAndGet();
            lastMessage.set(message);
            routeMessages.computeIfAbsent(route, ignored -> new CopyOnWriteArrayList<>()).add(message);
            return routeCounts.computeIfAbsent(route, ignored -> new AtomicInteger()).incrementAndGet();
        }

        boolean awaitRoute(String route, Duration timeout) throws InterruptedException {
            return awaitRouteCount(route, 1, timeout);
        }

        boolean awaitRouteCount(String route, int expectedCount, Duration timeout) throws InterruptedException {
            long deadline = System.nanoTime() + timeout.toNanos();
            while (System.nanoTime() < deadline) {
                if (routeCount(route) >= expectedCount) {
                    return true;
                }
                Thread.sleep(10);
            }
            return routeCount(route) >= expectedCount;
        }

        int routeCount(String route) {
            AtomicInteger count = routeCounts.get(route);
            return count == null ? 0 : count.get();
        }

        List<Message> messages(String route) {
            return List.copyOf(routeMessages.getOrDefault(route, new CopyOnWriteArrayList<>()));
        }

        String lastHeader(String name) {
            return lastMessage.get().getMessageProperties().getHeader(name);
        }

        void controllerThread(String threadName) {
            controllerThread.set(threadName);
        }

        String controllerThread() {
            return controllerThread.get();
        }
    }

    record OrderLookupRequest(String orderId) {
    }

    record OrderResponse(String id, String status) {
    }
}
