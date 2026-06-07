package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliablePublisher;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

@Testcontainers(disabledWithoutDocker = true)
@SpringBootTest(
        classes = RabbitMvcBrokerBackedSampleSmokeTest.SampleApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
        properties = {
                "message.reliability.transport=rabbit",
                "message.reliability.service-name=orders",
                "message.reliability.rabbit.exchange=s2.mvc.rabbit.events",
                "message.reliability.rabbit.publisher-confirm=false",
                "message.reliability.retry.attempts=2",
                "message.reliability.retry.backoff[0]=100ms"
        }
)
class RabbitMvcBrokerBackedSampleSmokeTest {

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
    TestRestTemplate restTemplate;

    @Autowired
    SampleInvocationProbe probe;

    @Autowired
    ConfigurableApplicationContext context;

    @Test
    void httpEndpointPublishesRabbitEventAndListenerProcessesItOnce() throws Exception {
        ResponseEntity<Void> response = restTemplate.postForEntity(
                "http://localhost:" + port + "/orders/order-1/events",
                null,
                Void.class
        );

        assertEquals(HttpStatus.ACCEPTED, response.getStatusCode());
        assertTrue(probe.await(Duration.ofSeconds(10)));
        assertEquals(1, probe.invocations());
        assertEquals("order-1", probe.lastOrderId());
        assertNotNull(probe.lastMessage());
        assertEquals("order-1", probe.lastMessage().aggregateId());
        assertEquals("order-1-event", probe.lastMessage().idempotencyKey());
        assertEquals("order-1-correlation", probe.lastMessage().correlationId());
    }

    @Test
    void sampleUsesMvcRabbitEventBeansOnly() {
        assertTrue(context.containsBean("reliablePublisher"));
        assertTrue(context.containsBean("rabbitReliableListenerRegistrar"));
        assertFalse(context.containsBean("reactiveRabbitBridgePublisher"));
        assertFalse(context.containsBean("reactiveRabbitRpcClient"));
        assertThrows(NoSuchBeanDefinitionException.class, () -> context.getBean(AsyncRabbitTemplate.class));
    }

    @SpringBootConfiguration
    @EnableAutoConfiguration
    static class SampleApplication {

        public static void main(String[] args) {
            SpringApplication.run(SampleApplication.class, args);
        }

        @Bean
        SampleInvocationProbe sampleInvocationProbe() {
            return new SampleInvocationProbe();
        }

        @Bean
        SampleOrderListener sampleOrderListener(SampleInvocationProbe probe) {
            return new SampleOrderListener(probe);
        }

        @Bean
        SampleOrderController sampleOrderController(ReliablePublisher publisher) {
            return new SampleOrderController(publisher);
        }
    }

    @RestController
    static class SampleOrderController {
        private final ReliablePublisher publisher;

        SampleOrderController(ReliablePublisher publisher) {
            this.publisher = publisher;
        }

        @PostMapping("/orders/{orderId}/events")
        ResponseEntity<Void> publishOrderCreated(@PathVariable String orderId) {
            publisher.publish(
                    "order.created",
                    new OrderCreated(orderId),
                    PublishOptions.builder()
                            .aggregateId(orderId)
                            .idempotencyKey(orderId + "-event")
                            .correlationId(orderId + "-correlation")
                            .build()
            );
            return ResponseEntity.accepted().build();
        }
    }

    static class SampleOrderListener {
        private final SampleInvocationProbe probe;

        SampleOrderListener(SampleInvocationProbe probe) {
            this.probe = probe;
        }

        @ReliableListener("order.created")
        void onOrderCreated(ReliableMessage<OrderCreated> message) {
            probe.record(message);
        }
    }

    static final class SampleInvocationProbe {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final AtomicInteger invocations = new AtomicInteger();
        private final AtomicReference<ReliableMessage<OrderCreated>> lastMessage = new AtomicReference<>();

        void record(ReliableMessage<OrderCreated> message) {
            invocations.incrementAndGet();
            lastMessage.set(message);
            latch.countDown();
        }

        boolean await(Duration timeout) throws InterruptedException {
            return latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        int invocations() {
            return invocations.get();
        }

        String lastOrderId() {
            ReliableMessage<OrderCreated> message = lastMessage();
            return message == null ? null : message.payload().orderId();
        }

        ReliableMessage<OrderCreated> lastMessage() {
            return lastMessage.get();
        }
    }

    record OrderCreated(String orderId) {
    }
}
