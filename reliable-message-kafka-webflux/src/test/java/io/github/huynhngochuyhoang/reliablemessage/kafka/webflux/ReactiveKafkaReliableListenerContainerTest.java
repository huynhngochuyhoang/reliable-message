package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;

import java.util.concurrent.Semaphore;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

class ReactiveKafkaReliableListenerContainerTest {

    @Test
    void clampsInvalidPrefetchBeforeStartingReceivePipeline() {
        @SuppressWarnings("unchecked")
        KafkaReceiver<String, byte[]> receiver = org.mockito.Mockito.mock(KafkaReceiver.class);
        ReactiveKafkaReliableMessageHandler handler = org.mockito.Mockito.mock(ReactiveKafkaReliableMessageHandler.class);
        when(receiver.receive()).thenReturn(Flux.never());
        ReactiveKafkaReliableListenerContainer container = new ReactiveKafkaReliableListenerContainer(
                receiver,
                null,
                handler,
                1,
                0
        );

        assertDoesNotThrow(container::start);
        container.stop();
    }

    @Test
    void doesNotReleasePermitWhenAcquireFailsBeforeActionRuns() {
        Semaphore limiter = new Semaphore(0) {
            @Override
            public void acquire() throws InterruptedException {
                throw new InterruptedException("cancelled before acquire");
            }
        };

        StepVerifier.create(ReactiveKafkaReliableListenerContainer.withConcurrencyLimit(limiter, Mono.empty()))
                .expectError(IllegalStateException.class)
                .verify();

        assertEquals(0, limiter.availablePermits());
    }
}
