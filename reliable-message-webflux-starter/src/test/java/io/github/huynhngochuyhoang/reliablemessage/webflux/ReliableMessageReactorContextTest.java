package io.github.huynhngochuyhoang.reliablemessage.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.Map;

class ReliableMessageReactorContextTest {

    @Test
    void writesMessageMetadataToReactorContext() {
        ReliableMessage<String> message = message();

        Mono<String> chain = Mono.deferContextual(context -> Mono.just(
                ReliableMessageReactorContext.correlationId(context).orElseThrow()
        ));

        StepVerifier.create(ReliableMessageReactorContext.writeMessage(chain, message))
                .expectNext("correlation-1")
                .verifyComplete();
    }

    @Test
    void appliesContextToPublishOptionsWithoutOverridingExplicitOptions() {
        ReliableMessage<String> message = message();
        PublishOptions explicitOptions = PublishOptions.builder()
                .correlationId("explicit-correlation")
                .header("source", "publisher")
                .build();

        Mono<PublishOptions> chain = Mono.deferContextual(context -> Mono.just(
                ReliableMessageReactorContext.applyTo(explicitOptions, context)
        ));

        StepVerifier.create(ReliableMessageReactorContext.writeMessage(chain, message))
                .expectNextMatches(options ->
                        "explicit-correlation".equals(options.correlationId())
                                && "publisher".equals(options.headers().get("source"))
                                && "trace-1".equals(options.headers().get(ReliableMessageHeaders.TRACE_ID))
                )
                .verifyComplete();
    }

    private static ReliableMessage<String> message() {
        return new ReliableMessage<>(
                "message-1",
                "order.created",
                "order-1",
                "event-1",
                "correlation-1",
                "trace-1",
                Instant.now(),
                Map.of("source", "consumer"),
                "payload"
        );
    }
}
