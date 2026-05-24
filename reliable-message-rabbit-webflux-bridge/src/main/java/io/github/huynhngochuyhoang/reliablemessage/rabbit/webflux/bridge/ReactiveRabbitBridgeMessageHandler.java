package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyStartResult;
import io.github.huynhngochuyhoang.reliablemessage.webflux.IdempotencyState;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReliableMessageReactorContext;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReactiveRabbitBridgeMessageHandler implements ChannelAwareMessageListener {

    private static final Duration DEFAULT_IDEMPOTENCY_TTL = Duration.ofHours(24);

    private final ReactiveRabbitBridgeListenerEndpoint endpoint;
    private final MessageSerializer serializer;
    private final ReactiveRabbitBridgeListenerMethodInvoker invoker;
    private final ReactiveIdempotencyStore idempotencyStore;
    private final Duration idempotencyTtl;
    private final ReactiveRabbitBridgeFailureHandler failureHandler;

    public ReactiveRabbitBridgeMessageHandler(
            ReactiveRabbitBridgeListenerEndpoint endpoint,
            MessageSerializer serializer,
            ReactiveRabbitBridgeListenerMethodInvoker invoker
    ) {
        this(endpoint, serializer, invoker, null, DEFAULT_IDEMPOTENCY_TTL, ReactiveRabbitBridgeFailureHandler.noop());
    }

    public ReactiveRabbitBridgeMessageHandler(
            ReactiveRabbitBridgeListenerEndpoint endpoint,
            MessageSerializer serializer,
            ReactiveRabbitBridgeListenerMethodInvoker invoker,
            ReactiveIdempotencyStore idempotencyStore,
            Duration idempotencyTtl,
            ReactiveRabbitBridgeFailureHandler failureHandler
    ) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.invoker = Objects.requireNonNull(invoker, "invoker");
        this.idempotencyStore = idempotencyStore;
        this.idempotencyTtl = idempotencyTtl == null ? DEFAULT_IDEMPOTENCY_TTL : idempotencyTtl;
        this.failureHandler = failureHandler == null ? ReactiveRabbitBridgeFailureHandler.noop() : failureHandler;
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        ReliableMessage<?> reliableMessage = null;
        try {
            reliableMessage = serializer.deserialize(message.getBody(), endpoint.payloadType());
            process(reliableMessage);
        } catch (RuntimeException | Error error) {
            notifyFailure(reliableMessage, message, error);
            nackFailedDelivery(channel, deliveryTag, error);
            throw error;
        }
        try {
            channel.basicAck(deliveryTag, false);
        } catch (IOException | RuntimeException error) {
            notifyFailure(reliableMessage, message, error);
            throw error;
        }
    }

    private void process(ReliableMessage<?> reliableMessage) {
        if (!isIdempotencyEnabled(reliableMessage)) {
            awaitHandler(reliableMessage);
            return;
        }

        String idempotencyKey = reliableMessage.idempotencyKey();
        IdempotencyStartResult startResult = await(idempotencyStore.tryStart(idempotencyKey, idempotencyTtl));
        if (!startResult.started()) {
            if (startResult.state() == IdempotencyState.SUCCESS) {
                return;
            }
            throw new IllegalStateException("Duplicate idempotency key " + idempotencyKey + " is " + startResult.state());
        }

        try {
            awaitHandler(reliableMessage);
            await(idempotencyStore.markSuccess(idempotencyKey));
        } catch (RuntimeException | Error error) {
            markFailed(idempotencyKey, error);
            throw error;
        }
    }

    private boolean isIdempotencyEnabled(ReliableMessage<?> reliableMessage) {
        return idempotencyStore != null
                && reliableMessage.idempotencyKey() != null
                && !reliableMessage.idempotencyKey().isBlank();
    }

    private void markFailed(String idempotencyKey, Throwable failure) {
        try {
            await(idempotencyStore.markFailed(idempotencyKey, failure));
        } catch (RuntimeException markFailedError) {
            failure.addSuppressed(markFailedError);
        } catch (Error markFailedError) {
            markFailedError.addSuppressed(failure);
            throw markFailedError;
        }
    }

    private void notifyFailure(ReliableMessage<?> reliableMessage, Message message, Throwable failure) {
        try {
            failureHandler.handleFailure(endpoint, reliableMessage, message, failure);
        } catch (RuntimeException | Error hookFailure) {
            failure.addSuppressed(hookFailure);
        }
    }

    private static void nackFailedDelivery(Channel channel, long deliveryTag, Throwable failure) throws IOException {
        try {
            channel.basicNack(deliveryTag, false, true);
        } catch (IOException | RuntimeException nackFailure) {
            nackFailure.addSuppressed(failure);
            throw nackFailure;
        }
    }

    private void awaitHandler(ReliableMessage<?> reliableMessage) {
        Mono<Void> handling = ReliableMessageReactorContext.writeMessage(
                invoker.invoke(endpoint, reliableMessage),
                reliableMessage
        );
        await(handling);
    }

    private static <T> T await(Mono<T> operation) {
        CompletableFuture<T> future = null;
        try {
            future = operation.toFuture();
            return future.get();
        } catch (InterruptedException error) {
            if (future != null) {
                future.cancel(true);
            }
            Thread.currentThread().interrupt();
            throw new CancellationException("Reactive Rabbit bridge listener was interrupted");
        } catch (ExecutionException error) {
            throw propagate(error.getCause());
        }
    }

    private static RuntimeException propagate(Throwable error) {
        if (error instanceof RuntimeException runtimeException) {
            return runtimeException;
        }
        if (error instanceof Error fatal) {
            throw fatal;
        }
        return new IllegalStateException(error);
    }
}
