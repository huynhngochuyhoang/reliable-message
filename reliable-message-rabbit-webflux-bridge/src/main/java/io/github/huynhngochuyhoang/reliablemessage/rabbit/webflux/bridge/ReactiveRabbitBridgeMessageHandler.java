package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import com.rabbitmq.client.Channel;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReliableMessageReactorContext;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.listener.api.ChannelAwareMessageListener;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ReactiveRabbitBridgeMessageHandler implements ChannelAwareMessageListener {

    private final ReactiveRabbitBridgeListenerEndpoint endpoint;
    private final MessageSerializer serializer;
    private final ReactiveRabbitBridgeListenerMethodInvoker invoker;

    public ReactiveRabbitBridgeMessageHandler(
            ReactiveRabbitBridgeListenerEndpoint endpoint,
            MessageSerializer serializer,
            ReactiveRabbitBridgeListenerMethodInvoker invoker
    ) {
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.invoker = Objects.requireNonNull(invoker, "invoker");
    }

    @Override
    public void onMessage(Message message, Channel channel) throws IOException {
        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        try {
            ReliableMessage<?> reliableMessage = serializer.deserialize(message.getBody(), endpoint.payloadType());
            awaitHandler(reliableMessage);
        } catch (RuntimeException | Error error) {
            nackFailedDelivery(channel, deliveryTag, error);
            throw error;
        }
        channel.basicAck(deliveryTag, false);
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
        CompletableFuture<Void> future = null;
        try {
            Mono<Void> handling = ReliableMessageReactorContext.writeMessage(
                    invoker.invoke(endpoint, reliableMessage),
                    reliableMessage
            );
            future = handling.toFuture();
            future.get();
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
