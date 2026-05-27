package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.springframework.amqp.rabbit.AsyncRabbitTemplate;
import reactor.core.publisher.Mono;

import java.util.Objects;

public class UnsupportedReactiveRabbitRpcClient implements ReactiveRabbitRpcClient {

    private final AsyncRabbitTemplate asyncRabbitTemplate;

    public UnsupportedReactiveRabbitRpcClient(AsyncRabbitTemplate asyncRabbitTemplate) {
        this.asyncRabbitTemplate = Objects.requireNonNull(asyncRabbitTemplate, "asyncRabbitTemplate must not be null");
    }

    public AsyncRabbitTemplate asyncRabbitTemplate() {
        return asyncRabbitTemplate;
    }

    @Override
    public <T> Mono<T> request(String route, Object request, Class<T> responseType) {
        return Mono.error(new UnsupportedOperationException(
                "Rabbit RPC request/reply execution is planned for Phase 14.10"
        ));
    }
}
