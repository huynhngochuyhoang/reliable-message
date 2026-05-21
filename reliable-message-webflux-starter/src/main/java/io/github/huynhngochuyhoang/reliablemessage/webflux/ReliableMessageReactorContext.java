package io.github.huynhngochuyhoang.reliablemessage.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessageHeaders;
import reactor.core.publisher.Mono;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

public final class ReliableMessageReactorContext {

    public static final String MESSAGE_KEY = ReliableMessageReactorContext.class.getName() + ".message";
    public static final String CORRELATION_ID_KEY = ReliableMessageReactorContext.class.getName() + ".correlationId";
    public static final String TRACE_ID_KEY = ReliableMessageReactorContext.class.getName() + ".traceId";
    public static final String HEADERS_KEY = ReliableMessageReactorContext.class.getName() + ".headers";

    private ReliableMessageReactorContext() {
    }

    public static Function<Context, Context> writeMessage(ReliableMessage<?> message) {
        Objects.requireNonNull(message, "message must not be null");
        return context -> withMessage(context, message);
    }

    public static <T> Mono<T> writeMessage(Mono<T> mono, ReliableMessage<?> message) {
        Objects.requireNonNull(mono, "mono must not be null");
        return mono.contextWrite(writeMessage(message));
    }

    public static Context withMessage(Context context, ReliableMessage<?> message) {
        Objects.requireNonNull(context, "context must not be null");
        Objects.requireNonNull(message, "message must not be null");

        Context updated = context
                .put(MESSAGE_KEY, message)
                .put(HEADERS_KEY, message.headers());
        updated = putIfText(updated, CORRELATION_ID_KEY, message.correlationId());
        updated = putIfText(updated, TRACE_ID_KEY, message.traceId());
        return updated;
    }

    public static Optional<ReliableMessage<?>> message(ContextView context) {
        return get(context, MESSAGE_KEY, ReliableMessage.class).map(message -> (ReliableMessage<?>) message);
    }

    public static Optional<String> correlationId(ContextView context) {
        return get(context, CORRELATION_ID_KEY, String.class);
    }

    public static Optional<String> traceId(ContextView context) {
        return get(context, TRACE_ID_KEY, String.class);
    }

    public static Map<String, String> headers(ContextView context) {
        return get(context, HEADERS_KEY, Map.class)
                .map(ReliableMessageReactorContext::stringMap)
                .orElseGet(Map::of);
    }

    public static PublishOptions applyTo(PublishOptions options, ContextView context) {
        PublishOptions safeOptions = options == null ? PublishOptions.empty() : options;
        Map<String, String> headers = new LinkedHashMap<>(headers(context));
        headers.putAll(safeOptions.headers());
        traceId(context).ifPresent(traceId -> headers.putIfAbsent(ReliableMessageHeaders.TRACE_ID, traceId));

        return PublishOptions.builder()
                .aggregateId(safeOptions.aggregateId())
                .idempotencyKey(safeOptions.idempotencyKey())
                .correlationId(safeOptions.correlationId() != null ? safeOptions.correlationId() : correlationId(context).orElse(null))
                .partitionKey(safeOptions.partitionKey())
                .headers(headers)
                .build();
    }

    private static Context putIfText(Context context, String key, String value) {
        if (value == null || value.isBlank()) {
            return context;
        }
        return context.put(key, value);
    }

    private static <T> Optional<T> get(ContextView context, String key, Class<T> type) {
        Objects.requireNonNull(context, "context must not be null");
        if (!context.hasKey(key)) {
            return Optional.empty();
        }
        Object value = context.get(key);
        if (!type.isInstance(value)) {
            return Optional.empty();
        }
        return Optional.of(type.cast(value));
    }

    private static Map<String, String> stringMap(Map<?, ?> values) {
        Map<String, String> headers = new LinkedHashMap<>();
        values.forEach((key, value) -> {
            if (key instanceof String headerName && value instanceof String headerValue) {
                headers.put(headerName, headerValue);
            }
        });
        return Map.copyOf(headers);
    }
}
