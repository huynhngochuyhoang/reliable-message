package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcExceptionClassifier;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcMetrics;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcRetryPolicy;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcTimeoutPolicy;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.Objects;

public class ReactiveRpcOperator {

    private final RpcRetryPolicy retryPolicy;
    private final RpcTimeoutPolicy timeoutPolicy;
    private final RpcExceptionClassifier exceptionClassifier;
    private final RpcMetrics metrics;

    public ReactiveRpcOperator(
            RpcRetryPolicy retryPolicy,
            RpcTimeoutPolicy timeoutPolicy,
            RpcExceptionClassifier exceptionClassifier,
            RpcMetrics metrics
    ) {
        this.retryPolicy = Objects.requireNonNull(retryPolicy, "retryPolicy must not be null");
        this.timeoutPolicy = Objects.requireNonNull(timeoutPolicy, "timeoutPolicy must not be null");
        this.exceptionClassifier = Objects.requireNonNull(exceptionClassifier, "exceptionClassifier must not be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics must not be null");
    }

    public <T> Mono<T> apply(Mono<T> source) {
        Objects.requireNonNull(source, "source must not be null");
        Mono<T> result = timeoutPolicy.enabled() ? source.timeout(timeoutPolicy.requestTimeout()) : source;
        if (retryPolicy.maxAttempts() <= 1) {
            return result;
        }
        Duration firstDelay = retryPolicy.delayForAttempt(1);
        Retry retry = Retry.backoff(retryPolicy.maxAttempts() - 1L, firstDelay.isZero() ? Duration.ofMillis(1) : firstDelay)
                .filter(exceptionClassifier::retryable)
                .doBeforeRetry(signal -> metrics.retry("webflux", "http"));
        return result.retryWhen(retry);
    }
}
