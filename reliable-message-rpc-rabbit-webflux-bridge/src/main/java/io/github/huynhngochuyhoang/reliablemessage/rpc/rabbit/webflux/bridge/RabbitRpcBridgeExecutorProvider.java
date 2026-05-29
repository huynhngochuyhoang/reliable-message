package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import reactor.core.Disposable;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public interface RabbitRpcBridgeExecutorProvider extends AutoCloseable {

    Scheduler scheduler();

    default <T> Mono<T> execute(Callable<CompletableFuture<T>> task) {
        return Mono.create(sink -> {
            AtomicBoolean cancelled = new AtomicBoolean();
            AtomicReference<CompletableFuture<T>> futureRef = new AtomicReference<>();
            AtomicReference<Disposable> scheduledRef = new AtomicReference<>();
            sink.onCancel(() -> {
                cancelled.set(true);
                CompletableFuture<T> future = futureRef.get();
                if (future != null) {
                    future.cancel(true);
                }
                Disposable scheduled = scheduledRef.get();
                if (scheduled != null) {
                    scheduled.dispose();
                }
            });

            try {
                Disposable scheduled = scheduler().schedule(() -> {
                    if (cancelled.get()) {
                        return;
                    }
                    try {
                        CompletableFuture<T> future = task.call();
                        futureRef.set(future);
                        if (cancelled.get()) {
                            future.cancel(true);
                        }
                        future.whenComplete((value, error) -> {
                            if (error != null) {
                                sink.error(error);
                            } else {
                                sink.success(value);
                            }
                        });
                    } catch (Exception error) {
                        sink.error(error);
                    } catch (Error error) {
                        sink.error(error);
                        throw error;
                    }
                });
                scheduledRef.set(scheduled);
                if (cancelled.get()) {
                    scheduled.dispose();
                }
            } catch (RejectedExecutionException error) {
                sink.error(new RabbitRpcBridgeRejectedException("Rabbit RPC bridge executor rejected request", error));
            }
        });
    }

    static RabbitRpcBridgeExecutorProvider create(RabbitRpcWebFluxBridgeProperties properties) {
        return switch (properties.getExecutorMode()) {
            case PLATFORM -> new PlatformThreadRabbitRpcBridgeExecutorProvider(properties);
            case VIRTUAL_THREAD -> new VirtualThreadRabbitRpcBridgeExecutorProvider(properties);
        };
    }

    @Override
    void close();
}
