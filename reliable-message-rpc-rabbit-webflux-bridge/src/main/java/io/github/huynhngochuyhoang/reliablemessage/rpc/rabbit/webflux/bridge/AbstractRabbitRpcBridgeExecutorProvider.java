package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

abstract class AbstractRabbitRpcBridgeExecutorProvider implements RabbitRpcBridgeExecutorProvider {

    private final ExecutorService executor;
    private final Scheduler scheduler;
    private final Semaphore permits;

    AbstractRabbitRpcBridgeExecutorProvider(ExecutorService executor, int maxConcurrency) {
        if (maxConcurrency <= 0) {
            throw new IllegalArgumentException("maxConcurrency must be positive");
        }
        this.executor = Objects.requireNonNull(executor, "executor must not be null");
        this.permits = new Semaphore(maxConcurrency);
        this.scheduler = Schedulers.fromExecutor(this::executeScheduled);
    }

    @Override
    public Scheduler scheduler() {
        return scheduler;
    }

    @Override
    public <T> Mono<T> execute(Callable<CompletableFuture<T>> task) {
        return Mono.create(sink -> {
            if (!permits.tryAcquire()) {
                sink.error(new RabbitRpcBridgeRejectedException("Rabbit RPC bridge concurrency limit reached"));
                return;
            }

            AtomicBoolean released = new AtomicBoolean();
            AtomicBoolean cancelled = new AtomicBoolean();
            AtomicReference<CompletableFuture<T>> futureRef = new AtomicReference<>();
            Runnable release = releaseOnce(released);

            sink.onCancel(() -> {
                cancelled.set(true);
                CompletableFuture<T> future = futureRef.get();
                if (future != null) {
                    future.cancel(true);
                }
            });

            try {
                executor.execute(() -> {
                    try {
                        if (cancelled.get()) {
                            release.run();
                            return;
                        }
                        CompletableFuture<T> future = task.call();
                        futureRef.set(future);
                        if (cancelled.get()) {
                            future.cancel(true);
                        }
                        future.whenComplete((value, error) -> {
                            release.run();
                            if (error != null) {
                                sink.error(unwrapCompletionException(error));
                            } else {
                                sink.success(value);
                            }
                        });
                    } catch (Exception error) {
                        release.run();
                        sink.error(unwrapCompletionException(error));
                    } catch (Error error) {
                        release.run();
                        sink.error(unwrapCompletionException(error));
                        throw error;
                    }
                });
            } catch (RejectedExecutionException error) {
                release.run();
                sink.error(new RabbitRpcBridgeRejectedException("Rabbit RPC bridge executor rejected request", error));
            }
        });
    }

    @Override
    public void close() {
        scheduler.dispose();
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            executor.shutdownNow();
        }
    }

    private void executeScheduled(Runnable command) {
        if (!permits.tryAcquire()) {
            throw new RejectedExecutionException("Rabbit RPC bridge concurrency limit reached");
        }
        AtomicBoolean released = new AtomicBoolean();
        Runnable release = releaseOnce(released);
        try {
            executor.execute(() -> {
                try {
                    command.run();
                } finally {
                    release.run();
                }
            });
        } catch (RejectedExecutionException error) {
            release.run();
            throw error;
        }
    }

    private static Throwable unwrapCompletionException(Throwable error) {
        if (error instanceof CompletionException && error.getCause() != null) {
            return error.getCause();
        }
        return error;
    }

    private Runnable releaseOnce(AtomicBoolean released) {
        return () -> {
            if (released.compareAndSet(false, true)) {
                permits.release();
            }
        };
    }
}
