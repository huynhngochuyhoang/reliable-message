package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.scheduler.Scheduler;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

class RabbitRpcBridgeExecutorProviderTest {

    @Test
    void cancellationBeforeTaskReturnsDoesNotReleasePermitEarly() throws Exception {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        CountDownLatch taskStarted = new CountDownLatch(1);
        CountDownLatch releaseTask = new CountDownLatch(1);
        CompletableFuture<String> firstFuture = new CompletableFuture<>();

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            Disposable subscription = provider.execute(() -> {
                taskStarted.countDown();
                releaseTask.await(2, TimeUnit.SECONDS);
                return firstFuture;
            }).subscribe();

            assertThat(taskStarted.await(2, TimeUnit.SECONDS)).isTrue();
            subscription.dispose();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("second")))
                    .expectError(RabbitRpcBridgeRejectedException.class)
                    .verify();

            releaseTask.countDown();
            firstFuture.complete("first");

            awaitSuccessfulExecution(provider, "third");
        }
    }

    @Test
    void queuedCancellationSkipsTaskCallAndReleasesPermit() throws Exception {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        properties.setExecutorThreads(1);
        properties.setExecutorQueueCapacity(1);
        properties.setMaxConcurrency(2);
        CountDownLatch runningTaskStarted = new CountDownLatch(1);
        CountDownLatch releaseRunningTask = new CountDownLatch(1);
        AtomicInteger queuedCalls = new AtomicInteger();
        AtomicBoolean queuedSkipped = new AtomicBoolean();

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            Disposable runningSubscription = provider.execute(() -> {
                runningTaskStarted.countDown();
                releaseRunningTask.await(2, TimeUnit.SECONDS);
                return CompletableFuture.completedFuture("running");
            }).subscribe();
            assertThat(runningTaskStarted.await(2, TimeUnit.SECONDS)).isTrue();

            Disposable queuedSubscription = provider.execute(() -> {
                queuedCalls.incrementAndGet();
                return CompletableFuture.completedFuture("queued");
            }).doFinally(signal -> queuedSkipped.set(true)).subscribe();
            queuedSubscription.dispose();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("after-queued-cancel")))
                    .expectSubscription()
                    .then(() -> {
                        releaseRunningTask.countDown();
                        runningSubscription.dispose();
                    })
                    .expectNext("after-queued-cancel")
                    .verifyComplete();
            awaitTrue(queuedSkipped);
            assertThat(queuedCalls).hasValue(0);
        }
    }

    @Test
    void defaultExecuteSkipsScheduledTaskWhenCancellationWinsBeforeDisposableIsStored() {
        AtomicReference<Subscription> subscriptionRef = new AtomicReference<>();
        AtomicInteger calls = new AtomicInteger();
        InterleavingScheduler scheduler = new InterleavingScheduler(() -> subscriptionRef.get().cancel());
        RabbitRpcBridgeExecutorProvider provider = new RabbitRpcBridgeExecutorProvider() {
            @Override
            public Scheduler scheduler() {
                return scheduler;
            }

            @Override
            public void close() {
            }
        };

        provider.execute(() -> {
            calls.incrementAndGet();
            return CompletableFuture.completedFuture("late");
        }).subscribe(new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                subscriptionRef.set(subscription);
                request(Long.MAX_VALUE);
            }
        });

        scheduler.runScheduled();

        assertThat(calls).hasValue(0);
    }

    @Test
    void taskFailureReleasesPermit() {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        RuntimeException failure = new RuntimeException("send creation failed");

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            StepVerifier.create(provider.execute(() -> {
                        throw failure;
                    }))
                    .expectErrorMatches(error -> error == failure)
                    .verify();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("after-failure")))
                    .expectNext("after-failure")
                    .verifyComplete();
        }
    }

    @Test
    void executorRejectionReleasesPermit() {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties);
        provider.close();

        StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("rejected")))
                .expectError(RabbitRpcBridgeRejectedException.class)
                .verify();

        try (RabbitRpcBridgeExecutorProvider nextProvider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            StepVerifier.create(nextProvider.execute(() -> CompletableFuture.completedFuture("after-rejection")))
                    .expectNext("after-rejection")
                    .verifyComplete();
        }
    }

    @Test
    void futureSuccessReleasesPermit() {
        RabbitRpcWebFluxBridgeProperties properties = properties();

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("ok")))
                    .expectNext("ok")
                    .verifyComplete();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("after-success")))
                    .expectNext("after-success")
                    .verifyComplete();
        }
    }

    @Test
    void futureFailureReleasesPermit() {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        RuntimeException failure = new RuntimeException("reply failed");

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            CompletableFuture<String> failed = new CompletableFuture<>();
            failed.completeExceptionally(failure);

            StepVerifier.create(provider.execute(() -> failed))
                    .expectErrorMatches(error -> error == failure)
                    .verify();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("after-failure")))
                    .expectNext("after-failure")
                    .verifyComplete();
        }
    }

    @Test
    void futureCompletionExceptionIsUnwrapped() {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        RuntimeException cause = new RuntimeException("conversion failed");
        CompletableFuture<String> failed = new CompletableFuture<>();
        failed.completeExceptionally(new CompletionException(cause));

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            StepVerifier.create(provider.execute(() -> failed))
                    .expectErrorMatches(error -> error == cause)
                    .verify();
        }
    }

    @Test
    void defaultExecuteUnwrapsFutureCompletionException() {
        RuntimeException cause = new RuntimeException("conversion failed");
        CompletableFuture<String> failed = new CompletableFuture<>();
        failed.completeExceptionally(new CompletionException(cause));
        RabbitRpcBridgeExecutorProvider provider = new RabbitRpcBridgeExecutorProvider() {
            @Override
            public Scheduler scheduler() {
                return new ImmediateScheduler();
            }

            @Override
            public void close() {
            }
        };

        StepVerifier.create(provider.execute(() -> failed))
                .expectErrorMatches(error -> error == cause)
                .verify();
    }

    @Test
    void cancellationAfterFutureReturnedCancelsFutureAndReleaseComesFromFutureTerminalCallback() throws Exception {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        CountDownLatch futureReturned = new CountDownLatch(1);
        CompletableFuture<String> future = new CompletableFuture<>();

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            Disposable subscription = provider.execute(() -> {
                futureReturned.countDown();
                return future;
            }).subscribe();

            assertThat(futureReturned.await(2, TimeUnit.SECONDS)).isTrue();
            subscription.dispose();

            awaitTrue(future::isCancelled);
            awaitSuccessfulExecution(provider, "after-cancel");
        }
    }

    @Test
    void virtualThreadModeRejectsWhenSaturatedAndAllowsNextAfterCompletion() throws Exception {
        RabbitRpcWebFluxBridgeProperties properties = properties();
        properties.setExecutorMode(RabbitRpcExecutorMode.VIRTUAL_THREAD);
        CountDownLatch firstStarted = new CountDownLatch(1);
        CompletableFuture<String> firstFuture = new CompletableFuture<>();
        AtomicBoolean firstWasVirtualThread = new AtomicBoolean();

        try (RabbitRpcBridgeExecutorProvider provider = RabbitRpcBridgeExecutorProvider.create(properties)) {
            Disposable firstSubscription = provider.execute(() -> {
                firstWasVirtualThread.set(Thread.currentThread().isVirtual());
                firstStarted.countDown();
                return firstFuture;
            }).subscribe();

            assertThat(firstStarted.await(2, TimeUnit.SECONDS)).isTrue();
            assertThat(firstWasVirtualThread).isTrue();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("second")))
                    .expectError(RabbitRpcBridgeRejectedException.class)
                    .verify();

            firstFuture.complete("first");
            firstSubscription.dispose();

            awaitSuccessfulExecution(provider, "third");
        }
    }

    private static void awaitTrue(AtomicBoolean value) throws InterruptedException {
        awaitTrue(value::get);
    }

    private static void awaitTrue(BooleanSupplier value) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (!value.getAsBoolean() && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertThat(value.getAsBoolean()).isTrue();
    }

    private static void awaitSuccessfulExecution(RabbitRpcBridgeExecutorProvider provider, String expected)
            throws InterruptedException {
        AssertionError lastFailure = null;
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (System.nanoTime() < deadline) {
            try {
                StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture(expected)))
                        .expectNext(expected)
                        .verifyComplete();
                return;
            } catch (AssertionError error) {
                if (!String.valueOf(error).contains(RabbitRpcBridgeRejectedException.class.getName())) {
                    throw error;
                }
                lastFailure = error;
                Thread.sleep(10);
            }
        }
        throw lastFailure == null ? new AssertionError("RPC bridge did not accept execution") : lastFailure;
    }

    private static final class ImmediateScheduler implements Scheduler {

        @Override
        public Disposable schedule(Runnable task) {
            task.run();
            return () -> {
            };
        }


        @Override
        public Worker createWorker() {
            throw new UnsupportedOperationException("not used");
        }
    }

    private static final class InterleavingScheduler implements Scheduler {
        private final Runnable afterAccept;
        private Runnable scheduled;
        private boolean disposed;

        private InterleavingScheduler(Runnable afterAccept) {
            this.afterAccept = afterAccept;
        }

        @Override
        public Disposable schedule(Runnable task) {
            this.scheduled = task;
            afterAccept.run();
            return () -> disposed = true;
        }

        private void runScheduled() {
            if (!disposed && scheduled != null) {
                scheduled.run();
            }
        }

        @Override
        public Worker createWorker() {
            throw new UnsupportedOperationException("not used");
        }
    }

    private static RabbitRpcWebFluxBridgeProperties properties() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setMaxConcurrency(1);
        return properties;
    }
}
