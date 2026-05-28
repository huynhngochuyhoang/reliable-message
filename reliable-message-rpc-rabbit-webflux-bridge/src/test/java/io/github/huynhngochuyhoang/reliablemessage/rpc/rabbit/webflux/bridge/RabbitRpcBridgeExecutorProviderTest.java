package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("third")))
                    .expectNext("third")
                    .verifyComplete();
        }
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

            assertThat(future.isCancelled()).isTrue();

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("after-cancel")))
                    .expectNext("after-cancel")
                    .verifyComplete();
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

            StepVerifier.create(provider.execute(() -> CompletableFuture.completedFuture("third")))
                    .expectNext("third")
                    .verifyComplete();
        }
    }

    private static RabbitRpcWebFluxBridgeProperties properties() {
        RabbitRpcWebFluxBridgeProperties properties = new RabbitRpcWebFluxBridgeProperties();
        properties.setMaxConcurrency(1);
        return properties;
    }
}
