package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitBridgeConcurrencyGuardTest {

    @Test
    void constructorRejectsNonPositiveMaxConcurrency() {
        assertThatThrownBy(() -> new RabbitBridgeConcurrencyGuard(0))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new RabbitBridgeConcurrencyGuard(-1))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void saturationFailsFastWithRabbitBridgeRejectedException() throws Exception {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try {
            Future<?> first = guard.submit(executor, () -> {
                started.countDown();
                await(release);
            });
            assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

            assertThatThrownBy(() -> guard.submit(executor, () -> { }))
                    .isInstanceOf(RabbitBridgeRejectedException.class);

            release.countDown();
            first.get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void saturatedGuardDoesNotSubmitToExecutor() {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        QueuingExecutorService executor = new QueuingExecutorService();

        guard.submit(executor, () -> { });

        assertThatThrownBy(() -> guard.submit(executor, () -> { }))
                .isInstanceOf(RabbitBridgeRejectedException.class);
        assertThat(executor.executeCalls()).isEqualTo(1);
    }

    @Test
    void releasesPermitAfterSuccess() throws Exception {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            assertThat(guard.submit(executor, () -> "first").get(1, TimeUnit.SECONDS)).isEqualTo("first");
            assertThat(guard.submit(executor, () -> "second").get(1, TimeUnit.SECONDS)).isEqualTo("second");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void releasesPermitAfterFailure() throws Exception {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<String> failed = guard.submit(executor, () -> {
                throw new IllegalStateException("boom");
            });

            assertThatThrownBy(() -> failed.get(1, TimeUnit.SECONDS))
                    .isInstanceOf(ExecutionException.class)
                    .hasCauseInstanceOf(IllegalStateException.class);
            assertThat(guard.submit(executor, () -> "recovered").get(1, TimeUnit.SECONDS)).isEqualTo("recovered");
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void releasesPermitAfterCancellationBeforeRun() {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        QueuingExecutorService executor = new QueuingExecutorService();

        Future<?> queued = guard.submit(executor, () -> { });
        assertThat(queued.cancel(true)).isTrue();

        guard.submit(executor, () -> { });
        assertThat(executor.executeCalls()).isEqualTo(2);
    }

    @Test
    void cancelWithInterruptInterruptsRunningBridgeTask() throws Exception {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch interrupted = new CountDownLatch(1);
        try {
            Future<?> running = guard.submit(executor, () -> {
                started.countDown();
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(10));
                } catch (InterruptedException exception) {
                    interrupted.countDown();
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

            assertThat(running.cancel(true)).isTrue();

            assertThat(interrupted.await(1, TimeUnit.SECONDS)).isTrue();
            assertThatThrownBy(running::get).isInstanceOf(CancellationException.class);
            executor.submit(() -> { }).get(1, TimeUnit.SECONDS);
            guard.submit(executor, () -> { }).get(1, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void cancelWithoutInterruptDoesNotInterruptRunningBridgeTask() throws Exception {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch finish = new CountDownLatch(1);
        AtomicBoolean interrupted = new AtomicBoolean();
        try {
            Future<?> running = guard.submit(executor, () -> {
                started.countDown();
                try {
                    finish.await();
                } catch (InterruptedException exception) {
                    interrupted.set(true);
                    Thread.currentThread().interrupt();
                }
            });
            assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

            assertThat(running.cancel(false)).isTrue();
            Thread.sleep(100);

            assertThat(interrupted).isFalse();
            finish.countDown();
            executor.submit(() -> { }).get(1, TimeUnit.SECONDS);
            guard.submit(executor, () -> { }).get(1, TimeUnit.SECONDS);
        } finally {
            finish.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void nullCallableDoesNotLeakPermit() {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        QueuingExecutorService executor = new QueuingExecutorService();

        assertThatThrownBy(() -> guard.submit(executor, (Callable<Object>) null))
                .isInstanceOf(NullPointerException.class);

        guard.submit(executor, () -> { });
        assertThat(executor.executeCalls()).isEqualTo(1);
    }

    @Test
    void releasesPermitWhenExecutorThrowsErrorDuringSubmission() {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        ErrorOnceExecutorService executor = new ErrorOnceExecutorService();

        assertThatThrownBy(() -> guard.submit(executor, () -> { }))
                .isInstanceOf(AssertionError.class);

        guard.submit(executor, () -> { });
        assertThat(executor.accepted()).isEqualTo(1);
    }

    @Test
    void releasesPermitWhenExecutorRejectsSubmission() {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        RejectOnceExecutorService executor = new RejectOnceExecutorService();

        assertThatThrownBy(() -> guard.submit(executor, () -> { }))
                .isInstanceOf(RabbitBridgeRejectedException.class)
                .hasCauseInstanceOf(RejectedExecutionException.class);

        guard.submit(executor, () -> { });
        assertThat(executor.accepted()).isEqualTo(1);
    }
    @Test
    void submitFutureCompletesOnlyAfterPermitIsReleased() throws Exception {
        RabbitBridgeConcurrencyGuard guard = guard(1);
        DirectExecutorService executor = new DirectExecutorService();
        AtomicBoolean completed = new AtomicBoolean();

        CompletableFuture<String> first = guard.submitFuture(executor, () -> "first");
        first.whenComplete((result, error) -> {
            guard.submit(executor, () -> { });
            completed.set(true);
        });

        assertThat(first.get(1, TimeUnit.SECONDS)).isEqualTo("first");
        assertThat(completed).isTrue();
    }

    private static final class DirectExecutorService extends AbstractExecutorService {
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.of();
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            command.run();
        }
    }

    private static RabbitBridgeConcurrencyGuard guard(int maxConcurrency) {
        RabbitWebFluxBridgeProperties.Bridge bridge = new RabbitWebFluxBridgeProperties.Bridge();
        bridge.setMaxConcurrency(maxConcurrency);
        return new RabbitBridgeConcurrencyGuard(bridge);
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static class QueuingExecutorService extends AbstractExecutorService {
        private final List<Runnable> tasks = new ArrayList<>();
        private final AtomicInteger executeCalls = new AtomicInteger();
        private boolean shutdown;

        @Override
        public void shutdown() {
            shutdown = true;
        }

        @Override
        public List<Runnable> shutdownNow() {
            shutdown = true;
            return List.copyOf(tasks);
        }

        @Override
        public boolean isShutdown() {
            return shutdown;
        }

        @Override
        public boolean isTerminated() {
            return shutdown;
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) {
            return shutdown;
        }

        @Override
        public void execute(Runnable command) {
            executeCalls.incrementAndGet();
            tasks.add(command);
        }

        int executeCalls() {
            return executeCalls.get();
        }
    }

    private static final class RejectOnceExecutorService extends QueuingExecutorService {
        private boolean reject = true;
        private final AtomicInteger accepted = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            if (reject) {
                reject = false;
                throw new RejectedExecutionException("reject once");
            }
            accepted.incrementAndGet();
            super.execute(command);
        }

        int accepted() {
            return accepted.get();
        }
    }

    private static final class ErrorOnceExecutorService extends QueuingExecutorService {
        private boolean fail = true;
        private final AtomicInteger accepted = new AtomicInteger();

        @Override
        public void execute(Runnable command) {
            if (fail) {
                fail = false;
                throw new AssertionError("submission failed");
            }
            accepted.incrementAndGet();
            super.execute(command);
        }

        int accepted() {
            return accepted.get();
        }
    }
}
