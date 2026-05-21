package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import java.time.Duration;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RabbitBridgeExecutorProviderTest {

    @Test
    void platformProviderUsesBoundedWorkerCountAndQueueCapacity() {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(2, 3);

        try (PlatformThreadRabbitBridgeExecutorProvider provider = new PlatformThreadRabbitBridgeExecutorProvider(bridge)) {
            ExecutorService executor = provider.getExecutor();

            assertThat(executor).isInstanceOf(ThreadPoolExecutor.class);
            ThreadPoolExecutor threadPool = (ThreadPoolExecutor) executor;
            assertThat(threadPool.getCorePoolSize()).isEqualTo(2);
            assertThat(threadPool.getMaximumPoolSize()).isEqualTo(2);
            assertThat(threadPool.getQueue().remainingCapacity()).isEqualTo(3);
        }
    }

    @Test
    void platformProviderRejectsWhenWorkersAndQueueAreSaturated() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);

        try (PlatformThreadRabbitBridgeExecutorProvider provider = new PlatformThreadRabbitBridgeExecutorProvider(bridge)) {
            ExecutorService executor = provider.getExecutor();
            CountDownLatch firstStarted = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);

            executor.submit(() -> {
                firstStarted.countDown();
                await(release);
            });
            assertThat(firstStarted.await(1, TimeUnit.SECONDS)).isTrue();
            executor.submit(() -> await(release));

            assertThatThrownBy(() -> executor.submit(() -> { }))
                    .isInstanceOf(RejectedExecutionException.class);

            release.countDown();
        }
    }

    @Test
    void platformProviderNamesThreadsClearly() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);

        try (PlatformThreadRabbitBridgeExecutorProvider provider = new PlatformThreadRabbitBridgeExecutorProvider(bridge)) {
            String threadName = provider.getExecutor().submit(() -> Thread.currentThread().getName()).get(1, TimeUnit.SECONDS);

            assertThat(threadName).startsWith("reliable-message-rabbit-bridge-platform-");
        }
    }

    @Test
    void platformProviderUsesNonDaemonWorkers() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);

        try (PlatformThreadRabbitBridgeExecutorProvider provider = new PlatformThreadRabbitBridgeExecutorProvider(bridge)) {
            boolean daemon = provider.getExecutor().submit(() -> Thread.currentThread().isDaemon()).get(1, TimeUnit.SECONDS);

            assertThat(daemon).isFalse();
        }
    }

    @Test
    void platformProviderCloseLetsActiveTasksFinishBeforeInterrupting() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);
        PlatformThreadRabbitBridgeExecutorProvider provider = new PlatformThreadRabbitBridgeExecutorProvider(bridge);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean interrupted = new AtomicBoolean();

        provider.getExecutor().submit(() -> {
            started.countDown();
            try {
                release.await();
                completed.set(true);
            } catch (InterruptedException exception) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

        Thread closeThread = new Thread(provider::close);
        closeThread.start();
        Thread.sleep(100);

        assertThat(closeThread.isAlive()).isTrue();
        assertThat(interrupted).isFalse();

        release.countDown();
        closeThread.join(1000);

        assertThat(closeThread.isAlive()).isFalse();
        assertThat(completed).isTrue();
        assertThat(interrupted).isFalse();
    }

    @Test
    void virtualThreadProviderRunsTasksOnVirtualThreads() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);
        bridge.setExecutorMode(RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD);

        try (VirtualThreadRabbitBridgeExecutorProvider provider = new VirtualThreadRabbitBridgeExecutorProvider(bridge)) {
            boolean virtual = provider.getExecutor().submit(() -> Thread.currentThread().isVirtual()).get(1, TimeUnit.SECONDS);

            assertThat(virtual).isTrue();
        }
    }

    @Test
    void virtualThreadProviderDoesNotApplyConcurrencyLimitBeforeGuardPhase() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 0);
        bridge.setExecutorMode(RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD);

        try (VirtualThreadRabbitBridgeExecutorProvider provider = new VirtualThreadRabbitBridgeExecutorProvider(bridge)) {
            ExecutorService executor = provider.getExecutor();
            CountDownLatch started = new CountDownLatch(3);
            CountDownLatch release = new CountDownLatch(1);

            Future<?> first = executor.submit(() -> {
                started.countDown();
                await(release);
            });
            Future<?> second = executor.submit(() -> {
                started.countDown();
                await(release);
            });
            Future<?> third = executor.submit(() -> {
                started.countDown();
                await(release);
            });

            assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

            release.countDown();
            first.get(1, TimeUnit.SECONDS);
            second.get(1, TimeUnit.SECONDS);
            third.get(1, TimeUnit.SECONDS);
        }
    }

    @Test
    void virtualThreadProviderCloseLetsActiveTasksFinishBeforeInterrupting() throws Exception {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);
        bridge.setExecutorMode(RabbitWebFluxBridgeProperties.ExecutorMode.VIRTUAL_THREAD);
        VirtualThreadRabbitBridgeExecutorProvider provider = new VirtualThreadRabbitBridgeExecutorProvider(bridge);
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        AtomicBoolean completed = new AtomicBoolean();
        AtomicBoolean interrupted = new AtomicBoolean();

        provider.getExecutor().submit(() -> {
            started.countDown();
            try {
                release.await();
                completed.set(true);
            } catch (InterruptedException exception) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
            }
        });
        assertThat(started.await(1, TimeUnit.SECONDS)).isTrue();

        Thread closeThread = new Thread(provider::close);
        closeThread.start();
        Thread.sleep(100);

        assertThat(closeThread.isAlive()).isTrue();
        assertThat(interrupted).isFalse();

        release.countDown();
        closeThread.join(1000);

        assertThat(closeThread.isAlive()).isFalse();
        assertThat(completed).isTrue();
        assertThat(interrupted).isFalse();
    }

    @Test
    void schedulerAdapterRunsWorkOnBridgeExecutor() {
        RabbitWebFluxBridgeProperties.Bridge bridge = bridge(1, 1);

        try (PlatformThreadRabbitBridgeExecutorProvider provider = new PlatformThreadRabbitBridgeExecutorProvider(bridge)) {
            Scheduler scheduler = RabbitBridgeSchedulerFactory.create(provider);
            try {
                String threadName = Mono.fromCallable(() -> Thread.currentThread().getName())
                        .subscribeOn(scheduler)
                        .block(Duration.ofSeconds(1));

                assertThat(threadName)
                        .startsWith("reliable-message-rabbit-bridge-platform-")
                        .doesNotContain("parallel")
                        .doesNotContain("ForkJoinPool")
                        .doesNotContain("reactor-http-nio");
            } finally {
                scheduler.dispose();
            }
        }
    }

    private static RabbitWebFluxBridgeProperties.Bridge bridge(int workerThreads, int queueCapacity) {
        RabbitWebFluxBridgeProperties.Bridge bridge = new RabbitWebFluxBridgeProperties.Bridge();
        bridge.setWorkerThreads(workerThreads);
        bridge.setQueueCapacity(queueCapacity);
        return bridge;
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }
}
