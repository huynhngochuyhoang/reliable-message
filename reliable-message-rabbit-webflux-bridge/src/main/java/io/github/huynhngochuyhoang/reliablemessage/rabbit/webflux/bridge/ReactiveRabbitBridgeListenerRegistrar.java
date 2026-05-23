package io.github.huynhngochuyhoang.reliablemessage.rabbit.webflux.bridge;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.lang.NonNull;
import org.springframework.util.ReflectionUtils;
import reactor.core.publisher.Mono;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class ReactiveRabbitBridgeListenerRegistrar implements SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private static final Duration DEFAULT_IDEMPOTENCY_TTL = Duration.ofHours(24);

    private final ConnectionFactory connectionFactory;
    private final MessageSerializer serializer;
    private final RabbitWebFluxBridgeProperties properties;
    private final ReactiveRabbitBridgeListenerMethodInvoker invoker;
    private final ReactiveRabbitBridgeTopologyAutoConfigurer topologyAutoConfigurer;
    private final ReactiveIdempotencyStore idempotencyStore;
    private final ReactiveRabbitBridgeFailureHandler failureHandler;
    private final List<ReactiveRabbitBridgeListenerEndpoint> endpoints = new ArrayList<>();
    private final List<SimpleMessageListenerContainer> containers = new ArrayList<>();
    private ApplicationContext applicationContext;

    public ReactiveRabbitBridgeListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties
    ) {
        this(connectionFactory, serializer, properties, new ReactiveRabbitBridgeTopologyAutoConfigurer(null, properties));
    }

    public ReactiveRabbitBridgeListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            ReactiveRabbitBridgeTopologyAutoConfigurer topologyAutoConfigurer
    ) {
        this(connectionFactory, serializer, properties, topologyAutoConfigurer, null, ReactiveRabbitBridgeFailureHandler.noop());
    }

    public ReactiveRabbitBridgeListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitWebFluxBridgeProperties properties,
            ReactiveRabbitBridgeTopologyAutoConfigurer topologyAutoConfigurer,
            ReactiveIdempotencyStore idempotencyStore,
            ReactiveRabbitBridgeFailureHandler failureHandler
    ) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory, "connectionFactory");
        this.serializer = Objects.requireNonNull(serializer, "serializer");
        this.properties = Objects.requireNonNull(properties, "properties");
        this.topologyAutoConfigurer = Objects.requireNonNull(topologyAutoConfigurer, "topologyAutoConfigurer");
        this.idempotencyStore = idempotencyStore;
        this.failureHandler = failureHandler == null ? ReactiveRabbitBridgeFailureHandler.noop() : failureHandler;
        this.invoker = new ReactiveRabbitBridgeListenerMethodInvoker();
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
    }

    @Override
    public void afterSingletonsInstantiated() {
        String[] beanNames = applicationContext.getBeanNamesForType(Object.class, false, true);
        for (String beanName : beanNames) {
            Object bean = applicationContext.getBean(beanName);
            Class<?> targetClass = AopUtils.getTargetClass(bean);
            ReflectionUtils.doWithMethods(targetClass, method -> registerEndpoint(beanName, bean, method));
        }
    }

    @Override
    public void destroy() {
        containers.forEach(SimpleMessageListenerContainer::destroy);
    }

    public List<ReactiveRabbitBridgeListenerEndpoint> endpoints() {
        return List.copyOf(endpoints);
    }

    public List<SimpleMessageListenerContainer> containers() {
        return List.copyOf(containers);
    }

    private void registerEndpoint(String beanName, Object bean, Method method) {
        ReactiveReliableListener listener = AnnotationUtils.findAnnotation(method, ReactiveReliableListener.class);
        if (listener == null) {
            return;
        }

        Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        validateListenerMethod(method);
        String eventName = listener.value();
        ReactiveRabbitBridgeListenerEndpoint endpoint = new ReactiveRabbitBridgeListenerEndpoint(
                beanName,
                bean,
                invocableMethod,
                eventName,
                properties.queueName(eventName),
                payloadType(method)
        );
        endpoints.add(endpoint);
        topologyAutoConfigurer.declareListenerTopology(endpoint.eventName(), endpoint.queueName());

        SimpleMessageListenerContainer container = container(endpoint);
        containers.add(container);
        container.afterPropertiesSet();
        if (properties.getRabbit().isListenerAutoStartup()) {
            container.start();
        }
    }

    private SimpleMessageListenerContainer container(ReactiveRabbitBridgeListenerEndpoint endpoint) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(endpoint.queueName());
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setPrefetchCount(1);
        container.setMessageListener(new ReactiveRabbitBridgeMessageHandler(
                endpoint,
                serializer,
                invoker,
                idempotencyStore,
                DEFAULT_IDEMPOTENCY_TTL,
                failureHandler
        ));
        container.setAutoStartup(properties.getRabbit().isListenerAutoStartup());
        return container;
    }

    private static void validateListenerMethod(Method method) {
        if (method.getReturnType() != Mono.class || !returnsMonoVoid(method)) {
            throw new IllegalStateException("@ReactiveReliableListener method must return Mono<Void>: " + method);
        }
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1 || parameterTypes[0] != ReliableMessage.class) {
            throw new IllegalStateException("@ReactiveReliableListener method must have one ReliableMessage<T> parameter: " + method);
        }
    }

    private static boolean returnsMonoVoid(Method method) {
        Type returnType = method.getGenericReturnType();
        if (returnType instanceof ParameterizedType parameterizedType) {
            Type rawType = parameterizedType.getRawType();
            Type argument = parameterizedType.getActualTypeArguments()[0];
            return rawType == Mono.class && argument == Void.class;
        }
        return false;
    }

    private static Class<?> payloadType(Method method) {
        Type type = method.getGenericParameterTypes()[0];
        if (type instanceof ParameterizedType parameterizedType) {
            Type argument = parameterizedType.getActualTypeArguments()[0];
            if (argument instanceof Class<?> payloadClass) {
                return payloadClass;
            }
        }
        return Object.class;
    }
}
