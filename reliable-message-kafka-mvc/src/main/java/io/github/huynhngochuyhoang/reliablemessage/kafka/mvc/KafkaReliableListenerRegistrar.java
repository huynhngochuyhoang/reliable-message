package io.github.huynhngochuyhoang.reliablemessage.kafka.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.IdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.github.huynhngochuyhoang.reliablemessage.observability.MessageObservability;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.observation.ObservationRegistry;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.SmartInitializingSingleton;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.lang.NonNull;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class KafkaReliableListenerRegistrar implements SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private final ConsumerFactory<String, byte[]> consumerFactory;
    private final MessageSerializer serializer;
    private final KafkaReliableMessageProperties properties;
    private final MessageObservability observability;
    private final KafkaTopologyAutoConfigurer topologyAutoConfigurer;
    private final KafkaRetryStrategy retryStrategy;
    private final IdempotencyStore idempotencyStore;
    private final List<KafkaMessageListenerContainer<String, byte[]>> containers = new ArrayList<>();
    private ApplicationContext applicationContext;

    public KafkaReliableListenerRegistrar(
            ConsumerFactory<String, byte[]> consumerFactory,
            MessageSerializer serializer,
            KafkaReliableMessageProperties properties,
            MeterRegistry meterRegistry,
            KafkaTopologyAutoConfigurer topologyAutoConfigurer,
            KafkaRetryStrategy retryStrategy
    ) {
        this(consumerFactory, serializer, properties, new MessageObservability(meterRegistry, ObservationRegistry.NOOP),
                topologyAutoConfigurer, retryStrategy, null);
    }

    public KafkaReliableListenerRegistrar(
            ConsumerFactory<String, byte[]> consumerFactory,
            MessageSerializer serializer,
            KafkaReliableMessageProperties properties,
            MessageObservability observability,
            KafkaTopologyAutoConfigurer topologyAutoConfigurer,
            KafkaRetryStrategy retryStrategy,
            IdempotencyStore idempotencyStore
    ) {
        this.consumerFactory = consumerFactory;
        this.serializer = serializer;
        this.properties = properties;
        this.observability = observability;
        this.topologyAutoConfigurer = topologyAutoConfigurer;
        this.retryStrategy = retryStrategy;
        this.idempotencyStore = idempotencyStore;
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
        containers.forEach(KafkaMessageListenerContainer::destroy);
    }

    public List<KafkaMessageListenerContainer<String, byte[]>> containers() {
        return List.copyOf(containers);
    }

    private void registerEndpoint(String beanName, Object bean, Method method) {
        ReliableListener reliableListener = AnnotationUtils.findAnnotation(method, ReliableListener.class);
        if (reliableListener == null) {
            return;
        }

        Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        KafkaReliableListenerEndpoint endpoint = endpoint(beanName, bean, invocableMethod, reliableListener);
        topologyAutoConfigurer.declareListenerTopology(endpoint.topicName(), endpoint.consumerGroup());
        KafkaMessageListenerContainer<String, byte[]> container = container(endpoint);
        containers.add(container);
        if (properties.getKafka().isListenerAutoStartup()) {
            container.start();
        }
    }

    private KafkaReliableListenerEndpoint endpoint(
            String beanName,
            Object bean,
            Method method,
            ReliableListener reliableListener
    ) {
        validateListenerMethod(method);
        String eventName = reliableListener.value();
        return new KafkaReliableListenerEndpoint(
                beanName,
                bean,
                method,
                eventName,
                properties.topicName(eventName),
                properties.consumerGroup(),
                payloadType(method)
        );
    }

    private KafkaMessageListenerContainer<String, byte[]> container(KafkaReliableListenerEndpoint endpoint) {
        ContainerProperties containerProperties = new ContainerProperties(listenerTopics(endpoint));
        containerProperties.setGroupId(endpoint.consumerGroup());
        containerProperties.setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        containerProperties.setMessageListener(new KafkaReliableMessageHandler(
                endpoint,
                serializer,
                observability,
                idempotencyStore,
                properties.getIdempotency().getTtl(),
                retryStrategy
        ));
        KafkaMessageListenerContainer<String, byte[]> container = new KafkaMessageListenerContainer<>(
                consumerFactory,
                containerProperties
        );
        container.setAutoStartup(properties.getKafka().isListenerAutoStartup());
        return container;
    }

    private String[] listenerTopics(KafkaReliableListenerEndpoint endpoint) {
        List<String> topics = new ArrayList<>();
        topics.add(endpoint.topicName());
        for (java.time.Duration delay : properties.getRetry().getBackoff()) {
            topics.add(KafkaTopicNames.retryTopic(endpoint.topicName(), endpoint.consumerGroup(), delay));
        }
        return topics.toArray(String[]::new);
    }

    private static void validateListenerMethod(Method method) {
        if (method.getReturnType() != Void.TYPE) {
            throw new IllegalStateException("@ReliableListener method must return void: " + method);
        }
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length != 1 || parameterTypes[0] != ReliableMessage.class) {
            throw new IllegalStateException("@ReliableListener method must have one ReliableMessage<T> parameter: " + method);
        }
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
