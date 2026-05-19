package io.github.huynhngochuyhoang.reliablemessage.kafka.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveIdempotencyStore;
import io.github.huynhngochuyhoang.reliablemessage.webflux.ReactiveReliableListener;
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
import java.util.ArrayList;
import java.util.List;

public class ReactiveKafkaReliableListenerRegistrar implements SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private final ReactiveKafkaReceiverFactory receiverFactory;
    private final MessageSerializer serializer;
    private final ReactiveKafkaReliableMessageProperties properties;
    private final ReactiveKafkaRetryStrategy retryStrategy;
    private final ReactiveIdempotencyStore idempotencyStore;
    private final List<ReactiveKafkaReliableListenerContainer> containers = new ArrayList<>();
    private ApplicationContext applicationContext;

    public ReactiveKafkaReliableListenerRegistrar(
            ReactiveKafkaReceiverFactory receiverFactory,
            MessageSerializer serializer,
            ReactiveKafkaReliableMessageProperties properties,
            ReactiveKafkaRetryStrategy retryStrategy,
            ReactiveIdempotencyStore idempotencyStore
    ) {
        this.receiverFactory = receiverFactory;
        this.serializer = serializer;
        this.properties = properties;
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
        containers.forEach(ReactiveKafkaReliableListenerContainer::stop);
    }

    public List<ReactiveKafkaReliableListenerContainer> containers() {
        return List.copyOf(containers);
    }

    private void registerEndpoint(String beanName, Object bean, Method method) {
        ReactiveReliableListener listener = AnnotationUtils.findAnnotation(method, ReactiveReliableListener.class);
        if (listener == null) {
            return;
        }

        Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        validateListenerMethod(invocableMethod);
        ReactiveKafkaReliableListenerEndpoint endpoint = endpoint(beanName, bean, invocableMethod, listener);
        ReactiveKafkaReliableListenerContainer container = container(endpoint);
        containers.add(container);
        if (properties.getKafka().isListenerAutoStartup()) {
            container.start();
        }
    }

    private ReactiveKafkaReliableListenerEndpoint endpoint(
            String beanName,
            Object bean,
            Method method,
            ReactiveReliableListener listener
    ) {
        String eventName = listener.value();
        return new ReactiveKafkaReliableListenerEndpoint(
                beanName,
                bean,
                method,
                eventName,
                properties.topicName(eventName),
                properties.consumerGroup(),
                payloadType(method)
        );
    }

    private ReactiveKafkaReliableListenerContainer container(ReactiveKafkaReliableListenerEndpoint endpoint) {
        ReactiveKafkaReliableMessageHandler handler = new ReactiveKafkaReliableMessageHandler(
                serializer,
                idempotencyStore,
                properties.getIdempotency().getTtl(),
                retryStrategy
        );
        return new ReactiveKafkaReliableListenerContainer(
                receiverFactory.create(listenerTopics(endpoint), endpoint.consumerGroup()),
                endpoint,
                handler,
                properties.getReactive().getMaxConcurrency(),
                properties.getReactive().getPrefetch()
        );
    }

    private List<String> listenerTopics(ReactiveKafkaReliableListenerEndpoint endpoint) {
        List<String> topics = new ArrayList<>();
        topics.add(endpoint.topicName());
        for (java.time.Duration delay : properties.getRetry().getBackoff()) {
            topics.add(ReactiveKafkaTopicNames.retryTopic(endpoint.topicName(), endpoint.consumerGroup(), delay));
        }
        return topics;
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
            return parameterizedType.getRawType() == Mono.class
                    && parameterizedType.getActualTypeArguments()[0] == Void.class;
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
