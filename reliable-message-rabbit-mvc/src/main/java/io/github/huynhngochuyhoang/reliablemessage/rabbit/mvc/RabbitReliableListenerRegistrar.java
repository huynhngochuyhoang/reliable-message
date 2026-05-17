package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import io.github.huynhngochuyhoang.reliablemessage.core.serialization.MessageSerializer;
import io.github.huynhngochuyhoang.reliablemessage.mvc.ReliableListener;
import io.micrometer.core.instrument.MeterRegistry;
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

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class RabbitReliableListenerRegistrar implements SmartInitializingSingleton, DisposableBean, ApplicationContextAware {

    private final ConnectionFactory connectionFactory;
    private final MessageSerializer serializer;
    private final RabbitReliableMessageProperties properties;
    private final MeterRegistry meterRegistry;
    private final RabbitTopologyAutoConfigurer topologyAutoConfigurer;
    private final List<SimpleMessageListenerContainer> containers = new ArrayList<>();
    private ApplicationContext applicationContext;

    public RabbitReliableListenerRegistrar(
            ConnectionFactory connectionFactory,
            MessageSerializer serializer,
            RabbitReliableMessageProperties properties,
            MeterRegistry meterRegistry,
            RabbitTopologyAutoConfigurer topologyAutoConfigurer
    ) {
        this.connectionFactory = connectionFactory;
        this.serializer = serializer;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.topologyAutoConfigurer = topologyAutoConfigurer;
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

    public List<SimpleMessageListenerContainer> containers() {
        return List.copyOf(containers);
    }

    private void registerEndpoint(String beanName, Object bean, Method method) {
        ReliableListener reliableListener = AnnotationUtils.findAnnotation(method, ReliableListener.class);
        if (reliableListener == null) {
            return;
        }

        Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        RabbitReliableListenerEndpoint endpoint = endpoint(beanName, bean, invocableMethod, reliableListener);
        topologyAutoConfigurer.declareListenerTopology(endpoint.eventName(), endpoint.queueName());
        SimpleMessageListenerContainer container = container(endpoint);
        containers.add(container);
        container.afterPropertiesSet();
        if (properties.getRabbit().isListenerAutoStartup()) {
            container.start();
        }
    }

    private RabbitReliableListenerEndpoint endpoint(
            String beanName,
            Object bean,
            Method method,
            ReliableListener reliableListener
    ) {
        validateListenerMethod(method);
        String eventName = reliableListener.value();
        return new RabbitReliableListenerEndpoint(
                beanName,
                bean,
                method,
                eventName,
                properties.queueName(eventName),
                payloadType(method)
        );
    }

    private SimpleMessageListenerContainer container(RabbitReliableListenerEndpoint endpoint) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(endpoint.queueName());
        container.setAcknowledgeMode(AcknowledgeMode.MANUAL);
        container.setMessageListener(new RabbitReliableMessageHandler(endpoint, serializer, meterRegistry));
        container.setAutoStartup(properties.getRabbit().isListenerAutoStartup());
        return container;
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
