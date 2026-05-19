package io.github.huynhngochuyhoang.reliablemessage.webflux;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.springframework.aop.support.AopUtils;
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

public class ReactiveReliableListenerRegistrar implements SmartInitializingSingleton, ApplicationContextAware {

    private final List<ReactiveReliableListenerEndpoint> endpoints = new ArrayList<>();
    private ApplicationContext applicationContext;

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

    public List<ReactiveReliableListenerEndpoint> endpoints() {
        return List.copyOf(endpoints);
    }

    private void registerEndpoint(String beanName, Object bean, Method method) {
        ReactiveReliableListener listener = AnnotationUtils.findAnnotation(method, ReactiveReliableListener.class);
        if (listener == null) {
            return;
        }

        Method invocableMethod = AopUtils.selectInvocableMethod(method, bean.getClass());
        validateListenerMethod(invocableMethod);
        endpoints.add(new ReactiveReliableListenerEndpoint(
                beanName,
                bean,
                invocableMethod,
                listener.value(),
                payloadType(invocableMethod)
        ));
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
