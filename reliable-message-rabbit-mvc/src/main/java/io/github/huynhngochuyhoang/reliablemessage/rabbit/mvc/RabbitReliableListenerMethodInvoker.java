package io.github.huynhngochuyhoang.reliablemessage.rabbit.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.ReliableMessage;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class RabbitReliableListenerMethodInvoker {

    private final Object bean;
    private final Method method;

    public RabbitReliableListenerMethodInvoker(Object bean, Method method) {
        this.bean = bean;
        this.method = method;
        ReflectionUtils.makeAccessible(method);
    }

    public void invoke(ReliableMessage<?> message) {
        try {
            method.invoke(bean, message);
        } catch (IllegalAccessException error) {
            throw new ReliableListenerInvocationException("Failed to access reliable listener method", error);
        } catch (InvocationTargetException error) {
            Throwable target = error.getTargetException();
            if (target instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (target instanceof Error jvmError) {
                throw jvmError;
            }
            throw new ReliableListenerInvocationException("Reliable listener method failed", target);
        }
    }
}
