package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.stream.Collectors;

public final class Sha256MessageAuditHasher implements MessageAuditHasher {

    @Override
    public String hashPayload(Object payload) {
        return payload == null ? null : hash(stableValue(payload).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String hashHeaders(Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        return hash(stableValue(headers).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String hashRawBody(byte[] rawBody) {
        return rawBody == null ? null : hash(rawBody);
    }

    private static String hash(byte[] value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return Base64.getEncoder().encodeToString(digest.digest(value));
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is not available", error);
        }
    }

    private static String stableValue(Object value) {
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean || value instanceof Enum<?>) {
            return String.valueOf(value);
        }
        if (value instanceof Map<?, ?> map) {
            TreeMap<String, String> sorted = new TreeMap<>();
            map.forEach((key, mapValue) -> sorted.put(String.valueOf(key), stableValue(mapValue)));
            return sorted.toString();
        }
        if (value instanceof Collection<?> collection) {
            List<String> items = collection.stream().map(Sha256MessageAuditHasher::stableValue).collect(Collectors.toList());
            return items.toString();
        }
        return stableObjectValue(value);
    }
    private static String stableObjectValue(Object value) {
        TreeMap<String, String> fields = new TreeMap<>();
        Class<?> type = value.getClass();
        while (type != null && type != Object.class) {
            for (Field field : type.getDeclaredFields()) {
                if (Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                field.setAccessible(true);
                try {
                    fields.putIfAbsent(field.getName(), stableValue(field.get(value)));
                } catch (IllegalAccessException ignored) {
                    // ignore inaccessible fields
                }
            }
            type = type.getSuperclass();
        }
        if (fields.isEmpty()) {
            return value.getClass().getName() + ":" + String.valueOf(value);
        }
        return value.getClass().getName() + fields;
    }

}
