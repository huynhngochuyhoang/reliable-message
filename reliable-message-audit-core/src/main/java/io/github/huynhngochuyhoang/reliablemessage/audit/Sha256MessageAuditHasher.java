package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public final class Sha256MessageAuditHasher implements MessageAuditHasher {

    @Override
    public String hashPayload(Object payload) {
        return payload == null ? null : hash(stableValue(payload, activeObjects()).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public String hashHeaders(Map<String, Object> headers) {
        if (headers == null || headers.isEmpty()) {
            return null;
        }
        return hash(stableValue(headers, activeObjects()).getBytes(StandardCharsets.UTF_8));
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

    private static Set<Object> activeObjects() {
        return Collections.newSetFromMap(new IdentityHashMap<>());
    }

    private static String stableValue(Object value, Set<Object> activeObjects) {
        if (value == null) {
            return "null";
        }
        if (value instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean || value instanceof Enum<?>) {
            return String.valueOf(value);
        }
        if (value.getClass().isArray()) {
            return stableArrayValue(value, activeObjects);
        }
        if (value instanceof Map<?, ?> map) {
            if (!activeObjects.add(value)) {
                return cycleValue(value);
            }
            try {
                TreeMap<String, String> sorted = new TreeMap<>();
                map.forEach((key, mapValue) -> sorted.put(String.valueOf(key), stableValue(mapValue, activeObjects)));
                return sorted.toString();
            } finally {
                activeObjects.remove(value);
            }
        }
        if (value instanceof Collection<?> collection) {
            if (!activeObjects.add(value)) {
                return cycleValue(value);
            }
            try {
                List<String> items = collection.stream()
                        .map(item -> stableValue(item, activeObjects))
                        .collect(Collectors.toList());
                return items.toString();
            } finally {
                activeObjects.remove(value);
            }
        }
        return stableObjectValue(value, activeObjects);
    }

    private static String stableArrayValue(Object value, Set<Object> activeObjects) {
        if (!activeObjects.add(value)) {
            return cycleValue(value);
        }
        try {
            int length = Array.getLength(value);
            List<String> items = new ArrayList<>(length);
            for (int index = 0; index < length; index++) {
                items.add(stableValue(Array.get(value, index), activeObjects));
            }
            return value.getClass().getName() + items;
        } finally {
            activeObjects.remove(value);
        }
    }

    private static String stableObjectValue(Object value, Set<Object> activeObjects) {
        if (!activeObjects.add(value)) {
            return cycleValue(value);
        }
        TreeMap<String, String> fields = new TreeMap<>();
        try {
            Class<?> type = value.getClass();
            while (type != null && type != Object.class) {
                for (Field field : type.getDeclaredFields()) {
                    if (Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    try {
                        if (!field.trySetAccessible()) {
                            continue;
                        }
                        fields.putIfAbsent(field.getName(), stableValue(field.get(value), activeObjects));
                    } catch (IllegalAccessException | RuntimeException ignored) {
                        // ignore inaccessible fields
                    }
                }
                type = type.getSuperclass();
            }
            if (fields.isEmpty()) {
                return value.getClass().getName() + ":" + String.valueOf(value);
            }
            return value.getClass().getName() + fields;
        } finally {
            activeObjects.remove(value);
        }
    }

    private static String cycleValue(Object value) {
        return "<cycle:" + value.getClass().getName() + ">";
    }

}
