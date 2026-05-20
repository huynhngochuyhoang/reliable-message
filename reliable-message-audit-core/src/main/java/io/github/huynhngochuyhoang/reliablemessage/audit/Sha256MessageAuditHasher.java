package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
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
        return hash(new TreeMap<>(headers).toString().getBytes(StandardCharsets.UTF_8));
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
        return value.getClass().getName() + ":" + value;
    }
}
