package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Map;
import java.util.TreeMap;

public final class Sha256MessageAuditHasher implements MessageAuditHasher {

    @Override
    public String hashPayload(Object payload) {
        return payload == null ? null : hash(String.valueOf(payload).getBytes(StandardCharsets.UTF_8));
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
}
