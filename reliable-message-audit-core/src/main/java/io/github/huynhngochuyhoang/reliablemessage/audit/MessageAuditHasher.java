package io.github.huynhngochuyhoang.reliablemessage.audit;

import java.util.Map;

public interface MessageAuditHasher {

    String hashPayload(Object payload);

    String hashHeaders(Map<String, Object> headers);

    String hashRawBody(byte[] rawBody);
}
