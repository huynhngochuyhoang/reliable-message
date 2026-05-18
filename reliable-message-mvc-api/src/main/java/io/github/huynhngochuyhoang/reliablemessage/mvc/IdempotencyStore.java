package io.github.huynhngochuyhoang.reliablemessage.mvc;

import java.time.Duration;

public interface IdempotencyStore {

    IdempotencyStartResult tryStart(String key, Duration ttl);

    void markSuccess(String key);

    void markFailed(String key, Throwable error);
}
