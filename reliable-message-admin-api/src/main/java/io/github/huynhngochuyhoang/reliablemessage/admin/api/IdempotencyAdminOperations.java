package io.github.huynhngochuyhoang.reliablemessage.admin.api;

public interface IdempotencyAdminOperations {

    IdempotencyRecord find(String key);

    void clear(String key);
}
