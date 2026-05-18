package io.github.huynhngochuyhoang.reliablemessage.admin.api;

public interface OutboxAdminOperations {

    void retry(String id);
}
