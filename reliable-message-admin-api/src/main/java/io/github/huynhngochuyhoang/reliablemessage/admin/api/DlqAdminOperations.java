package io.github.huynhngochuyhoang.reliablemessage.admin.api;

import io.github.huynhngochuyhoang.reliablemessage.core.DeadLetterRecord;

import java.util.List;

public interface DlqAdminOperations {

    List<DeadLetterRecord> find(int limit);

    void retry(String id);

    DeadLetterRecord discard(String id, String reason);
}
