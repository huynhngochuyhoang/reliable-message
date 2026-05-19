package io.github.huynhngochuyhoang.reliablemessage.mvc;

import java.time.Instant;
import java.util.List;

public interface OutboxStore {

    void save(OutboxMessage message);

    List<OutboxMessage> findPending(int limit);

    default List<OutboxMessage> findForAdmin(int limit) {
        return findPending(limit);
    }

    void markPublished(String id);

    void markFailed(String id, Throwable error, Instant nextRetryAt);
}
