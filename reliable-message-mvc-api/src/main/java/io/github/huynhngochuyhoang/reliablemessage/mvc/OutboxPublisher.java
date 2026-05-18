package io.github.huynhngochuyhoang.reliablemessage.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;

public interface OutboxPublisher {

    void publishLater(String eventName, Object payload, PublishOptions options);
}
