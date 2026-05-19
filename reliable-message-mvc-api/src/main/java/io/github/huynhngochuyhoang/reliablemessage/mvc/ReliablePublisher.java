package io.github.huynhngochuyhoang.reliablemessage.mvc;

import io.github.huynhngochuyhoang.reliablemessage.core.PublishOptions;

public interface ReliablePublisher {

    void publish(String eventName, Object payload, PublishOptions options);
}
