package io.github.huynhngochuyhoang.reliablemessage.rpc;

import java.time.Duration;

public record RpcTimeoutPolicy(Duration requestTimeout) {

    public static RpcTimeoutPolicy none() {
        return new RpcTimeoutPolicy(null);
    }

    public boolean enabled() {
        return requestTimeout != null && !requestTimeout.isZero() && !requestTimeout.isNegative();
    }
}
