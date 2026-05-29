package io.github.huynhngochuyhoang.reliablemessage.rpc.rabbit.webflux.bridge;

import java.time.Duration;
import java.util.Objects;

public final class RpcOptions {

    private final RpcResponseMode responseMode;
    private final Duration timeout;

    private RpcOptions(RpcResponseMode responseMode, Duration timeout) {
        this.responseMode = Objects.requireNonNull(responseMode, "responseMode must not be null");
        if (timeout != null && (timeout.isZero() || timeout.isNegative())) {
            throw new IllegalArgumentException("timeout must be positive");
        }
        this.timeout = timeout;
    }

    public static RpcOptions raw() {
        return new RpcOptions(RpcResponseMode.RAW, null);
    }

    public static RpcOptions envelope() {
        return new RpcOptions(RpcResponseMode.ENVELOPE, null);
    }

    public static RpcOptions of(RpcResponseMode responseMode) {
        return new RpcOptions(responseMode, null);
    }

    public RpcOptions withTimeout(Duration timeout) {
        return new RpcOptions(responseMode, timeout);
    }

    public RpcResponseMode responseMode() {
        return responseMode;
    }

    Duration timeoutOr(Duration defaultTimeout) {
        return timeout == null ? defaultTimeout : timeout;
    }
}
