package io.github.huynhngochuyhoang.reliablemessage.rpc;

import java.util.Optional;

public final class RpcContextHolder {

    private static final ThreadLocal<RpcContext> CONTEXT = new ThreadLocal<>();

    private RpcContextHolder() {
    }

    public static Optional<RpcContext> current() {
        return Optional.ofNullable(CONTEXT.get());
    }

    public static void set(RpcContext context) {
        if (context == null) {
            clear();
            return;
        }
        CONTEXT.set(context);
    }

    public static void clear() {
        CONTEXT.remove();
    }
}
