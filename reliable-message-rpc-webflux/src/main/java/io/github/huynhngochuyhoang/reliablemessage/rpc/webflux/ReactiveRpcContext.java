package io.github.huynhngochuyhoang.reliablemessage.rpc.webflux;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import reactor.util.context.Context;
import reactor.util.context.ContextView;

import java.util.Optional;
import java.util.function.Function;

public final class ReactiveRpcContext {

    public static final String RPC_CONTEXT_KEY = ReactiveRpcContext.class.getName() + ".context";

    private ReactiveRpcContext() {
    }

    public static Function<Context, Context> write(RpcContext context) {
        if (context == null) {
            throw new IllegalArgumentException("context must not be null");
        }
        return reactorContext -> reactorContext.put(RPC_CONTEXT_KEY, context);
    }

    public static Optional<RpcContext> current(ContextView context) {
        if (!context.hasKey(RPC_CONTEXT_KEY)) {
            return Optional.empty();
        }
        Object value = context.get(RPC_CONTEXT_KEY);
        return value instanceof RpcContext rpcContext ? Optional.of(rpcContext) : Optional.empty();
    }
}
