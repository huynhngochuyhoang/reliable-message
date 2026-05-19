package io.github.huynhngochuyhoang.reliablemessage.rpc.mvc;

import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcContext;
import io.github.huynhngochuyhoang.reliablemessage.rpc.RpcHeaders;

import java.util.Map;

public final class RpcGrpcMetadata {

    private RpcGrpcMetadata() {
    }

    public static Map<String, String> headers(RpcContext context) {
        return RpcHeaders.from(context);
    }
}
