package io.github.huynhngochuyhoang.reliablemessage.rpc;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class RpcHeadersTest {

    @Test
    void createsSharedPropagationHeaders() {
        RpcContext context = RpcContext.builder()
                .correlationId("correlation-1")
                .requestId("request-1")
                .traceId("trace-1")
                .tenantId("tenant-1")
                .header("x-custom", "custom")
                .build();

        Map<String, String> headers = RpcHeaders.from(context);

        assertThat(headers)
                .containsEntry(RpcHeaders.CORRELATION_ID, "correlation-1")
                .containsEntry(RpcHeaders.REQUEST_ID, "request-1")
                .containsEntry(RpcHeaders.TRACE_ID, "trace-1")
                .containsEntry(RpcHeaders.TENANT_ID, "tenant-1")
                .containsEntry("x-custom", "custom");
    }
}
