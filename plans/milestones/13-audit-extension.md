# Reliable Message Spring Boot - Audit Logging Extension Design

Audit logging is an optional extension for systems that need compliance-grade message evidence.

Audit logging is different from normal observability logging.

Observability logging is for:

```text
debugging
operations
tracing
incident investigation
metrics and dashboards
```

Audit logging is for:

```text
compliance
legal retention
banking and finance requirements
regulatory evidence
full request or message reconstruction
```

## Current Status

Milestone 13 is implemented as an optional audit extension. The design remains compatible with the current architecture:

```text
MVC stack may use blocking audit infrastructure.
WebFlux stack uses reactive audit infrastructure.
Rabbit WebFlux bridge remains a blocking bridge and must not hide audit blocking inside reactive pipelines.
Audit is not outbox messaging and does not change event/RPC semantics.
```

Audit behavior is opt-in. Default framework behavior remains metadata-only and safe.

## 1. Core Principles

Default behavior:

```text
do not log full payload
do not log full headers
do not persist raw message body
do not enable audit storage automatically
```

Audit logging must be:

```text
optional
explicitly enabled
pluggable
controlled by the service owner
observable when it affects business flow
```

The framework should provide extension points, not hardcode compliance policy.

## 2. Observability Log vs Audit Log

Observability log content:

```text
messageId
eventName
correlationId
traceId
runtime
transport
destination
status
duration
attempt
errorClass
```

Audit record content may include:

```text
full headers
full payload/body
raw message
message direction
transport metadata
producer/consumer identity
timestamp
audit status
hash/signature
error details
```

Audit logging can be sensitive and expensive. It must be explicitly configured.

## 3. Extension Architecture

Framework boundary:

```text
MessageObservationProxy
 -> metrics
 -> tracing
 -> safe structured logs
 -> optional MessageAuditSink or ReactiveMessageAuditSink
```

Publish side:

```text
application
 -> ReliablePublisher or ReactiveReliablePublisher
 -> observation/audit boundary
 -> transport adapter
```

Consume side:

```text
transport listener
 -> deserialize
 -> observation/audit boundary
 -> idempotency check
 -> business handler
 -> ack/commit after success
 -> final audit status
```

Rabbit WebFlux bridge note:

```text
The Rabbit bridge is hybrid mode.
Blocking Rabbit work is isolated by the bridge executor.
Audit sinks used from WebFlux paths must not introduce hidden blocking unless explicitly configured as blocking bridge behavior.
```

## 4. Main Interfaces

MVC/blocking sink:

```java
public interface MessageAuditSink {
    void record(MessageAuditRecord record);
}
```

WebFlux/reactive sink:

```java
public interface ReactiveMessageAuditSink {
    Mono<Void> record(MessageAuditRecord record);
}
```

Default implementations are no-op.

Audit capture policy:

```java
public interface MessageAuditCapturePolicy {
    boolean shouldAudit(MessageAuditContext context);
    boolean includeHeaders(MessageAuditContext context);
    boolean includePayload(MessageAuditContext context);
    boolean includeRawBody(MessageAuditContext context);
}
```

Sanitizer:

```java
public interface MessageAuditSanitizer {
    Map<String, Object> sanitizeHeaders(Map<String, Object> headers, MessageAuditContext context);
    Object sanitizePayload(Object payload, MessageAuditContext context);
    byte[] sanitizeRawBody(byte[] rawBody, MessageAuditContext context);
}
```

Hasher:

```java
public interface MessageAuditHasher {
    String hashPayload(Object payload);
    String hashHeaders(Map<String, Object> headers);
    String hashRawBody(byte[] rawBody);
}
```

Optional signer:

```java
public interface MessageAuditSigner {
    String sign(MessageAuditRecord record);
}
```

## 5. Audit Record

```java
public record MessageAuditRecord(
    String auditId,
    MessageDirection direction,
    String runtime,
    String transport,
    String serviceName,
    String eventName,
    String messageId,
    String correlationId,
    String traceId,
    String aggregateId,
    String idempotencyKey,
    String destination,
    Map<String, Object> headers,
    Object payload,
    byte[] rawBody,
    Instant occurredAt,
    MessageAuditStatus status,
    Integer attempt,
    Long durationMs,
    String errorClass,
    String errorMessage,
    String payloadHash,
    String headersHash,
    String signature
) {}
```

Directions:

```text
PUBLISH
CONSUME
```

If RPC audit support is added later, keep request and response audit semantics separate from event publish and consume semantics.

Statuses:

```text
RECEIVED
PUBLISHED
CONSUMED
DUPLICATE
FAILED
RETRIED
DLQ
DISCARDED
```

Event retry/DLQ status must remain event messaging oriented. RPC audit, if added later, must remain request/response oriented.

## 6. Capture Policy and Sanitization

Default policy:

```text
audit disabled
includeHeaders false
includePayload false
includeRawBody false
```

Sensitive header deny-list:

```text
authorization
cookie
set-cookie
x-api-key
api-key
token
secret
password
```

Hashing and signatures support tamper evidence:

```text
payloadHash
headersHash
signature
```

Use cases:

```text
prove payload was not changed
deduplicate audit records
correlate messages without exposing full content
support legal evidence chain
```

## 7. Delivery Strategy

Supported modes:

```text
sync
async
fire-and-forget
durable-buffer
```

Sync:

```text
business publish/consume waits for audit write
stronger compliance
higher latency
audit outage may fail business flow
```

Async:

```text
audit record sent in background
lower latency
possible audit loss unless durable buffering exists
```

Durable buffer:

```text
audit record is written to local DB/outbox first
audit shipper sends to audit center or SIEM later
```

Recommended for strict banking workflows:

```text
durable-buffer
```

Audit outbox modules are separate from event outbox modules:

```text
reliable-message-audit-outbox-jdbc
reliable-message-audit-outbox-r2dbc
```

## 8. Configuration

Default safe config:

```yaml
message:
  reliability:
    audit:
      enabled: false
      mode: noop
      include-headers: false
      include-payload: false
      include-raw-body: false
```

Strict compliance example:

```yaml
message:
  reliability:
    audit:
      enabled: true
      mode: custom
      delivery: durable-buffer
      on-failure: buffer-and-retry
      include-headers: true
      include-payload: true
      include-raw-body: true
      hash:
        enabled: true
      signature:
        enabled: true
```

## 9. Stack Integration

MVC modules:

```text
reliable-message-audit-core
reliable-message-audit-mvc
reliable-message-audit-outbox-jdbc
```

MVC audit may use blocking infrastructure, but latency impact must be visible through metrics.

WebFlux modules:

```text
reliable-message-audit-core
reliable-message-audit-webflux
reliable-message-audit-outbox-r2dbc
```

WebFlux audit rules:

```text
no block()
no JDBC
no blocking Redis
no blocking HTTP client
preserve Reactor Context
```

Rabbit WebFlux bridge rule:

```text
If audit is invoked from the Rabbit blocking bridge, the blocking boundary must remain explicit.
Do not claim this path is fully reactive.
```

## 10. Failure Handling

Audit failure behavior:

```text
fail-business
continue-and-log
buffer-and-retry
```

`fail-business`:

```text
publish/consume/RPC boundary fails when audit fails
use only for strict compliance
```

`continue-and-log`:

```text
business flow continues
audit failure is logged and metriced
```

`buffer-and-retry`:

```text
audit failure writes to durable buffer and retries later
recommended for strict systems that cannot block business forever
```

Audit failures must not silently change event messaging or RPC semantics.

## 11. Metrics

MVC audit metrics:

```text
message_audit_records_total
message_audit_failed_total
message_audit_duration
message_audit_outbox_pending_total
```

WebFlux audit metrics:

```text
message_audit_reactive_records_total
message_audit_reactive_failed_total
message_audit_reactive_duration
message_audit_reactive_outbox_pending_total
```

Tags should include:

```text
runtime=mvc|webflux|webflux-bridge
transport=rabbit|kafka|http|grpc
direction=publish|consume|rpc_request|rpc_response
status=success|failed|duplicate|retry|dlq
```

## 12. Security Requirements

Audit capture must consider:

```text
PII
PCI data
banking secrecy
access control
encryption at rest
encryption in transit
retention policy
right-to-delete constraints where applicable
tamper evidence
least privilege access
```

The starter provides extension points. Compliance-specific policy belongs to the service owner or platform team.

## 13. Design Rules

```text
audit disabled by default
no payload by default
no full headers by default
noop sink by default
service owner must explicitly enable full capture
observability log is not audit log
metrics are not audit records
trace spans are not compliance storage
MVC audit may block when configured
WebFlux audit must stay reactive unless explicitly isolated as bridge behavior
```

Final recommendation:

```text
Keep normal systems metadata-only by default.
Let strict compliance systems plug in full capture, hashing, signature and durable audit delivery explicitly.
```
