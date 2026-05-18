# Reliable Message Spring Boot - Audit Logging Extension Design

This document describes the optional audit logging extension for the Reliable Message framework.

Audit logging is different from normal observability logging.

Normal observability logging is for:
- debugging
- operations
- tracing
- production incident investigation

Audit logging is for:
- compliance
- legal retention
- banking/finance requirements
- regulatory evidence
- full request/message reconstruction

This extension allows systems that require full message capture to plug in their own audit pipeline.

---

# 1. Core Principle

Default framework behavior must remain safe.

Default:

```text
do not log full payload
do not log full headers
log metadata only
```

Audit logging must be:

```text
optional
explicitly enabled
pluggable
controlled by service owner
```

The framework should not decide by itself to persist full body and headers.

---

# 2. Observability Log vs Audit Log

## Observability Log

Used for operational visibility.

Typical content:

```text
messageId
eventName
correlationId
traceId
transport
topic/routingKey/queue
status
duration
attempt
errorClass
```

Default level:

```text
INFO metadata only
DEBUG sanitized payload preview
TRACE local/dev only
```

## Audit Log

Used for compliance and evidence.

May include:

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

Audit logging can be sensitive and expensive.

It must be opt-in.

---

# 3. Extension Architecture

```text
MessageObservationProxy
  -> metrics
  -> tracing
  -> safe structured logs
  -> optional MessageAuditSink
```

Audit should be invoked at framework boundaries.

## Publish Side

```text
application
 -> ReliablePublisher
 -> MessageObservationProxy
 -> MessageAuditSink before/after publish
 -> transport adapter
```

## Consume Side

```text
transport listener
 -> deserialize
 -> MessageObservationProxy
 -> MessageAuditSink receive event
 -> idempotency check
 -> business handler
 -> ack/commit
 -> MessageAuditSink final status
```

---

# 4. Main Extension Interface

```java
public interface MessageAuditSink {

    void record(MessageAuditRecord record);

}
```

Default implementation:

```java
public final class NoopMessageAuditSink implements MessageAuditSink {

    @Override
    public void record(MessageAuditRecord record) {
        // default: do nothing
    }
}
```

Service owners can override it:

```java
@Component
public class BankingMessageAuditSink implements MessageAuditSink {

    @Override
    public void record(MessageAuditRecord record) {
        // encrypt
        // hash/sign
        // send to SIEM/log center/audit database
    }
}
```

---

# 5. Audit Record

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

Enums:

```java
public enum MessageDirection {
    PUBLISH,
    CONSUME
}
```

```java
public enum MessageAuditStatus {
    RECEIVED,
    PUBLISHED,
    CONSUMED,
    DUPLICATE,
    FAILED,
    RETRIED,
    DLQ,
    DISCARDED
}
```

---

# 6. Reactive Audit Sink

For WebFlux stack, provide a separate reactive interface.

```java
public interface ReactiveMessageAuditSink {

    Mono<Void> record(MessageAuditRecord record);

}
```

Default implementation:

```java
public final class NoopReactiveMessageAuditSink
        implements ReactiveMessageAuditSink {

    @Override
    public Mono<Void> record(MessageAuditRecord record) {
        return Mono.empty();
    }
}
```

Rules:

```text
do not block reactive pipelines
do not call blocking log center clients
do not call JDBC
do not call blocking Redis
```

---

# 7. Capture Policy

Do not hardcode what gets captured.

```java
public interface MessageAuditCapturePolicy {

    boolean shouldAudit(MessageAuditContext context);

    boolean includeHeaders(MessageAuditContext context);

    boolean includePayload(MessageAuditContext context);

    boolean includeRawBody(MessageAuditContext context);

}
```

Context:

```java
public record MessageAuditContext(
    MessageDirection direction,
    String runtime,
    String transport,
    String serviceName,
    String eventName,
    String destination,
    Map<String, Object> headers
) {}
```

Default policy:

```text
audit disabled
includeHeaders false
includePayload false
includeRawBody false
```

Banking policy example:

```java
@Component
public class BankingAuditCapturePolicy
        implements MessageAuditCapturePolicy {

    @Override
    public boolean shouldAudit(MessageAuditContext context) {
        return context.eventName().startsWith("payment.")
            || context.eventName().startsWith("transfer.");
    }

    @Override
    public boolean includeHeaders(MessageAuditContext context) {
        return true;
    }

    @Override
    public boolean includePayload(MessageAuditContext context) {
        return true;
    }

    @Override
    public boolean includeRawBody(MessageAuditContext context) {
        return true;
    }
}
```

---

# 8. Sanitization and Masking

Even when full audit is enabled, some systems may still need masking.

```java
public interface MessageAuditSanitizer {

    Map<String, Object> sanitizeHeaders(
        Map<String, Object> headers,
        MessageAuditContext context
    );

    Object sanitizePayload(
        Object payload,
        MessageAuditContext context
    );

    byte[] sanitizeRawBody(
        byte[] rawBody,
        MessageAuditContext context
    );
}
```

Default behavior:

```text
mask known sensitive headers
do not mutate payload unless configured
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

---

# 9. Hashing and Signature

Audit records should support tamper evidence.

Recommended fields:

```text
payloadHash
headersHash
signature
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

Use cases:
- prove that payload was not changed
- deduplicate audit records
- correlate messages without exposing full content
- support legal evidence chain

---

# 10. Delivery Strategy

Audit sink delivery should be explicit.

Supported modes:

```text
sync
async
fire-and-forget
durable-buffer
```

## Sync

```text
business publish/consume waits for audit write
```

Pros:
- stronger compliance guarantee

Cons:
- increases latency
- audit system outage can block business flow

## Async

```text
audit record is sent in background
```

Pros:
- lower latency

Cons:
- audit loss possible unless durable buffer exists

## Durable Buffer

```text
audit record is written to local DB/outbox first
then shipped to audit center
```

Pros:
- stronger reliability
- better for banking

Cons:
- more complex
- more storage

Recommended for banking:

```text
durable-buffer
```

---

# 11. Optional Audit Outbox

For strict systems, add audit outbox.

Modules:

```text
reliable-message-audit-outbox-jdbc
reliable-message-audit-outbox-r2dbc
```

Table example:

```sql
create table message_audit_outbox (
    id varchar(64) primary key,
    audit_id varchar(64) not null,
    direction varchar(32) not null,
    runtime varchar(32) not null,
    transport varchar(32) not null,
    service_name varchar(255) not null,
    event_name varchar(255),
    message_id varchar(255),
    correlation_id varchar(255),
    headers jsonb,
    payload jsonb,
    raw_body bytea,
    payload_hash varchar(128),
    headers_hash varchar(128),
    signature text,
    status varchar(32) not null,
    retry_count int not null default 0,
    next_retry_at timestamp,
    created_at timestamp not null,
    delivered_at timestamp,
    last_error text
);
```

Audit outbox flow:

```text
message publish/consume
 -> create audit record
 -> save audit outbox record
 -> audit shipper sends to log center/SIEM
 -> mark delivered
```

---

# 12. Configuration

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

Banking example:

```yaml
message:
  reliability:
    audit:
      enabled: true
      mode: custom
      delivery: durable-buffer
      include-headers: true
      include-payload: true
      include-raw-body: true
      hash:
        enabled: true
      signature:
        enabled: true
      sink:
        type: custom
```

Partial capture example:

```yaml
message:
  reliability:
    audit:
      enabled: true
      mode: structured-log
      include-headers: true
      include-payload: false
      include-raw-body: false
      hash:
        enabled: true
```

---

# 13. MVC Stack Integration

Modules:

```text
reliable-message-audit-core
reliable-message-audit-mvc
reliable-message-audit-outbox-jdbc
```

MVC interfaces:

```text
MessageAuditSink
MessageAuditCapturePolicy
MessageAuditSanitizer
MessageAuditHasher
MessageAuditSigner
```

MVC implementation can use:
- JDBC
- blocking HTTP client
- file appender
- Kafka producer
- SIEM SDK

Rule:

```text
MVC audit can be blocking, but latency impact must be visible by metrics.
```

Metrics:

```text
message_audit_records_total
message_audit_failed_total
message_audit_duration
message_audit_outbox_pending_total
```

---

# 14. WebFlux Stack Integration

Modules:

```text
reliable-message-audit-core
reliable-message-audit-webflux
reliable-message-audit-outbox-r2dbc
```

Reactive interfaces:

```text
ReactiveMessageAuditSink
MessageAuditCapturePolicy
MessageAuditSanitizer
MessageAuditHasher
MessageAuditSigner
```

WebFlux implementation can use:
- R2DBC
- WebClient
- Reactor Kafka
- non-blocking SIEM client

Rules:

```text
no block()
no JDBC
no blocking Redis
no blocking HTTP client
preserve Reactor Context
```

Metrics:

```text
message_audit_reactive_records_total
message_audit_reactive_failed_total
message_audit_reactive_duration
message_audit_reactive_outbox_pending_total
```

---

# 15. Failure Handling

Audit failure behavior must be configurable.

Options:

```text
fail-business
continue-and-log
buffer-and-retry
```

## fail-business

If audit logging fails, publish/consume fails.

Use for strict compliance.

## continue-and-log

Business flow continues, audit failure is logged/metriced.

Use for lower-risk systems.

## buffer-and-retry

Audit failure writes to durable buffer and retries later.

Recommended for banking.

Configuration:

```yaml
message:
  reliability:
    audit:
      on-failure: buffer-and-retry
```

---

# 16. Security Requirements

If full payload and headers are captured, the audit extension must consider:

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

The starter should provide extension points, not hardcode compliance policy.

Compliance-specific behavior belongs to the service owner or platform team.

---

# 17. Design Rules

## Default Safety

```text
audit disabled by default
no payload by default
no full headers by default
Noop sink by default
```

## Explicit Opt-In

```text
service owner must enable full capture explicitly
custom sink should be registered explicitly
```

## Separation of Concerns

```text
observability log is not audit log
metrics are not audit records
trace spans are not compliance storage
```

## Pluggability

```text
custom capture policy
custom sanitizer
custom sink
custom hasher
custom signer
custom failure behavior
```

## Stack Correctness

```text
MVC audit may use blocking infrastructure
WebFlux audit must use reactive infrastructure
```

---

# 18. Recommended Roadmap Placement

Add audit logging after base observability is stable.

Recommended order:

```text
1. message observability metadata logs
2. safe header logging
3. audit extension core
4. custom MessageAuditSink
5. custom capture policy
6. audit hashing/signature
7. audit outbox JDBC
8. audit outbox R2DBC
9. SIEM/log center sample implementation
```

---

# 19. Final Recommendation

Banking and compliance systems may require full body and header retention.

The framework should support that through an optional extension:

```text
MessageAuditSink
ReactiveMessageAuditSink
MessageAuditCapturePolicy
MessageAuditSanitizer
MessageAuditHasher
MessageAuditSigner
```

But the framework default must remain safe:

```text
metadata-only observability
no full payload
no full headers
audit disabled unless explicitly enabled
```

This gives normal systems safe defaults while allowing banking systems to plug in strict audit behavior.
