# Audit Extension Example

Audit logging is opt-in compliance capture. Observability log != audit log.

Use audit when you need controlled payload/header capture, tamper-evident hashes/signatures, or a custom compliance sink.

## Modules

MVC audit:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-audit-mvc</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

WebFlux audit:

```xml
<dependency>
  <groupId>io.github.huynhngochuyhoang</groupId>
  <artifactId>reliable-message-audit-webflux</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
```

## Default Safe Config

```yaml
message:
  reliability:
    audit:
      enabled: false
      include-headers: false
      include-payload: false
      include-raw-body: false
      hash-enabled: false
      on-failure: continue-and-log
```

## Enable Metadata And Header Capture

```yaml
message:
  reliability:
    audit:
      enabled: true
      include-headers: true
      include-payload: false
      include-raw-body: false
      hash-enabled: true
      on-failure: continue-and-log
```

Do not enable full payload capture without retention, encryption and access-control policy.

## Custom MVC Audit Sink

```java
@Component
class ComplianceAuditSink implements MessageAuditSink {

    @Override
    public void record(MessageAuditRecord record) {
        auditRepository.save(record.auditId(), record.eventName(), record.status().name());
    }
}
```

## Custom WebFlux Audit Sink

```java
@Component
class ReactiveComplianceAuditSink implements ReactiveMessageAuditSink {

    @Override
    public Mono<Void> record(MessageAuditRecord record) {
        return auditRepository.save(record.auditId(), record.eventName(), record.status().name());
    }
}
```

The reactive sink must not call blocking clients unless that blocking boundary is explicitly isolated outside the reactive pipeline.

## Capture Policy Example

```java
@Component
class PaymentAuditCapturePolicy implements MessageAuditCapturePolicy {

    @Override
    public boolean shouldAudit(MessageAuditContext context) {
        return context.eventName() != null && context.eventName().startsWith("payment.");
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
        return false;
    }
}
```

## Sanitizer Example

```java
@Component
class PaymentAuditSanitizer implements MessageAuditSanitizer {

    @Override
    public Map<String, Object> sanitizeHeaders(Map<String, Object> headers, MessageAuditContext context) {
        Map<String, Object> sanitized = new LinkedHashMap<>(headers);
        sanitized.put("authorization", "[REDACTED]");
        sanitized.put("x-api-key", "[REDACTED]");
        return Map.copyOf(sanitized);
    }

    @Override
    public Object sanitizePayload(Object payload, MessageAuditContext context) {
        return payload;
    }

    @Override
    public byte[] sanitizeRawBody(byte[] rawBody, MessageAuditContext context) {
        return rawBody == null ? null : rawBody.clone();
    }
}
```

## Durable Audit Buffer Note

Durable audit buffer/audit outbox modules are part of the audit design direction, but they are not present as runtime modules in the current repo. Use a custom `MessageAuditSink` or `ReactiveMessageAuditSink` if your platform already has a durable audit pipeline.

## Do Not Do This

- Do not treat observability logs as audit records.
- Do not enable full payload capture without a retention and security policy.
- Do not call blocking audit clients from WebFlux reactive sinks.
- Do not let audit failures silently change event messaging or RPC semantics.
