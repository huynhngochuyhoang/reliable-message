# Audit Extension Documentation

This document describes the completed Milestone 13 audit extension baseline.

## Modules

- `reliable-message-audit-core`
- `reliable-message-audit-mvc`
- `reliable-message-audit-webflux`

Audit logging is optional and separate from normal observability logging.

## Safe Defaults

Default behavior remains safe:

```text
audit disabled
no full payload capture
no full header capture
no raw body capture
noop sink
```

The framework does not persist full bodies or headers unless a service owner explicitly opts in.

## Core Contracts

`reliable-message-audit-core` provides:

- `MessageAuditRecord`
- `MessageAuditSink`
- `ReactiveMessageAuditSink`
- `MessageAuditCapturePolicy`
- `MessageAuditSanitizer`
- `MessageAuditHasher`
- `MessageAuditSigner`
- no-op sinks and signer
- disabled capture policy
- default sanitizer
- SHA-256 hasher

Sensitive headers are masked by default:

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

## MVC Support

`reliable-message-audit-mvc` provides:

- `MessageAuditRecorder`
- safe auto-configuration
- configurable capture flags
- audit metrics

Configuration:

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

MVC audit may use blocking sinks, but latency is visible through metrics.

## WebFlux Support

`reliable-message-audit-webflux` provides:

- `ReactiveMessageAuditRecorder`
- `ReactiveMessageAuditSink`
- safe auto-configuration
- reactive audit metrics

Reactive audit sinks must not call blocking clients, JDBC, blocking Redis, or `block()`.

## Metrics

MVC metrics:

```text
message_audit_records_total
message_audit_failed_total
message_audit_duration
```

WebFlux metrics:

```text
message_audit_reactive_records_total
message_audit_reactive_failed_total
message_audit_reactive_duration
```

## Current Limitations

- Audit outbox JDBC/R2DBC modules are not included in this baseline.
- SIEM/log-center implementations are service-owner extensions.
- Signature behavior is pluggable; the default signer returns no signature.
