# Reliable Message Architecture Flow

Reliable Message provides reliability and observability patterns for Spring Boot message-driven systems. It targets effectively-once workflows through outbox, idempotency, retry/DLQ conventions, metrics and tracing. It is not an exactly-once framework.

## Overall Architecture

```mermaid
flowchart TB
    Core[Shared core<br/>ReliableMessage<br/>PublishOptions<br/>serialization<br/>headers]
    Obs[Observability<br/>metrics<br/>tracing<br/>safe logs]
    Audit[Audit extension<br/>optional capture<br/>sanitize/hash/sign<br/>sink or buffer]
    RPC[RPC extension<br/>ReliableRpcClient<br/>ReactiveRabbitRpcClient]

    MVC[MVC stack<br/>blocking runtime]
    WebFlux[WebFlux stack<br/>reactive runtime]

    MVCStore[JDBC outbox<br/>JDBC/Redis idempotency]
    WebFluxStore[R2DBC outbox<br/>R2DBC/Reactive Redis idempotency]

    RabbitMvc[Rabbit MVC adapter<br/>RabbitTemplate<br/>listener containers]
    KafkaMvc[Kafka MVC adapter]
    KafkaWebFlux[Kafka WebFlux adapter<br/>Reactor Kafka]
    RabbitBridge[Rabbit WebFlux blocking bridge<br/>RabbitTemplate<br/>bridge executor]
    RabbitRpc[Rabbit RPC bridge<br/>AsyncRabbitTemplate<br/>request/reply]

    Core --> MVC
    Core --> WebFlux
    Core --> Obs
    Core --> Audit
    Core --> RPC

    MVC --> MVCStore
    MVC --> RabbitMvc
    MVC --> KafkaMvc

    WebFlux --> WebFluxStore
    WebFlux --> KafkaWebFlux
    WebFlux --> RabbitBridge

    RPC --> RabbitRpc
    Obs --> RabbitBridge
    Audit --> RabbitBridge
```

## Runtime Stack Choices

| Application style | Recommended path | Notes |
| --- | --- | --- |
| Spring MVC + RabbitMQ | `reliable-message-mvc-starter` + Rabbit MVC modules | Blocking, production-oriented path. |
| Spring MVC + Kafka | MVC starter + Kafka MVC modules | Blocking app runtime with Kafka transport. |
| Spring WebFlux + Kafka | WebFlux starter + Kafka WebFlux modules | Reactive messaging path. |
| Spring WebFlux + RabbitMQ | `reliable-message-rabbit-webflux-bridge` | Blocking bridge, hybrid mode, migration support. |
| Rabbit request/reply RPC | `reliable-message-rpc-rabbit-webflux-bridge` | Uses `AsyncRabbitTemplate`, not event outbox. |
| Audit logging | Audit extension modules | Opt-in compliance capture. |

## MVC Event Outbox Flow

```mermaid
sequenceDiagram
    participant HTTP as HTTP request
    participant Service as MVC service
    participant DB as DB transaction
    participant Business as Business data
    participant Outbox as Outbox row
    participant Flush as Outbox flush job
    participant Broker as Rabbit/Kafka publish
    participant Consumer as Consumer
    participant Idem as Idempotency store
    participant Ack as Ack/commit

    HTTP->>Service: call business operation
    Service->>DB: begin transaction
    DB->>Business: save business data
    DB->>Outbox: save message outbox row
    DB-->>Service: commit
    Flush->>Outbox: load pending rows
    Flush->>Broker: publish event
    Broker->>Consumer: deliver message
    Consumer->>Idem: tryStart(idempotencyKey)
    Idem-->>Consumer: new or duplicate state
    Consumer->>Consumer: run handler when new
    Consumer->>Idem: markSuccess after handler success
    Consumer->>Ack: ack Rabbit or commit Kafka offset
```

Key rule: event outbox belongs to event messaging. It is not the default model for RPC.

## WebFlux Kafka Flow

```mermaid
sequenceDiagram
    participant Handler as WebFlux handler
    participant Tx as R2DBC transaction
    participant Data as Business data
    participant Outbox as Reactive outbox
    participant Publisher as Kafka publisher
    participant Kafka as Kafka topic
    participant Consumer as Reactive consumer
    participant Idem as Reactive idempotency
    participant Offset as Offset commit

    Handler->>Tx: TransactionalOperator
    Tx->>Data: save with R2DBC
    Tx->>Outbox: save outbox row
    Tx-->>Handler: commit reactive transaction
    Publisher->>Outbox: read pending rows
    Publisher->>Kafka: publish record
    Kafka->>Consumer: receive record
    Consumer->>Idem: tryStart(idempotencyKey)
    Idem-->>Consumer: new or duplicate state
    Consumer->>Consumer: invoke handler Mono
    Consumer->>Idem: markSuccess after Mono success
    Consumer->>Offset: commit offset after success
```

Rules:

- Use R2DBC and Reactive Redis in WebFlux flows.
- Do not use JDBC or blocking Redis inside reactive pipelines.
- Keep concurrency and prefetch bounded.

R2DBC outbox schema DDL is configurable under `message.reliability.outbox.schema`. Column type resolution uses explicit user config first, dialect defaults second, and generic fallback last. PostgreSQL `json/jsonb` uses dialect-aware binding. `payload-storage=binary` is planned and currently fails fast until binary payload codec and `payload_bytes` read/write support are implemented. Auto-configuration does not create the `message_outbox` table; provision it with a migration before enabling flushing. Claiming uses a non-PostgreSQL select-ID plus conditional-update fallback with `LIMIT` pagination and a PostgreSQL atomic `FOR UPDATE SKIP LOCKED` plus `UPDATE ... RETURNING` strategy without window functions in the locked query. Oracle and SQL Server are not supported by the current `LIMIT`-based fallback.

## Rabbit WebFlux Blocking Bridge Publish Flow

```mermaid
flowchart TD
    A[ReactiveReliablePublisher.publish] --> B[Serialize ReliableMessage]
    B --> C[Event-loop safety check]
    C --> D[Acquire RabbitBridgeConcurrencyGuard permit]
    D --> E{Bridge executor mode}
    E --> F[Platform thread executor]
    E --> G[Virtual-thread executor]
    F --> H[RabbitTemplate.convertAndSend]
    G --> H
    H --> I[Record success or failure metrics]
    I --> J[Release permit]
    J --> K[Mono completes or errors]

    H:::blocking
    classDef blocking fill:#ffe6e6,stroke:#cc0000,color:#111
```

RabbitTemplate is blocking. `RabbitTemplate.convertAndSend` is blocking. The bridge offloads it to an explicit bridge executor. This is a blocking bridge, not fully reactive RabbitMQ, and not non-blocking broker I/O.

## Rabbit WebFlux Blocking Bridge Consume Flow

```mermaid
flowchart TD
    A[Spring AMQP listener thread] --> B[Deserialize ReliableMessage]
    B --> C[ReactiveIdempotencyStore.tryStart]
    C --> D{Result}
    D -->|Duplicate SUCCESS| E[Ack and skip handler]
    D -->|Duplicate PROCESSING or FAILED| F[Failure path]
    D -->|New message| G[Invoke @ReactiveReliableListener Mono<Void>]
    G --> H[Wait at bridge boundary]
    H --> I{Mono completed successfully?}
    I -->|Yes| J[markSuccess]
    J --> K{markSuccess succeeded?}
    K -->|Yes| L[Ack after success]
    K -->|No| F
    I -->|No| M[markFailed where possible]
    M --> F
    F --> N[Nack / retry / DLQ hook]
```

Current listener mode is Strategy A only. No Strategy B async ack coordination:

- The listener waits at the bridge boundary until the handler `Mono` completes.
- Ack happens only after handler `Mono` and `markSuccess` complete successfully.
- Strategy B async ack coordination is not implemented.

## Rabbit RPC WebFlux Bridge Flow

```mermaid
sequenceDiagram
    participant Caller as WebFlux caller
    participant Client as ReactiveRabbitRpcClient
    participant Executor as RPC bridge executor
    participant AsyncRabbit as AsyncRabbitTemplate
    participant Future as CompletableFuture
    participant Mono as Mono boundary
    participant Resilience as timeout/retry/bounded bulkhead
    participant Result as response or error

    Caller->>Client: request(route, payload, options)
    Client->>Executor: offload request creation
    Executor->>AsyncRabbit: request/reply
    AsyncRabbit-->>Future: future response
    Client->>Mono: bridge future to Mono
    Mono->>Resilience: apply RPC resilience
    Resilience-->>Result: response or caller-visible error
```

AsyncRabbitTemplate is RPC only. RPC does not use outbox by default. RPC uses request/response semantics. Timeout and cancellation are caller-visible, but may not cancel broker-side or remote work. The implemented RPC bridge supports raw replies, explicit response envelopes, generic response types, bounded retry, a bounded fail-fast bulkhead, platform or virtual-thread executor modes, and RPC-specific metrics. Rabbit RPC circuit-breaker integration is not implemented.

## Audit Extension Flow

```mermaid
flowchart TD
    A[Publish or consume boundary] --> B[Observation<br/>metrics/tracing/safe logs]
    B --> C{Audit enabled?}
    C -->|No| D[Continue normal flow]
    C -->|Yes| E[Audit capture policy]
    E --> F[Sanitizer]
    F --> G[Hash/signature]
    G --> H[Audit sink]
    H --> I{Durable audit buffer configured?}
    I -->|No| J[Sink completes]
    I -->|Yes| K[Audit durable buffer]
```

Audit is opt-in. observability log != audit log. Observability logs are not audit logs, and metrics are not compliance records.

## Limits To Keep Visible

- Rabbit WebFlux bridge is hybrid mode over blocking Spring AMQP.
- Virtual threads reduce blocking cost, but they are not reactive and do not provide unlimited concurrency.
- Blocking Rabbit work must not run on Netty event-loop threads.
- Event retry/DLQ is separate from RPC timeout, retry, and bulkhead behavior. Rabbit RPC circuit-breaker integration is not implemented.
- Effectively-once depends on idempotency, outbox where configured, and correct ack/commit ordering.
