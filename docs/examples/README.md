# Reliable Message Examples

Copy and adapt these examples for the current Reliable Message modules.

| Example | Use when |
| --- | --- |
| [MVC RabbitMQ](mvc-rabbit.md) | Blocking Spring MVC service publishes and consumes RabbitMQ events. |
| [MVC Kafka](mvc-kafka.md) | Blocking Spring MVC service publishes and consumes Kafka events. |
| [WebFlux Kafka](webflux-kafka.md) | Reactive WebFlux service uses Kafka with reactive handlers. |
| [Rabbit WebFlux blocking bridge](rabbit-webflux-bridge.md) | WebFlux service must use RabbitMQ through Spring AMQP. |
| [Rabbit RPC WebFlux](rabbit-rpc-webflux.md) | WebFlux-friendly Rabbit request/reply bridge using `AsyncRabbitTemplate`. |
| [Audit extension](audit-extension.md) | Optional compliance audit capture for publish/consume boundaries. |

Keep these boundaries clear:

- `RabbitTemplate` is for event messaging.
- `AsyncRabbitTemplate` is for Rabbit RPC only.
- RPC does not use outbox by default.
- Rabbit WebFlux bridge is blocking bridge / hybrid mode, not fully reactive RabbitMQ.
- Audit is opt-in; observability logs are not audit logs.
