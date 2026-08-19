# Event processing reliability

## Delivery model

The API waits for a RabbitMQ publisher confirmation before reporting an event as accepted. RabbitMQ messages are persistent and carry the domain `event_id` as their AMQP `message_id`. A publisher confirmation means the broker accepted responsibility for the publication; it does not mean a consumer processed it.

After a message is routed to the main queue, the consumer side provides **at-least-once delivery**, not exactly-once processing. RabbitMQ uses manual acknowledgements, and the Worker acknowledges a delivery only after ClickHouse reports a successful batch insert. If the process or connection closes while a delivery is unacknowledged, RabbitMQ makes it available for redelivery.

An insert can commit in ClickHouse while its success response or the subsequent RabbitMQ acknowledgement is lost. Retrying or redelivering that event can therefore create a duplicate. RabbitMQ explicitly requires consumers using manual acknowledgements to be prepared for redelivery; an exactly-once claim would be incorrect for this architecture.

## Topology

The application declares two durable direct exchanges and two durable classic queues. Names are derived from the configured main topology:

| Purpose | Default name | Binding key |
| --- | --- | --- |
| Main exchange | `app.events` | `app.events` |
| Main queue | `app.events` | `app.events` |
| Dead-letter exchange | `app.events.dead-letter` | `app.events.dead-letter` |
| Dead-letter queue | `app.events.dead-letter` | `app.events.dead-letter` |

The main queue sets `x-dead-letter-exchange` and `x-dead-letter-routing-key`. A delivery rejected with `requeue=false` is routed by RabbitMQ to the dead-letter queue and receives RabbitMQ death headers such as `x-first-death-reason`.

RabbitMQ checks queue argument equivalence when a durable queue is redeclared. An environment upgrading from an older increment must first drain and delete the old `app.events` queue, then restart the API or Worker to recreate it. For larger deployments, RabbitMQ recommends applying dead-letter configuration through an operator-managed policy because policies can be changed without deleting the queue. This project keeps the topology application-owned so isolated local and Testcontainers environments are self-contained.

References: [RabbitMQ publisher confirms and consumer acknowledgements](https://www.rabbitmq.com/docs/confirms), [dead-letter exchanges](https://www.rabbitmq.com/docs/dlx), [queue declaration and optional arguments](https://www.rabbitmq.com/docs/queues).

## Failure handling

The handling rules are deliberately bounded:

1. Malformed JSON or a structurally invalid event is a poison message. The consumer rejects it without requeue, RabbitMQ routes it to the dead-letter queue, and the consumer continues with the next delivery.
2. A ClickHouse batch failure is treated as potentially transient. The Worker makes at most three insert attempts, waiting 100 ms and then 200 ms.
3. If all attempts fail, every delivery in that batch is rejected without requeue and routed to the dead-letter queue. The Worker returns the persistence error so process supervision can surface and restart the failed worker.
4. An acknowledgement or rejection error is returned. Closing the unhealthy channel leaves any unsettled deliveries eligible for RabbitMQ redelivery.

Immediate broker requeue is not used for application failures because multiple consumers can otherwise create a tight requeue/redelivery loop. The retry delay stays inside the Worker and is capped. See [RabbitMQ acknowledgement and requeue guidance](https://www.rabbitmq.com/docs/confirms#automatic-requeueing).

## Duplicate strategy and limits

`event_id` is the stable identity of an accepted event. Before each ClickHouse insert, the Worker keeps the first occurrence of every `event_id` in that batch. All matching RabbitMQ deliveries are acknowledged after the one unique row is stored. This handles duplicates that arrive together at negligible operational cost.

The deduplication map is deliberately batch-local. It does not survive a Worker restart and cannot prevent duplicates delivered in later batches or produced by an ambiguous ClickHouse insert result. The current `MergeTree` table also does not enforce uniqueness. Consumers of analytical results must therefore treat `event_id` as the reconciliation key, and operators should retain dead-letter messages until they have been inspected or replayed safely.

A durable cross-batch strategy would require additional state or a ClickHouse table/query design change and failure coordination between that state, ClickHouse, and RabbitMQ. That complexity is not justified by the current increment and would still need explicit semantics before any exactly-once claim.

## Verification

Unit tests cover bounded retry, exhausted-retry rejection, poison-message handling, and within-batch `event_id` deduplication. The Testcontainers suite additionally proves that RabbitMQ routes poison messages to the declared dead-letter queue and redelivers an event when its first consumer connection closes without acknowledging it.
