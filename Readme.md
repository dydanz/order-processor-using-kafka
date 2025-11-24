# High-Throughput Order Processing System with Kafka

An order processing system built with Apache Kafka, Go, and Docker, designed (hopefully, fool proof enough) to handle 1 million transactions per second.

## Quick Start

### Running the System

```bash
# Start all services
docker-compose up -d

# Check service health
docker-compose ps

# View producer logs
docker-compose logs -f producer

# View consumer logs
docker-compose logs -f consumer

# Stop all services
docker-compose down
```

### Testing the API

**Single Order:**
```bash
curl -X POST http://localhost:8080/order \
  -H "Content-Type: application/json" \
  -d '{
    "customer_id": "CUST001",
    "product_id": "PROD123",
    "quantity": 5,
    "amount": 99.99
  }'
```

**Batch Orders (Load Testing):**
```bash
curl -X POST http://localhost:8080/orders/batch \
  -H "Content-Type: application/json" \
  -d '[
    {"customer_id": "CUST001", "product_id": "PROD123", "quantity": 5, "amount": 99.99},
    {"customer_id": "CUST002", "product_id": "PROD456", "quantity": 3, "amount": 149.99}
  ]'
```

**Health Check:**
```bash
curl http://localhost:8080/health
```

### Accessing Kafka UI

Open http://localhost:8090 in your browser to view:
- Topics and partitions
- Consumer groups and lag
- Message browser
- Broker health

## Architecture

### Data Flow
```
Customer → Producer API (Gin) → Kafka Cluster → Consumer Workers → Processing Pipeline
           (Port 8080)         (orders-placed)   (Goroutines)
```

### Key Components
- **Producer**: Go/Gin REST API with async Kafka producer
- **Kafka**: Single broker setup (scale to 20+ for 1M TPS)
- **Consumer**: Multi-threaded consumer with worker pool
- **Monitoring**: Kafka UI for real-time observability

## Scaling for Production

### Horizontal Scaling

**Scale Consumers:**
```bash
docker-compose up -d --scale consumer=10
```

**Scale Kafka Brokers:**
Update `docker-compose.yml` to add broker-2, broker-3, etc. with unique `KAFKA_BROKER_ID`.

### Configuration Tuning

**For Higher Throughput:**
- Increase Kafka partitions: 100-500 per topic
- Add more consumer instances: 1 per 2-3 partitions
- Increase producer batch size: 2000-5000 messages
- Use multiple producer instances behind a load balancer

**Recommended Production Setup:**
- 20 Kafka brokers (16 cores, 64GB RAM each)
- 250+ consumer instances
- 10+ producer instances (load balanced)
- Multi-region replication with MirrorMaker2

## Performance Optimization

### Producer Optimizations
- Async mode with batching (1000 msgs/batch)
- Snappy compression (3x reduction)
- Connection pooling
- `acks=1` for balanced durability

### Consumer Optimizations
- Worker pool (10 goroutines per instance)
- Batch processing (100 msgs/batch)
- Large fetch sizes (10MB)
- Manual offset commits

### Go-Specific
- `GOMAXPROCS` set to CPU count
- Buffered channels (cap=1000)
- Zero-copy `[]byte` handling
- Context-based cancellation

## Monitoring

### Key Metrics
- **Consumer Lag**: < 100K messages (alert threshold)
- **Network I/O**: < 70% utilization
- **Under-replicated Partitions**: = 0
- **Producer Throughput**: msgs/sec
- **Request Latency**: p99 < 100ms

### Alerting
Configure alerts using Prometheus + Grafana:
- Consumer lag > 100K
- Network bandwidth > 70%
- Error rate > 0.1%

## Disaster Recovery

### Fault Tolerance
- Replication Factor: 3 (tolerates 2 broker failures)
- Min In-Sync Replicas: 2
- Rack awareness across availability zones

### Multi-Region Setup
- Primary DC: 20 brokers (active)
- Secondary DC: 20 brokers (standby)
- MirrorMaker2 replication (< 30s lag)
- DNS-based failover (5 min RTO)


## Design Decisions

### Top 5 Key Design Decisions

1. **Partitioning by `customer_id`**: Ensures order-per-customer consistency by guaranteeing all orders from the same customer land on the same partition, preventing race conditions.

2. **Async Producer with Batching**: Using async mode with 1000-message batches and 10ms linger time achieves 10x throughput improvement while maintaining sub-100ms latency.

3. **Worker Pool Pattern in Consumer**: 10 goroutines per consumer instance enable parallel processing of messages while maintaining offset commit control, maximizing CPU utilization.

4. **Snappy Compression**: Provides 2-3x compression ratio with minimal CPU overhead, significantly reducing network I/O—the primary bottleneck at 1M TPS.

5. **Manual Offset Commits with Batching**: Batch-committing offsets every 100 messages reduces commit overhead by 99% while maintaining at-least-once delivery guarantees.
