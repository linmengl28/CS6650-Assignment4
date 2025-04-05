# CS6650-Assignment3
# DynamoDB Design for Skier Ride Data Processing

## Database Selection Rationale

After evaluating several database options (Redis, MySQL/RDS, DynamoDB, MongoDB), we selected **DynamoDB** for the following reasons:

- **Write Throughput**: Superior performance for high-volume write operations (crucial for our message processing)
- **Scalability**: Ability to handle massive throughput without complex sharding strategies
- **GSI Support**: Built-in indexing for efficient query access patterns without write locks
- **Managed Service**: No operational overhead for scaling, replication, or failover
- **Integration**: Native AWS integration with our EC2-based consumer architecture

## Database Design Overview

### Table Structure
- **Primary Table**: `SkierRides`
  - Partition Key: `skierId` (Number)
  - Sort Key: `sortKey` (String) - format: "dayId#liftId#timestamp"
  - Attributes: resortId, dayId, liftId, time, vertical

### Global Secondary Indexes (GSIs)
- **resort-day-index**:
  - Partition Key: `resortId`
  - Sort Key: `dayId`
  - Projected Attributes: skierId
  - Purpose: Count unique skiers per resort/day

- **skier-day-index**:
  - Partition Key: `skierId`
  - Sort Key: `dayId`
  - Projected Attributes: vertical, liftId
  - Purpose: Analyze skier activity by day

## Deployment Topology

### Compute Resources
- **Consumer Application**: Java application running on EC2
  - Instance Type: t3.medium
  - Role: Consumes messages from RabbitMQ, processes data, writes to DynamoDB
  - Concurrency: 512 threads, 250 prefetch count per thread

### Database Provisioning
- **DynamoDB Table**: Provisioned capacity mode
  - Base Table: 10 RCU, 2000 WCU
  - GSIs: 2000 RCU, 2000 WCU each
  - Region: US-West-2 (Oregon)

### Message Queue
- **RabbitMQ**: Deployed on EC2
  - Instance Type: t2.micro
  - Configuration: Persistent queues, durable messages

## Performance Optimization

- **Batch Writing**: Implemented custom batching with 25 items per batch
- **Write Efficiency**: Combined flush interval of 100ms with maximum batch size
- **GSI Scaling**: Matched GSI write capacity to base table (2000 WCU)
- **HTTP Client Tuning**: MaxConcurrency=250, ConnectionTimeout=5s
