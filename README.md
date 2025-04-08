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
# SkierRides Table - DynamoDB Schema

## Overview

The `SkierRides` table stores information about individual lift rides taken by skiers at ski resorts. Each entry records who rode a lift, when and where they did it, and how much vertical distance they covered.

---

## Primary Table: `SkierRides`

### Key Schema

- **Partition Key:** `skierId` (Number)  
- **Sort Key:** `sortKey` (String)  
  - Format: `seasonId#dayId#liftId#time`  
  - Example: `"2022#34#7#157832"`  

### Attributes

| Attribute   | Type     | Description                                                  |
|-------------|----------|--------------------------------------------------------------|
| `skierId`   | Number   | Unique identifier for a skier                                |
| `sortKey`   | String   | Composite key in the format: `seasonId#dayId#liftId#time`    |
| `resortId`  | Number   | ID of the resort where the lift ride took place              |
| `seasonId`  | String   | Season identifier (e.g., `"2025"`)                           |
| `dayId`     | Number   | Day number within the ski season                             |
| `liftId`    | Number   | Identifier for the ski lift used                             |
| `time`      | Number   | Timestamp (or time of day) of the lift ride                  |
| `vertical`  | Number   | Vertical distance traveled in the ride (calculated as `liftId × 10`) |

---

## Global Secondary Indexes (GSIs)

### 1. `resort-day-index`

- **Purpose:** Query skiers who visited a specific resort on a given day.
- **Partition Key:** `resortId` (Number)
- **Sort Key:** `dayId` (Number)
- **Projected Attributes:** `skierId`, `seasonId`

### 2. `skier-day-index`

- **Purpose:** Query lift rides for a skier on a specific day.
- **Partition Key:** `skierId` (Number)
- **Sort Key:** `dayId` (Number)
- **Projected Attributes:** `vertical`, `liftId`, `resortId`, `seasonId`

---

## Sample Item

```json
{
  "skierId": 101,
  "sortKey": "2022#34#7#157832",
  "resortId": 55,
  "seasonId": "2022",
  "dayId": 34,
  "liftId": 7,
  "time": 157832,
  "vertical": 70
}


## Deployment Topology

### Compute Resources
- **Consumer Application**: Java application running on EC2
  - Instance Type: c3.large
  - Role: Consumes messages from RabbitMQ, processes data, writes to DynamoDB
  - Concurrency: 256 threads, 50 prefetch count per thread

### Database Provisioning
- **DynamoDB Table**: Provisioned capacity mode
  - Base Table: 50 RCU, 5000 WCU
  - GSIs: 20 RCU, 2000 WCU each
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
