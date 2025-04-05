package cs6650;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.AMQP;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SkierConsumer {
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final String RABBITMQ_HOST = "172.31.20.160";
    private static final int NUM_THREADS = 512;
    private static final int PREFETCH_COUNT = 250;

    // DynamoDB table name
    private static final String SKIER_RIDES_TABLE = "SkierRides";

    // AWS Region - Set to Oregon (US-WEST-2)
    private static final Region AWS_REGION = Region.US_WEST_2;

    // DynamoDB clients
    private static DynamoDbAsyncClient dynamoDbAsyncClient;
    private static DynamoDbClient dynamoDbClient;

    // Batch writer for DynamoDB
    private static final BatchWriter batchWriter = new BatchWriter();

    // Counters for statistics
    private static final AtomicInteger messagesProcessed = new AtomicInteger(0);
    private static final AtomicInteger dbWriteErrors = new AtomicInteger(0);
    private static final AtomicInteger processingErrors = new AtomicInteger(0);
    private static final AtomicInteger lastProcessedCount = new AtomicInteger(0);

    // Flag to control application lifecycle
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    public static void main(String[] args) {
        System.out.println("Starting SkierConsumer with DynamoDB persistence...");

        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered - gracefully terminating...");
            isRunning.set(false);
        }));

        // Initialize DynamoDB clients
        initDynamoDb();

        // Ensure table exists (or create it)
        createTablesIfNotExist();

        // Start the batch writer
        batchWriter.start();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(5672);
        factory.setUsername("myuser");
        factory.setPassword("mypassword");

        // Configure RabbitMQ connection factory for better performance
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(1000);
        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(5000);
        factory.setRequestedChannelMax(0); // Unlimited channels

        // Create a thread pool for consumers
        ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
        ScheduledExecutorService statsExecutor = null;

        try {
            // Create a single shared connection
            Connection connection = factory.newConnection();
            System.out.println("Connected to RabbitMQ successfully");

            // Test queue connection and check message count
            try (Channel testChannel = connection.createChannel()) {
                AMQP.Queue.DeclareOk queueInfo = testChannel.queueDeclare(QUEUE_NAME, true, false, false, null);
                System.out.println("Queue '" + QUEUE_NAME + "' exists and has " + queueInfo.getMessageCount() + " messages");
            } catch (Exception e) {
                System.err.println("Error testing RabbitMQ queue: " + e.getMessage());
            }

            // Start statistics reporter thread
            statsExecutor = startStatsReporter();

            // Create multiple consumer threads
            CountDownLatch threadCompletionLatch = new CountDownLatch(1); // Will never count down in normal operation

            for (int i = 0; i < NUM_THREADS; i++) {
                final int threadId = i;
                executorService.submit(() -> {
                    try {
                        consumeMessages(connection, threadId);
                    } catch (Exception e) {
                        // Minimal error logging, just report the class name
                        System.err.println("Thread " + threadId + " failed: " + e.getClass().getName());
                    }
                });
            }

            System.out.println("All consumer threads started. Running indefinitely until termination signal...");

            // Wait indefinitely - the application will run until Ctrl+C is pressed
            threadCompletionLatch.await();

        } catch (Exception e) {
            System.err.println("Failed to start consumer: " + e.getMessage());
        } finally {
            System.out.println("Main thread reaching finally block - program is shutting down");

            // Cancel stats reporter
            if (statsExecutor != null) {
                statsExecutor.shutdown();
            }

            // Graceful shutdown
            isRunning.set(false);
            executorService.shutdown();

            try {
                if (!executorService.awaitTermination(10, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException ie) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Allow the batch writer to finish processing
            System.out.println("Shutting down - waiting for batch writer to complete...");
            batchWriter.shutdown();

            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            // Close DynamoDB clients last
            if (dynamoDbAsyncClient != null) {
                dynamoDbAsyncClient.close();
            }
            if (dynamoDbClient != null) {
                dynamoDbClient.close();
            }
        }
    }

    private static void initDynamoDb() {
        try {
            // Configure HTTP client with optimized settings
            NettyNioAsyncHttpClient.Builder asyncHttpClientBuilder = NettyNioAsyncHttpClient.builder()
                    .connectionTimeout(Duration.ofSeconds(5))
                    .maxConcurrency(250)
                    .connectionAcquisitionTimeout(Duration.ofSeconds(10))
                    .connectionTimeToLive(Duration.ofMinutes(10));

            ApacheHttpClient.Builder syncHttpClientBuilder = ApacheHttpClient.builder()
                    .connectionTimeout(Duration.ofSeconds(5))
                    .socketTimeout(Duration.ofSeconds(30))
                    .connectionTimeToLive(Duration.ofMinutes(10))
                    .maxConnections(250);

            // When using IAM Role attached to EC2, no explicit credentials are needed
            dynamoDbAsyncClient = DynamoDbAsyncClient.builder()
                    .region(AWS_REGION)
                    .httpClientBuilder(asyncHttpClientBuilder)
                    .build();

            dynamoDbClient = DynamoDbClient.builder()
                    .region(AWS_REGION)
                    .httpClientBuilder(syncHttpClientBuilder)
                    .build();

            System.out.println("Successfully initialized DynamoDB clients");

        } catch (Exception e) {
            System.err.println("Failed to initialize DynamoDB clients: " + e.getMessage());
            throw new RuntimeException("DynamoDB initialization failed", e);
        }
    }

    private static void createTablesIfNotExist() {
        try {
            // Check if SkierRides table exists
            try {
                dynamoDbClient.describeTable(DescribeTableRequest.builder()
                        .tableName(SKIER_RIDES_TABLE)
                        .build());
                System.out.println(SKIER_RIDES_TABLE + " table already exists");
            } catch (ResourceNotFoundException e) {
                // Create SkierRides table with provisioned capacity and GSIs
                CreateTableRequest createSkierRidesTable = CreateTableRequest.builder()
                        .tableName(SKIER_RIDES_TABLE)
                        .keySchema(
                                KeySchemaElement.builder().attributeName("skierId").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("sortKey").keyType(KeyType.RANGE).build())
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName("skierId").attributeType(ScalarAttributeType.N).build(),
                                AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("resortId").attributeType(ScalarAttributeType.N).build(),
                                AttributeDefinition.builder().attributeName("dayId").attributeType(ScalarAttributeType.N).build())
                        .billingMode(BillingMode.PROVISIONED)
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(10L)
                                .writeCapacityUnits(2000L)
                                .build())
                        // Add GSI for resort-day queries
                        .globalSecondaryIndexes(
                                GlobalSecondaryIndex.builder()
                                        .indexName("resort-day-index")
                                        .keySchema(
                                                KeySchemaElement.builder().attributeName("resortId").keyType(KeyType.HASH).build(),
                                                KeySchemaElement.builder().attributeName("dayId").keyType(KeyType.RANGE).build())
                                        .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                                .nonKeyAttributes("skierId").build())
                                        .provisionedThroughput(ProvisionedThroughput.builder()
                                                .readCapacityUnits(20L)
                                                .writeCapacityUnits(2000L)
                                                .build())
                                        .build(),
                                GlobalSecondaryIndex.builder()
                                        .indexName("skier-day-index")
                                        .keySchema(
                                                KeySchemaElement.builder().attributeName("skierId").keyType(KeyType.HASH).build(),
                                                KeySchemaElement.builder().attributeName("dayId").keyType(KeyType.RANGE).build())
                                        .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                                .nonKeyAttributes("vertical", "liftId").build())
                                        .provisionedThroughput(ProvisionedThroughput.builder()
                                                .readCapacityUnits(20L)
                                                .writeCapacityUnits(2000L)
                                                .build())
                                        .build())
                        .build();

                dynamoDbClient.createTable(createSkierRidesTable);
                System.out.println("Created " + SKIER_RIDES_TABLE + " table with provisioned capacity and GSIs");

                // Wait for the table to be active
                boolean tableActive = false;
                while (!tableActive) {
                    DescribeTableResponse response = dynamoDbClient.describeTable(
                            DescribeTableRequest.builder().tableName(SKIER_RIDES_TABLE).build());
                    tableActive = response.table().tableStatus().equals(TableStatus.ACTIVE);
                    if (!tableActive) {
                        System.out.println("Waiting for " + SKIER_RIDES_TABLE + " table to be active...");
                        Thread.sleep(5000);
                    }
                }
            }

            System.out.println("DynamoDB tables are ready");

        } catch (Exception e) {
            System.err.println("Error creating DynamoDB tables: " + e.getMessage());
            throw new RuntimeException("Failed to initialize database tables", e);
        }
    }

    private static void consumeMessages(Connection connection, int threadId) throws IOException {
        Gson gson = new Gson();

        // Create a channel for each consumer thread
        Channel channel = connection.createChannel();

        // Declare the queue (to ensure it exists)
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);

        // Set prefetch count - how many messages the server will deliver before requiring acknowledgements
        channel.basicQos(PREFETCH_COUNT);

        // Define message handling callback
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (!isRunning.get()) {
                return;
            }

            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
            try {
                // Parse the message
                LiftRideEvent liftRide = gson.fromJson(message, LiftRideEvent.class);

                // Process the lift ride data
                processLiftRide(liftRide);

                // Acknowledge the message was processed
                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);

                // Increment message counter
                messagesProcessed.incrementAndGet();

            } catch (Exception e) {
                processingErrors.incrementAndGet();
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
            }
        };

        // Start consuming messages (autoAck = false for manual acknowledgments)
        String consumerTag = channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag1 -> { });

        // Keep thread alive until shutdown is requested
        while (isRunning.get()) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }

        // Cancel consumer when shutting down
        try {
            channel.basicCancel(consumerTag);
            channel.close();
        } catch (Exception e) {
            // Minimal error logging
        }
    }

    private static void processLiftRide(LiftRideEvent liftRide) {
        try {
            // Write to DynamoDB
            writeToDynamoDB(liftRide);
        } catch (Exception e) {
            dbWriteErrors.incrementAndGet();
            throw e; // Rethrow to be handled by the message consumer
        }
    }

    private static void writeToDynamoDB(LiftRideEvent liftRide) {
        try {
            // Compute vertical as liftId * 10
            int vertical = liftRide.getLiftId() * 10;

            // Create sortKey in format "dayId#liftId#timestamp" for efficient querying
            String sortKey = liftRide.getDayId() + "#" + liftRide.getLiftId() + "#" + liftRide.getTime();

            // Create a record for the SkierRides table
            Map<String, AttributeValue> skierRideItem = new HashMap<>();
            skierRideItem.put("skierId", AttributeValue.builder().n(String.valueOf(liftRide.getSkierId())).build());
            skierRideItem.put("sortKey", AttributeValue.builder().s(sortKey).build());
            skierRideItem.put("resortId", AttributeValue.builder().n(String.valueOf(liftRide.getResortId())).build());
            skierRideItem.put("dayId", AttributeValue.builder().n(String.valueOf(liftRide.getDayId())).build());
            skierRideItem.put("liftId", AttributeValue.builder().n(String.valueOf(liftRide.getLiftId())).build());
            skierRideItem.put("time", AttributeValue.builder().n(String.valueOf(liftRide.getTime())).build());
            skierRideItem.put("vertical", AttributeValue.builder().n(String.valueOf(vertical)).build());

            // Add to batch writer without excessive logging
            batchWriter.addItem(SKIER_RIDES_TABLE, skierRideItem);

        } catch (Exception e) {
            dbWriteErrors.incrementAndGet();
            throw e;
        }
    }

    /**
     * BatchWriter class for efficient batch writing to DynamoDB
     * Uses a scheduled executor to flush batches periodically
     */
    private static class BatchWriter {
        private static final int MAX_BATCH_SIZE = 25; // DynamoDB's max limit is 25 items per batch
        private static final int FLUSH_INTERVAL_MS = 20; // Reduced from 50ms to 20ms
        private static final int MAX_RETRY_ATTEMPTS = 2; // Reduced from 3 to 2

        private final Map<String, List<Map<String, AttributeValue>>> batchItems = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
        private final AtomicInteger pendingOperations = new AtomicInteger(0);

        public void start() {
            scheduler.scheduleAtFixedRate(this::flushAll, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);
        }

        public void shutdown() {
            scheduler.shutdown();
            flushAll();

            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }

                // Wait for pending operations to complete
                int attempts = 0;
                while (pendingOperations.get() > 0 && attempts < 10) {
                    Thread.sleep(1000);
                    attempts++;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void addItem(String tableName, Map<String, AttributeValue> item) {
            List<Map<String, AttributeValue>> items = batchItems.computeIfAbsent(tableName, k -> new ArrayList<>());

            synchronized(items) {
                items.add(item);

                // If batch size reaches the limit, flush this table's batch
                if (items.size() >= MAX_BATCH_SIZE) {
                    flush(tableName);
                }
            }
        }

        public void flushAll() {
            for (String tableName : new HashSet<>(batchItems.keySet())) {
                flush(tableName);
            }
        }

        private void flush(String tableName) {
            List<Map<String, AttributeValue>> items = batchItems.get(tableName);
            if (items == null) {
                return;
            }

            List<Map<String, AttributeValue>> itemsToSend;
            synchronized(items) {
                if (items.isEmpty()) {
                    return;
                }

                // Create a copy of items to send
                itemsToSend = new ArrayList<>(items);
                items.clear();
            }

            // Create write requests for each item
            List<WriteRequest> writeRequests = new ArrayList<>();
            for (Map<String, AttributeValue> item : itemsToSend) {
                writeRequests.add(WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(item).build())
                        .build());
            }

            // Create the batch write request
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(tableName, writeRequests);

            BatchWriteItemRequest batchWriteItemRequest = BatchWriteItemRequest.builder()
                    .requestItems(requestItems)
                    .build();

            // Track this operation
            pendingOperations.incrementAndGet();

            // Use sync client for more reliable operation
            try {
                BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(batchWriteItemRequest);

                // Handle unprocessed items if any
                if (response.hasUnprocessedItems() && !response.unprocessedItems().isEmpty()) {
                    retryUnprocessedItems(response.unprocessedItems(), 0);
                }
            } catch (Exception e) {
                dbWriteErrors.incrementAndGet();

                // Add items back to the batch for retry on next flush
                synchronized(items) {
                    items.addAll(itemsToSend);
                }
            } finally {
                pendingOperations.decrementAndGet();
            }
        }

        private int countUnprocessedItems(Map<String, List<WriteRequest>> unprocessedItems) {
            int count = 0;
            for (List<WriteRequest> requests : unprocessedItems.values()) {
                count += requests.size();
            }
            return count;
        }

        private void retryUnprocessedItems(Map<String, List<WriteRequest>> unprocessedItems, int attempt) {
            if (unprocessedItems.isEmpty()) {
                return;
            }

            if (attempt >= MAX_RETRY_ATTEMPTS) {
                dbWriteErrors.addAndGet(countUnprocessedItems(unprocessedItems));
                return;
            }

            pendingOperations.incrementAndGet();

            try {
                // Use synchronous call for retry with minimal delay
                BatchWriteItemRequest retryRequest = BatchWriteItemRequest.builder()
                        .requestItems(unprocessedItems)
                        .build();

                BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(retryRequest);

                // Check if we still have unprocessed items
                if (response.hasUnprocessedItems() && !response.unprocessedItems().isEmpty()) {
                    retryUnprocessedItems(response.unprocessedItems(), attempt + 1);
                }
            } catch (Exception e) {
                dbWriteErrors.addAndGet(countUnprocessedItems(unprocessedItems));
            } finally {
                pendingOperations.decrementAndGet();
            }
        }
    }

    private static ScheduledExecutorService startStatsReporter() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final long startTime = System.currentTimeMillis();

        // Report stats every 60 seconds instead of every 10 seconds
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            double elapsedSeconds = (currentTime - startTime) / 1000.0;
            int totalMessages = messagesProcessed.get();
            int errors = processingErrors.get();
            int dbErrors = dbWriteErrors.get();

            // Calculate recent throughput
            int previousCount = lastProcessedCount.getAndSet(totalMessages);
            double recentThroughput = (totalMessages - previousCount) / 60.0; // 60 seconds

            // Calculate overall throughput
            double throughput = totalMessages / elapsedSeconds;

            System.out.println("STATS [" + formatTime(elapsedSeconds) + "] Total: " + totalMessages +
                    " | Throughput: " + String.format("%.2f", throughput) +
                    " | Recent: " + String.format("%.2f", recentThroughput) +
                    " | Errors: " + errors +
                    " | DB Errors: " + dbErrors +
                    " | Pending: " + batchWriter.pendingOperations.get());

        }, 60, 60, TimeUnit.SECONDS); // Only output every 60 seconds

        return scheduler;
    }

    private static String formatTime(double seconds) {
        long hours = (long) (seconds / 3600);
        long minutes = (long) ((seconds % 3600) / 60);
        long secs = (long) (seconds % 60);
        return String.format("%02d:%02d:%02d", hours, minutes, secs);
    }
}