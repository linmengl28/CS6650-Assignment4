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
    private static final String RABBITMQ_HOST = "35.94.176.139";
    private static final String RMQusername = "admin";
    private static final String RMQpswd = "rmq";

    // Optimized thread count - can be adjusted based on CPU cores
    private static final int NUM_THREADS = 1024; // Increased from 512

    // Significantly increased prefetch count for better throughput
    private static final int PREFETCH_COUNT = 1000; // Up from 500

    // Channel pool configuration - increased
    private static final int CHANNEL_POOL_SIZE = 128; // Up from 64

    // DynamoDB table name
    private static final String SKIER_RIDES_TABLE = "SkierRides";

    // AWS Region - Set to Oregon (US-WEST-2)
    private static final Region AWS_REGION = Region.US_WEST_2;

    // DynamoDB clients
    private static DynamoDbAsyncClient dynamoDbAsyncClient;
    private static DynamoDbClient dynamoDbClient;

    // Batch writer for DynamoDB with optimized settings
    private static BatchWriter batchWriter;

    // Counters for statistics
    private static final AtomicInteger messagesProcessed = new AtomicInteger(0);
    private static final AtomicInteger dbWriteErrors = new AtomicInteger(0);
    private static final AtomicInteger processingErrors = new AtomicInteger(0);
    private static final AtomicInteger lastProcessedCount = new AtomicInteger(0);

    // Flag to control application lifecycle
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    // Optimized LRU cache for deduplication - increased size
    private static final int CACHE_SIZE = 10000; // Up from 1000
    private static final Map<String, Long> recentWrites = new ConcurrentHashMap<>(CACHE_SIZE);

    // Thread-local GSON instances for better performance
    private static final ThreadLocal<Gson> gsonThreadLocal = ThreadLocal.withInitial(Gson::new);

    // Optimized connection management
    private static Connection connection;

    public static void main(String[] args) {
        System.out.println("Starting SkierConsumer with DynamoDB persistence...");

        // Add shutdown hook for graceful termination
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown hook triggered - gracefully terminating...");
            isRunning.set(false);

            try {
                // Allow some time for shutdown procedures
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));

        // Initialize DynamoDB clients
        initDynamoDb();

        // Ensure table exists (or create it)
        createTablesIfNotExist();

        // Initialize the batch writer with optimized settings
        batchWriter = new BatchWriter();
        batchWriter.start();

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(5672);
        factory.setUsername(RMQusername);
        factory.setPassword(RMQpswd);

        // Configure RabbitMQ connection factory for better performance
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(1000);
        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(30000); // Increased from 15000 to 30000ms
        factory.setRequestedChannelMax(0); // Unlimited channels

        // Set higher thread pool size for RabbitMQ connection
        factory.setSharedExecutor(Executors.newFixedThreadPool(CHANNEL_POOL_SIZE));

        // Set larger frame size for better throughput
        factory.setRequestedFrameMax(131072); // 128KB frame size

        // Create thread pools
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_THREADS,
                r -> {
                    Thread t = new Thread(r);
                    t.setDaemon(true); // Make threads daemon
                    return t;
                });
        ScheduledExecutorService statsExecutor = null;

        try {
            // Create a single shared connection with optimized thread pool
            connection = factory.newConnection(
                    Executors.newFixedThreadPool(CHANNEL_POOL_SIZE, r -> {
                        Thread t = new Thread(r);
                        t.setDaemon(true);
                        return t;
                    })
            );
            System.out.println("Connected to RabbitMQ successfully");

            // Initialize channel pool with larger size
            BlockingQueue<Channel> channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);
            for (int i = 0; i < CHANNEL_POOL_SIZE; i++) {
                Channel channel = connection.createChannel();
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                channel.basicQos(PREFETCH_COUNT);
                channelPool.offer(channel);
            }
            System.out.println("Created channel pool with " + CHANNEL_POOL_SIZE + " channels");

            // Test queue connection and check message count
            try (Channel testChannel = connection.createChannel()) {
                AMQP.Queue.DeclareOk queueInfo = testChannel.queueDeclare(QUEUE_NAME, true, false, false, null);
                System.out.println("Queue '" + QUEUE_NAME + "' exists and has " + queueInfo.getMessageCount() + " messages");
            } catch (Exception e) {
                System.err.println("Error testing RabbitMQ queue: " + e.getMessage());
            }

            // Start statistics reporter thread with more frequent updates
            statsExecutor = startStatsReporter();

            // Create multiple consumer threads - each with its own channel
            for (int i = 0; i < NUM_THREADS; i++) {
                final int threadId = i;
                consumerExecutor.submit(() -> {
                    try {
                        // Each consumer gets its own channel for better parallelism
                        Channel channel = connection.createChannel();
                        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
                        channel.basicQos(PREFETCH_COUNT);

                        consumeMessages(channel, threadId);
                    } catch (Exception e) {
                        // Minimal error logging
                        System.err.println("Thread " + threadId + " failed: " + e.getClass().getName());
                    }
                });
            }

            System.out.println("All consumer threads started. Running indefinitely until termination signal...");

            // Wait until shutdown is requested
            while (isRunning.get()) {
                Thread.sleep(1000);
            }

        } catch (Exception e) {
            System.err.println("Failed to start consumer: " + e.getMessage());
            e.printStackTrace();
        } finally {
            System.out.println("Main thread reaching finally block - program is shutting down");

            // Cancel stats reporter
            if (statsExecutor != null) {
                statsExecutor.shutdown();
            }

            // Graceful shutdown
            isRunning.set(false);
            consumerExecutor.shutdown();

            try {
                if (!consumerExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    consumerExecutor.shutdownNow();
                }
            } catch (InterruptedException ie) {
                consumerExecutor.shutdownNow();
                Thread.currentThread().interrupt();
            }

            // Allow the batch writer to finish processing
            System.out.println("Shutting down - waiting for batch writer to complete...");
            batchWriter.shutdown();

            // Close RabbitMQ connection
            try {
                if (connection != null && connection.isOpen()) {
                    connection.close(5000); // 5 second timeout
                }
            } catch (Exception e) {
                System.err.println("Error closing RabbitMQ connection: " + e.getMessage());
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
                    .maxConcurrency(NUM_THREADS) // Match with consumer thread count
                    .connectionAcquisitionTimeout(Duration.ofSeconds(10))
                    .connectionTimeToLive(Duration.ofMinutes(30)); // Increased from 10 to 30 mins

            ApacheHttpClient.Builder syncHttpClientBuilder = ApacheHttpClient.builder()
                    .connectionTimeout(Duration.ofSeconds(5))
                    .socketTimeout(Duration.ofSeconds(30))
                    .connectionTimeToLive(Duration.ofMinutes(30)) // Increased from 10 to 30 mins
                    .maxConnections(NUM_THREADS); // Match with consumer thread count

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
                // Create SkierRides table with high provisioned capacity
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
                                .readCapacityUnits(50L) // Increased from 10 to 50
                                .writeCapacityUnits(5000L) // Increased from 2000 to 5000
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
                                                .readCapacityUnits(50L) // Increased from 20 to 50
                                                .writeCapacityUnits(5000L) // Increased from 2000 to 5000
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
                                                .readCapacityUnits(50L) // Increased from 20 to 50
                                                .writeCapacityUnits(5000L) // Increased from 2000 to 5000
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

    private static void consumeMessages(Channel channel, int threadId) throws IOException {
        // Get thread-local Gson instance for better performance
        Gson gson = gsonThreadLocal.get();

        // Define message handling callback with multi-ack capability
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
                try {
                    // Requeue only on specific exceptions that might be temporary
                    boolean requeue = !(e instanceof IllegalArgumentException);
                    channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, requeue);
                } catch (IOException ioe) {
                    // Handle channel errors - minimal logging
                    System.err.println("Error NACKing message on thread " + threadId);
                }
            }
        };

        // Start consuming messages (autoAck = false for manual acknowledgments)
        String consumerTag = channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag1 -> { });

        // Keep thread alive until shutdown is requested
        while (isRunning.get()) {
            try {
                Thread.sleep(500); // Reduced sleep time
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
            // Minimal error handling
        }
    }

    private static void processLiftRide(LiftRideEvent liftRide) {
        try {
            // Faster deduplication with ConcurrentHashMap
            // Create a key for deduplication
            String dedupeKey = liftRide.getSkierId() + ":" + liftRide.getDayId() + ":" +
                    liftRide.getLiftId() + ":" + liftRide.getTime();

            // Check if we've recently processed this exact event (within last minute)
            Long lastSeen = recentWrites.get(dedupeKey);
            long currentTime = System.currentTimeMillis();

            if (lastSeen != null && (currentTime - lastSeen) < 60000) {
                // Skip duplicate within the last minute
                return;
            }

            // Update cache - use putIfAbsent for better concurrency
            recentWrites.put(dedupeKey, currentTime);

            // Cleanup old entries periodically (every ~1000 entries)
            if (currentTime % 1000 == 0) {
                cleanupOldEntries(currentTime);
            }

            // Write to DynamoDB
            writeToDynamoDB(liftRide);
        } catch (Exception e) {
            dbWriteErrors.incrementAndGet();
            throw e; // Rethrow to be handled by the message consumer
        }
    }

    private static void cleanupOldEntries(long currentTime) {
        // Remove entries older than 5 minutes to prevent memory bloat
        recentWrites.entrySet().removeIf(entry ->
                (currentTime - entry.getValue()) > 300000); // 5 minutes
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
        private static final int FLUSH_INTERVAL_MS = 5; // Reduced from 10ms to 5ms
        private static final int MAX_RETRY_ATTEMPTS = 3; // Increased from 2 to 3
        private static final int BATCH_WRITER_THREADS = 32; // Increased from 8 to 32

        private final Map<String, List<Map<String, AttributeValue>>> batchItems = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(BATCH_WRITER_THREADS);
        private final AtomicInteger pendingOperations = new AtomicInteger(0);
        // For adaptive flushing
        private final AtomicInteger backlogSize = new AtomicInteger(0);

        public void start() {
            // Schedule fixed-rate flushing
            scheduler.scheduleAtFixedRate(this::flushAll, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

            // Schedule adaptive flushing based on backlog
            scheduler.scheduleAtFixedRate(() -> {
                int currentBacklog = backlogSize.get();
                if (currentBacklog > 1000) {
                    // Additional flush if backlog is high
                    flushAll();
                }
            }, 50, 50, TimeUnit.MILLISECONDS);
        }

        public void shutdown() {
            scheduler.shutdown();
            flushAll(); // Final flush

            try {
                // Wait longer for shutdown to complete
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }

                // Wait for pending operations to complete
                int attempts = 0;
                while (pendingOperations.get() > 0 && attempts < 20) {
                    Thread.sleep(500);
                    attempts++;
                    // Force a flush every few attempts
                    if (attempts % 5 == 0) {
                        flushAll();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        public void addItem(String tableName, Map<String, AttributeValue> item) {
            List<Map<String, AttributeValue>> items = batchItems.computeIfAbsent(tableName, k ->
                    Collections.synchronizedList(new ArrayList<>()));

            items.add(item);
            backlogSize.incrementAndGet();

            // If batch size reaches the limit, trigger async flush
            if (items.size() >= MAX_BATCH_SIZE) {
                scheduler.execute(() -> flush(tableName));
            }
        }

        public void flushAll() {
            Set<String> tableNames = new HashSet<>(batchItems.keySet());
            for (String tableName : tableNames) {
                flush(tableName);
            }
        }

        private void flush(String tableName) {
            List<Map<String, AttributeValue>> items = batchItems.get(tableName);
            if (items == null || items.isEmpty()) {
                return;
            }

            List<Map<String, AttributeValue>> itemsToSend;
            synchronized(items) {
                if (items.isEmpty()) {
                    return;
                }

                // Take up to MAX_BATCH_SIZE items
                int batchSize = Math.min(items.size(), MAX_BATCH_SIZE);
                itemsToSend = new ArrayList<>(batchSize);

                // Use iterator for more efficient removal
                Iterator<Map<String, AttributeValue>> iterator = items.iterator();
                for (int i = 0; i < batchSize && iterator.hasNext(); i++) {
                    itemsToSend.add(iterator.next());
                    iterator.remove();
                    backlogSize.decrementAndGet();
                }
            }

            if (itemsToSend.isEmpty()) {
                return;
            }

            // Create write requests for each item
            List<WriteRequest> writeRequests = new ArrayList<>(itemsToSend.size());
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

            // Use async client for better throughput
            dynamoDbAsyncClient.batchWriteItem(batchWriteItemRequest)
                    .whenComplete((response, error) -> {
                        try {
                            if (error != null) {
                                handleBatchWriteError(tableName, itemsToSend, error);
                            } else if (response.hasUnprocessedItems() && !response.unprocessedItems().isEmpty()) {
                                retryUnprocessedItems(response.unprocessedItems(), 0);
                            }
                        } finally {
                            pendingOperations.decrementAndGet();
                        }
                    });
        }

        private void handleBatchWriteError(String tableName, List<Map<String, AttributeValue>> items, Throwable error) {
            dbWriteErrors.incrementAndGet();

            // Only log every 100th error to reduce console spam
            if (dbWriteErrors.get() % 100 == 1) {
                System.err.println("Batch write error: " + error.getClass().getSimpleName());
            }

            // Add items back to the batch for retry
            List<Map<String, AttributeValue>> tableItems = batchItems.computeIfAbsent(tableName,
                    k -> Collections.synchronizedList(new ArrayList<>()));

            synchronized(tableItems) {
                tableItems.addAll(items);
                backlogSize.addAndGet(items.size());
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

                // Add unprocessed items back to batch queue after max retries
                for (Map.Entry<String, List<WriteRequest>> entry : unprocessedItems.entrySet()) {
                    String tableName = entry.getKey();
                    List<WriteRequest> requests = entry.getValue();

                    List<Map<String, AttributeValue>> tableItems = batchItems.computeIfAbsent(tableName,
                            k -> Collections.synchronizedList(new ArrayList<>()));

                    synchronized(tableItems) {
                        for (WriteRequest request : requests) {
                            if (request.putRequest() != null) {
                                tableItems.add(request.putRequest().item());
                                backlogSize.incrementAndGet();
                            }
                        }
                    }
                }
                return;
            }

            pendingOperations.incrementAndGet();

            try {
                // Use exponential backoff for retries
                int delay = (int) Math.pow(2, attempt);
                Thread.sleep(delay);

                // Use asynchronous call for retry
                BatchWriteItemRequest retryRequest = BatchWriteItemRequest.builder()
                        .requestItems(unprocessedItems)
                        .build();

                dynamoDbAsyncClient.batchWriteItem(retryRequest)
                        .whenComplete((response, error) -> {
                            try {
                                if (error != null) {
                                    dbWriteErrors.addAndGet(countUnprocessedItems(unprocessedItems));
                                } else if (response.hasUnprocessedItems() && !response.unprocessedItems().isEmpty()) {
                                    retryUnprocessedItems(response.unprocessedItems(), attempt + 1);
                                }
                            } finally {
                                pendingOperations.decrementAndGet();
                            }
                        });
            } catch (Exception e) {
                dbWriteErrors.addAndGet(countUnprocessedItems(unprocessedItems));
                pendingOperations.decrementAndGet();
            }
        }
    }

    private static ScheduledExecutorService startStatsReporter() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final long startTime = System.currentTimeMillis();

        // Report stats every 30 seconds instead of every 60 seconds
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            double elapsedSeconds = (currentTime - startTime) / 1000.0;
            int totalMessages = messagesProcessed.get();
            int errors = processingErrors.get();
            int dbErrors = dbWriteErrors.get();

            // Calculate recent throughput
            int previousCount = lastProcessedCount.getAndSet(totalMessages);
            double recentThroughput = (totalMessages - previousCount) / 30.0; // 30 seconds

            // Calculate overall throughput
            double throughput = totalMessages / elapsedSeconds;

            System.out.println("STATS [" + formatTime(elapsedSeconds) + "] Total: " + totalMessages +
                    " | Throughput: " + String.format("%.2f", throughput) +
                    " | Recent: " + String.format("%.2f", recentThroughput) +
                    " | Errors: " + errors +
                    " | DB Errors: " + dbErrors +
                    " | Pending: " + batchWriter.pendingOperations.get());

        }, 30, 30, TimeUnit.SECONDS); // Output every 30 seconds

        return scheduler;
    }

    private static String formatTime(double seconds) {
        long hours = (long) (seconds / 3600);
        long minutes = (long) ((seconds % 3600) / 60);
        long secs = (long) (seconds % 60);
        return String.format("%02d:%02d:%02d", hours, minutes, secs);
    }
}