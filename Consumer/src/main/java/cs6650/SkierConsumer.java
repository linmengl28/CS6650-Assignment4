package cs6650;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.AMQP;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class SkierConsumer {
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final String RABBITMQ_HOST = "172.31.44.71";
    private static final String RMQusername = "admin";
    private static final String RMQpswd = "admin";

    // Thread configuration
    private static final int NUM_CONSUMER_THREADS = 256;
    private static final int NUM_BATCH_WRITERS = 32; // Number of parallel batch writers

    // Significantly increased prefetch count for better throughput
    private static final int PREFETCH_COUNT = 50;

    // Channel pool configuration - optimized for sharing
    private static final int CHANNEL_POOL_SIZE = 256;

    // Maximum wait time to get a channel from the pool
    private static final long CHANNEL_POOL_TIMEOUT = 3000; // ms

    // DynamoDB table name
    private static final String SKIER_RIDES_TABLE = "SkierRides";

    // AWS Region - Set to Oregon (US-WEST-2)
    private static final Region AWS_REGION = Region.US_WEST_2;

    // DynamoDB client
    private static DynamoDbClient dynamoDbClient;

    // BlockingQueue for storing processed items before writing to DynamoDB
    private static final BlockingQueue<Map<String, AttributeValue>> itemQueue = new LinkedBlockingQueue<>(200000);

    // Counters for statistics
    private static final AtomicInteger messagesProcessed = new AtomicInteger(0);
    private static final AtomicInteger messagesQueued = new AtomicInteger(0);
    private static final AtomicInteger dbWriteErrors = new AtomicInteger(0);
    private static final AtomicInteger processingErrors = new AtomicInteger(0);
    private static final AtomicInteger lastProcessedCount = new AtomicInteger(0);
    private static final AtomicInteger channelPoolBorrows = new AtomicInteger(0);
    private static final AtomicInteger channelPoolWaits = new AtomicInteger(0);
    private static final AtomicInteger totalItemsWritten = new AtomicInteger(0);

    // Flag to control application lifecycle
    private static final AtomicBoolean isRunning = new AtomicBoolean(true);

    // Thread-local GSON instances for better performance
    private static final ThreadLocal<Gson> gsonThreadLocal = ThreadLocal.withInitial(Gson::new);

    // Optimized connection management
    private static Connection connection;

    // Channel pool for shared usage
    private static BlockingQueue<Channel> channelPool;

    // List to hold all BatchWriter instances
    private static List<BatchWriter> batchWriters;

    public static void main(String[] args) {
        System.out.println("Starting SkierConsumer with multiple BatchWriters and DynamoDB persistence...");

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

        // Initialize DynamoDB client
        initDynamoDb();

        // Ensure table exists (or create it)
        createTablesIfNotExist();

        // Initialize multiple BatchWriter instances
        batchWriters = new ArrayList<>(NUM_BATCH_WRITERS);
        for (int i = 0; i < NUM_BATCH_WRITERS; i++) {
            BatchWriter writer = new BatchWriter(i);
            writer.start();
            batchWriters.add(writer);
        }
        System.out.println("Created " + NUM_BATCH_WRITERS + " BatchWriter instances");

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(5672);
        factory.setUsername(RMQusername);
        factory.setPassword(RMQpswd);

        // Configure RabbitMQ connection factory for better performance
        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(1000);
        factory.setRequestedHeartbeat(30);
        factory.setConnectionTimeout(30000);
        factory.setRequestedChannelMax(0); // Unlimited channels

        // Set higher thread pool size for RabbitMQ connection
        factory.setSharedExecutor(Executors.newFixedThreadPool(CHANNEL_POOL_SIZE));

        // Set larger frame size for better throughput
        factory.setRequestedFrameMax(131072); // 128KB frame size

        // Create thread pools
        ExecutorService consumerExecutor = Executors.newFixedThreadPool(NUM_CONSUMER_THREADS,
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

            // Initialize channel pool with proper size - using LinkedBlockingQueue for thread safety
            channelPool = new LinkedBlockingQueue<>(CHANNEL_POOL_SIZE);
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

            // Create multiple consumer threads - each using channels from the pool
            for (int i = 0; i < NUM_CONSUMER_THREADS; i++) {
                final int threadId = i;
                consumerExecutor.submit(() -> {
                    try {
                        // Start consumer using channels from the pool
                        consumeMessages(threadId);
                    } catch (Exception e) {
                        // Minimal error logging
                        System.err.println("Thread " + threadId + " failed: " + e.getClass().getName());
                    }
                });
            }

            System.out.println("All consumer and BatchWriter threads started. Running indefinitely until termination signal...");

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

            // Shutdown all BatchWriter instances
            for (BatchWriter writer : batchWriters) {
                writer.shutdown();
            }

            // Close RabbitMQ channels in the pool
            drainChannelPool();

            // Close RabbitMQ connection
            try {
                if (connection != null && connection.isOpen()) {
                    connection.close(5000); // 5 second timeout
                }
            } catch (Exception e) {
                System.err.println("Error closing RabbitMQ connection: " + e.getMessage());
            }

            if (dynamoDbClient != null) {
                dynamoDbClient.close();
            }
        }
    }

    /**
     * Helper method to close all channels in the pool during shutdown
     */
    private static void drainChannelPool() {
        List<Channel> channels = new ArrayList<>(CHANNEL_POOL_SIZE);
        channelPool.drainTo(channels);

        for (Channel channel : channels) {
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception e) {
                // Minimal error handling during shutdown
                System.err.println("Error closing channel: " + e.getClass().getSimpleName());
            }
        }
    }

    private static void initDynamoDb() {
        dynamoDbClient = DynamoDbClient.builder()
                .region(AWS_REGION)
                .build();
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
                // Create SkierRides table with optimized structure for queries
                CreateTableRequest createSkierRidesTable = CreateTableRequest.builder()
                        .tableName(SKIER_RIDES_TABLE)
                        .keySchema(
                                KeySchemaElement.builder().attributeName("skierId").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("sortKey").keyType(KeyType.RANGE).build())
                        .attributeDefinitions(
                                AttributeDefinition.builder().attributeName("skierId").attributeType(ScalarAttributeType.N).build(),
                                AttributeDefinition.builder().attributeName("sortKey").attributeType(ScalarAttributeType.S).build(),
                                AttributeDefinition.builder().attributeName("dayId").attributeType(ScalarAttributeType.N).build()
                        )
                        .billingMode(BillingMode.PROVISIONED)
                        .provisionedThroughput(ProvisionedThroughput.builder()
                                .readCapacityUnits(2000L)
                                .writeCapacityUnits(5000L)
                                .build())
                        // Simplified GSI structure with just one GSI for dayId-skierId queries
                        .globalSecondaryIndexes(
                                // GSI for day-skier queries
                                GlobalSecondaryIndex.builder()
                                        .indexName("day-skier-index")
                                        .keySchema(
                                                KeySchemaElement.builder().attributeName("dayId").keyType(KeyType.HASH).build(),
                                                KeySchemaElement.builder().attributeName("skierId").keyType(KeyType.RANGE).build())
                                        .projection(Projection.builder().projectionType(ProjectionType.INCLUDE)
                                                .nonKeyAttributes("vertical", "liftId", "time").build())
                                        .provisionedThroughput(ProvisionedThroughput.builder()
                                                .readCapacityUnits(2000L)
                                                .writeCapacityUnits(2000L)
                                                .build())
                                        .build()
                        )
                        .build();

                dynamoDbClient.createTable(createSkierRidesTable);
                System.out.println("Created " + SKIER_RIDES_TABLE + " table with provisioned capacity and optimized GSI for fixed resortId/seasonId");

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

    /**
     * Helper method to borrow a channel from the pool with timeout
     */
    private static Channel borrowChannelFromPool() throws Exception {
        Channel channel = channelPool.poll(CHANNEL_POOL_TIMEOUT, TimeUnit.MILLISECONDS);
        if (channel == null) {
            channelPoolWaits.incrementAndGet();
            // If no channel available within timeout, create a new temporary one
            // This helps prevent deadlocks during high load
            channel = connection.createChannel();
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channel.basicQos(PREFETCH_COUNT);
            // Note: This temporary channel won't be returned to the pool
            System.out.println("Created temporary channel due to pool exhaustion");
        } else {
            channelPoolBorrows.incrementAndGet();
        }
        return channel;
    }

    /**
     * Helper method to return a channel to the pool
     */
    private static void returnChannelToPool(Channel channel) {
        if (channel != null && channel.isOpen()) {
            try {
                // Try to return to pool, but don't block if pool is full
                boolean returned = channelPool.offer(channel);
                if (!returned) {
                    // If pool is full, close the channel
                    channel.close();
                }
            } catch (Exception e) {
                // If there's an issue with the channel, don't return it to the pool
                try {
                    channel.close();
                } catch (Exception ignored) {
                    // Ignore close errors during cleanup
                }
            }
        }
    }

    private static void consumeMessages(int threadId) throws Exception {
        // Get thread-local Gson instance for better performance
        Gson gson = gsonThreadLocal.get();

        while (isRunning.get()) {
            Channel channel = null;

            try {
                // Borrow a channel from the pool
                channel = borrowChannelFromPool();

                // Process a batch of messages with this channel
                processMessageBatch(channel, gson, threadId);

            } catch (Exception e) {
                processingErrors.incrementAndGet();
//                System.err.println("Error in consumer thread " + threadId + ": " + e.getClass().getSimpleName());

                // If there was a problem with the channel, don't return it
                if (channel != null && channel.isOpen()) {
                    try {
                        channel.close();
                        channel = null;
                    } catch (Exception ignored) {
                        // Ignore errors when closing already problematic channels
                    }
                }

                // Brief pause before trying again
                Thread.sleep(100);
            } finally {
                // Always return the channel to the pool if it's still valid
                if (channel != null) {
                    returnChannelToPool(channel);
                }
            }
        }
    }

    /**
     * Process a batch of messages with the given channel
     */
    private static void processMessageBatch(Channel channel, Gson gson, int threadId) throws IOException, InterruptedException {
        // Define message handling callback
        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            if (!isRunning.get()) {
                channel.basicNack(delivery.getEnvelope().getDeliveryTag(), false, true);
                return;
            }

            long deliveryTag = delivery.getEnvelope().getDeliveryTag();
            String message = new String(delivery.getBody(), StandardCharsets.UTF_8);

            try {
                // Parse the message
                LiftRideEvent liftRide = gson.fromJson(message, LiftRideEvent.class);

                // Create the DynamoDB item
                Map<String, AttributeValue> item = createDynamoDbItem(liftRide);

                // Add to the queue
                boolean added = itemQueue.offer(item, 1000, TimeUnit.MILLISECONDS);
                if (added) {
                    // Successfully added to queue - ACK immediately
                    channel.basicAck(deliveryTag, false);
                    messagesQueued.incrementAndGet();
                    messagesProcessed.incrementAndGet();
                } else {
                    // Queue is full - NACK and requeue
                    channel.basicNack(deliveryTag, false, true);
                    dbWriteErrors.incrementAndGet();
                    System.err.println("Warning: Item queue is full, couldn't add item - requeuing message");
                }
            } catch (Exception e) {
                processingErrors.incrementAndGet();
                // Error processing message - NACK
                try {
                    // Requeue only on specific exceptions that might be temporary
                    boolean requeue = !(e instanceof IllegalArgumentException);
                    channel.basicNack(deliveryTag, false, requeue);
                } catch (IOException ioe) {
                    // Handle channel errors - minimal logging
                    System.err.println("Error NACKing message on thread " + threadId);
                }
            }
        };

        // Set a reasonable prefetch to avoid overwhelming this consumer
        channel.basicQos(PREFETCH_COUNT);

        // Start consuming messages with manual acknowledgment
        String consumerTag = channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag1 -> { });

        try {
            // Process messages for a fixed duration
            Thread.sleep(2000);

            // Cancel the consumer when done with this batch
            channel.basicCancel(consumerTag);
        } catch (Exception e) {
            System.err.println("Error in consumer processing: " + e.getMessage());
            throw e; // Rethrow to be handled by the caller
        }
    }

    private static void processLiftRide(LiftRideEvent liftRide) {
        try {
            // Create the DynamoDB item
            Map<String, AttributeValue> item = createDynamoDbItem(liftRide);

            // Add to the queue instead of writing directly
            boolean added = itemQueue.offer(item, 1000, TimeUnit.MILLISECONDS);
            if (added) {
                messagesQueued.incrementAndGet();
            } else {
                // If queue is full, this is an error condition
                dbWriteErrors.incrementAndGet();
                System.err.println("Warning: Item queue is full, couldn't add item");
            }
        } catch (Exception e) {
            dbWriteErrors.incrementAndGet();
            throw new RuntimeException("Error processing lift ride", e);
        }
    }

    private static Map<String, AttributeValue> createDynamoDbItem(LiftRideEvent liftRide) {
        // Compute vertical as liftId * 10
        int vertical = liftRide.getLiftId() * 10;

        // Create sortKey in format "seasonId#dayId#liftId#timestamp" for efficient querying
        // Since seasonId is fixed, we can still include it in the sortKey for future-proofing
        String sortKey = liftRide.getSeasonId() + "#" + liftRide.getDayId() + "#" + liftRide.getLiftId() + "#" + liftRide.getTime();

        // Create a record for the SkierRides table
        Map<String, AttributeValue> skierRideItem = new HashMap<>();
        skierRideItem.put("skierId", AttributeValue.builder().n(String.valueOf(liftRide.getSkierId())).build());
        skierRideItem.put("sortKey", AttributeValue.builder().s(sortKey).build());

        // Store fixed values (not part of the GSI keys but still stored as regular attributes)
        skierRideItem.put("resortId", AttributeValue.builder().n(String.valueOf(liftRide.getResortId())).build());
        skierRideItem.put("seasonId", AttributeValue.builder().s(liftRide.getSeasonId()).build());

        // Required for the day-skier-index GSI
        skierRideItem.put("dayId", AttributeValue.builder().n(String.valueOf(liftRide.getDayId())).build());

        // Additional data attributes
        skierRideItem.put("liftId", AttributeValue.builder().n(String.valueOf(liftRide.getLiftId())).build());
        skierRideItem.put("time", AttributeValue.builder().n(String.valueOf(liftRide.getTime())).build());
        skierRideItem.put("vertical", AttributeValue.builder().n(String.valueOf(vertical)).build());

        return skierRideItem;
    }


    /**
     * BatchWriter class for efficient batch writing to DynamoDB
     * Each BatchWriter runs in its own thread and pulls from the shared queue
     */
    private static class BatchWriter {
        private static final int MAX_BATCH_SIZE = 25; // DynamoDB's max limit is 25 items per batch
        private static final int FLUSH_INTERVAL_MS = 5; // Flush every 5ms
        private static final int MAX_RETRY_ATTEMPTS = 3;

        private final int writerId;
        private final Map<String, List<Map<String, AttributeValue>>> batchItems = new ConcurrentHashMap<>();
        private final ScheduledExecutorService scheduler;
        private final ExecutorService pollingExecutor;
        private final AtomicInteger pendingOperations = new AtomicInteger(0);
        private final AtomicInteger backlogSize = new AtomicInteger(0);
        private final AtomicBoolean isRunning = new AtomicBoolean(true);
        private final AtomicInteger itemsWritten = new AtomicInteger(0);

        public BatchWriter(int id) {
            this.writerId = id;

            // Initialize executor services after writerId is set
            this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "batch-writer-" + writerId);
                t.setDaemon(true);
                return t;
            });

            this.pollingExecutor = Executors.newSingleThreadExecutor(r -> {
                Thread t = new Thread(r, "queue-poller-" + writerId);
                t.setDaemon(true);
                return t;
            });
        }

        public void start() {
            // Start thread to poll from the shared queue
            pollingExecutor.submit(this::pollQueue);

            // Schedule fixed-rate flushing
            scheduler.scheduleAtFixedRate(this::flushAll, FLUSH_INTERVAL_MS, FLUSH_INTERVAL_MS, TimeUnit.MILLISECONDS);

            // Schedule adaptive flushing based on backlog
            scheduler.scheduleAtFixedRate(() -> {
                int currentBacklog = backlogSize.get();
                if (currentBacklog > 100) {
                    // Additional flush if backlog is high
                    flushAll();
                }
            }, 50, 50, TimeUnit.MILLISECONDS);
        }

        private void pollQueue() {
            try {
                while (isRunning.get()) {
                    try {
                        // Poll from the shared queue
                        Map<String, AttributeValue> item = itemQueue.poll(100, TimeUnit.MILLISECONDS);
                        if (item != null) {
                            addItem(SKIER_RIDES_TABLE, item);
                        }
                    } catch (Exception e) {
                        // Log error but keep trying
                        if (isRunning.get()) {
                            System.err.println("Error in queue poller " + writerId + ": " + e.getMessage());
                        }
                    }
                }
            } catch (Exception e) {
                System.err.println("Fatal error in queue poller " + writerId + ": " + e.getMessage());
            }
        }

        public void shutdown() {
            isRunning.set(false);
            pollingExecutor.shutdown();
            scheduler.shutdown();
            flushAll(); // Final flush

            try {
                // Wait for shutdown to complete
                if (!scheduler.awaitTermination(10, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                if (!pollingExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
                    pollingExecutor.shutdownNow();
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

            try {
                // Execute the batch write
                BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(batchWriteItemRequest);

                // Update statistics
                int itemsInBatch = itemsToSend.size();
                itemsWritten.addAndGet(itemsInBatch);
                totalItemsWritten.addAndGet(itemsInBatch);

                // Handle unprocessed items if any
                Map<String, List<WriteRequest>> unprocessedItems = response.unprocessedItems();
                if (!unprocessedItems.isEmpty()) {
                    retryUnprocessedItems(unprocessedItems, 0);
                }

                pendingOperations.decrementAndGet();
            } catch (Exception e) {
                pendingOperations.decrementAndGet();
                handleBatchWriteError(tableName, itemsToSend, e);
            }
        }

        private void handleBatchWriteError(String tableName, List<Map<String, AttributeValue>> items, Throwable error) {
            dbWriteErrors.incrementAndGet();

            // Only log every 100th error to reduce console spam
            if (dbWriteErrors.get() % 100 == 1) {
                System.err.println("BatchWriter " + writerId + " error: " + error.getClass().getSimpleName());
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

                // Retry the batch write
                BatchWriteItemRequest retryRequest = BatchWriteItemRequest.builder()
                        .requestItems(unprocessedItems)
                        .build();

                BatchWriteItemResponse response = dynamoDbClient.batchWriteItem(retryRequest);

                // Handle any remaining unprocessed items
                Map<String, List<WriteRequest>> remainingUnprocessed = response.unprocessedItems();
                if (!remainingUnprocessed.isEmpty()) {
                    retryUnprocessedItems(remainingUnprocessed, attempt + 1);
                }

                pendingOperations.decrementAndGet();
            } catch (Exception e) {
                dbWriteErrors.addAndGet(countUnprocessedItems(unprocessedItems));
                pendingOperations.decrementAndGet();
            }
        }

        public int getPendingOperations() {
            return pendingOperations.get();
        }

        public int getBacklogSize() {
            return backlogSize.get();
        }

        public int getItemsWritten() {
            return itemsWritten.get();
        }
    }

    private static ScheduledExecutorService startStatsReporter() {
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        final long startTime = System.currentTimeMillis();

        // Report stats every 30 seconds
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            double elapsedSeconds = (currentTime - startTime) / 1000.0;
            int totalMessages = messagesProcessed.get();
            int totalQueued = messagesQueued.get();
            int queueSize = itemQueue.size();
            int errors = processingErrors.get();
            int dbErrors = dbWriteErrors.get();

            // Calculate recent throughput
            int previousCount = lastProcessedCount.getAndSet(totalMessages);
            double recentThroughput = (totalMessages - previousCount) / 30.0; // 30 seconds

            // Calculate overall throughput
            double throughput = totalMessages / elapsedSeconds;

            System.out.println("STATS [" + formatTime(elapsedSeconds) + "] Total: " + totalMessages +
                    " | Queued: " + totalQueued +
                    " | Queue Size: " + queueSize +
                    " | Throughput: " + String.format("%.2f", throughput) +
                    " | Recent: " + String.format("%.2f", recentThroughput) +
                    " | Errors: " + errors +
                    " | DB Errors: " + dbErrors);

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