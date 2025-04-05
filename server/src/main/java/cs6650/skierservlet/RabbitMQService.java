package cs6650.skierservlet;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RabbitMQ implementation of the MessageQueueService with a channel pool
 * for improved performance and resource management
 */
class RabbitMQService implements MessageQueueService {
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final String RABBITMQ_HOST = "172.31.20.160";
    private static final int POOL_SIZE = 32; // Number of channels to create in the pool
    private static final int THREAD_POOL_SIZE = 16; // Thread pool for async operations

    private ConnectionFactory factory;
    private Connection connection;
    private ExecutorService executorService;
    private BlockingQueue<Channel> channelPool;
    private AtomicInteger activeChannels;
    private volatile boolean isInitialized = false;

    @Override
    public void initialize() throws IOException, TimeoutException {
        if (isInitialized) {
            return;
        }

        System.out.println("Initializing RabbitMQ Service");

        // Initialize RabbitMQ connection factory
        factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(5672);
        factory.setUsername("myuser");
        factory.setPassword("mypassword");

        // Create thread pool for async operations
        executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        // Establish connection
        connection = factory.newConnection();
        System.out.println("Connected to RabbitMQ successfully");

        // Initialize channel pool
        channelPool = new LinkedBlockingQueue<>(POOL_SIZE);
        activeChannels = new AtomicInteger(0);

        // Create channels and add to pool
        for (int i = 0; i < POOL_SIZE; i++) {
            Channel channel = connection.createChannel();
            // Declare a durable queue (survives broker restarts)
            channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            channelPool.add(channel);
        }

        System.out.println("Initialized channel pool with " + POOL_SIZE + " channels");
        isInitialized = true;
    }

    @Override
    public CompletableFuture<Boolean> sendMessage(String message) {
        return CompletableFuture.supplyAsync(() -> {
            Channel channel = null;
            try {
                // Get a channel from the pool
                channel = borrowChannel();
                if (channel == null) {
                    System.err.println("Failed to obtain channel from pool");
                    return false;
                }

                // Publish the message
                channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
                return true;
            } catch (Exception e) {
                System.err.println("Failed to send message to queue: " + e.getMessage());
                e.printStackTrace();
                return false;
            } finally {
                // Return the channel to the pool
                if (channel != null) {
                    returnChannel(channel);
                }
            }
        }, executorService);
    }

    private Channel borrowChannel() {
        try {
            Channel channel = channelPool.poll();
            if (channel == null) {
                // If no channels available in pool, create a new one if under limit
                if (activeChannels.incrementAndGet() <= POOL_SIZE * 2) { // Allow up to 2x pool size
                    try {
                        System.out.println("Creating additional channel, active: " + activeChannels.get());
                        Channel newChannel = connection.createChannel();
                        // Ensure queue exists
                        newChannel.queueDeclarePassive(QUEUE_NAME);
                        return newChannel;
                    } catch (Exception e) {
                        activeChannels.decrementAndGet();
                        throw e;
                    }
                } else {
                    activeChannels.decrementAndGet();
                    // Wait a bit and try again from pool
                    Thread.sleep(50);
                    return channelPool.poll();
                }
            }
            return channel;
        } catch (Exception e) {
            System.err.println("Error borrowing channel: " + e.getMessage());
            return null;
        }
    }

    private void returnChannel(Channel channel) {
        try {
            if (channel.isOpen()) {
                // If channel is still valid, add it back to the pool
                boolean added = channelPool.offer(channel);
                if (!added) {
                    // If pool is full, close the channel
                    channel.close();
                    activeChannels.decrementAndGet();
                }
            } else {
                // If channel is closed, create a new one and add to pool
                Channel newChannel = connection.createChannel();
                newChannel.queueDeclarePassive(QUEUE_NAME);
                channelPool.offer(newChannel);
            }
        } catch (Exception e) {
            System.err.println("Error returning channel to pool: " + e.getMessage());
            try {
                if (channel.isOpen()) {
                    channel.close();
                }
            } catch (Exception ignored) {
                // Ignore close errors
            }
            activeChannels.decrementAndGet();
        }
    }

    @Override
    public void shutdown() {
        isInitialized = false;

        // Shutdown executor service
        if (executorService != null) {
            executorService.shutdown();
        }

        // Close all channels in the pool
        try {
            while (!channelPool.isEmpty()) {
                Channel channel = channelPool.poll();
                if (channel != null && channel.isOpen()) {
                    channel.close();
                }
            }
        } catch (Exception e) {
            System.err.println("Error closing channels: " + e.getMessage());
        }

        // Close connection
        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing RabbitMQ connection: " + e.getMessage());
        }

        System.out.println("RabbitMQ service shutdown complete");
    }
}