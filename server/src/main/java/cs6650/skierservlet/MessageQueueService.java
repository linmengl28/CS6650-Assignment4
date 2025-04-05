package cs6650.skierservlet;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

/**
 * Interface for message queue operations, following the Single Responsibility Principle
 */
public interface MessageQueueService {
    void initialize() throws IOException, TimeoutException;
    CompletableFuture<Boolean> sendMessage(String message);
    void shutdown();
}