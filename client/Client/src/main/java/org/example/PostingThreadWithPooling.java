package org.example;

import com.google.gson.Gson;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PostingThreadWithPooling implements Runnable {
    private final int requestCount;
    private final CloseableHttpClient httpClient;
    private final BlockingQueue<String> eventQueue;

    // Phase-specific counters
    private final AtomicInteger phaseSuccessfulRequests;
    private final AtomicInteger phaseFailedRequests;
    private final AtomicInteger phaseCompletedRequests;

    // Overall counters
    private final AtomicInteger totalSuccessfulRequests;
    private final AtomicInteger totalFailedRequests;
    private final AtomicInteger totalCompletedRequests;

    private final String baseUrl;
    private final int phaseId; // 1 or 2

    private static final String LOG_FILE_TEMPLATE = "request_latency_phase%d.csv";

    // Use separate latency tracking for each phase
    private static final Queue<Long> phase1Latencies = new ConcurrentLinkedQueue<>();
    private static final Queue<Long> phase2Latencies = new ConcurrentLinkedQueue<>();
    private static final Queue<Long> allLatencies = new ConcurrentLinkedQueue<>();

    private final Gson gson = new Gson();

    public PostingThreadWithPooling(int requestCount,
                                    BlockingQueue<String> eventQueue,
                                    AtomicInteger phaseSuccessfulRequests,
                                    AtomicInteger phaseFailedRequests,
                                    AtomicInteger phaseCompletedRequests,
                                    AtomicInteger totalSuccessfulRequests,
                                    AtomicInteger totalFailedRequests,
                                    AtomicInteger totalCompletedRequests,
                                    String baseUrl,
                                    PoolingHttpClientConnectionManager connectionManager,
                                    int phaseId) {
        this.requestCount = requestCount;
        this.eventQueue = eventQueue;
        this.phaseSuccessfulRequests = phaseSuccessfulRequests;
        this.phaseFailedRequests = phaseFailedRequests;
        this.phaseCompletedRequests = phaseCompletedRequests;
        this.totalSuccessfulRequests = totalSuccessfulRequests;
        this.totalFailedRequests = totalFailedRequests;
        this.totalCompletedRequests = totalCompletedRequests;
        this.baseUrl = baseUrl;
        this.phaseId = phaseId;

        // Optimize request timeout settings
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectTimeout(3000)          // 3 seconds connection timeout
                .setSocketTimeout(5000)           // 5 seconds socket timeout
                .setConnectionRequestTimeout(3000) // 3 seconds to get connection from pool
                .build();

        // Build the HttpClient with the shared connection manager
        this.httpClient = HttpClients.custom()
                .setConnectionManager(connectionManager)
                .setConnectionManagerShared(true) // Important: don't close connection manager when client is closed
                .setDefaultRequestConfig(requestConfig)
                .build();
    }

    @Override
    public void run() {
        String logFile = String.format(LOG_FILE_TEMPLATE, phaseId);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(logFile, true))) {
            List<String> batch = new ArrayList<>(100); // Pre-allocate size for efficiency
            for (int i = 0; i < requestCount; i++) {
                String eventJson = eventQueue.take();
                sendRequest(eventJson, batch);
                phaseCompletedRequests.incrementAndGet(); // Phase-specific counter
                totalCompletedRequests.incrementAndGet(); // Overall counter

                if (batch.size() >= 100) { // Write batch to file
                    synchronized (writer) {
                        for (String entry : batch) {
                            writer.write(entry);
                        }
                        writer.flush();
                    }
                    batch.clear();
                }
            }
            // Write remaining entries
            if (!batch.isEmpty()) {
                synchronized (writer) {
                    for (String entry : batch) {
                        writer.write(entry);
                    }
                    writer.flush();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("Thread interrupted: " + Thread.currentThread().getName());
        } catch (IOException e) {
            System.err.println("IO error in thread " + Thread.currentThread().getName() + ": " + e.getMessage());
        }
    }

    private void sendRequest(String eventWrapperJson, List<String> batch) {
        try {
            // Parse the event wrapper
            LiftRideGenerator.LiftRideEventWrapper eventWrapper =
                    gson.fromJson(eventWrapperJson, LiftRideGenerator.LiftRideEventWrapper.class);

            // Construct the full URL including the path from the wrapper
            String fullUrl = baseUrl + eventWrapper.getUrlPath();

            // Create the HTTP request
            HttpPost request = new HttpPost(fullUrl);
            request.setHeader("Content-Type", "application/json");

            // Set the request body (only liftID and time)
            String requestBodyJson = gson.toJson(eventWrapper.getRequestBody());
            StringEntity entity = new StringEntity(requestBodyJson, StandardCharsets.UTF_8);
            request.setEntity(entity);

            // Optimized retry logic with shorter delays
            for (int attempt = 0; attempt < 5; attempt++) {
                long startTime = System.currentTimeMillis();
                int responseCode = 0;

                try (CloseableHttpResponse response = httpClient.execute(request)) {
                    responseCode = response.getStatusLine().getStatusCode();
                    HttpEntity responseEntity = response.getEntity();

                    if (responseEntity != null) {
                        // Consume entity content to release connection back to pool
                        EntityUtils.consume(responseEntity);
                    }

                    long latency = System.currentTimeMillis() - startTime;

                    // Store latency for analysis - both phase-specific and overall
                    if (phaseId == 1) {
                        phase1Latencies.add(latency);
                    } else {
                        phase2Latencies.add(latency);
                    }
                    allLatencies.add(latency);

                    // Add to batch
                    batch.add(String.format("%d,POST,%d,%d,%d\n", startTime, latency, responseCode, phaseId));

                    if (responseCode == 201) {
                        phaseSuccessfulRequests.incrementAndGet();
                        totalSuccessfulRequests.incrementAndGet();
                        return;
                    }

                    if (responseCode < 500) {
                        phaseFailedRequests.incrementAndGet();
                        totalFailedRequests.incrementAndGet();
                        break; // Don't retry for 4xx errors
                    }

                    // Only retry for server errors (5xx)
                    // More aggressive backoff with shorter initial delays
                    int backoffMs = 30 * (attempt + 1);
                    Thread.sleep(backoffMs);
                }
            }

            // If we get here, all attempts failed
            phaseFailedRequests.incrementAndGet();
            totalFailedRequests.incrementAndGet();

        } catch (Exception e) {
            phaseFailedRequests.incrementAndGet();
            totalFailedRequests.incrementAndGet();

            // Add error entry to batch
            long currentTime = System.currentTimeMillis();
            batch.add(String.format("%d,POST,%d,%d,%d\n", currentTime, 0, 0, phaseId));
        }
    }

    public static void printPerformanceMetrics(long totalRequests, long wallTimeMillis) {
        System.out.println("\n=== Overall Performance Metrics ===");
        printLatencyMetrics(allLatencies, totalRequests, wallTimeMillis, "Overall");

        if (!phase1Latencies.isEmpty()) {
            System.out.println("\n=== Phase 1 Performance Metrics ===");
            printLatencyMetrics(phase1Latencies, phase1Latencies.size(), 0, "Phase 1");
        }

        if (!phase2Latencies.isEmpty()) {
            System.out.println("\n=== Phase 2 Performance Metrics ===");
            printLatencyMetrics(phase2Latencies, phase2Latencies.size(), 0, "Phase 2");
        }
    }

    private static void printLatencyMetrics(Queue<Long> latencies, long requestCount, long wallTimeMillis, String label) {
        List<Long> latencyList = new ArrayList<>(latencies);
        if (latencyList.isEmpty()) {
            System.out.println("No latency data available for " + label);
            return;
        }

        Collections.sort(latencyList);
        long sum = latencyList.stream().mapToLong(Long::longValue).sum();
        double mean = sum / (double) latencyList.size();
        long median = latencyList.get(latencyList.size() / 2);
        long min = latencyList.get(0);
        long max = latencyList.get(latencyList.size() - 1);

        // Calculate percentiles
        long p90 = latencyList.get((int) (latencyList.size() * 0.9));
        long p95 = latencyList.get((int) (latencyList.size() * 0.95));
        long p99 = latencyList.get((int) (latencyList.size() * 0.99));

        System.out.printf("Sample size: %d requests\n", latencyList.size());
        System.out.printf("Mean Response Time: %.2f ms\n", mean);
        System.out.printf("Median Response Time: %d ms\n", median);
        System.out.printf("Min Response Time: %d ms\n", min);
        System.out.printf("Max Response Time: %d ms\n", max);
        System.out.printf("90th Percentile Response Time (p90): %d ms\n", p90);
        System.out.printf("95th Percentile Response Time (p95): %d ms\n", p95);
        System.out.printf("99th Percentile Response Time (p99): %d ms\n", p99);

        if (wallTimeMillis > 0) {
            double throughput = requestCount / (wallTimeMillis / 1000.0);
            System.out.printf("Throughput: %.2f requests/sec\n", throughput);
        }
    }
}