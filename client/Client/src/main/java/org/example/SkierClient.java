package org.example;

import com.google.gson.Gson;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class SkierClient {
    // Server configuration
    private static final String BASE_URL = "http://54.69.130.34:8080/SkierServlet-1.0-SNAPSHOT";
//    private static final String BASE_URL = "http://SkierServletLB-1807112057.us-west-2.elb.amazonaws.com/SkierServlet-1.0-SNAPSHOT";
    // Request configuration
    private static final int TOTAL_REQUESTS = 200000;
    private static final int INITIAL_THREADS = 32;
    private static final int REQUESTS_PER_INITIAL_THREAD = 1000;
    private static final int PHASE_2_THREAD_COUNT = 300;
    private static final int BATCH_SIZE = 560;

    // Queue and counter configuration
    private static final BlockingQueue<String> eventQueue = new LinkedBlockingQueue<>(20000);

    // Phase 1 counters
    private static final AtomicInteger phase1SuccessfulRequests = new AtomicInteger(0);
    private static final AtomicInteger phase1FailedRequests = new AtomicInteger(0);
    private static final AtomicInteger phase1CompletedRequests = new AtomicInteger(0);

    // Phase 2 counters
    private static final AtomicInteger phase2SuccessfulRequests = new AtomicInteger(0);
    private static final AtomicInteger phase2FailedRequests = new AtomicInteger(0);
    private static final AtomicInteger phase2CompletedRequests = new AtomicInteger(0);

    // Overall counters (convenience for total calculations)
    private static final AtomicInteger totalSuccessfulRequests = new AtomicInteger(0);
    private static final AtomicInteger totalFailedRequests = new AtomicInteger(0);
    private static final AtomicInteger totalCompletedRequests = new AtomicInteger(0);

    private static final AtomicBoolean generatorRunning = new AtomicBoolean(true);
    private static final AtomicBoolean phase1Completed = new AtomicBoolean(false);
    private static final AtomicBoolean phase2Completed = new AtomicBoolean(false);

    public static void main(String[] args) throws InterruptedException {
        final long startTime = System.currentTimeMillis();
        final AtomicLong phase2StartTime = new AtomicLong(0);

        System.out.println("Initializing client components...");

        // Start the event generator thread
        Thread generator = new Thread(new LiftRideGenerator(eventQueue, generatorRunning));
        generator.start();

        // Create shared connection manager
        PoolingHttpClientConnectionManager connectionManager = createConnectionManager();

        // Calculate requests for each phase
        int initialPhaseRequests = INITIAL_THREADS * REQUESTS_PER_INITIAL_THREAD;
        int remainingRequests = TOTAL_REQUESTS - initialPhaseRequests;

        // Create separate thread pools for each phase
        ThreadPoolExecutor phase1Executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                INITIAL_THREADS, new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Phase1-Worker-" + counter.incrementAndGet());
                    }
                });

        ExecutorCompletionService<Integer> completionService =
                new ExecutorCompletionService<>(phase1Executor);

        // Start progress monitor for both phases
        ScheduledExecutorService monitorExecutor = startProgressMonitor(
                startTime, phase2StartTime, phase1Executor, null);

        // Submit Phase 1 tasks
        System.out.println("Starting Phase 1: " + INITIAL_THREADS + " threads with " +
                REQUESTS_PER_INITIAL_THREAD + " requests each");

        for (int i = 0; i < INITIAL_THREADS; i++) {
            completionService.submit(new PostingThreadWithPooling(
                    REQUESTS_PER_INITIAL_THREAD,
                    eventQueue,
                    phase1SuccessfulRequests, // Phase 1 specific counters
                    phase1FailedRequests,
                    phase1CompletedRequests,
                    totalSuccessfulRequests, // Overall counters
                    totalFailedRequests,
                    totalCompletedRequests,
                    BASE_URL,
                    connectionManager,
                    1 // Phase identifier
            ), i); // Return thread ID for tracking
        }

        // Create Phase 2 executor but don't start tasks yet
        ThreadPoolExecutor phase2Executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(
                PHASE_2_THREAD_COUNT, new ThreadFactory() {
                    private final AtomicInteger counter = new AtomicInteger(0);
                    @Override
                    public Thread newThread(Runnable r) {
                        return new Thread(r, "Phase2-Worker-" + counter.incrementAndGet());
                    }
                });

        // Start Phase 2 as soon as ANY Phase 1 thread completes
        Thread phase2Starter = new Thread(() -> {
            try {
                // Wait for the first Phase 1 thread to complete
                Future<Integer> firstCompleted = completionService.take();
                int threadId = firstCompleted.get();
                phase2StartTime.set(System.currentTimeMillis());

                System.out.println("\nPhase 1 thread " + threadId + " completed. Starting Phase 2...");
                System.out.println("Phase 2 will use " + PHASE_2_THREAD_COUNT + " threads to process " +
                        remainingRequests + " remaining requests");

                // Update monitor to include phase 2 executor
                updateProgressMonitor(monitorExecutor, startTime, phase2StartTime, phase1Executor, phase2Executor);

                // Calculate how many tasks we need for the remaining requests
                int tasksForRemaining = (int) Math.ceil((double) remainingRequests / BATCH_SIZE);

                // Submit tasks for remaining requests to Phase 2 executor
                for (int i = 0; i < tasksForRemaining; i++) {
                    int requestCount = (i == tasksForRemaining - 1) ?
                            remainingRequests - (BATCH_SIZE * (tasksForRemaining - 1)) :
                            BATCH_SIZE;

                    phase2Executor.submit(new PostingThreadWithPooling(
                            requestCount,
                            eventQueue,
                            phase2SuccessfulRequests, // Phase 2 specific counters
                            phase2FailedRequests,
                            phase2CompletedRequests,
                            totalSuccessfulRequests, // Overall counters
                            totalFailedRequests,
                            totalCompletedRequests,
                            BASE_URL,
                            connectionManager,
                            2 // Phase identifier
                    ));
                }

                // Wait for Phase 1 to complete fully
                phase1Executor.shutdown();
                boolean phase1Done = phase1Executor.awaitTermination(1, TimeUnit.HOURS);
                phase1Completed.set(true);

                if (!phase1Done) {
                    System.out.println("Warning: Phase 1 did not complete in the allowed timeframe");
                } else {
                    System.out.println("Phase 1 completed successfully");
                    // Print Phase 1 metrics
                    printPhaseResults(startTime, phase2StartTime.get(), initialPhaseRequests, INITIAL_THREADS, 1,
                            phase1SuccessfulRequests.get(), phase1FailedRequests.get());
                }

                // Wait for Phase 2 to complete
                phase2Executor.shutdown();
                boolean phase2Done = phase2Executor.awaitTermination(2, TimeUnit.HOURS);
                phase2Completed.set(true);

                if (!phase2Done) {
                    System.out.println("Warning: Phase 2 did not complete in the allowed timeframe");
                } else {
                    System.out.println("Phase 2 completed successfully");
                }

            } catch (Exception e) {
                System.err.println("Error managing phases: " + e.getMessage());
                e.printStackTrace();
            }
        }, "Phase2-Starter");

        phase2Starter.start();

        // Wait for both phases to complete
        phase2Starter.join();

        // Ensure both phases are properly shut down
        if (!phase1Completed.get()) {
            phase1Executor.shutdownNow();
        }
        if (!phase2Completed.get()) {
            phase2Executor.shutdownNow();
        }

        // Shutdown the monitor
        monitorExecutor.shutdownNow();

        // Stop the event generator
        generatorRunning.set(false);
        generator.interrupt();

        long endTime = System.currentTimeMillis();

        // Print final results for Phase 2
        if (phase2StartTime.get() > 0) {
            printPhaseResults(phase2StartTime.get(), endTime, remainingRequests, PHASE_2_THREAD_COUNT, 2,
                    phase2SuccessfulRequests.get(), phase2FailedRequests.get());
        }

        // Print overall results
        printOverallResults(startTime, endTime, TOTAL_REQUESTS,
                totalSuccessfulRequests.get(), totalFailedRequests.get());

        // Print detailed performance metrics
        PostingThreadWithPooling.printPerformanceMetrics(totalCompletedRequests.get(), endTime - startTime);

        // Close the connection manager
        connectionManager.close();
    }

    private static PoolingHttpClientConnectionManager createConnectionManager() {
        PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager();
        // Optimize the connection manager
        connectionManager.setMaxTotal(PHASE_2_THREAD_COUNT * 2); // Max connections overall
        connectionManager.setDefaultMaxPerRoute(PHASE_2_THREAD_COUNT); // Max connections per route
        connectionManager.setValidateAfterInactivity(1000); // Reduce connection validation frequency
        return connectionManager;
    }

    private static ScheduledExecutorService startProgressMonitor(
            long startTime,
            AtomicLong phase2StartTime,
            ThreadPoolExecutor phase1Executor,
            ThreadPoolExecutor phase2Executor) {

        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(() -> {
            long currentTime = System.currentTimeMillis();
            double elapsedSeconds = (currentTime - startTime) / 1000.0;

            int phase1Completed = phase1CompletedRequests.get();
            int phase1Successful = phase1SuccessfulRequests.get();
            int phase1Failed = phase1FailedRequests.get();

            int phase2Completed = phase2CompletedRequests.get();
            int phase2Successful = phase2SuccessfulRequests.get();
            int phase2Failed = phase2FailedRequests.get();

            int totalCompleted = totalCompletedRequests.get();
            double overallThroughput = totalCompleted / elapsedSeconds;

            int activePhase1Threads = phase1Executor != null ? phase1Executor.getActiveCount() : 0;
            int poolSize1 = phase1Executor != null ? phase1Executor.getPoolSize() : 0;

            int activePhase2Threads = phase2Executor != null ? phase2Executor.getActiveCount() : 0;
            int poolSize2 = phase2Executor != null ? phase2Executor.getPoolSize() : 0;

            int queueSize = eventQueue.size();

            // Calculate phase-specific throughput
            double phase1Throughput = phase1Completed / elapsedSeconds;
            double phase2Throughput = 0;
            if (phase2StartTime.get() > 0) {
                double phase2ElapsedSeconds = (currentTime - phase2StartTime.get()) / 1000.0;
                if (phase2ElapsedSeconds > 0) {
                    phase2Throughput = phase2Completed / phase2ElapsedSeconds;
                }
            }

            System.out.println("\n===== PROGRESS REPORT =====");
            System.out.printf("Time elapsed: %.2f seconds\n", elapsedSeconds);
            System.out.printf("Queue size: %d\n", queueSize);

            System.out.println("\nPHASE 1:");
            System.out.printf("Completed: %d/%d (%.2f%%) | Success: %d | Failed: %d\n",
                    phase1Completed, INITIAL_THREADS * REQUESTS_PER_INITIAL_THREAD,
                    (phase1Completed * 100.0 / (INITIAL_THREADS * REQUESTS_PER_INITIAL_THREAD)),
                    phase1Successful, phase1Failed);
            System.out.printf("Active threads: %d/%d | Throughput: %.2f req/sec\n",
                    activePhase1Threads, poolSize1, phase1Throughput);

            if (phase2StartTime.get() > 0) {
                System.out.println("\nPHASE 2:");
                System.out.printf("Completed: %d/%d (%.2f%%) | Success: %d | Failed: %d\n",
                        phase2Completed, TOTAL_REQUESTS - (INITIAL_THREADS * REQUESTS_PER_INITIAL_THREAD),
                        (phase2Completed * 100.0 / (TOTAL_REQUESTS - (INITIAL_THREADS * REQUESTS_PER_INITIAL_THREAD))),
                        phase2Successful, phase2Failed);
                System.out.printf("Active threads: %d/%d | Throughput: %.2f req/sec\n",
                        activePhase2Threads, poolSize2, phase2Throughput);
            }

            System.out.println("\nOVERALL:");
            System.out.printf("Completed: %d/%d (%.2f%%)\n",
                    totalCompleted, TOTAL_REQUESTS,
                    (totalCompleted * 100.0 / TOTAL_REQUESTS));
            System.out.printf("Overall throughput: %.2f req/sec\n", overallThroughput);
            System.out.println("===========================");

        }, 5, 5, TimeUnit.SECONDS);

        return scheduler;
    }

    private static void updateProgressMonitor(
            ScheduledExecutorService monitorExecutor,
            long startTime,
            AtomicLong phase2StartTime,
            ThreadPoolExecutor phase1Executor,
            ThreadPoolExecutor phase2Executor) {
        // Nothing to do - the monitor will automatically pick up phase2Executor
        // since we're passing it by reference
    }

    private static void printPhaseResults(
            long startTime, long endTime, int requests, int threads, int phase,
            int successful, int failed) {

        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        double throughput = requests / totalTimeSeconds;

        System.out.println("\nPhase " + phase + " Results:");
        System.out.println("========================================");
        System.out.println("Thread count: " + threads);
        System.out.println("Total requests: " + requests);
        System.out.println("Successful requests: " + successful);
        System.out.println("Failed requests: " + failed);
        System.out.println("Total time: " + String.format("%.2f", totalTimeSeconds) + " seconds");
        System.out.println("Throughput: " + String.format("%.2f", throughput) + " requests/second");
        System.out.println("========================================");
    }

    private static void printOverallResults(
            long startTime, long endTime, int totalRequests,
            int successful, int failed) {

        double totalTimeSeconds = (endTime - startTime) / 1000.0;
        double throughput = totalRequests / totalTimeSeconds;

        System.out.println("\nOverall Results:");
        System.out.println("========================================");
        System.out.println("Phase 1: " + INITIAL_THREADS + " threads with " + REQUESTS_PER_INITIAL_THREAD + " requests each");
        System.out.println("Phase 2: " + PHASE_2_THREAD_COUNT + " threads with remaining requests");
        System.out.println("Total Requests: " + totalRequests);
        System.out.println("Successful requests: " + successful);
        System.out.println("Failed requests: " + failed);
        System.out.println("Total time: " + String.format("%.2f", totalTimeSeconds) + " seconds");
        System.out.println("Overall Throughput: " + String.format("%.2f", throughput) + " requests/second");
        System.out.println("========================================");
    }
}