package org.example;

import com.google.gson.Gson;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

public class LiftRideGenerator implements Runnable {
    private final BlockingQueue<String> eventQueue;
    private final AtomicBoolean isRunning;
    private final int queueThreshold;
    private final Gson gson = new Gson();
    private final static Random random = new Random();

    public LiftRideGenerator(BlockingQueue<String> eventQueue, AtomicBoolean isRunning) {
        this.eventQueue = eventQueue;
        this.isRunning = isRunning;
        // Set threshold to 80% of queue capacity
        this.queueThreshold = (int) (eventQueue.remainingCapacity() * 0.8);
    }

    @Override
    public void run() {
        try {
            while (isRunning.get()) {
                // Only generate events if queue is below threshold
                if (eventQueue.size() < queueThreshold) {
                    LiftRideEventWrapper eventWrapper = generateEventWrapper();
                    eventQueue.put(gson.toJson(eventWrapper));
                } else {
                    // Sleep briefly if queue is nearly full
                    Thread.sleep(10);
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static LiftRideEventWrapper generateEventWrapper() {
        // Create a complete event with all fields
        LiftRideEvent fullEvent = new LiftRideEvent(
                random.nextInt(100000) + 1,  // skierID
                1,                    // resortID
                random.nextInt(40) + 1,      // liftID
                "2025",                      // seasonID
                random.nextInt(3) + 1,                           // dayID
                random.nextInt(360) + 1      // time
        );

        // Create a wrapper that contains the full event for processing
        // and the URL path to use for the request
        return new LiftRideEventWrapper(
                fullEvent,
                String.format("/skiers/%d/seasons/%s/days/%d/skiers/%d",
                        fullEvent.getResortID(),
                        fullEvent.getSeasonID(),
                        fullEvent.getDayID(),
                        fullEvent.getSkierID()),
                new LiftRideRequestBody(fullEvent.getLiftID(), fullEvent.getTime())
        );
    }

    // Static class to represent the wrapper for an event
    public static class LiftRideEventWrapper {
        private final LiftRideEvent fullEvent;
        private final String urlPath;
        private final LiftRideRequestBody requestBody;

        public LiftRideEventWrapper(LiftRideEvent fullEvent, String urlPath, LiftRideRequestBody requestBody) {
            this.fullEvent = fullEvent;
            this.urlPath = urlPath;
            this.requestBody = requestBody;
        }

        public LiftRideEvent getFullEvent() {
            return fullEvent;
        }

        public String getUrlPath() {
            return urlPath;
        }

        public LiftRideRequestBody getRequestBody() {
            return requestBody;
        }
    }

    // Class to represent the request body that only includes liftID and time
    public static class LiftRideRequestBody {
        private final int liftID;
        private final int time;

        public LiftRideRequestBody(int liftID, int time) {
            this.liftID = liftID;
            this.time = time;
        }

        public int getLiftID() {
            return liftID;
        }

        public int getTime() {
            return time;
        }
    }
}