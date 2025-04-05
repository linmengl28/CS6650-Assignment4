package cs6650.skierservlet;

import com.google.gson.Gson;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@WebServlet(value = "/skiers/*", loadOnStartup = 1)
public class SkierServlet extends HttpServlet {
    private static final String QUEUE_NAME = "lift_ride_queue";
    private static final String RABBITMQ_HOST = "172.31.20.160";
    private static final int THREAD_POOL_SIZE = 32; // Adjust based on your server capacity

    private final Gson gson = new Gson();
    private ConnectionFactory factory;
    private Connection connection;
    private ExecutorService executorService;

    // URL validation pattern - matches /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    private static final Pattern SKIER_URL_PATTERN =
            Pattern.compile("^/(\\d+)/seasons/([^/]+)/days/(\\d+)/skiers/(\\d+)$");

    @Override
    public void init() throws ServletException {
        super.init();
        System.out.println("SkierServlet initializing");

        // Initialize RabbitMQ connection factory
        factory = new ConnectionFactory();
        factory.setHost(RABBITMQ_HOST);
        factory.setPort(5672);
        factory.setUsername("myuser");         // Replace with your RabbitMQ username
        factory.setPassword("mypassword");     // Replace with your RabbitMQ password

        // Initialize thread pool for async message publishing
        executorService = Executors.newFixedThreadPool(THREAD_POOL_SIZE);

        try {
            // Establish the connection once during initialization
            connection = factory.newConnection();
            System.out.println("Connected to RabbitMQ successfully");

            // Create a channel to declare the queue
            try (Channel channel = connection.createChannel()) {
                // Declare a durable queue (survives broker restarts)
                channel.queueDeclare(QUEUE_NAME, true, false, false, null);
            }
        } catch (IOException | TimeoutException e) {
            System.err.println("Failed to connect to RabbitMQ: " + e.getMessage());
            e.printStackTrace();
            throw new ServletException("Failed to initialize RabbitMQ connection", e);
        }
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        res.setContentType("application/json");
        PrintWriter out = res.getWriter();

        try {
            // 1. Validate URL path
            String pathInfo = req.getPathInfo();

            if (pathInfo == null) {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid URL format");
                return;
            }

            // Check if URL matches expected pattern
            Matcher matcher = SKIER_URL_PATTERN.matcher(pathInfo);
            if (!matcher.matches()) {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid URL format. Expected: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}");
                return;
            }

            // Extract URL parameters
            int resortID = Integer.parseInt(matcher.group(1));
            String seasonID = matcher.group(2);
            int dayID = Integer.parseInt(matcher.group(3));
            int skierID = Integer.parseInt(matcher.group(4));

            // 2. Parse JSON payload (only liftID and time should be in the body)
            BufferedReader reader = req.getReader();
            LiftRideRequest liftRideRequest = gson.fromJson(reader, LiftRideRequest.class);

            if (liftRideRequest == null || liftRideRequest.getLiftID() <= 0 || liftRideRequest.getTime() <= 0) {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid JSON body. Must include liftID and time.");
                return;
            }

            // 3. Create a complete LiftRideEvent from URL params and JSON body
            LiftRideEvent liftRide = new LiftRideEvent(
                    skierID,
                    resortID,
                    liftRideRequest.getLiftID(),
                    seasonID,
                    dayID,
                    liftRideRequest.getTime()
            );

            // 4. Validate the complete lift ride object
            if (!liftRide.isValid()) {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid lift ride parameters");
                return;
            }

            // 5. Send to queue asynchronously
            final String message = gson.toJson(liftRide);
            executorService.submit(() -> sendToQueue(message));

            // 6. Return success to client immediately
            res.setStatus(HttpServletResponse.SC_CREATED);
            out.println("{\"message\": \"Lift ride recorded successfully\"}");

        } catch (NumberFormatException e) {
            sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid numeric parameters");
        } catch (Exception e) {
            System.err.println("Error processing request: " + e.getMessage());
            e.printStackTrace();
            sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                    "Server error: " + e.getMessage());
        }
    }

    private void sendErrorResponse(HttpServletResponse res, int status, String message) throws IOException {
        res.setStatus(status);
        PrintWriter out = res.getWriter();
        out.println("{\"message\": \"" + message + "\"}");
    }

    private void sendToQueue(String message) {
        try (Channel channel = connection.createChannel()) {
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
        } catch (Exception e) {
            System.err.println("Failed to send message to queue: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void destroy() {
        // Clean up resources
        if (executorService != null) {
            executorService.shutdown();
        }

        try {
            if (connection != null && connection.isOpen()) {
                connection.close();
            }
        } catch (IOException e) {
            System.err.println("Error closing RabbitMQ connection: " + e.getMessage());
        }

        super.destroy();
    }

    // Inner class to parse the JSON request body (which only contains liftID and time)
    private static class LiftRideRequest {
        private int liftID;
        private int time;

        public int getLiftID() {
            return liftID;
        }

        public int getTime() {
            return time;
        }
    }
}