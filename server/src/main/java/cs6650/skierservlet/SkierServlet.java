package cs6650.skierservlet;

import com.google.gson.Gson;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@WebServlet(value = "/skiers/*", loadOnStartup = 1)
public class SkierServlet extends HttpServlet {
    private final Gson gson = new Gson();
    private MessageQueueService messageQueueService;
    private DynamoDbClient dynamoDbClient;
    private static final Region AWS_REGION = Region.US_WEST_2;

    // URL validation pattern for post - matches /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    private static final Pattern SKIER_URL_PATTERN =
            Pattern.compile("^/(\\d+)/seasons/([^/]+)/days/(\\d+)/skiers/(\\d+)$");

    @Override
    public void init() throws ServletException {
        super.init();
        System.out.println("SkierServlet initializing");

        try {
            // Initialize the message queue service - single responsibility
            messageQueueService = new RabbitMQService();
            messageQueueService.initialize();

            // Initialize DynamoDB client
            dynamoDbClient = DynamoDbClient.builder()
                    .region(AWS_REGION)
                    .build();
            System.out.println("DynamoDB client initialized successfully");



        } catch (Exception e) {
            System.err.println("Failed to initialize services: " + e.getMessage());
            e.printStackTrace();
            throw new ServletException("Failed to initialize services", e);
        }
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        // Return error, this instance does not support GET requests
        sendErrorResponse(res, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                "This server instance does not accept GET requests");
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

            // 5. Send to queue and wait for result
            final String message = gson.toJson(liftRide);
            CompletableFuture<Boolean> future = messageQueueService.sendMessage(message);

            // Wait for the send operation to complete and check result
            boolean sendSuccess = future.join();

            if (sendSuccess) {
                // 6. Return success to client
                res.setStatus(HttpServletResponse.SC_CREATED);
                out.println("{\"message\": \"Lift ride recorded successfully\"}");
            } else {
                // Return error if sending to queue failed
                sendErrorResponse(res, HttpServletResponse.SC_SERVICE_UNAVAILABLE,
                        "Unable to process lift ride data. Please try again later.");
            }

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

    @Override
    public void destroy() {
        // Clean up resources
        if (messageQueueService != null) {
            messageQueueService.shutdown();
        }
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
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