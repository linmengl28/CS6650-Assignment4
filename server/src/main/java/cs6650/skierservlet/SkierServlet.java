package cs6650.skierservlet;

import com.google.gson.Gson;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@WebServlet(value = "/skiers/*", loadOnStartup = 1)
public class SkierServlet extends HttpServlet {
    private final Gson gson = new Gson();
    private MessageQueueService messageQueueService;

    // URL validation pattern for post - matches /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    private static final Pattern SKIER_URL_PATTERN =
            Pattern.compile("^/(\\d+)/seasons/([^/]+)/days/(\\d+)/skiers/(\\d+)$");
    // URL validation pattern for get
    private static final Pattern skierDayPattern = Pattern.compile("^/(\\d+)/seasons/([^/]+)/days/(\\d+)/skiers/(\\d+)$");
    private static final Pattern verticalPattern = Pattern.compile("^/(\\d+)/vertical$");
    @Override
    public void init() throws ServletException {
        super.init();
        System.out.println("SkierServlet initializing");

        try {
            // Initialize the message queue service - single responsibility
            messageQueueService = new RabbitMQService();
            messageQueueService.initialize();
        } catch (Exception e) {
            System.err.println("Failed to initialize message queue service: " + e.getMessage());
            e.printStackTrace();
            throw new ServletException("Failed to initialize message queue service", e);
        }
    }
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        res.setContentType("application/json");
        PrintWriter out = res.getWriter();
        String pathInfo = req.getPathInfo();

        try {
            if (pathInfo == null) {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid URL format");
                return;
            }

            // Pattern for: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
            Matcher skierDayMatcher = skierDayPattern.matcher(pathInfo);

            // Pattern for: /skiers/{skierID}/vertical
            Matcher verticalMatcher = verticalPattern.matcher(pathInfo);

            if (skierDayMatcher.matches()) {
                // Handle GET for specific skier's vertical on a specific day
                int resortID = Integer.parseInt(skierDayMatcher.group(1));
                String seasonID = skierDayMatcher.group(2);
                int dayID = Integer.parseInt(skierDayMatcher.group(3));
                int skierID = Integer.parseInt(skierDayMatcher.group(4));

                // Get the total vertical for this skier on this specific day
                int totalVertical = getSkierDayVertical(skierID, resortID, seasonID, dayID);

                // Return the result
                res.setStatus(HttpServletResponse.SC_OK);
                out.println("{\"resortID\": " + resortID +
                        ", \"seasonID\": \"" + seasonID +
                        "\", \"dayID\": " + dayID +
                        ", \"skierID\": " + skierID +
                        ", \"totalVert\": " + totalVertical + "}");

            } else if (verticalMatcher.matches()) {
                // Handle GET for total vertical across specified seasons
                int skierID = Integer.parseInt(verticalMatcher.group(1));

                // Get optional query parameters
                String resort = req.getParameter("resort");
                String season = req.getParameter("season");

                // Get the total vertical
                VerticalData verticalData = getSkierVertical(skierID, resort, season);

                // Return the result
                res.setStatus(HttpServletResponse.SC_OK);
                out.println(gson.toJson(verticalData));

            } else {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid URL format. Expected: /skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID} or /skiers/{skierID}/vertical");
            }
        } catch (NumberFormatException e) {
            sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid numeric parameters");
        } catch (Exception e) {
            System.err.println("Error processing request: " + e.getMessage());
            e.printStackTrace();
            sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server error: " + e.getMessage());
        }
    }

    // Get vertical for a skier on a specific day
    private int getSkierDayVertical(int skierID, int resortID, String seasonID, int dayID) {
        // TODO: Implement actual database query to DynamoDB
        // For now, return mock data
        return 12345; // Mock vertical feet
    }

    // Get vertical data for a skier across multiple seasons
    private VerticalData getSkierVertical(int skierID, String resortParam, String seasonParam) {
        // TODO: Implement actual database query to DynamoDB
        // For now, return mock data
        VerticalData data = new VerticalData();
        data.setSkierID(skierID);

        // Add some mock resort data
        ResortVertical resort1 = new ResortVertical();
        resort1.setResortID(1);
        resort1.setTotalVert(42000);

        ResortVertical resort2 = new ResortVertical();
        resort2.setResortID(2);
        resort2.setTotalVert(36000);

        data.addResort(resort1);
        data.addResort(resort2);

        return data;
    }

    // Helper classes for vertical data
    private static class VerticalData {
        private int skierID;
        private List<ResortVertical> resorts = new ArrayList<>();

        public void setSkierID(int skierID) {
            this.skierID = skierID;
        }

        public void addResort(ResortVertical resort) {
            resorts.add(resort);
        }

        // Getters omitted for brevity
    }

    private static class ResortVertical {
        private int resortID;
        private int totalVert;

        public void setResortID(int resortID) {
            this.resortID = resortID;
        }

        public void setTotalVert(int totalVert) {
            this.totalVert = totalVert;
        }

        // Getters omitted for brevity
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