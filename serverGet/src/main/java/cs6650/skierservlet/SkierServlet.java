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
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@WebServlet(value = "/skiers/*", loadOnStartup = 1)
public class SkierServlet extends HttpServlet {
    private final Gson gson = new Gson();
    private DynamoDbClient dynamoDbClient;
    private RedisService redisService; // Redis service
    private static final String SKIER_RIDES_TABLE = "SkierRides";
    private static final Region AWS_REGION = Region.US_WEST_2;

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
            // Initialize DynamoDB client
            dynamoDbClient = DynamoDbClient.builder()
                    .region(AWS_REGION)
                    .build();
            System.out.println("DynamoDB client initialized successfully");

            // Initialize Redis service
            redisService = new RedisService();
            redisService.initialize();

        } catch (Exception e) {
            System.err.println("Failed to initialize services: " + e.getMessage());
            e.printStackTrace();
            throw new ServletException("Failed to initialize services", e);
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

                // Get the total vertical
                VerticalData verticalData = getSkierVertical(skierID);

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

    // skiers/{resortID}/seasons/{seasonID}/days/{dayID}/skiers/{skierID}
    // Get vertical for a skier on a specific day
    private int getSkierDayVertical(int skierID, int resortID, String seasonID, int dayID) {
        // Check Redis cache first
        String cacheKey = redisService.generateSkierDayVerticalKey(skierID, resortID, seasonID, dayID);
        Integer cachedValue = redisService.getInt(cacheKey);

        if (cachedValue != null) {
            return cachedValue;
        }

        // If not in cache, query DynamoDB using new day-skier-index
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":dayId", AttributeValue.builder().n(String.valueOf(dayID)).build());
        expressionValues.put(":skierId", AttributeValue.builder().n(String.valueOf(skierID)).build());
        // Add filter for the fixed values
        expressionValues.put(":resortId", AttributeValue.builder().n(String.valueOf(resortID)).build());
        expressionValues.put(":seasonId", AttributeValue.builder().s(seasonID).build());

        // Use day-skier-index with appropriate filter
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(SKIER_RIDES_TABLE)
                .indexName("day-skier-index")
                .keyConditionExpression("dayId = :dayId AND skierId = :skierId")
                .filterExpression("resortId = :resortId AND seasonId = :seasonId")
                .expressionAttributeValues(expressionValues)
                // Reduce response size with projection
                .projectionExpression("vertical")
                .build();

        int totalVertical = 0;
        try {
            QueryResponse response = dynamoDbClient.query(queryRequest);
            for (Map<String, AttributeValue> item : response.items()) {
                if (item.containsKey("vertical")) {
                    totalVertical += Integer.parseInt(item.get("vertical").n());
                }
            }

            Map<String, AttributeValue> lastEvaluatedKey = response.lastEvaluatedKey();
            while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                queryRequest = queryRequest.toBuilder()
                        .exclusiveStartKey(lastEvaluatedKey)
                        .build();

                response = dynamoDbClient.query(queryRequest);
                for (Map<String, AttributeValue> item : response.items()) {
                    if (item.containsKey("vertical")) {
                        totalVertical += Integer.parseInt(item.get("vertical").n());
                    }
                }

                lastEvaluatedKey = response.lastEvaluatedKey();
            }

            // Cache the result - Fixed: Added this missing cache update
            redisService.setInt(cacheKey, totalVertical);

            return totalVertical;
        } catch (DynamoDbException e) {
            System.err.println("Error querying DynamoDB: " + e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }

    // Get vertical data for a skier across all days
    // /skiers/{skierID}/vertical
    // get the total vertical for the skier with fixed resort and season values
    private VerticalData getSkierVertical(int skierID) {
        // Fixed: Using the proper method to generate the cache key
        String cacheKey = redisService.generateSkierVerticalKey(skierID, null, null);
        String cachedJson = redisService.get(cacheKey);

        if (cachedJson != null) {
            return gson.fromJson(cachedJson, VerticalData.class);
        }

        // If not in cache, query DynamoDB
        VerticalData data = new VerticalData();
        data.setSkierID(skierID);

        // Since we know resort and season are fixed, we can just query by skierId
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":skierId", AttributeValue.builder().n(String.valueOf(skierID)).build());

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(SKIER_RIDES_TABLE)
                .keyConditionExpression("skierId = :skierId")
                .expressionAttributeValues(expressionValues)
                // Add projection to include only needed fields
                .projectionExpression("resortId, vertical")
                // Add a limit to reduce response time by processing in smaller batches
                .limit(100)
                .build();

        try {
            // We maintain the resort map structure even though we have one fixed resort
            // This maintains compatibility with the existing VerticalData class
            Map<Integer, Integer> resortVerticalMap = new HashMap<>();

            QueryResponse response = dynamoDbClient.query(queryRequest);
            processSkierVerticalResults(response.items(), resortVerticalMap);

            Map<String, AttributeValue> lastEvaluatedKey = response.lastEvaluatedKey();
            while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                queryRequest = queryRequest.toBuilder()
                        .exclusiveStartKey(lastEvaluatedKey)
                        .build();

                response = dynamoDbClient.query(queryRequest);
                processSkierVerticalResults(response.items(), resortVerticalMap);

                lastEvaluatedKey = response.lastEvaluatedKey();
            }

            // Convert the map to the expected format
            for (Map.Entry<Integer, Integer> entry : resortVerticalMap.entrySet()) {
                ResortVertical resort = new ResortVertical();
                resort.setResortID(entry.getKey());
                resort.setTotalVert(entry.getValue());
                data.addResort(resort);
            }

            // Cache the result with the proper key
            redisService.set(cacheKey, gson.toJson(data));
            return data;
        } catch (DynamoDbException e) {
            System.err.println("Error querying DynamoDB: " + e.getMessage());
            e.printStackTrace();
            return data;
        }
    }

    // Fixed: Added proper null check for resortId
    private void processSkierVerticalResults(List<Map<String, AttributeValue>> items,
                                             Map<Integer, Integer> resortVerticalMap) {
        for (Map<String, AttributeValue> item : items) {
            if (item.containsKey("vertical") && item.containsKey("resortId")) {
                int resortId = Integer.parseInt(item.get("resortId").n());
                int vertical = Integer.parseInt(item.get("vertical").n());
                // update total vertical distance
                resortVerticalMap.merge(resortId, vertical, Integer::sum);
            }
        }
    }

    private static class VerticalData {
        private int skierID;
        private List<ResortVertical> resorts = new ArrayList<>();

        public void setSkierID(int skierID) {
            this.skierID = skierID;
        }
        public int getSkierID() {return skierID;}
        public void addResort(ResortVertical resort) {
            resorts.add(resort);
        }
        public List<ResortVertical> getResorts() {return resorts;}
    }

    private static class ResortVertical {
        private int resortID;
        private int totalVert;

        public void setResortID(int resortID) {
            this.resortID = resortID;
        }
        public int getResortID() {return resortID;}
        public void setTotalVert(int totalVert) {
            this.totalVert = totalVert;
        }
        public int getTotalVert() {return totalVert;}
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse res)
            throws ServletException, IOException {
        // Return error, this instance cannot support post request
        sendErrorResponse(res, HttpServletResponse.SC_METHOD_NOT_ALLOWED,
                "This server instance does not accept POST requests");
    }

    private void sendErrorResponse(HttpServletResponse res, int status, String message) throws IOException {
        res.setStatus(status);
        PrintWriter out = res.getWriter();
        out.println("{\"message\": \"" + message + "\"}");
    }

    @Override
    public void destroy() {
        // Clean up resources
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        if (redisService != null) {
            redisService.shutdown();
        }
        super.destroy();
    }
}