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
import java.util.HashMap;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@WebServlet(value = "/resorts/*", loadOnStartup = 1)
public class ResortServlet extends HttpServlet {
    private final Gson gson = new Gson();
    private DynamoDbClient dynamoDbClient;
    private RedisService redisService; // New Redis service
    private static final String SKIER_RIDES_TABLE = "SkierRides";
    private static final Region AWS_REGION = Region.US_WEST_2;

    @Override
    public void init() throws ServletException {
        super.init();
        System.out.println("ResortServlet initializing");
        try{
            // Initialize DynamoDB client
            dynamoDbClient = DynamoDbClient.builder().region(AWS_REGION).build();
            System.out.println("DynamoDB client initialized successfully");

            // Initialize Redis service
            redisService = new RedisService();
            redisService.initialize();

        } catch (Exception e) {
            System.err.println("Error initializing ResortServlet: " + e.getMessage());
            e.printStackTrace();
            throw new ServletException("Failed to initialize ResortServlet", e);
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

            // Pattern for: /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
            Pattern uniqueSkiersPattern = Pattern.compile("^/(\\d+)/seasons/([^/]+)/day/(\\d+)/skiers$");
            Matcher uniqueSkiersMatcher = uniqueSkiersPattern.matcher(pathInfo);

            if (uniqueSkiersMatcher.matches()) {
                int resortID = Integer.parseInt(uniqueSkiersMatcher.group(1));
                String seasonID = uniqueSkiersMatcher.group(2);
                int dayID = Integer.parseInt(uniqueSkiersMatcher.group(3));

                // Get the number of unique skiers
                int numUniqueSkiers = getUniqueSkiersCount(resortID, seasonID, dayID);

                // Return the result
                res.setStatus(HttpServletResponse.SC_OK);
                out.println("{\"resortID\": " + resortID +
                        ", \"seasonID\": \"" + seasonID +
                        "\", \"dayID\": " + dayID +
                        ", \"numSkiers\": " + numUniqueSkiers + "}");
            } else {
                sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST,
                        "Invalid URL format. Expected: /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers");
            }
        } catch (NumberFormatException e) {
            sendErrorResponse(res, HttpServletResponse.SC_BAD_REQUEST, "Invalid numeric parameters");
        } catch (Exception e) {
            System.err.println("Error processing request: " + e.getMessage());
            e.printStackTrace();
            sendErrorResponse(res, HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Server error: " + e.getMessage());
        }
    }

    // /resorts/{resortID}/seasons/{seasonID}/day/{dayID}/skiers
    // get number of unique skiers at resort/season/day
    private int getUniqueSkiersCount(int resortID, String seasonID, int dayID) {
        // Check Redis cache first
        String cacheKey = redisService.generateUniqueSkiersKey(resortID, seasonID, dayID);
        Integer cachedValue = redisService.getInt(cacheKey);

        if (cachedValue != null) {
            System.out.println("Cache hit for " + cacheKey);
            return cachedValue;
        }

        System.out.println("Cache miss for " + cacheKey + ", querying DynamoDB");

        // If not in cache, query DynamoDB
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":resortId", AttributeValue.builder().n(String.valueOf(resortID)).build());
        expressionValues.put(":dayId", AttributeValue.builder().n(String.valueOf(dayID)).build());
        expressionValues.put(":seasonId", AttributeValue.builder().s(seasonID).build());

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(SKIER_RIDES_TABLE)
                .indexName("resort-day-index")
                .keyConditionExpression("resortId = :resortId AND dayId = :dayId")
                .filterExpression("seasonId = :seasonId")
                .expressionAttributeValues(expressionValues)
                .build();

        Set<String> uniqueSkiers = new HashSet<>();
        try {
            QueryResponse response = dynamoDbClient.query(queryRequest);

            for (Map<String, AttributeValue> item : response.items()) {
                if (item.containsKey("skierId")) {
                    uniqueSkiers.add(item.get("skierId").n());
                }
            }

            Map<String, AttributeValue> lastEvaluatedKey = response.lastEvaluatedKey();
            while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty()) {
                queryRequest = queryRequest.toBuilder()
                        .exclusiveStartKey(lastEvaluatedKey)
                        .build();

                response = dynamoDbClient.query(queryRequest);
                for (Map<String, AttributeValue> item : response.items()) {
                    if (item.containsKey("skierId")) {
                        uniqueSkiers.add(item.get("skierId").n());
                    }
                }

                lastEvaluatedKey = response.lastEvaluatedKey();
            }

            int count = uniqueSkiers.size();

            // Cache the result
            redisService.setInt(cacheKey, count);
//            redisService.setUniqueSkierCount(cacheKey, count);
            return count;
        } catch (DynamoDbException e) {
            System.err.println("Error querying DynamoDB: " + e.getMessage());
            e.printStackTrace();
            return 0;
        }
    }

    private void sendErrorResponse(HttpServletResponse res, int status, String message) throws IOException {
        res.setStatus(status);
        PrintWriter out = res.getWriter();
        out.println("{\"message\": \"" + message + "\"}");
    }

    @Override
    public void destroy() {
        if (dynamoDbClient != null) {
            dynamoDbClient.close();
        }
        if (redisService != null) {
            redisService.shutdown();
        }
        super.destroy();
    }
}