package cs6650.skierservlet;

import com.google.gson.Gson;
import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

@WebServlet(value = "/resorts/*", loadOnStartup = 1)
public class ResortServlet extends HttpServlet {
    private final Gson gson = new Gson();

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

    private int getUniqueSkiersCount(int resortID, String seasonID, int dayID) {
        // TODO: Implement actual database query to DynamoDB
        // For now, return mock data
        return 78; // Mock number of unique skiers
    }

    private void sendErrorResponse(HttpServletResponse res, int status, String message) throws IOException {
        res.setStatus(status);
        PrintWriter out = res.getWriter();
        out.println("{\"message\": \"" + message + "\"}");
    }
}