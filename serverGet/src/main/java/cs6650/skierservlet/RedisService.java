package cs6650.skierservlet;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.servlet.ServletException;

public class RedisService {
    private JedisPool jedisPool;
    private static final String REDIS_HOST = System.getenv("REDIS_HOST") != null ?
            System.getenv("REDIS_HOST") : "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int CACHE_TTL = 120; // TTL in seconds for cache entries

    public void initialize() throws ServletException {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(128); // Same as JMeter thread count
            poolConfig.setMaxIdle(32);

            // Create pool without authentication - fixes the error in your stack trace
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
            System.out.println("Redis connection pool initialized successfully at " + REDIS_HOST);
        } catch (Exception e) {
            System.err.println("Failed to initialize Redis: " + e.getMessage());
            e.printStackTrace();
            throw new ServletException("Failed to initialize Redis", e);
        }
    }

    public void shutdown() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
        }
    }

    // Cache key generation methods
    public String generateSkierDayVerticalKey(int skierID, int resortID, String seasonID, int dayID) {
        return String.format("skier:%d:resort:%d:season:%s:day:%d:vertical",
                skierID, resortID, seasonID, dayID);
    }

    public String generateSkierVerticalKey(int skierID, String resortParam, String seasonParam) {
        String resort = (resortParam != null && !resortParam.isEmpty()) ? resortParam : "all";
        String season = (seasonParam != null && !seasonParam.isEmpty()) ? seasonParam : "all";
        return String.format("skier:%d:resort:%s:season:%s:vertical", skierID, resort, season);
    }

    public String generateUniqueSkiersKey(int resortID, String seasonID, int dayID) {
        return String.format("resort:%d:season:%s:day:%d:uniqueskiers", resortID, seasonID, dayID);
    }

    // String value getter/setter with TTL
    public String get(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        } catch (Exception e) {
            System.err.println("Error getting from Redis: " + e.getMessage());
            return null;
        }
    }

    public void set(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, CACHE_TTL, value);
        } catch (Exception e) {
            System.err.println("Error setting in Redis: " + e.getMessage());
        }
    }

    // Integer value getter/setter with TTL
    public Integer getInt(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key);
            return (value != null) ? Integer.parseInt(value) : null;
        } catch (Exception e) {
            System.err.println("Error getting integer from Redis: " + e.getMessage());
            return null;
        }
    }

    public void setInt(String key, int value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, CACHE_TTL, String.valueOf(value));
        } catch (Exception e) {
            System.err.println("Error setting integer in Redis: " + e.getMessage());
        }
    }
}