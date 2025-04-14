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
    private static final int CACHE_TTL = 60; // TTL in seconds for cache entries

    public void initialize() throws ServletException {
        try {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(128); // Match your JMeter thread count
            poolConfig.setMaxIdle(32);

//            poolConfig.setTestOnBorrow(true);
//            poolConfig.setTestWhileIdle(true);
//            poolConfig.setMaxWaitMillis(1000); // 1 second timeout for connection
//            poolConfig.setBlockWhenExhausted(false); // Don't block when pool is exhausted
            jedisPool = new JedisPool(poolConfig, REDIS_HOST, REDIS_PORT);
            System.out.println("Redis connection pool initialized successfully");
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
    // For vertical data, which changes less frequently
    public void setVerticalData(String key, String value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, 300, value); // 5 minutes for vertical data
        } catch (Exception e) {
            System.err.println("Error setting in Redis: " + e.getMessage());
        }
    }

    // For unique skier counts, which may change more frequently
    public void setUniqueSkierCount(String key, int value) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, 60, String.valueOf(value)); // 1 minute
        } catch (Exception e) {
            System.err.println("Error setting in Redis: " + e.getMessage());
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
    // Add retry logic for Redis get operations
    public String get(String key) {
        int maxRetries = 2;
        for (int i = 0; i <= maxRetries; i++) {
            try (Jedis jedis = jedisPool.getResource()) {
                return jedis.get(key);
            } catch (Exception e) {
                if (i == maxRetries) {
                    System.err.println("Error getting from Redis: " + e.getMessage());
                    return null;
                }
                // Brief exponential backoff
                try {
                    Thread.sleep(50 * (long)Math.pow(2, i));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return null;
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