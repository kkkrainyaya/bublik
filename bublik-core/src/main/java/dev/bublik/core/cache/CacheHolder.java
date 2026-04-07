
package dev.bublik.core.cache;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Глобальный кэш для хранения значений из PostgreSQL (например, open_date).
 * Используется при маппинге cassandra -> cassandra для расчёта TTL.
 */
public class CacheHolder {
    private static final Map<Long, Instant> cache = new ConcurrentHashMap<>();
    private static String sourceColumnToCacheKey;

    public static Map<Long, Instant> getCache() {
        return cache;
    }

    public static void clear() {
        cache.clear();
        sourceColumnToCacheKey = null;
    }

    public static Instant get(Long key) {
        return cache.get(key);
    }

    public static void put(Long key, Instant value) {
        cache.put(key, value);
    }

    public static int size() {
        return cache.size();
    }

    public static void setSourceColumnToCacheKey(String columnName) {
        sourceColumnToCacheKey = columnName;
    }

    public static String getSourceColumnToCacheKey() {
        return sourceColumnToCacheKey;
    }
}
