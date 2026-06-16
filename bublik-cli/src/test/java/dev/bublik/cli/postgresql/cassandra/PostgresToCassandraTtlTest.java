package dev.bublik.cli.postgresql.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import dev.bublik.cli.App;
import dev.bublik.cli.TestResult;
import dev.bublik.cli.TestUtils;
import dev.bublik.cli.addons.Utils;
import dev.bublik.core.model.Config;
import dev.bublik.core.model.ConnectionProperty;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.cassandra.CassandraContainer;
import org.testcontainers.containers.JdbcDatabaseContainer;
import org.testcontainers.containers.PostgreSQLContainer;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static dev.bublik.cli.App.getConfigs;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PostgresToCassandraTtlTest {

    private static int rows = 50000;
    private static boolean sync = false;

    private static JdbcDatabaseContainer<?> source = new PostgreSQLContainer<>("postgres")
            .withDatabaseName("postgres")
            .withInitScript("./postgresql/cassandra/sql/pg-init-cache.sql")
            .withExposedPorts(5432);
    private static CassandraContainer target = new CassandraContainer("cassandra")
            .withEnv("CASSANDRA_USER", "cassandra")
            .withEnv("CASSANDRA_PASSWORD", "cassandra")
            .withEnv("CASSANDRA_AUTHENTICATOR", "PasswordAuthenticator")
            .withEnv("CASSANDRA_NUM_TOKENS", "16")
            .withInitScript("./postgresql/cassandra/sql/cs-init-cache.cql")
            .withEnv("TZ", "Europe/Moscow")
            .withExposedPorts(9042);

    @BeforeAll
    static void setUp() throws Exception {
        source.setPortBindings(Collections.singletonList("5432:5432"));
        source.start();
        target.setPortBindings(Collections.singletonList("9042:9042"));
        target.start();

        // Ожидание готовности PostgreSQL
        while (!source.isRunning()) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // Явная инициализация схемы Cassandra после старта контейнера
        // Это гарантирует применение изменений из cs-init-cache.cql при переиспользовании контейнера
        try (CqlSession session = CqlSession.builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials("cassandra", "cassandra")
                .build()) {
            session.execute("DROP KEYSPACE IF EXISTS test");
            session.execute("CREATE KEYSPACE test WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : '1' }");
            session.execute("CREATE TABLE test.ttl_check (client_id text, offer_id bigint, client_type tinyint, flags tinyint, temp_aud text, primary key (client_id))");
        }
    }

    @AfterAll
    static void clear() {
        source.stop();
        target.stop();
        while (source.isRunning() || target.isRunning()) {
            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @DisplayName("TTL из кэша - проверка значений TTL для разных сервисов")
    @Test
    public void ttlFromCacheValues() throws IOException, SQLException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);

        TestResult result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-cache.yaml",
                "./postgresql/cassandra/json/pg2cs-cache.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);

        assertEquals(result.sourceCount(), result.targetCount());

        // Получаем фактические TTL из Cassandra
        Map<Long, Integer> actualTtlMap =
                getTargetTtlByOfferId(targetProperties, "SELECT client_id, offer_id, ttl(offer_id) as ttl FROM test.ttl_check");

        // Рассчитываем ожидаемые TTL из кэш-таблицы ПОСЛЕ миграции (т.к. TTL записывается в момент миграции)
        Map<Long, Integer> expectedTtlMap = getExpectedTtlMap(sourceProperties);

        System.out.println("Expected TTL map: " + expectedTtlMap);
        System.out.println("Actual TTL map: " + actualTtlMap);

        // Сравниваем TTL для каждой записи
        for (Map.Entry<Long, Integer> entry : actualTtlMap.entrySet()) {
            Long offerId = entry.getKey();
            Integer actualTtl = entry.getValue();

            if (offerId == 99999L) {
                // offer_id=99999 отсутствует в кэше - TTL по умолчанию 2 года
                assert actualTtl != null && actualTtl > 62900000 && actualTtl <= 63100000 :
                        "TTL для offer_id=99999 (отсутствует в кэше) должен быть 2 года, фактически: " + actualTtl;
            } else if (offerId == 77777L) {
                // offer_id=77777 - отрицательный TTL (оффер уже закрыт), заменён на 1 неделю
                assert actualTtl != null && actualTtl >= 604700 && actualTtl <= 604900 :
                        "TTL для offer_id=77777 (отрицательный - оффер уже закрыт) должен быть 1 неделя, фактически: " + actualTtl;
            } else {
                // Остальные записи - сравниваем с расчётным значением (допуск ±2 дня на время выполнения миграции)
                // Для HOTELS_POSTPAY (12345, 23456) TTL ограничен 4 годами
                Integer expectedTtl = expectedTtlMap.get(offerId);
                assert expectedTtl != null : "Ожидаемый TTL не найден для offer_id=" + offerId;
                assert actualTtl != null && Math.abs(actualTtl - expectedTtl) <= 172900 :
                        "TTL для offer_id=" + offerId + " должен быть ~" + expectedTtl + ", фактически: " + actualTtl + " (разница: "
                        + Math.abs(actualTtl - expectedTtl) + ")";
            }
        }
    }

    @DisplayName("flags из кэша - проверка значений flags для разных типов таргетирования")
    @Test
    public void flagsFromCacheValues() throws IOException, SQLException {
        Properties sourceProperties = getJdbcProperties(source);
        Properties targetProperties = getJdbcPropertiesOfCassandra(target);

        TestResult result = getResult(
                "./postgresql/cassandra/yaml/pg2cs-cache.yaml",
                "./postgresql/cassandra/json/pg2cs-cache.json",
                rows,
                sync,
                sourceProperties,
                targetProperties);

        assertEquals(result.sourceCount(), result.targetCount());

        // Получаем ожидаемые flags из кэш-таблицы (targeting_type='DYNAMIC' → 4, иначе 0)
        Map<Long, Byte> expectedFlags = getExpectedFlagsMap(sourceProperties);

        // Получаем фактические flags из Cassandra
        Map<Long, Byte> actualFlagsMap = getTargetByOfferId(targetProperties, "SELECT client_id, offer_id, flags FROM test.ttl_check");
        System.out.println("Expected flags map: " + expectedFlags);
        System.out.println("Actual flags map: " + actualFlagsMap);

        // Сравниваем flags для каждой записи
        for (Map.Entry<Long, Byte> entry : actualFlagsMap.entrySet()) {
            Long offerId = entry.getKey();
            Byte actualFlags = entry.getValue();
            Byte expectedFlagsValue = expectedFlags.get(offerId);

            if (expectedFlagsValue == null) {
                // offer_id отсутствует в кэше targeting_type
                assert actualFlags != null && actualFlags == 0 :
                        "flags для offer_id=" + offerId + " (отсутствует в кэше) должен быть 0, фактически: " + actualFlags;
            } else {
                assert actualFlags != null && actualFlags == expectedFlagsValue :
                        "flags для offer_id=" + offerId + " должен быть " + expectedFlagsValue + ", фактически: " + actualFlags;
            }
        }
    }

    /**
     * Получает ожидаемые flags из кэш-таблицы offer.
     * targeting_type='DYNAMIC' → flags=4, иначе flags=0
     */
    private Map<Long, Byte> getExpectedFlagsMap(Properties pgProperties) throws SQLException {
        Map<Long, Byte> expectedFlags = new HashMap<>();
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                pgProperties.getProperty("url"), pgProperties.getProperty("user"), pgProperties.getProperty("password"))) {
            java.sql.Statement stmt = conn.createStatement();
            java.sql.ResultSet rs = stmt.executeQuery("SELECT id, targeting_type FROM public.offer");
            while (rs.next()) {
                long id = rs.getLong("id");
                String targetingType = rs.getString("targeting_type");
                byte flags = "DYNAMIC".equals(targetingType) ? (byte) 0b00000100 : (byte) 0;
                expectedFlags.put(id, flags);
            }
        }
        return expectedFlags;
    }


    public static TestResult getResult(String connectionPropertyFile,
            String mappingFile,
            int rows,
            boolean sync,
            Properties sourceProperties,
            Properties targetProperties) throws IOException {
        ConnectionProperty cp = Utils.connectionProperty(TestUtils.getFilePath(connectionPropertyFile));

        List<Config> configs = getConfigs(TestUtils.getFilePath(mappingFile));
        App.runProcess(cp, configs, rows, sync, null);

        long sourceCount = 0;
        long targetCount = 0;
        for (Config config : configs) {
            sourceCount += countRows(sourceProperties,
                    "SELECT count(1) FROM " + config.fromSchemaName() + "." + config.fromTableName());
            targetCount += countCassandra(targetProperties,
                    "SELECT client_id, offer_id FROM ",
                    (config.toSchemaName() == null ? config.fromSchemaName() : config.toSchemaName()) + "." +
                    (config.toTableName() == null ? config.fromTableName() : config.toTableName()),
                    null);
        }
        return new TestResult(sourceCount, targetCount);
    }

    private static long countCassandra(Properties properties, String query, String tableName, Object p) {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .build();

        String q = query + tableName;
        System.out.println(q);
        ResultSet resultSet = cqlSession.execute(q);
        long rowCount = 0;
        for (Row row : resultSet) {
            rowCount++;
        }
        cqlSession.close();
        return rowCount;
    }

    private static long countRows(Properties p, String query) {
        try (java.sql.Connection connection =
                java.sql.DriverManager.getConnection(p.getProperty("url"), p.getProperty("user"), p.getProperty("password"))) {
            java.sql.Statement statement = connection.createStatement();
            java.sql.ResultSet resultSet = statement.executeQuery(query);
            resultSet.next();
            return resultSet.getLong(1);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public Map<Long, Integer> getTargetTtlByOfferId(Properties properties, String query) {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .build();

        ResultSet resultSet = cqlSession.execute(query);
        Map<Long, Integer> ttlMap = new HashMap<>(4);
        for (Row row : resultSet) {
            long offerId = row.getLong("offer_id");
            int ttl = row.getInt("ttl");
            ttlMap.put(offerId, ttl);
        }
        cqlSession.close();
        return ttlMap;
    }

    public Map<Long, Byte> getTargetByOfferId(Properties properties, String query) {
        CqlSession cqlSession = CqlSession
                .builder()
                .addContactPoint(java.net.InetSocketAddress.createUnresolved("localhost", 9042))
                .withLocalDatacenter("datacenter1")
                .withAuthCredentials(properties.getProperty("user"), properties.getProperty("password"))
                .build();

        ResultSet resultSet = cqlSession.execute(query);
        Map<Long, Byte> ttlMap = new HashMap<>(4);
        for (Row row : resultSet) {
            long offerId = row.getLong("offer_id");
            byte flags = row.getByte("flags");
            ttlMap.put(offerId, flags);
        }
        cqlSession.close();
        return ttlMap;
    }

    private Properties getJdbcProperties(JdbcDatabaseContainer<?> db) {
        Properties properties = new Properties();
        properties.setProperty("url", db.getJdbcUrl());
        properties.setProperty("user", db.getUsername());
        properties.setProperty("password", db.getPassword());
        return properties;
    }

    private Properties getJdbcPropertiesOfCassandra(CassandraContainer target) {
        Properties properties = new Properties();
        properties.setProperty("class", "dev.bublik.cassandra.storage.CassandraStorage");
        properties.setProperty("keyspace", "test");
        properties.setProperty("hosts", "localhost:9042");
        properties.setProperty("user", "cassandra");
        properties.setProperty("password", "cassandra");
        properties.setProperty("datacenter", "datacenter1");
        properties.setProperty("batchSize", "256");
        return properties;
    }

    /**
     * Рассчитывает ожидаемое TTL в секундах на основе close_date и cb_service_name.
     * Формула: EXTRACT(EPOCH FROM (close_date + interval - NOW()))
     * interval зависит от сервиса: HOTELS_POSTPAY=2 года, AVIA/CONCERT=6 месяцев, остальные=1 месяц
     * Максимальное значение TTL ограничено 4 годами (как в CassandraStorage)
     */
    private int calculateExpectedTtl(long offerId, String serviceName, java.sql.Timestamp closeDate) {
        long now = System.currentTimeMillis() / 1000;
        long closeTime = closeDate.getTime() / 1000;
        
        long intervalSeconds;
        if ("HOTELS_POSTPAY".equals(serviceName) || "HOTELS_POSTPAY_PREDICTOR".equals(serviceName)) {
            intervalSeconds = 2L * 365 * 24 * 60 * 60; // 2 года = 63072000 сек
        } else if ("AVIA".equals(serviceName) || "CONCERT".equals(serviceName) || 
                   "SPECTACLE".equals(serviceName) || "EXHIBITION".equals(serviceName) ||
                   "MOVIE".equals(serviceName) || "SHOPPING_BANK".equals(serviceName)) {
            intervalSeconds = 180L * 24 * 60 * 60; // 6 месяцев ≈ 15552000 сек
        } else {
            intervalSeconds = 30L * 24 * 60 * 60; // 1 месяц ≈ 2592000 сек
        }
        
        long ttl = closeTime + intervalSeconds - now;
        
        // Отрицательный TTL заменяется на 1 неделю
        if (ttl < 0) {
            ttl = 7L * 24 * 60 * 60; // 604800 сек
        }
        
        // Ограничиваем TTL сверху 4 годами (как в CassandraStorage)
        int fourYearsInSeconds = 4 * 365 * 24 * 60 * 60;
        if (ttl > fourYearsInSeconds) {
            ttl = fourYearsInSeconds;
        }
        
        return (int) ttl;
    }

    /**
     * Получает данные из кэш-таблицы offer для расчёта ожидаемых TTL
     */
    private Map<Long, Integer> getExpectedTtlMap(Properties pgProperties) throws SQLException {
        Map<Long, Integer> expectedTtl = new HashMap<>();
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                pgProperties.getProperty("url"), pgProperties.getProperty("user"), pgProperties.getProperty("password"))) {
            java.sql.Statement stmt = conn.createStatement();
            java.sql.ResultSet rs = stmt.executeQuery(
                    "SELECT id, cb_service_name, close_date FROM public.offer");
            while (rs.next()) {
                long id = rs.getLong("id");
                String service = rs.getString("cb_service_name");
                java.sql.Timestamp closeDate = rs.getTimestamp("close_date");
                expectedTtl.put(id, calculateExpectedTtl(id, service, closeDate));
            }
        }
        return expectedTtl;
    }
}
