
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import okhttp3.*;
import org.bson.Document;

import java.io.IOException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class MongoDBClusterMonitor {
    // Configuration placeholders (should be injected via environment variables or other config methods)
    private static final String GROUP_ID = System.getenv("MONGO_GROUP_ID"); 
    private static final String CLUSTER_NAME = System.getenv("MONGO_CLUSTER_NAME"); 
    private static final String API_PUBLIC_KEY = System.getenv("MONGO_API_PUBLIC_KEY"); 
    private static final String API_PRIVATE_KEY = System.getenv("MONGO_API_PRIVATE_KEY"); 
    private static final String PROCESS_ID = System.getenv("MONGO_PROCESS_ID");
    private static final String MONGO_URI = System.getenv("MONGO_URI"); 

    private static final String MEASUREMENTS_URL = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + GROUP_ID + "/processes/" + PROCESS_ID + "/measurements";
    private static final String CONFIG_URL = "https://cloud.mongodb.com/api/atlas/v1.0/groups/" + GROUP_ID + "/clusters/" + CLUSTER_NAME;

    private static final Map<String, Integer> TIER_MAP = new HashMap<>() {{
        put("M10", 1500); put("M20", 3000); put("M30", 3000); put("M40", 6000);
        put("M50", 16000); put("M60", 32000); put("M80", 96000); put("M140", 96000);
        put("M200", 128000); put("M300", 128000);
    }};

    private static final Map<String, Long> MEMORY_MAP = new HashMap<>() {{
        put("M10", 2L * 1024 * 1024 * 1024); put("M20", 4L * 1024 * 1024 * 1024);
        put("M30", 8L * 1024 * 1024 * 1024); put("M40", 16L * 1024 * 1024 * 1024);
        put("M50", 32L * 1024 * 1024 * 1024); put("M60", 64L * 1024 * 1024 * 1024);
    }};

    private static MongoClient mongoClient;
    private static final AtomicReference<String> currentTier = new AtomicReference<>("M10");
    private static final AtomicInteger maxConnections = new AtomicInteger(TIER_MAP.get("M10"));
    private static final AtomicBoolean monitoringActive = new AtomicBoolean(true);
    private static final OkHttpClient httpClient = new OkHttpClient.Builder()
            .authenticator(new DigestAuthenticator(API_PUBLIC_KEY, API_PRIVATE_KEY))
            .build();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static void main(String[] args) throws InterruptedException {
        validateConfig();
        initializeMongoClient();
        configureAutoScaling();
        simulateTenantRequestsWithMonitoring(5000, 500);
        mongoClient.close();
        System.out.println("[" + Instant.now().toString() + "] MongoDB client closed");
    }

    // Validate configuration completeness
    private static void validateConfig() {
        String[] requiredConfigs = {GROUP_ID, CLUSTER_NAME, API_PUBLIC_KEY, API_PRIVATE_KEY, PROCESS_ID, MONGO_URI};
        for (String config : requiredConfigs) {
            if (config == null || config.isEmpty()) {
                throw new IllegalStateException("Missing required configuration. Check environment variables: MONGO_GROUP_ID, MONGO_CLUSTER_NAME, MONGO_API_PUBLIC_KEY, MONGO_API_PRIVATE_KEY, MONGO_PROCESS_ID, MONGO_URI");
            }
        }
    }

    private static void initializeMongoClient() {
        maxConnections.set(getMaxConnectionsFromAtlas());
        mongoClient = MongoClients.create(
                MongoClientSettings.builder()
                        .applyConnectionString(new com.mongodb.ConnectionString(MONGO_URI))
                        .applyToConnectionPoolSettings(builder -> builder
                                .maxSize(maxConnections.get())
                                .minSize(100)
                                .maxConnectionIdleTime(30, TimeUnit.SECONDS))
                        .applyToSocketSettings(builder -> builder.connectTimeout(10, TimeUnit.SECONDS))
                        .build()
        );
        System.out.println("[" + Instant.now().toString() + "] MongoDB client initialized. Current tier: " + currentTier.get() + ", Max connections: " + maxConnections.get());
    }

    private static int getMaxConnectionsFromAtlas() {
        try {
            Request request = new Request.Builder()
                    .url(CONFIG_URL)
                    .header("Accept", "application/json")
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.code() != 200) {
                    throw new IOException("Failed to fetch cluster config. Status: " + response.code() + ", Response: " + response.body().string());
                }
                JsonNode config = objectMapper.readTree(response.body().string());
                String tier = config.path("providerSettings").path("instanceSizeName").asText();
                currentTier.set(tier);
                int connections = TIER_MAP.getOrDefault(tier, 1500);
                maxConnections.set(connections);
                System.out.println("[" + Instant.now().toString() + "] Retrieved cluster tier: " + tier + ", Max connections: " + connections);
                return connections;
            }
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Failed to get max connections, using default 1500: " + e.getMessage());
            return 1500;
        }
    }

    private static void configureAutoScaling() {
        JsonNode currentConfig;
        try {
            Request request = new Request.Builder().url(CONFIG_URL).header("Accept", "application/json").build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.code() != 200) {
                    throw new IOException("Failed to fetch current cluster config. Status: " + response.code());
                }
                currentConfig = objectMapper.readTree(response.body().string());
            }
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Failed to fetch current cluster config: " + e.getMessage());
            currentConfig = objectMapper.createObjectNode()
                    .putObject("providerSettings")
                    .put("providerName", "AWS")
                    .put("instanceSizeName", "M10")
                    .put("regionName", "US_EAST_1");
        }

        JsonNode providerSettings = currentConfig.path("providerSettings");
        String payload = """
                {
                    "autoScaling": {
                        "compute": {"enabled": true},
                        "diskGBEnabled": true
                    },
                    "providerSettings": {
                        "providerName": "%s",
                        "instanceSizeName": "%s",
                        "regionName": "%s",
                        "autoScaling": {
                            "compute": {
                                "maxInstanceSize": "M60",
                                "minInstanceSize": "M10"
                            }
                        }
                    }
                }
                """.formatted(
                providerSettings.path("providerName").asText("AWS"),
                providerSettings.path("instanceSizeName").asText("M10"),
                providerSettings.path("regionName").asText("US_EAST_1")
        );

        try {
            Request request = new Request.Builder()
                    .url(CONFIG_URL)
                    .patch(RequestBody.create(payload, MediaType.parse("application/json")))
                    .header("Content-Type", "application/json")
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.code() == 200) {
                    System.out.println("[" + Instant.now().toString() + "] Auto-scaling enabled, range: M10 to M60");
                } else {
                    System.out.println("[" + Instant.now().toString() + "] Auto-scaling configuration failed: Status " + response.code() + ", Response " + response.body().string());
                }
            }
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Error configuring auto-scaling: " + e.getMessage());
        }
    }

    private static Map<String, Double> getProcessMeasurements() {
        HttpUrl.Builder urlBuilder = HttpUrl.parse(MEASUREMENTS_URL).newBuilder()
                .addQueryParameter("granularity", "PT1M")
                .addQueryParameter("period", "PT10M")
                .addQueryParameter("m", "PROCESS_CPU_USER")
                .addQueryParameter("m", "PROCESS_CPU_KERNEL")
                .addQueryParameter("m", "MEMORY_RESIDENT");
        Request request = new Request.Builder().url(urlBuilder.build()).header("Accept", "application/json").build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.code() != 200) {
                throw new IOException("Failed to fetch measurements. Status: " + response.code());
            }
            JsonNode measurements = objectMapper.readTree(response.body().string()).path("measurements");
            double cpuUserAvg = calculateAverage(measurements, "PROCESS_CPU_USER");
            double cpuKernelAvg = calculateAverage(measurements, "PROCESS_CPU_KERNEL");
            double memoryAvg = calculateAverage(measurements, "MEMORY_RESIDENT");
            double totalCpuUsage = cpuUserAvg + cpuKernelAvg;
            double memoryUsage = (memoryAvg / MEMORY_MAP.getOrDefault(currentTier.get(), 2L * 1024 * 1024 * 1024)) * 100;
            return Map.of("cpuUsage", totalCpuUsage, "memoryUsage", memoryUsage);
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Failed to fetch measurements: " + e.getMessage());
            return Map.of("cpuUsage", 0.0, "memoryUsage", 0.0);
        }
    }

    private static double calculateAverage(JsonNode measurements, String name) {
        for (JsonNode m : measurements) {
            if (m.path("name").asText().equals(name)) {
                double sum = 0;
                int count = 0;
                for (JsonNode dp : m.path("dataPoints")) {
                    if (!dp.path("value").isNull()) {
                        sum += dp.path("value").asDouble();
                        count++;
                    }
                }
                return count > 0 ? sum / count : 0;
            }
        }
        return 0;
    }

    private static int getCurrentConnections() {
        try {
            Document serverStatus = mongoClient.getDatabase("admin").runCommand(new Document("serverStatus", 1));
            return serverStatus.getEmbedded(List.of("connections", "current"), Integer.class);
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Failed to get current connections: " + e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static void monitorResources() {
        List<String> instanceSizes = List.of("M10", "M20", "M30", "M40", "M50", "M60");

        while (monitoringActive.get()) {
            int currentConnections = getCurrentConnections();
            double connectionUsage = (double) currentConnections / maxConnections.get();
            Map<String, Double> measurements = getProcessMeasurements();
            double cpuUsage = measurements.get("cpuUsage");
            double memoryUsage = measurements.get("memoryUsage");

            System.out.printf("[%s] Real-time monitoring - Connections: %d/%d (%.2f%%)%n", Instant.now(), currentConnections, maxConnections.get(), connectionUsage * 100);
            System.out.printf("[%s] Real-time monitoring - CPU Usage: %.2f%%, Memory Usage: %.2f%%%n", Instant.now(), cpuUsage, memoryUsage);

            int currentIndex = instanceSizes.indexOf(currentTier.get());
            JsonNode currentConfig;
            try {
                Request request = new Request.Builder().url(CONFIG_URL).header("Accept", "application/json").build();
                try (Response response = httpClient.newCall(request).execute()) {
                    currentConfig = objectMapper.readTree(response.body().string());
                }
            } catch (Exception e) {
                System.out.println("[" + Instant.now().toString() + "] Failed to fetch current cluster config: " + e.getMessage());
                currentConfig = objectMapper.createObjectNode().putObject("providerSettings")
                        .put("providerName", "AWS").put("instanceSizeName", currentTier.get()).put("regionName", "US_EAST_1");
            }

            JsonNode providerSettings = currentConfig.path("providerSettings");
            String payloadTemplate = """
                    {
                        "autoScaling": {
                            "compute": {"enabled": true},
                            "diskGBEnabled": true
                        },
                        "providerSettings": {
                            "providerName": "%s",
                            "instanceSizeName": "%s",
                            "regionName": "%s",
                            "autoScaling": {
                                "compute": {
                                    "maxInstanceSize": "M60",
                                    "minInstanceSize": "M10"
                                }
                            }
                        }
                    }
                    """;

            if (connectionUsage > 0.1 || cpuUsage > 70 || memoryUsage > 80) {
                if (currentIndex < instanceSizes.size() - 1) {
                    String nextTier = instanceSizes.get(currentIndex + 1);
                    System.out.println("[" + Instant.now().toString() + "] Resource overload, upgrading to: " + nextTier);
                    String payload = payloadTemplate.formatted(providerSettings.path("providerName").asText("AWS"), nextTier, providerSettings.path("regionName").asText("US_EAST_1"));
                    updateCluster(payload, nextTier);
                }
            } else if (connectionUsage < 0.4 && cpuUsage < 35 && memoryUsage < 40 && currentIndex > 0) {
                String prevTier = instanceSizes.get(currentIndex - 1);
                System.out.println("[" + Instant.now().toString() + "] Low resource usage, downgrading to: " + prevTier);
                String payload = payloadTemplate.formatted(providerSettings.path("providerName").asText("AWS"), prevTier, providerSettings.path("regionName").asText("US_EAST_1"));
                updateCluster(payload, prevTier);
            }

            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static void updateCluster(String payload, String newTier) {
        try {
            Request request = new Request.Builder()
                    .url(CONFIG_URL)
                    .patch(RequestBody.create(payload, MediaType.parse("application/json")))
                    .header("Content-Type", "application/json")
                    .build();
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.code() == 200) {
                    currentTier.set(newTier);
                    maxConnections.set(TIER_MAP.get(newTier));
                    System.out.println("[" + Instant.now().toString() + "] Successfully adjusted to " + newTier + ", New max connections: " + maxConnections.get());
                } else {
                    System.out.println("[" + Instant.now().toString() + "] Adjustment failed: Status " + response.code() + ", Response " + response.body().string());
                }
            }
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Error during adjustment: " + e.getMessage());
        }
    }

    private static void processTenantRequest(String tenantId, String operation) {
        try {
            MongoCollection<Document> collection = mongoClient.getDatabase(tenantId + "_db").getCollection("data");
            for (int i = 0; i < 50; i++) {
                Document doc = new Document("operation", operation)
                        .append("tenant", tenantId)
                        .append("timestamp", System.currentTimeMillis());
                collection.insertOne(doc);
            }
            System.out.println("[" + Instant.now().toString() + "] Tenant " + tenantId + " executed operation: " + operation + " completed");
        } catch (Exception e) {
            System.out.println("[" + Instant.now().toString() + "] Tenant " + tenantId + " operation failed: " + e.getMessage());
        }
    }

    private static void simulateTenantRequestsWithMonitoring(int tenantCount, int batchSize) throws InterruptedException {
        Thread monitorThread = new Thread(MongoDBClusterMonitor::monitorResources);
        monitorThread.start();

        int totalBatches = (tenantCount + batchSize - 1) / batchSize;
        ExecutorService executor = Executors.newFixedThreadPool(batchSize);

        for (int batch = 0; batch < totalBatches; batch++) {
            int startIdx = batch * batchSize;
            int endIdx = Math.min(startIdx + batchSize, tenantCount);

            int currentConnections = getCurrentConnections();
            if ((double) currentConnections / maxConnections.get() > 0.9) {
                System.out.println("[" + Instant.now().toString() + "] Connection count nearing limit (" + currentConnections + "/" + maxConnections.get() + "), pausing new requests...");
                Thread.sleep(5000);
                continue;
            }

            List<CompletableFuture<Void>> futures = new ArrayList<>();
            for (int i = startIdx; i < endIdx; i++) {
                String tenantId = "tenant" + i;
                futures.add(CompletableFuture.runAsync(() -> processTenantRequest(tenantId, "Test request"), executor));
            }
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

            System.out.println("[" + Instant.now().toString() + "] Completed batch " + (batch + 1) + "/" + totalBatches);
            Thread.sleep(100);
        }

        monitoringActive.set(false);
        executor.shutdown();
        monitorThread.join();
    }
}

// Simple Digest Authentication implementation
class DigestAuthenticator implements Authenticator {
    private final String username;
    private final String password;

    public DigestAuthenticator(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public Request authenticate(Route route, Response response) throws IOException {
        String credential = Credentials.basic(username, password); // Simplified to Basic auth; actual Digest implementation needed
        return response.request().newBuilder()
                .header("Authorization", credential)
                .build();
    }
}

