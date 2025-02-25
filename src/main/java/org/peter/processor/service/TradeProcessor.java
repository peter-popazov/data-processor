package org.peter.processor.service;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.exception.UnsupportedFormatException;
import org.peter.processor.io.ProcessType;
import org.peter.processor.io.exporter.TradeExporter;
import org.peter.processor.io.importer.TradeImporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Slf4j
@Service
public class TradeProcessor {

    @Value("${app.batch-size}")
    private int batchSize;

    @Value("${app.thread-count}")
    private int threadCount;

    @Value("${app.date-format}")
    private String dateFormat;

    private final StringRedisTemplate redisTemplate;
    private final Map<String, TradeExporter> exporters;
    private final Map<String, TradeImporter> importers;

    @Autowired
    public TradeProcessor(StringRedisTemplate redisTemplate, List<TradeExporter> exporters, List<TradeImporter> importers) {
        this.redisTemplate = redisTemplate;
        this.exporters = exporters.stream().collect(Collectors.toMap(TradeExporter::getType, exporter -> exporter));
        this.importers = importers.stream().collect(Collectors.toMap(TradeImporter::getType, importer -> importer));
    }

    @SneakyThrows
    public String processTrades(InputStream inputStream, String acceptHeader) {
        String format = ProcessType.fromMimeType(acceptHeader).getType();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<List<Map<String, String>>>> futures = new ArrayList<>();
        List<Map<String, String>> enrichedTrades = new ArrayList<>();
        TradeImporter importer = importers.get(format);
        TradeExporter exporter = exporters.get(format);

        if (importer == null || exporter == null) {
            throw new UnsupportedFormatException("Unsupported file type: " + format);
        }

        try (Stream<Map<String, String>> tradeStream = importer.importData(inputStream)) {

            List<Map<String, String>> batch = new ArrayList<>();
            tradeStream.forEach(trade -> {
                batch.add(trade);

                if (batch.size() >= batchSize) {
                    futures.add(processBatchAsync(new ArrayList<>(batch), executor));
                    batch.clear();
                }
            });

            if (!batch.isEmpty()) {
                futures.add(processBatchAsync(batch, executor));
            }

            for (Future<List<Map<String, String>>> future : futures) {
                enrichedTrades.addAll(future.get());
            }

        } catch (Exception e) {
            log.error("Error processing trade file: {}", e.getMessage(), e);
            throw new RuntimeException("Error processing trade file", e);
        } finally {
            shutdownExecutor(executor);
        }

        return exporter.export(enrichedTrades);

    }

    private Future<List<Map<String, String>>> processBatchAsync(List<Map<String, String>> batch, ExecutorService executor) {
        return executor.submit(() -> {
            List<Map<String, String>> enrichedBatch = new ArrayList<>();
            List<String> productIds = batch.stream().map(trade -> trade.get("productId")).collect(Collectors.toList());

            List<String> productNames = redisTemplate.opsForValue().multiGet(productIds);

            for (int i = 0; i < batch.size(); i++) {
                Map<String, String> trade = batch.get(i);
                String date = trade.get("date");
                String productId = trade.get("productId");
                String currency = trade.get("currency");
                String price = trade.get("price");

                if (!isValidDate(date)) {
                    log.error("Invalid date {}, skipping...", date);
                    continue;
                }

                String productName = (productNames != null && i < productNames.size() && productNames.get(i) != null)
                        ? productNames.get(i)
                        : "Missing Product Name";

                if ("Missing Product Name".equals(productName)) {
                    log.error("Missing Product Name for id {}", productId);
                }

                Map<String, String> tradeData = new HashMap<>();
                tradeData.put("date", date);
                tradeData.put("productName", productName);
                tradeData.put("currency", currency);
                tradeData.put("price", price);

                enrichedBatch.add(tradeData);
            }
            return enrichedBatch;
        });
    }

    private boolean isValidDate(String date) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(dateFormat);
        try {
            LocalDate.parse(date, formatter);
            return true;
        } catch (DateTimeParseException e) {
            return false;
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
                log.warn("Forcing executor shutdown...");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.warn("Executor interrupted, forcing shutdown...");
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}