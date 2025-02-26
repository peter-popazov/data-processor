package org.peter.processor.service;

import lombok.extern.slf4j.Slf4j;
import org.peter.processor.exception.UnsupportedFormatException;
import org.peter.processor.io.ProcessType;
import org.peter.processor.io.exporter.TradeExporter;
import org.peter.processor.io.importer.TradeImporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    public void processTrades(InputStream inputStream, String acceptHeader, BufferedWriter writer) {
        String format = ProcessType.fromMimeType(acceptHeader).getType();
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        TradeImporter importer = importers.get(format);
        TradeExporter exporter = exporters.get(format);

        try (Stream<Map<String, String>> tradeStream = importer.importData(inputStream)) {
            List<Map<String, String>> batch = new ArrayList<>();
            List<CompletableFuture<List<Map<String, String>>>> futures = new ArrayList<>();

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

            CompletableFuture<Void> allFutures = CompletableFuture
                    .allOf(futures.toArray(new CompletableFuture[0]));

            allFutures.thenApply(v -> futures.stream()
                            .map(CompletableFuture::join)
                            .flatMap(List::stream)
                            .collect(Collectors.toList()))
                    .thenAccept(trades -> {
                        try {
                            exporter.writeTrades(writer, trades);
                            writer.flush(); // Ensure everything is written
                        } catch (IOException e) {
                            log.error("Error writing trades: {}", e.getMessage(), e);
                        }
                    }).exceptionally(ex -> {
                        log.error("Error processing trades: {}", ex.getMessage(), ex);
                        return null;
                    }).join(); // Ensure full completion before proceeding

        } catch (Exception e) {
            log.error("Error processing trades: {}", e.getMessage(), e);
            throw new RuntimeException("Error processing trades", e);
        } finally {
            shutdownExecutor(executor); // Shutdown only after all tasks are finished
        }
    }


    private CompletableFuture<List<Map<String, String>>> processBatchAsync(List<Map<String, String>> batch, ExecutorService executor) {
        return CompletableFuture.supplyAsync(() -> {
            List<Map<String, String>> enrichedBatch = new ArrayList<>();
            List<String> productIds = batch.stream().map(trade -> trade.get("productId")).collect(Collectors.toList());

            Map<String, String> productMap = new HashMap<>();
            List<String> productNames = redisTemplate.opsForValue().multiGet(productIds);
            if (productNames == null) {
                productNames = new ArrayList<>(Collections.nCopies(productIds.size(), "Missing Product Name"));
            }

            for (int i = 0; i < productIds.size(); i++) {
                productMap.put(productIds.get(i), productNames.get(i) != null ? productNames.get(i) : "Missing Product Name");
            }

            for (Map<String, String> trade : batch) {
                String date = trade.get("date");
                String productId = trade.get("productId");
                String currency = trade.get("currency");
                String price = trade.get("price");

                if (!isValidDate(date)) {
                    log.error("Invalid date {}, skipping...", date);
                    continue;
                }

                String productName = productMap.getOrDefault(productId, "Missing Product Name");

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
        }, executor);
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