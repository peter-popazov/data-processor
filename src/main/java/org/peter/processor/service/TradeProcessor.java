package org.peter.processor.service;

import lombok.extern.slf4j.Slf4j;
import org.peter.processor.exporter.TradeExporter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

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

    @Autowired
    public TradeProcessor(StringRedisTemplate redisTemplate, List<TradeExporter> tradeExporter) {
        this.redisTemplate = redisTemplate;
        exporters = tradeExporter.stream().collect(Collectors.toMap(TradeExporter::getType, exporter -> exporter));
    }

    public String processTrades(String filePath, String exportType) {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<List<Map<String, String>>>> futures = new ArrayList<>();
        List<Map<String, String>> enrichedTrades = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(new ClassPathResource(filePath).getFile()))) {
            br.readLine();
            List<String[]> batch = new ArrayList<>();

            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");
                if (columns.length == 4) {
                    batch.add(new String[]{columns[0].trim(), columns[1].trim(), columns[2].trim(), columns[3].trim()});
                } else {
                    log.error("Invalid row, skipping...");
                }

                if (batch.size() >= batchSize) {
                    futures.add(processBatchAsync(new ArrayList<>(batch), executor));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                futures.add(processBatchAsync(batch, executor));
            }

            for (Future<List<Map<String, String>>> future : futures) {
                enrichedTrades.addAll(future.get());
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            throw new RuntimeException("Error processing trade file", e);
        } finally {
            executor.shutdown();
        }

        return exporters.get(exportType).export(enrichedTrades);
    }

    private Future<List<Map<String, String>>> processBatchAsync(List<String[]> batch, ExecutorService executor) {
        return executor.submit(() -> {
            List<Map<String, String>> enrichedBatch = new ArrayList<>();
            List<String> productIds = batch.stream().map(data -> data[1]).collect(Collectors.toList());

            List<String> productNames = redisTemplate.opsForValue().multiGet(productIds);

            for (int i = 0; i < batch.size(); i++) {
                String[] trade = batch.get(i);
                String date = trade[0];
                String productId = trade[1];
                String currency = trade[2];
                String price = trade[3];

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
        SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
        try {
            formatter.setLenient(false);
            formatter.parse(date);
            return true;
        } catch (ParseException e) {
            return false;
        }
    }
}