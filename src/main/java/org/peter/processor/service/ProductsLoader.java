package org.peter.processor.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@Component
@RequiredArgsConstructor
public class ProductsLoader {

    @Value("${app.batch-size}")
    private int batchSize;

    @Value("${app.thread-count}")
    private int threadCount;

    @Value("${app.products-file}")
    private String productsFile;

    private final StringRedisTemplate redisTemplate;

    @PostConstruct
    public void loadProductsIntoRedis() {
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Void>> futures = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new InputStreamReader(
                new ClassPathResource(productsFile).getInputStream(), StandardCharsets.UTF_8))) {

            br.readLine(); // Skip header
            List<String[]> batch = new ArrayList<>();

            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");
                if (columns.length == 2) {
                    batch.add(new String[]{columns[0].trim(), columns[1].trim()});
                } else {
                    log.warn("Skipping invalid line: {}", line);
                }

                if (batch.size() >= batchSize) {
                    futures.add(processBatchAsync(new ArrayList<>(batch), executor));
                    batch.clear();
                }
            }

            if (!batch.isEmpty()) {
                futures.add(processBatchAsync(batch, executor));
            }

            for (Future<Void> future : futures) {
                future.get(); // Ensure all tasks are completed
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            log.error("Error processing product file: {}", e.getMessage(), e);
            throw new RuntimeException("Error reading product CSV file", e);
        } finally {
            executor.shutdown();
        }
    }

    private Future<Void> processBatchAsync(List<String[]> batch, ExecutorService executor) {
        return executor.submit(() -> {
            redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
                for (String[] data : batch) {
                    connection.set(data[0].getBytes(StandardCharsets.UTF_8),
                            data[1].getBytes(StandardCharsets.UTF_8));
                }
                return null;
            });

            log.info("Processed batch of {} products.", batch.size());
            return null;
        });
    }
}
