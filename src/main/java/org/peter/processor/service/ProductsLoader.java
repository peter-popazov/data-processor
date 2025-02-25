package org.peter.processor.service;

import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Slf4j
//@Service
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

        try (BufferedReader br = new BufferedReader(new FileReader(new ClassPathResource(productsFile).getFile()))) {
            br.readLine();
            List<String[]> batch = new ArrayList<>();

            String line;
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(",");
                if (columns.length == 2) {
                    batch.add(new String[]{columns[0].trim(), columns[1].trim()});
                } else {
                    log.error("Error parsing line: {}", line);
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
                future.get();
            }

        } catch (IOException | InterruptedException | ExecutionException e) {
            log.error(e.getMessage());
            throw new RuntimeException("Error reading product CSV file", e);
        } finally {
            executor.shutdown();
        }
    }

    private Future<Void> processBatchAsync(List<String[]> batch, ExecutorService executor) {
        return executor.submit(() -> {
            Map<String, String> productMap = batch.stream()
                    .collect(Collectors.toMap(data -> data[0], data -> data[1]));
            redisTemplate.opsForValue().multiSet(productMap);
            log.info("Processed batch of {} products.", batch.size());
            return null;
        });
    }
}
