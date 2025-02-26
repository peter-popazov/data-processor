package org.peter.processor.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ProductsLoaderTest {

    @Mock
    private StringRedisTemplate redisTemplate;

    @InjectMocks
    private ProductsLoader productsLoader;

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(productsLoader, "batchSize", 2);
        ReflectionTestUtils.setField(productsLoader, "threadCount", 1);
        ReflectionTestUtils.setField(productsLoader, "productsFile", "test-products.csv");
    }

    @Test
    void shouldLoadProductsIntoRedis() {

        when(redisTemplate.executePipelined(any(RedisCallback.class))).thenReturn(Collections.emptyList());

        productsLoader.loadProductsIntoRedis();

        verify(redisTemplate, atLeastOnce()).executePipelined(any(RedisCallback.class));
    }

    @Test
    void shouldSkipInvalidLines() {

        when(redisTemplate.executePipelined(any(RedisCallback.class))).thenReturn(Collections.emptyList());

        productsLoader.loadProductsIntoRedis();

        verify(redisTemplate, times(1)).executePipelined(any(RedisCallback.class));
    }

    @Test
    void shouldHandleIOException() {
        ReflectionTestUtils.setField(productsLoader, "productsFile", "non-existent-file.csv");

        assertThrows(RuntimeException.class, productsLoader::loadProductsIntoRedis);
    }
}
