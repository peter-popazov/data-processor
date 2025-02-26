package org.peter.processor.service;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import org.peter.processor.io.ProcessType;
import org.peter.processor.io.exporter.TradeExporter;
import org.peter.processor.io.importer.TradeImporter;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.stream.Stream;

import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.data.redis.core.ValueOperations;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;

@ExtendWith(MockitoExtension.class)
class TradeProcessorTest {

    private TradeProcessor tradeProcessor;

    @Mock
    private StringRedisTemplate redisTemplate;

    @Mock
    private ValueOperations<String, String> valueOperations;

    @Mock
    private TradeExporter mockExporter;

    @Mock
    private TradeImporter mockImporter;

    private static MockedStatic<ProcessType> mockedProcessType;

    private static final String CSV_TYPE = "csv";
    private static final String CSV_MIME_TYPE = "text/csv";

    @BeforeAll
    static void init() {
        mockedProcessType = mockStatic(ProcessType.class);
    }

    @AfterAll
    static void tearDown() {
        mockedProcessType.close();
    }

    @BeforeEach
    void setUp() {
        when(mockExporter.getType()).thenReturn(CSV_TYPE);
        when(mockImporter.getType()).thenReturn(CSV_TYPE);

        tradeProcessor = new TradeProcessor(
                redisTemplate,
                List.of(mockExporter),
                List.of(mockImporter)
        );

        ReflectionTestUtils.setField(tradeProcessor, "batchSize", 2);
        ReflectionTestUtils.setField(tradeProcessor, "threadCount", 1);
        ReflectionTestUtils.setField(tradeProcessor, "dateFormat", "yyyy-MM-dd");

        when(redisTemplate.opsForValue()).thenReturn(valueOperations);

        ProcessType mockProcessType = mock(ProcessType.class);
        when(mockProcessType.getType()).thenReturn(CSV_TYPE);
        mockedProcessType.when(() -> ProcessType.fromMimeType(CSV_MIME_TYPE)).thenReturn(mockProcessType);
    }

    @Test
    void testProcessTrades_SuccessfulProcessing() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("dummy data".getBytes());
        StringWriter stringWriter = new StringWriter();
        BufferedWriter writer = new BufferedWriter(stringWriter);

        Stream<Map<String, String>> mockTradeStream = Stream.of(
                Map.of("productId", "1", "date", "2024-01-01", "currency", "USD", "price", "100")
        );

        when(mockImporter.importData(any())).thenReturn(mockTradeStream);
        when(valueOperations.multiGet(anyList())).thenReturn(
                Collections.singletonList("Product A")
        );
        doNothing().when(mockExporter).writeTrades(any(), anyList());

        assertDoesNotThrow(() -> tradeProcessor.processTrades(inputStream, CSV_MIME_TYPE, writer));

        verify(mockImporter, times(1)).importData(any());
        verify(valueOperations, times(1)).multiGet(anyList());
        verify(mockExporter, times(1)).writeTrades(any(), anyList());
    }

    @Test
    void testProcessTrades_InvalidDateSkipped() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("dummy data".getBytes());
        StringWriter stringWriter = new StringWriter();
        BufferedWriter writer = new BufferedWriter(stringWriter);

        Stream<Map<String, String>> mockTradeStream = Stream.of(
                Map.of("productId", "1", "date", "invalid-date", "currency", "USD", "price", "100")
        );

        when(mockImporter.importData(any())).thenReturn(mockTradeStream);
        when(valueOperations.multiGet(anyList())).thenReturn(
                Collections.singletonList("Product A")
        );

        doAnswer(invocation -> {
            List<Map<String, String>> trades = invocation.getArgument(1);
            assertEquals(0, trades.size());
            return null;
        }).when(mockExporter).writeTrades(any(), anyList());

        assertDoesNotThrow(() -> tradeProcessor.processTrades(inputStream, CSV_MIME_TYPE, writer));

        verify(mockImporter, times(1)).importData(any());
        verify(valueOperations, times(1)).multiGet(anyList());
        verify(mockExporter, times(1)).writeTrades(any(), anyList());
    }

    @Test
    void testProcessTrades_MultipleBatches() throws IOException {
        InputStream inputStream = new ByteArrayInputStream("dummy data".getBytes());
        StringWriter stringWriter = new StringWriter();
        BufferedWriter writer = new BufferedWriter(stringWriter);

        Stream<Map<String, String>> mockTradeStream = Stream.of(
                Map.of("productId", "1", "date", "2024-01-01", "currency", "USD", "price", "100"),
                Map.of("productId", "2", "date", "2024-01-02", "currency", "EUR", "price", "200"),
                Map.of("productId", "3", "date", "2024-01-03", "currency", "GBP", "price", "300")
        );

        when(mockImporter.importData(any())).thenReturn(mockTradeStream);
        when(valueOperations.multiGet(anyList())).thenReturn(
                List.of("Product A", "Product B"),
                List.of("Product C")
        );

        doNothing().when(mockExporter).writeTrades(any(), anyList());
        assertDoesNotThrow(() -> tradeProcessor.processTrades(inputStream, CSV_MIME_TYPE, writer));

        verify(mockImporter, times(1)).importData(any());
        verify(valueOperations, times(2)).multiGet(anyList());
        verify(mockExporter, times(1)).writeTrades(any(), anyList());
    }
}