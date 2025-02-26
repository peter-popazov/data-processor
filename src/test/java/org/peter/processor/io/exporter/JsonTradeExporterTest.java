package org.peter.processor.io.exporter;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonTradeExporterTest {

    private final JsonTradeExporter jsonTradeExporter = new JsonTradeExporter();

    @Test
    void testWriteTrades_WritesValidJsonToBufferedWriter() throws IOException {
        StringWriter stringWriter = new StringWriter();
        BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);

        List<Map<String, String>> trades = List.of(
                Map.of("date", "2025-02-26", "productName", "Apple", "currency", "USD", "price", "10.5"),
                Map.of("date", "2025-02-27", "productName", "Banana", "currency", "EUR", "price", "5.0")
        );

        jsonTradeExporter.writeTrades(bufferedWriter, trades);
        bufferedWriter.close();
        
        String expectedJson = new ObjectMapper().writeValueAsString(trades);
        assertEquals(expectedJson, stringWriter.toString());
    }

    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("application/json", jsonTradeExporter.getType());
    }
}