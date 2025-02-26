package org.peter.processor.io.importer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;


class JsonTradeImporterTest {

    private final JsonTradeImporter jsonTradeImporter = new JsonTradeImporter();

    @Test
    void testImportData_ValidJson_ReturnsCorrectData() {
        String jsonData = "[{\"date\":\"2025-02-26\",\"productId\":\"123\",\"currency\":\"USD\",\"price\":\"10.5\"}," +
                "{\"date\":\"2025-02-27\",\"productId\":\"456\",\"currency\":\"EUR\",\"price\":\"5.0\"}]";
        InputStream inputStream = new ByteArrayInputStream(jsonData.getBytes());

        List<Map<String, String>> result = jsonTradeImporter.importData(inputStream).toList();

        assertEquals(2, result.size());
        assertEquals(Map.of("date", "2025-02-26", "productId", "123", "currency", "USD", "price", "10.5"), result.get(0));
        assertEquals(Map.of("date", "2025-02-27", "productId", "456", "currency", "EUR", "price", "5.0"), result.get(1));
    }

    @Test
    void testImportData_EmptyArray_ReturnsEmptyStream() {
        String jsonData = "[]";
        InputStream inputStream = new ByteArrayInputStream(jsonData.getBytes());

        List<Map<String, String>> result = jsonTradeImporter.importData(inputStream).toList();

        assertTrue(result.isEmpty());
    }

    @Test
    void testImportData_InvalidJson_ThrowsException() {
        String invalidJsonData = "{ \"date\": \"2025-02-26\" }";
        InputStream inputStream = new ByteArrayInputStream(invalidJsonData.getBytes());

        assertThrows(RuntimeException.class, () -> jsonTradeImporter.importData(inputStream).toList());
    }

    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("application/json", jsonTradeImporter.getType());
    }

}