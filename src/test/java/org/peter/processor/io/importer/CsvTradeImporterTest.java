package org.peter.processor.io.importer;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CsvTradeImporterTest {

    private final CsvTradeImporter csvTradeImporter = new CsvTradeImporter();

    @Test
    void testImportData_ValidCsv_ReturnsCorrectData() {
        String csvData = "date,productId,currency,price\n" +
                "2025-02-26,123,USD,10.5\n" +
                "2025-02-27,456,EUR,5.0\n";
        InputStream inputStream = new ByteArrayInputStream(csvData.getBytes());

        List<Map<String, String>> result = csvTradeImporter.importData(inputStream).toList();

        assertEquals(2, result.size());
        assertEquals(Map.of("date", "2025-02-26", "productId", "123", "currency", "USD", "price", "10.5"), result.get(0));
        assertEquals(Map.of("date", "2025-02-27", "productId", "456", "currency", "EUR", "price", "5.0"), result.get(1));
    }

    @Test
    void testImportData_InvalidLine_IsSkipped() {
        String csvData = "date,productId,currency,price\n" +
                "2025-02-26,123,USD,10.5\n" +
                "Invalid line with missing fields\n" +
                "2025-02-27,456,EUR,5.0\n";
        InputStream inputStream = new ByteArrayInputStream(csvData.getBytes());

        List<Map<String, String>> result = csvTradeImporter.importData(inputStream).toList();

        assertEquals(2, result.size());
        assertEquals(Map.of("date", "2025-02-26", "productId", "123", "currency", "USD", "price", "10.5"), result.get(0));
        assertEquals(Map.of("date", "2025-02-27", "productId", "456", "currency", "EUR", "price", "5.0"), result.get(1));
    }

    @Test
    void testImportData_EmptyFile_ReturnsEmptyStream() {
        String csvData = "date,productId,currency,price\n";
        InputStream inputStream = new ByteArrayInputStream(csvData.getBytes());

        List<Map<String, String>> result = csvTradeImporter.importData(inputStream).toList();

        assertTrue(result.isEmpty());
    }

    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("text/csv", csvTradeImporter.getType());
    }


}