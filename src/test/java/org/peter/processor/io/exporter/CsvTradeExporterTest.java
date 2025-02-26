package org.peter.processor.io.exporter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CsvTradeExporterTest {

    private CsvTradeExporter csvTradeExporter;
    private StringWriter stringWriter;
    private BufferedWriter bufferedWriter;

    @BeforeEach
    void setUp() {
        csvTradeExporter = new CsvTradeExporter();
        stringWriter = new StringWriter();
        bufferedWriter = new BufferedWriter(stringWriter);
    }

    @Test
    void testWriteTrades_WritesHeaderAndTrades() throws IOException {
        List<Map<String, String>> trades = List.of(
                Map.of("date", "2025-02-26", "productName", "Apple", "currency", "USD", "price", "10.5"),
                Map.of("date", "2025-02-27", "productName", "Banana", "currency", "EUR", "price", "8.0")
        );

        csvTradeExporter.writeTrades(bufferedWriter, trades);
        bufferedWriter.close();

        String expectedCsv = """
                date,productName,currency,price
                2025-02-26,Apple,USD,10.5
                2025-02-27,Banana,EUR,8.0
                """;
        assertEquals(expectedCsv, stringWriter.toString());
    }

    @Test
    void testWriteTrades_HandlesEmptyTradeList() throws IOException {
        List<Map<String, String>> trades = List.of();

        csvTradeExporter.writeTrades(bufferedWriter, trades);
        bufferedWriter.close();

        String expectedCsv = "date,productName,currency,price\n";
        assertEquals(expectedCsv, stringWriter.toString());
    }

    @Test
    void testWriteTrades_WritesToBufferedWriter() throws IOException {
        StringWriter stringWriter = new StringWriter();
        BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);
        List<Map<String, String>> trades = List.of(
                Map.of("date", "2025-02-26", "productName", "Apple", "currency", "USD", "price", "10.5")
        );

        csvTradeExporter.writeTrades(bufferedWriter, trades);
        bufferedWriter.close();

        String expectedCsv = """
            date,productName,currency,price
            2025-02-26,Apple,USD,10.5
            """;
        assertEquals(expectedCsv, stringWriter.toString());
    }


    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("text/csv", csvTradeExporter.getType());
    }
}
