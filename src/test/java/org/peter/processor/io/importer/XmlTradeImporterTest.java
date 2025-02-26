package org.peter.processor.io.importer;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;
import java.util.Map;

class XmlTradeImporterTest {

    private final XmlTradeImporter xmlTradeImporter = new XmlTradeImporter();

    @Test
    void testImportData_ValidXml_ReturnsCorrectData() {
        String xmlData = "<Trades>"
                + "<Trade><date>2025-02-26</date><productId>123</productId><currency>USD</currency><price>10.5</price></Trade>"
                + "<Trade><date>2025-02-27</date><productId>456</productId><currency>EUR</currency><price>5.0</price></Trade>"
                + "</Trades>";
        InputStream inputStream = new ByteArrayInputStream(xmlData.getBytes());

        List<Map<String, String>> result = xmlTradeImporter.importData(inputStream).toList();

        assertEquals(2, result.size());
        assertEquals(Map.of("date", "2025-02-26", "productId", "123", "currency", "USD", "price", "10.5"), result.get(0));
        assertEquals(Map.of("date", "2025-02-27", "productId", "456", "currency", "EUR", "price", "5.0"), result.get(1));
    }

    @Test
    void testImportData_EmptyXml_ReturnsEmptyStream() {
        String xmlData = "<Trades></Trades>";
        InputStream inputStream = new ByteArrayInputStream(xmlData.getBytes());

        List<Map<String, String>> result = xmlTradeImporter.importData(inputStream).toList();

        assertTrue(result.isEmpty());
    }

    @Test
    void testImportData_InvalidXml_ThrowsException() {
        String invalidXmlData = "<Trade><date>2025-02-26</date></Trade>";
        InputStream inputStream = new ByteArrayInputStream(invalidXmlData.getBytes());

        assertThrows(RuntimeException.class, () -> xmlTradeImporter.importData(inputStream).toList());
    }

    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("application/xml", xmlTradeImporter.getType());
    }
}
