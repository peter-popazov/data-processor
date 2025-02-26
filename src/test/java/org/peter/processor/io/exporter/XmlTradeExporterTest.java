package org.peter.processor.io.exporter;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.BeforeEach;

class XmlTradeExporterTest {
    private XmlTradeExporter exporter;

    @BeforeEach
    void setUp() {
        exporter = new XmlTradeExporter();
    }

    @Test
    void testWriteTrades() throws IOException {
        StringWriter stringWriter = new StringWriter();
        BufferedWriter writer = new BufferedWriter(stringWriter);

        List<Map<String, String>> trades = List.of(
                Map.of("date", "2024-02-25", "productName", "Apple", "currency", "USD", "price", "10.50"),
                Map.of("date", "2024-02-26", "productName", "Banana", "currency", "EUR", "price", "8.30")
        );

        exporter.writeTrades(writer, trades);
        writer.close();

        String expectedXml = """
                <Trades>
                <Trade>
                  <date>2024-02-25</date>
                  <price>10.50</price>
                  <currency>USD</currency>
                  <productName>Apple</productName>
                </Trade>
                
                <Trade>
                  <date>2024-02-26</date>
                  <price>8.30</price>
                  <currency>EUR</currency>
                  <productName>Banana</productName>
                </Trade>
                
                </Trades>
                """.trim();

        assertEquals(expectedXml, stringWriter.toString().trim());
    }

    @Test
    void testWriteTradesEmptyList() throws IOException {
        StringWriter stringWriter = new StringWriter();
        BufferedWriter writer = new BufferedWriter(stringWriter);

        exporter.writeTrades(writer, List.of());
        writer.close();

        String expectedXml = "<Trades>\n</Trades>";
        assertEquals(expectedXml, stringWriter.toString().trim());
    }

    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("application/xml", exporter.getType());
    }
}