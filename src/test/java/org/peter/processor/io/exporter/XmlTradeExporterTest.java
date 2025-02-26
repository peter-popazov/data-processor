package org.peter.processor.io.exporter;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import org.junit.jupiter.api.Test;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class XmlTradeExporterTest {

    private final XmlTradeExporter xmlTradeExporter = new XmlTradeExporter();
    private final XmlMapper xmlMapper = new XmlMapper();

    @Test
    void testWriteTrades_WritesValidXmlToBufferedWriter() throws IOException {
        StringWriter stringWriter = new StringWriter();
        BufferedWriter bufferedWriter = new BufferedWriter(stringWriter);

        List<Map<String, String>> trades = List.of(
                Map.of("date", "2025-02-26", "productName", "Apple", "currency", "USD", "price", "10.5"),
                Map.of("date", "2025-02-27", "productName", "Banana", "currency", "EUR", "price", "5.0")
        );

        xmlTradeExporter.writeTrades(bufferedWriter, trades);
        bufferedWriter.close();

        StringBuilder expectedXml = new StringBuilder("<Trades>\n");
        for (Map<String, String> trade : trades) {
            expectedXml.append(xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(trade)).append("\n");
        }
        expectedXml.append("</Trades>\n");

        assertEquals(expectedXml.toString(), stringWriter.toString());
    }

    @Test
    void testGetType_ReturnsCorrectType() {
        assertEquals("application/xml", xmlTradeExporter.getType());
    }
}