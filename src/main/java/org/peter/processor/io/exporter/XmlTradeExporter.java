package org.peter.processor.io.exporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class XmlTradeExporter implements TradeExporter {
    private final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public void writeTrades(BufferedWriter writer, List<Map<String, String>> trades) throws IOException {
        writer.write("<Trades>\n");

        for (Map<String, String> trade : trades) {
            try {
                writer.write(xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(trade) + "\n");
            } catch (JsonProcessingException e) {
                log.error("Error converting trade to XML: {}", e.getMessage());
                throw new RuntimeException("Error converting trade to XML", e);
            }
        }

        writer.write("</Trades>\n");
        writer.flush();
    }

    @Override
    public String getType() {
        return ProcessType.XML.getType();
    }
}
