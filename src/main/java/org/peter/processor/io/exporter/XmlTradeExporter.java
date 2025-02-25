package org.peter.processor.io.exporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class XmlTradeExporter implements TradeExporter {
    private final XmlMapper xmlMapper = new XmlMapper();

    @Override
    public String export(List<Map<String, String>> trades) {
        try {
            return xmlMapper.writerWithDefaultPrettyPrinter().writeValueAsString(trades);
        } catch (JsonProcessingException e) {
            log.error("EError converting trades to XML {}", e.getMessage());
            throw new RuntimeException("Error converting trades to XML", e);
        }
    }

    @Override
    public String getType() {
        return ProcessType.XML.getType();
    }
}
