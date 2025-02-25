package org.peter.processor.exporter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class JsonTradeExporter implements TradeExporter {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String export(List<Map<String, String>> trades) {
        try {
            return objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(trades);
        } catch (JsonProcessingException e) {
            log.error("Error converting trades to JSON {}", e.getMessage());
            throw new RuntimeException("Error converting trades to JSON", e);
        }
    }

    @Override
    public String getType() {
        return ExportType.JSON.getType();
    }
}
