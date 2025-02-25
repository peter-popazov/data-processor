package org.peter.processor.io.exporter;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class JsonTradeExporter implements TradeExporter {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void writeTrades(BufferedWriter writer, List<Map<String, String>> trades) throws IOException {
        JsonGenerator jsonGenerator = objectMapper.getFactory().createGenerator(writer);
        jsonGenerator.writeStartArray();

        for (Map<String, String> trade : trades) {
            objectMapper.writeValue(jsonGenerator, trade);
        }

        jsonGenerator.writeEndArray();
        jsonGenerator.flush();
    }

    @Override
    public String getType() {
        return ProcessType.JSON.getType();
    }
}
