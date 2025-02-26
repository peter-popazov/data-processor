package org.peter.processor.io.importer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Service
public class JsonTradeImporter implements TradeImporter {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Stream<Map<String, String>> importData(InputStream inputStream) {
        return StreamSupport.stream(new JsonTradeSpliterator(inputStream, objectMapper), false);
    }

    @Override
    public String getType() {
        return ProcessType.JSON.getType();
    }

    private static class JsonTradeSpliterator extends Spliterators.AbstractSpliterator<Map<String, String>> {
        private final JsonParser parser;
        private final ObjectMapper objectMapper;

        public JsonTradeSpliterator(InputStream inputStream, ObjectMapper objectMapper) {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
            this.objectMapper = objectMapper;
            try {
                this.parser = objectMapper.getFactory().createParser(inputStream);
                if (parser.nextToken() != JsonToken.START_ARRAY) {
                    log.error("Unexpected token, expecting {}", JsonToken.START_ARRAY);
                    throw new IllegalArgumentException("JSON should start with an array");
                }
            } catch (Exception e) {
                throw new RuntimeException("Error initializing JSON parser", e);
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super Map<String, String>> action) {
            try {
                if (parser.nextToken() == JsonToken.END_ARRAY) {
                    return false;
                }

                if (parser.nextToken() == null) {
                    return false;
                }

                Map<String, Object> trade = objectMapper.readValue(parser, Map.class);
                log.debug("Parsed JSON Object: {}", trade);

                // Convert values to String
                Map<String, String> tradeAsString = trade.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> entry.getValue() != null ? entry.getValue().toString() : "null"
                        ));

                action.accept(tradeAsString);
                return true;
            } catch (Exception e) {
                log.error("Error parsing JSON", e);
                return false;
            } finally {
                if (parser.isClosed()) {
                    try {
                        parser.close();
                    } catch (IOException e) {
                        log.error("Error closing JSON parser", e);
                    }
                }
            }
        }
    }
}

