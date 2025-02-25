package org.peter.processor.io.importer;

import lombok.extern.slf4j.Slf4j;
import org.peter.processor.io.ProcessType;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.stream.Stream;

@Slf4j
@Service
public class CsvTradeImporter implements TradeImporter {

    @Override
    public Stream<Map<String, String>> importData(InputStream inputStream) {
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines()
                .skip(1)
                .map(this::parseLine)
                .filter(Objects::nonNull);
    }

    private Map<String, String> parseLine(String line) {
        String[] headers = {"date", "productId", "currency", "price"};
        String[] values = line.split(",");

        if (values.length != headers.length) {
            log.warn("Skipping invalid line: {}", line);
            return null;
        }

        Map<String, String> trade = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            trade.put(headers[i], values[i].trim());
        }
        return trade;
    }

    @Override
    public String getType() {
        return ProcessType.CSV.getType();
    }
}